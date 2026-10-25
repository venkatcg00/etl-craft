"""Short-lived credentials the engine obtains itself, once per new connection.

Two auth modes need one:

- ``oauth``: an OAuth 2.0 client-credentials grant (RFC 6749, section 4.4). The profile's client
  id and secret are exchanged at its token URL for an access token, which the dialect presents
  its own way (a password, a bearer header, a JWT).
- ``sts``: an AWS RDS or Aurora IAM auth token, signed locally, optionally after assuming a role
  through AWS STS. boto3 is the optional ``etl-craft[aws]`` extra, imported only here.

Both run inside the connection creator, so each new pooled connection gets a fresh credential,
and engines recycle pooled connections before one expires. Drivers that implement a flow
themselves (browser SSO, Snowflake's client credentials, libpq's OAuth) are given the settings
instead, and nothing here runs. Neither mode has run against a live identity provider or AWS
account in this project, which ``doctor`` reports.
"""

from __future__ import annotations

import base64
import json
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, BinaryIO

from etl_craft.core.enums import AuthMode, ObjectStoreAuth

if TYPE_CHECKING:
    from etl_craft.config import ConnectorConfig
    from etl_craft.config.object_models import ObjectStore
from etl_craft.core.errors import ConfigurationError
from etl_craft.core.http_client import open_request

TOKEN_REQUEST_TIMEOUT_SECONDS = 30
MINTED_CREDENTIAL_POOL_RECYCLE_SECONDS = 10 * 60
"""Pooled connections authenticated with a minted credential are replaced after this long."""

MINTED_AUTH_MODES = frozenset({AuthMode.OAUTH, AuthMode.STS, AuthMode.SSO})


def client_credentials_token(
    token_url: str,
    client_id: str,
    client_secret: str,
    scope: str | None = None,
    *,
    basic_auth: bool = False,
) -> str:
    """Exchange a client id and secret for an access token.

    The secret travels in the POST body or HTTP Basic header, never a URL. Redirects are refused;
    no error message repeats the client secret.
    Raises ``ConfigurationError`` when the endpoint refuses, cannot be reached, or answers
    without an access token.
    """
    form = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    if scope:
        form["scope"] = scope
    headers = {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}
    if basic_auth:
        form.pop("client_id")
        form.pop("client_secret")
        encoded = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode("ascii")
        headers["Authorization"] = f"Basic {encoded}"
    request = urllib.request.Request(
        token_url,
        data=urllib.parse.urlencode(form).encode("ascii"),
        headers=headers,
        method="POST",
    )
    try:
        with open_request(request, timeout=TOKEN_REQUEST_TIMEOUT_SECONDS) as response:
            payload: Any = json.load(response)
    except urllib.error.HTTPError as error:
        raise ConfigurationError(
            f"OAuth token request to {token_url} was refused: HTTP {error.code} "
            f"{_oauth_error(error)}"
        ) from error
    except (urllib.error.URLError, OSError, ValueError) as error:
        raise ConfigurationError(f"OAuth token request to {token_url} failed: {error}") from error
    token = payload.get("access_token") if isinstance(payload, dict) else None
    if not isinstance(token, str) or not token:
        raise ConfigurationError(f"OAuth token response from {token_url} had no access_token")
    return token


def _oauth_error(error: urllib.error.HTTPError) -> str:
    """Return the RFC 6749 error code and description from a refusal, if it sent them."""
    try:
        body = json.loads(error.read(2000).decode("utf-8", "replace"))
    except (OSError, ValueError):
        return ""
    if not isinstance(body, dict):
        return ""
    parts = [str(body.get(key)) for key in ("error", "error_description") if body.get(key)]
    return f"({': '.join(parts)})" if parts else ""


def aws_rds_auth_token(
    host: str, port: int, user: str, region: str, role_arn: str | None = None
) -> str:
    """Return an RDS or Aurora IAM auth token, used in place of a password.

    Credentials come from boto3's own chain (environment, shared config, instance or container
    role, web identity). With ``role_arn``, that identity first assumes the role and the token
    is signed as the role. Raises ``ConfigurationError`` without boto3 or when signing fails.
    """
    try:
        import boto3
    except ImportError as error:
        raise ConfigurationError(
            "Auth_mode STS needs boto3 — install it with `pip install etl-craft[aws]`"
        ) from error
    try:
        session = boto3.session.Session(region_name=region)
        if role_arn:
            assumed = session.client("sts").assume_role(
                RoleArn=role_arn, RoleSessionName="etl-craft"
            )["Credentials"]
            session = boto3.session.Session(
                aws_access_key_id=assumed["AccessKeyId"],
                aws_secret_access_key=assumed["SecretAccessKey"],
                aws_session_token=assumed["SessionToken"],
                region_name=region,
            )
        token = session.client("rds").generate_db_auth_token(
            DBHostname=host, Port=port, DBUsername=user, Region=region
        )
    except Exception as error:  # botocore's own exceptions are not importable without it
        raise ConfigurationError(f"could not obtain an AWS IAM database token: {error}") from error
    return str(token)


class S3Objects:
    """Bounded S3 operations; vendor imports and credential values stay in this adapter."""

    def __init__(self, config: ConnectorConfig, profile: ObjectStore) -> None:
        """Use the host chain or explicitly named secrets; never store credentials in a manifest."""
        from etl_craft.config.resolve import resolve_named_secret

        try:
            import boto3
        except ImportError:
            raise ConfigurationError(
                f"Object_stores.{profile.name}: boto3 is missing; expected the aws extra; "
                "install `pip install etl-craft[aws]`"
            ) from None
        settings: dict[str, str] = {}
        if profile.auth_mode == ObjectStoreAuth.KEYS:
            assert profile.access_key_id and profile.secret_access_key
            settings = {
                "aws_access_key_id": resolve_named_secret(config, profile.access_key_id),
                "aws_secret_access_key": resolve_named_secret(config, profile.secret_access_key),
            }
        self.name = profile.name
        try:
            self.client = boto3.client(
                "s3",
                endpoint_url=profile.endpoint,
                region_name=profile.region,
                config=boto3.session.Config(
                    connect_timeout=10,
                    read_timeout=30,
                    retries={"max_attempts": 1},
                    s3={"addressing_style": profile.url_style.lower()},
                ),
                **settings,
            )
        except Exception as error:
            # SDK initialization can include credential-provider URLs in its message.
            raise ConfigurationError(
                f"Object_stores.{profile.name}: client initialization failed "
                f"({type(error).__name__}); expected usable host credentials and "
                "an S3 endpoint; check the profile and credential provider"
            ) from None

    def read(self, bucket: str, key: str, maximum: int) -> bytes:
        """Read and close a bounded object, including error responses."""
        try:
            response = self.client.get_object(Bucket=bucket, Key=key)
            with response["Body"] as stream:
                data: bytes = stream.read(maximum + 1)
            if len(data) > maximum:
                raise ConfigurationError(
                    f"Object_stores.{self.name}, s3://{bucket}/{key}: object "
                    f"exceeds {maximum} bytes; expected a bounded artifact; "
                    f"restore the recorded object"
                )
            return data
        except Exception as error:
            # The provider boundary names the operation without echoing a signed URL or key.
            raise ConfigurationError(
                f"Object_stores.{self.name}, s3://{bucket}/{key}: read failed "
                f"({type(error).__name__}); expected a readable artifact; check "
                f"the prefix and its permissions"
            ) from None

    def write(self, bucket: str, key: str, content: bytes) -> None:
        """Write content-addressed immutable bytes; an identical writer is harmless."""
        try:
            self.client.put_object(Bucket=bucket, Key=key, Body=content, IfNoneMatch="*")
        except Exception as error:
            # S3's conditional write can lose to an identical uploader; verify, never overwrite.
            response = getattr(error, "response", {})
            if (
                isinstance(response, dict)
                and response.get("Error", {}).get("Code")
                in {
                    "PreconditionFailed",
                    "412",
                }
                and self.read(bucket, key, len(content)) == content
            ):
                return
            raise ConfigurationError(
                f"Object_stores.{self.name}, s3://{bucket}/{key}: immutable write "
                f"failed ({type(error).__name__}); expected a writable prefix; "
                f"check the object store and permissions"
            ) from None

    def probe(self, bucket: str, prefix: str) -> None:
        """Prove list/write/read/delete on a private, unique probe without changing bundles."""
        from uuid import uuid4

        key = (prefix.rstrip("/") + "/.doctor-" + uuid4().hex).lstrip("/")
        try:
            self.client.put_object(Bucket=bucket, Key=key, Body=b"etl-craft store probe")
            self.client.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
            if self.read(bucket, key, 64) != b"etl-craft store probe":
                raise ConfigurationError(
                    f"Object_stores.{self.name}: probe contents differ; expected "
                    f"the bytes written; check the store"
                )
        except Exception as error:
            # This provider boundary keeps credential-bearing driver messages out of diagnostics.
            raise ConfigurationError(
                f"Object_stores.{self.name}, s3://{bucket}/{prefix}: list/write "
                f"probe failed ({type(error).__name__}); expected list/write/read "
                f"access; fix prefix permissions"
            ) from None
        finally:
            try:
                self.client.delete_object(Bucket=bucket, Key=key)
            except Exception as error:
                # Provider cleanup errors must not expose a signed URL or credential.
                raise ConfigurationError(
                    f"Object_stores.{self.name}, s3://{bucket}/{prefix}: probe cleanup "
                    f"failed ({type(error).__name__}); expected delete access; "
                    "fix prefix permissions"
                ) from None

    def stream(self, bucket: str, key: str, offset: int) -> Iterator[bytes]:
        """Close a ranged response even if the caller cancels its download."""
        try:
            if offset >= self.client.head_object(Bucket=bucket, Key=key)["ContentLength"]:
                return
            response = self.client.get_object(Bucket=bucket, Key=key, Range=f"bytes={offset}-")
            with response["Body"] as stream:
                while data := stream.read(65536):
                    yield data
        except Exception as error:
            # The provider boundary never exposes signed URLs or credential-bearing messages.
            raise ConfigurationError(
                f"Object_stores.{self.name}, s3://{bucket}/{key}: log read failed "
                f"({type(error).__name__}); expected a readable stored log; "
                "restore the object and profile permissions"
            ) from None

    def upload(self, bucket: str, key: str, stream: BinaryIO) -> None:
        """Upload a seekable log using bounded multipart buffers without worker threads."""
        import boto3

        try:
            self.client.upload_fileobj(
                stream,
                bucket,
                key,
                Config=boto3.s3.transfer.TransferConfig(
                    multipart_chunksize=5 * 1024 * 1024, use_threads=False
                ),
            )
        except Exception as error:
            # Pending chunks remain the durable retry source after provider failures.
            raise ConfigurationError(
                f"Object_stores.{self.name}, s3://{bucket}/{key}: log upload failed "
                f"({type(error).__name__}); expected a writable log store; "
                "restore store access and retry the ending report or restart the controller"
            ) from None

    def remove(self, bucket: str, key: str) -> None:
        """Delete an exact engine-built key; a repeated deletion is harmless."""
        try:
            self.client.delete_object(Bucket=bucket, Key=key)
        except Exception as error:
            # Retention retains its durable cleanup row until provider deletion succeeds.
            raise ConfigurationError(
                f"Object_stores.{self.name}, s3://{bucket}/{key}: log removal failed "
                f"({type(error).__name__}); expected delete access; "
                "restore store permissions and rerun purge"
            ) from None
