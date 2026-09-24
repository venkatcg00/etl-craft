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

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from etl_craft.core.enums import AuthMode
from etl_craft.core.errors import ConfigurationError

TOKEN_REQUEST_TIMEOUT_SECONDS = 30
MINTED_CREDENTIAL_POOL_RECYCLE_SECONDS = 10 * 60
"""Pooled connections authenticated with a minted credential are replaced after this long."""

MINTED_AUTH_MODES = frozenset({AuthMode.OAUTH, AuthMode.STS, AuthMode.SSO})


def client_credentials_token(
    token_url: str, client_id: str, client_secret: str, scope: str | None = None
) -> str:
    """Exchange a client id and secret for an access token.

    The secret travels only in the POST body, never in a URL, and no error message repeats it.
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
    request = urllib.request.Request(
        token_url,
        data=urllib.parse.urlencode(form).encode("ascii"),
        headers={
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=TOKEN_REQUEST_TIMEOUT_SECONDS) as response:
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
            "auth_mode sts needs boto3 — install it with `pip install etl-craft[aws]`"
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
