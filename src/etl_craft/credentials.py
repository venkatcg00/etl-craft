"""Credentials the engine obtains itself, once per new connection.

[ADDITION, 2026-09-24] Per explicit instruction: "sso, oauth, sts, key files
should be supported but we cannot test them without elaborate support, so, say
these can be used but success is not guaranteed". Two auth modes need the
engine to *obtain* a short-lived credential rather than read a stored one:

* ``oauth`` -- an OAuth 2.0 client-credentials grant (RFC 6749 section 4.4):
  the profile's client id and secret are exchanged at its token URL for an
  access token, which the dialect then presents its own way (a password, a
  bearer header, a JWT).
* ``sts`` -- AWS IAM database authentication: a signed RDS/Aurora auth token,
  optionally after assuming a role through AWS STS. boto3 is an optional
  dependency (``etl-craft[aws]``), imported only here and only when used.

Both run inside the connection creator, so every new pooled connection gets a
fresh credential; the engines set ``pool_recycle`` below the credential's life.
Neither has been exercised against a live identity provider or AWS account in
this project -- they follow the vendors' documented protocols, and
``etl-craft doctor`` says so for any profile that uses them.

Drivers that implement a flow natively (Snowflake's OAUTH_CLIENT_CREDENTIALS,
browser SSO in Snowflake/Databricks/Trino, libpq 18's OAuth) are handed the
settings instead -- their dialect modules decide, and nothing here runs.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

# A token endpoint that has not answered in this long is not going to.
TOKEN_REQUEST_TIMEOUT_SECONDS = 30
# How long a pooled connection opened with a minted credential is reused.
# Comfortably under typical lifetimes (RDS IAM tokens: 15 minutes to *open* a
# connection; OAuth access tokens: usually an hour).
MINTED_CREDENTIAL_POOL_RECYCLE_SECONDS = 10 * 60
#: Auth modes whose credential is obtained per connection rather than stored.
MINTED_AUTH_MODES = frozenset({"oauth", "sts", "sso"})


def client_credentials_token(
    token_url: str, client_id: str, client_secret: str, scope: str | None = None
) -> str:
    """Exchange a client id and secret for an access token (OAuth 2.0 client credentials).

    The secret travels only in the POST body, never in a URL, and no error
    message repeats it.
    """
    from etl_craft.db import ConnectionError_

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
    except urllib.error.HTTPError as exc:
        raise ConnectionError_(
            f"OAuth token request to {token_url} was refused: HTTP {exc.code} "
            f"{_oauth_error(exc)}"
        ) from exc
    except (urllib.error.URLError, OSError, ValueError) as exc:
        raise ConnectionError_(f"OAuth token request to {token_url} failed: {exc}") from exc
    token = payload.get("access_token") if isinstance(payload, dict) else None
    if not isinstance(token, str) or not token:
        raise ConnectionError_(f"OAuth token response from {token_url} had no access_token")
    return token


def _oauth_error(exc: urllib.error.HTTPError) -> str:
    """Return the RFC 6749 error code and description from a refusal, if it sent them."""
    try:
        body = json.loads(exc.read(2000).decode("utf-8", "replace"))
    except (OSError, ValueError):
        return ""
    if not isinstance(body, dict):
        return ""
    parts = [str(body.get(key)) for key in ("error", "error_description") if body.get(key)]
    return f"({': '.join(parts)})" if parts else ""


def aws_rds_auth_token(
    host: str, port: int, user: str, region: str, role_arn: str | None = None
) -> str:
    """Return an RDS/Aurora IAM auth token, used in place of a password.

    Credentials come from boto3's own chain (environment, shared config,
    instance or container role, web identity). With `role_arn`, that identity
    first assumes the role through STS and the token is signed as the role.
    """
    from etl_craft.db import ConnectionError_

    try:
        import boto3
    except ImportError as exc:
        raise ConnectionError_(
            "auth_mode sts needs boto3 — install it with `pip install etl-craft[aws]`"
        ) from exc
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
    except Exception as exc:  # botocore's errors are not importable without it
        raise ConnectionError_(f"could not obtain an AWS IAM database token: {exc}") from exc
    return str(token)
