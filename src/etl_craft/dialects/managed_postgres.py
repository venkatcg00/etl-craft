"""Mint platform-specific database passwords for managed PostgreSQL connections."""

import json
import urllib.request

from sqlalchemy.dialects import registry

from etl_craft.config.managed_engine import validate_managed_engine
from etl_craft.config.model import ConnectionProfile
from etl_craft.core.enums import ManagedProvider
from etl_craft.core.errors import ConfigurationError
from etl_craft.core.http_client import open_request
from etl_craft.dialects.credentials import TOKEN_REQUEST_TIMEOUT_SECONDS, client_credentials_token


def database_token(profile: ConnectionProfile, secret: str) -> str:
    """Exchange the platform identity for a fresh database credential on each connect."""
    validate_managed_engine(profile)
    provider = profile.managed_provider
    try:
        token = client_credentials_token(
            str(profile.extra["token_url"]),
            str(profile.extra["client_id"]),
            secret,
            profile.extra.get("scope"),
            basic_auth=provider == ManagedProvider.DATABRICKS,
        )
        if provider == ManagedProvider.DATABRICKS:
            return _lakebase_token(profile, token)
        return _snowflake_token(profile, token)
    except Exception as error:
        # Provider boundary: credentials and credential-bearing responses never enter errors.
        raise ConfigurationError(
            f"Engine.{profile.name}: {provider} database credential issuance failed "
            f"({type(error).__name__}); expected a fresh database token; check provider "
            "OAuth settings, instance/endpoint access and installed optional connector"
        ) from None


def _lakebase_token(profile: ConnectionProfile, token: str) -> str:
    """Exchange the workspace bearer token for the selected Lakebase endpoint."""
    request = urllib.request.Request(
        str(profile.extra["workspace_url"]).rstrip("/") + "/api/2.0/postgres/credentials",
        data=json.dumps({"endpoint": profile.extra["endpoint"]}).encode(),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST",
    )
    with open_request(request, timeout=TOKEN_REQUEST_TIMEOUT_SECONDS) as response:
        payload = json.load(response)
    result = payload.get("token") if isinstance(payload, dict) else None
    if not isinstance(result, str) or not result:
        raise ConfigurationError("Lakebase credential response has no database token")
    return result


def _snowflake_token(profile: ConnectionProfile, token: str) -> str:
    """Use the existing optional Snowflake connector to issue the Postgres token."""
    connector = registry.load("snowflake").import_dbapi()

    with (
        connector.connect(
            account=str(profile.extra["account"]),
            authenticator="oauth",
            token=token,
            login_timeout=TOKEN_REQUEST_TIMEOUT_SECONDS,
        ) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(
            "SELECT GENERATE_POSTGRES_ACCESS_TOKEN_FOR_USER(%s, %s) AS database_token",
            (str(profile.extra["instance"]), profile.user),
        )
        row = cursor.fetchone()
    if row is None or not isinstance(row[0], str) or not row[0]:
        raise ConfigurationError("Snowflake credential response has no database token")
    return row[0]
