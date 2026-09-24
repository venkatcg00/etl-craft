"""DuckDB compute over an Iceberg REST catalog."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from etl_craft.config.auth import warehouse_by_key
from etl_craft.config.targets import attached_catalog_name
from etl_craft.core.enums import AuthMode
from etl_craft.core.errors import ConfigurationError
from etl_craft.dialects.warehouse.base import Presented, SurrogateKey
from etl_craft.dialects.warehouse.duckdb import DuckDBWarehouse

if TYPE_CHECKING:
    from etl_craft.config import ConnectionProfile
    from etl_craft.config.targets import WarehouseUrl


class DuckDBIcebergWarehouse(DuckDBWarehouse):
    """DuckDB compute, Iceberg storage through a REST catalog.

    Each connection attaches the catalog under the profile's ``catalog`` name, with the object
    storage and catalog credentials registered as DuckDB secrets. The tables live in the
    catalog, not a local file, so writers are not serialized.
    """

    spec = warehouse_by_key("duckdb_iceberg")
    single_writer = False
    surrogate_key: SurrogateKey = "computed"
    enforces_primary_keys = False
    bearer_needs_user = False

    def present(self, profile: ConnectionProfile, secret: str, url: WarehouseUrl) -> Presented:
        """Present nothing to DuckDB itself: the catalog login happens in ``on_connect``."""
        return Presented()

    def on_connect(self, dbapi_connection: Any, profile: ConnectionProfile, secret: str) -> None:
        """Load the extensions, register the storage and catalog credentials, and attach.

        The settings are written into DuckDB statements, so none may contain a quote. Object
        storage uses ``s3_key_id`` and ``s3_secret`` when both are set, else the AWS credential
        chain.
        """
        extra = profile.extra
        catalog = attached_catalog_name(extra)
        uri = str(extra.get("catalog_uri") or "").strip()
        warehouse = str(extra.get("iceberg_warehouse") or "").strip()
        if not uri or not warehouse:
            raise ConfigurationError(
                "a DuckDB Iceberg warehouse needs catalog_uri (the REST catalog endpoint) and "
                "iceberg_warehouse (e.g. s3://warehouse/)"
            )
        values = {k: v for k, v in extra.items() if isinstance(v, str)}
        for name, value in [*values.items(), ("secret", secret)]:
            if "'" in value:
                raise ConfigurationError(f"DuckDB Iceberg setting {name} must not contain a quote")

        cursor = dbapi_connection.cursor()
        for statement in ("INSTALL iceberg", "LOAD iceberg", "INSTALL httpfs", "LOAD httpfs"):
            cursor.execute(statement)
        cursor.execute(
            f"CREATE OR REPLACE SECRET etl_craft_s3 ({', '.join(_storage_secret(values))})"
        )
        attach_auth = "AUTHORIZATION_TYPE 'none'"
        if profile.auth_mode in {AuthMode.TOKEN, AuthMode.OAUTH}:
            cursor.execute(
                "CREATE OR REPLACE SECRET etl_craft_iceberg "
                f"(TYPE ICEBERG, {', '.join(_catalog_secret(profile.auth_mode, values, secret))})"
            )
            attach_auth = "SECRET etl_craft_iceberg"
        cursor.execute(
            f"ATTACH IF NOT EXISTS '{warehouse}' AS {catalog} (TYPE ICEBERG, ENDPOINT '{uri}', "
            f"{attach_auth}, ACCESS_DELEGATION_MODE 'none', READ_ONLY false)"
        )
        cursor.close()
        # An attached Iceberg catalog does not take part in DuckDB transactions; each
        # statement commits on its own.
        dbapi_connection.begin = _no_transaction

    def load_table_metadata(self, conn: Connection, schema: str, table: str) -> None:
        """Load an Iceberg table's schema so information_schema.columns reports it.

        DuckDB lists an attached Iceberg table lazily: until something reads the table,
        information_schema.columns holds one placeholder column named ``__`` for it. Selecting
        no rows loads the real schema.
        """
        rows = conn.execute(
            text(
                "SELECT table_catalog AS table_catalog FROM information_schema.tables "
                "WHERE lower(table_schema) = lower(:schema) AND lower(table_name) = lower(:table)"
            ),
            {"schema": schema, "table": table},
        ).all()
        for (catalog,) in rows:
            name = ".".join(_quote(part) for part in (catalog, schema, table))
            conn.execute(text(f"SELECT * FROM {name} LIMIT 0")).all()


def _storage_secret(values: dict[str, str]) -> list[str]:
    options = ["TYPE S3"]
    key_id, s3_secret = values.get("s3_key_id"), values.get("s3_secret")
    if key_id and s3_secret:
        options += [f"KEY_ID '{key_id}'", f"SECRET '{s3_secret}'"]
    else:
        options.append("PROVIDER credential_chain")
    for option, name in (("ENDPOINT", "s3_endpoint"), ("REGION", "s3_region")):
        if values.get(name):
            options.append(f"{option} '{values[name]}'")
    if values.get("s3_url_style"):
        options.append(f"URL_STYLE '{values['s3_url_style']}'")
    if values.get("s3_use_ssl"):
        use_ssl = values["s3_use_ssl"].strip().lower() in {"true", "1", "yes"}
        options.append(f"USE_SSL {'true' if use_ssl else 'false'}")
    return options


def _catalog_secret(auth_mode: str, values: dict[str, str], secret: str) -> list[str]:
    if auth_mode == AuthMode.TOKEN:
        return [f"TOKEN '{secret}'"]
    options = [
        f"CLIENT_ID '{values['client_id']}'",
        f"CLIENT_SECRET '{secret}'",
        f"OAUTH2_SERVER_URI '{values['token_url']}'",
    ]
    if values.get("scope"):
        options.append(f"OAUTH2_SCOPE '{values['scope']}'")
    return options


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _no_transaction() -> None:
    """Stand in for DuckDB's ``begin()``: each statement commits on its own."""
    return None
