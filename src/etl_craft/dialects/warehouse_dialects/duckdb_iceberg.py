"""DuckDB as compute over an Iceberg REST catalog.

[ADDITION, 2026-09-24] Per explicit instruction to add ``duckdb_iceberg``.
Verified against the repo's own local stack (Iceberg REST fixture + MinIO)
with DuckDB 1.5.5 before a line of this was written: CREATE TABLE AS, INSERT,
UPDATE, DELETE, ADD COLUMN, RENAME and TRUNCATE all work once the catalog is
attached with ``READ_ONLY false`` -- without it DuckDB attaches read-only and
refuses every write.

DuckDB runs in memory here; the data lives in the Iceberg catalog. So unlike a
DuckDB *file* this is not single-writer -- concurrent commits are Iceberg's
optimistic concurrency, not an OS file lock -- and, being Iceberg, it has no
sequences or primary keys, so ROW_ID is computed per insert.

[DEVIATION, 2026-09-24] Every statement commits on its own, as on Trino.
Verified against the local stack: inside one transaction the catalog cannot
DROP a table the same transaction created ("Table ... does not exist"), and a
RENAME of one fails at COMMIT -- after the earlier statements are already
applied. CREATE_TABLE (create, then rebuild with ROW_ID) and every schema
evolution do exactly that. So the connection never opens a transaction and
this warehouse gets the Iceberg guarantee sql_actions.py already states:
idempotency makes a retry safe, not atomicity.

Profile fields (each a variable name in craft-connector.yml, like every other
connection value):

    jdbc_url             jdbc:duckdb:              (in memory -- the data is in Iceberg)
    catalog              the name the catalog is attached as; the `catalog` in
                         catalog.schema.table
    catalog_uri          the Iceberg REST catalog endpoint
    iceberg_warehouse    the catalog's warehouse location, e.g. s3://warehouse/
    s3_endpoint, s3_region, s3_url_style, s3_use_ssl   object-storage settings
    s3_key_id, s3_secret the object-storage credentials; omit both to use the
                         ambient credential chain. auth_mode is `none`: the
                         catalog itself is not authenticated by this engine.

The ``iceberg`` and ``httpfs`` extensions are installed on first connection,
which needs network access once per machine.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from etl_craft.dialects.warehouse_dialects.base import SAFE_IDENTIFIER
from etl_craft.dialects.warehouse_dialects.duckdb import DuckDBWarehouse

#: Profile fields this dialect reads beyond jdbc_url/user/secret.
PROFILE_FIELDS = (
    "catalog",
    "catalog_uri",
    "iceberg_warehouse",
    "s3_endpoint",
    "s3_region",
    "s3_url_style",
    "s3_use_ssl",
    "s3_key_id",
    "s3_secret",
)


class DuckDBIcebergWarehouse(DuckDBWarehouse):
    """DuckDB compute, Iceberg storage through a REST catalog."""

    key = "duckdb_iceberg"
    table_format = "iceberg"
    single_writer = False
    surrogate_key = "computed"
    enforces_primary_keys = False

    def catalog_name(self, profile_extra: dict[str, str]) -> str | None:
        """Return the name the Iceberg catalog is attached as."""
        from etl_craft.db import ConnectionError_

        catalog = (profile_extra.get("catalog") or "").strip()
        if not SAFE_IDENTIFIER.match(catalog):
            raise ConnectionError_(
                "a DuckDB Iceberg warehouse needs `catalog` -- the name the Iceberg catalog is "
                f"attached as, a plain SQL identifier -- got {catalog!r}"
            )
        return catalog

    def on_connect(self, dbapi_connection: Any, profile_extra: dict[str, str]) -> None:
        """Load the extensions, register object-storage credentials, attach the catalog."""
        from etl_craft.db import ConnectionError_

        catalog = self.catalog_name(profile_extra)
        uri = (profile_extra.get("catalog_uri") or "").strip()
        warehouse = (profile_extra.get("iceberg_warehouse") or "").strip()
        if not uri or not warehouse:
            raise ConnectionError_(
                "a DuckDB Iceberg warehouse needs catalog_uri (the REST catalog endpoint) and "
                "iceberg_warehouse (e.g. s3://warehouse/)"
            )
        values = {k: v for k, v in profile_extra.items() if isinstance(v, str)}
        for name, value in values.items():
            # Interpolated into DuckDB's own SET/ATTACH statements, which take
            # literals rather than bind parameters -- so refuse, never escape.
            if "'" in value:
                raise ConnectionError_(f"DuckDB Iceberg setting {name} must not contain a quote")

        cursor = dbapi_connection.cursor()
        for statement in ("INSTALL iceberg", "LOAD iceberg", "INSTALL httpfs", "LOAD httpfs"):
            cursor.execute(statement)

        secret_options = ["TYPE S3"]
        key_id, secret = values.get("s3_key_id"), values.get("s3_secret")
        if key_id and secret:
            secret_options += [f"KEY_ID '{key_id}'", f"SECRET '{secret}'"]
        else:
            secret_options.append("PROVIDER credential_chain")
        for option, field in (("ENDPOINT", "s3_endpoint"), ("REGION", "s3_region")):
            if values.get(field):
                secret_options.append(f"{option} '{values[field]}'")
        if values.get("s3_url_style"):
            secret_options.append(f"URL_STYLE '{values['s3_url_style']}'")
        if values.get("s3_use_ssl"):
            use_ssl = values["s3_use_ssl"].strip().lower() in {"true", "1", "yes"}
            secret_options.append(f"USE_SSL {'true' if use_ssl else 'false'}")
        cursor.execute(f"CREATE OR REPLACE SECRET etl_craft_s3 ({', '.join(secret_options)})")
        cursor.execute(
            f"ATTACH IF NOT EXISTS '{warehouse}' AS {catalog} (TYPE ICEBERG, ENDPOINT '{uri}', "
            "AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none', READ_ONLY false)"
        )
        cursor.close()
        # SQLAlchemy begins every transaction through the DBAPI connection's
        # begin() (duckdb_engine's do_begin). Without one DuckDB autocommits
        # each statement -- see the module docstring for why that is required
        # here. commit() with nothing open is a no-op, and duckdb_engine's
        # do_rollback already ignores "no transaction is active".
        dbapi_connection.begin = _no_transaction

    def load_table_metadata(self, conn: Connection, schema: str, table: str) -> None:
        """Load an Iceberg table's schema so information_schema.columns reports it.

        DuckDB lists an attached Iceberg table lazily: until something reads the
        table, information_schema.columns holds one placeholder column named
        ``__`` for it -- verified -- so the shape check saw a target with none
        of its audit columns and refused the second run of every merge.
        Selecting zero rows loads the real schema.
        """
        rows = conn.execute(
            text(
                "SELECT table_catalog FROM information_schema.tables "
                "WHERE lower(table_schema) = lower(:schema) AND lower(table_name) = lower(:table)"
            ),
            {"schema": schema, "table": table},
        ).all()
        for (catalog,) in rows:
            name = ".".join(_quote(part) for part in (catalog, schema, table))
            conn.execute(text(f"SELECT * FROM {name} LIMIT 0")).all()


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _no_transaction() -> None:
    """Stand in for DuckDB's begin(): leave each statement to commit on its own."""
    return None
