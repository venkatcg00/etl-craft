"""The warehouse-dialect interface every warehouse module implements.

A warehouse dialect is one database *and* one table format: ``databricks`` and
``databricks_iceberg`` are separate modules because what CREATE TABLE emits,
and which ALTER keyword a later rename needs, differ between them. The engine's
SQL actions call only what is declared here, so a new warehouse is a new module
rather than a new branch in every action.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import parse_qsl

from sqlalchemy import text
from sqlalchemy.engine import Connection

if TYPE_CHECKING:
    from etl_craft.config import CloningConfig

SurrogateKey = Literal["identity", "sequence", "computed"]

# CAST target type for each engine-managed audit column, when a column needs
# to exist with zero rows (SETUP_TABLE's empty-shape creation) -- an untyped
# NULL literal would otherwise default to whatever the dialect's "unknown"
# type is, which some engines reject outright in a persisted CREATE TABLE AS
# SELECT.
# [DEVIATION, 2026-09-20, E2-31/E2-33] Two corrections here. CREATE_DATE and
# UPDATE_DATE were plain TIMESTAMP while the engine writes datetime.now(UTC) --
# every warehouse-side audit timestamp silently lost its offset. And bare
# VARCHAR with no length is rejected in DDL by several dialects, so
# CREATED_BY/UPDATED_BY carry one.
AUDIT_COLUMN_TYPES: dict[str, str] = {
    "HASH_KEY": "VARCHAR(32)",
    "CREATE_DATE": "TIMESTAMP WITH TIME ZONE",
    "UPDATE_DATE": "TIMESTAMP WITH TIME ZONE",
    "CREATED_BY": "VARCHAR(255)",
    "UPDATED_BY": "VARCHAR(255)",
    "DELETE_FLAG": "VARCHAR(1)",
    "ACTIVE_FLAG": "VARCHAR(1)",
}

_JDBC_URL_RE = re.compile(
    r"^jdbc:(?P<scheme>[a-zA-Z0-9_+-]+)://(?P<host>[^:/?]+)(:(?P<port>\d+))?"
    r"/(?P<database>[^?]*)(\?(?P<query>.*))?$"
)
SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class WarehouseDialect:
    """What differs between warehouses. Defaults are the ANSI behaviour Postgres follows."""

    #: This module's name, e.g. "databricks_iceberg".
    key: str = ""
    #: The value Warehouse.Name uses for this database, e.g. "Databricks".
    display_name: str = ""
    #: SQLAlchemy's dialect name, as a connected Engine reports it.
    sqlalchemy_name: str = ""
    #: "native" or "iceberg".
    table_format: str = "native"

    #: Whether CREATE TEMPORARY TABLE exists and behaves. Trino has none;
    #: Databricks refuses DROP on a name it shares with a temp table.
    temporary_tables: bool = True
    #: Whether an unqualified name resolves somewhere sensible. Databricks'
    #: session has no default schema, so scratch tables are qualified there.
    default_schema: bool = True
    #: Whether UPDATE/DELETE accept a target alias. Trino rejects one.
    mutation_alias: bool = True
    #: Whether ALTER TABLE ... RENAME TO takes (or needs) a qualified new name.
    qualified_rename: bool = False
    #: How ROW_ID is generated: an identity column, a sequence default, or
    #: computed per insert where the format has neither (Iceberg).
    surrogate_key: SurrogateKey = "identity"
    #: Whether the database enforces a PRIMARY KEY, which validate relies on.
    enforces_primary_keys: bool = True
    #: Whether only one OS process may write at a time (E2-61, DuckDB files).
    single_writer: bool = False
    #: The type text values are cast to before hashing.
    string_type: str = "VARCHAR"
    #: Whether a task may override Warehouse.Table_format. Not on DuckDB,
    #: where the format *is* the connection (a file, or an Iceberg catalog).
    per_task_format: bool = True

    # -- connection ----------------------------------------------------------

    def parse_jdbc(self, jdbc_url: str) -> tuple[str, dict[str, Any]]:
        """Split a JDBC URL into (SQLAlchemy dialect name, parts)."""
        return parse_generic_jdbc(jdbc_url)

    #: How a private-key credential is named in this dialect's connect args,
    #: as (path argument, passphrase argument); None where unsupported.
    key_file_connect_args: tuple[str, str] | None = None
    #: The username a static bearer token authenticates as, where fixed.
    token_username: str | None = None
    #: The separate connection fields this warehouse accepts in place of one
    #: packed JDBC URL (the tested shape for Databricks and Snowflake).
    preferred_fields: tuple[str, ...] = ()

    def preferred_connection_url(self, fields: Mapping[str, str]) -> str:
        """Build a credential-free JDBC URL from separate connection fields."""
        from etl_craft.db import ConnectionError_

        raise ConnectionError_(
            f"{self.display_name} does not take separate token connection fields -- "
            "only Databricks and Snowflake do"
        )

    def require_preferred_fields(self, fields: Mapping[str, str]) -> None:
        """Raise unless every non-secret preferred field is present."""
        from etl_craft.db import ConnectionError_

        for key in self.preferred_fields:
            if key != "token" and not fields.get(key):
                raise ConnectionError_(f"{self.display_name} connection requires {key}")

    def catalog_name(self, profile_extra: dict[str, str]) -> str | None:
        """Return the catalog for catalog.schema.table where the URL does not name it."""
        return None

    def on_connect(self, dbapi_connection: Any, profile_extra: dict[str, str]) -> None:
        """Run per-connection setup a dialect needs before any statement (none by default)."""
        return None

    def load_table_metadata(self, conn: Connection, schema: str, table: str) -> None:
        """Make information_schema.columns describe `schema.table` (it already does by default)."""
        return None

    # -- DDL -----------------------------------------------------------------

    def create_table_clause(self) -> str:
        """Return the clause between `CREATE TABLE <name>` and `AS <select>`."""
        return ""

    def create_table_as(
        self, conn: Connection, qualified_name: str, select_sql: str, params: dict[str, str]
    ) -> None:
        """Issue CREATE TABLE ... AS SELECT in this dialect's table format."""
        clause = self.create_table_clause()
        prefix = f"CREATE TABLE {qualified_name}"
        statement = f"{prefix} {clause} AS {select_sql}" if clause else f"{prefix} AS {select_sql}"
        conn.execute(text(statement))

    def mirror_table_ddl(self, name: str, column_ddl: str, cloning: CloningConfig) -> str | None:
        """Return DDL for a cloning mirror, or None to let SQLAlchemy create a plain table."""
        clause = self.create_table_clause()
        return f"CREATE TABLE {name} ({column_ddl}) {clause}" if clause else None

    def task_storage_problem(self, params: dict[str, str]) -> str | None:
        """Return why a task's storage parameters cannot work here, or None."""
        return None

    def cloning_storage_problem(self, cloning: CloningConfig) -> str | None:
        """Return why cloning cannot create mirrors here as configured, or None."""
        return None

    def alter_table_keyword(self) -> str:
        """Return the keyword that alters a table this dialect created."""
        return "ALTER TABLE"

    def audit_column_type(self, column: str) -> str:
        """Return the DDL type for an engine-managed audit column."""
        return AUDIT_COLUMN_TYPES[column]

    def scratch_table_keyword(self) -> str:
        """Return `TEMPORARY TABLE`, or `TABLE` where temporary tables are unusable."""
        return "TEMPORARY TABLE" if self.temporary_tables else "TABLE"

    # -- expressions ---------------------------------------------------------

    def hash_expression(self, values: list[str]) -> str:
        """MD5 over NULL-safe, `|`-joined text of `values`, as 32 hex characters.

        COALESCE to empty string per value stops one NULL from collapsing the
        whole concatenation to NULL, which would make every NULL-containing row
        hash identically regardless of its other values.
        """
        return f"MD5({self._hash_input(values)})"

    def _hash_input(self, values: list[str]) -> str:
        return " || '|' || ".join(
            f"COALESCE(CAST({value} AS {self.string_type}), '')" for value in values
        )

    def scalar_source_value(self, expression: str) -> str:
        """Wrap a value read by a correlated scalar subquery, where the dialect requires it."""
        return expression


def parse_generic_jdbc(jdbc_url: str) -> tuple[str, dict[str, Any]]:
    """Parse the ordinary `jdbc:<scheme>://host[:port]/database[?query]` form."""
    from etl_craft.db import ConnectionError_

    match = _JDBC_URL_RE.match(jdbc_url)
    if not match:
        raise ConnectionError_(
            f"not a recognized JDBC URL: {jdbc_url!r} — expected "
            "jdbc:<dialect>://host[:port]/database or jdbc:duckdb:<path>"
        )
    scheme = match["scheme"]
    dialect = {"postgresql": "postgresql+psycopg", "mysql": "mysql+pymysql"}.get(scheme, scheme)
    query = dict(parse_qsl(match["query"])) if match["query"] else {}
    database = match["database"]
    return dialect, {
        "host": match["host"],
        "port": int(match["port"]) if match["port"] else None,
        "database": database,
        # Trino (and any other engine whose path is `catalog/schema`) names the
        # catalog first; qualify() wants that half alone.
        "catalog": database.split("/", 1)[0],
        "query": query,
    }
