"""DuckDB warehouse, native tables in one file."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from etl_craft.config.auth import warehouse_by_key
from etl_craft.dialects.warehouse.base import ReplaceStrategy, SurrogateKey, WarehouseDialect

if TYPE_CHECKING:
    from etl_craft.config import ConnectionProfile


class DuckDBWarehouse(WarehouseDialect):
    """DuckDB, native tables in one file.

    Only one process may write a DuckDB file at a time, so warehouse access is queued.
    """

    spec = warehouse_by_key("duckdb")
    replace_strategy: ReplaceStrategy = "transactional"
    surrogate_key: SurrogateKey = "sequence"
    single_writer = True

    def timestamp_text(self, value: str, kind: str) -> str:
        """Normalize aware timestamps explicitly, including after a timezone change."""
        utc = (
            f"({value} AT TIME ZONE 'UTC')"
            if "WITH TIME ZONE" in kind or kind == "TIMESTAMPTZ"
            else f"CAST({value} AS TIMESTAMP)"
        )
        return f"STRFTIME({utc}, '%Y-%m-%dT%H:%M:%S.%f')"

    def on_connect(self, dbapi_connection: Any, profile: ConnectionProfile, secret: str) -> None:
        """Pin every new warehouse session to UTC."""
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("SET TimeZone = 'UTC'")
        finally:
            cursor.close()

    def replacement_comment(self, conn: Connection, target: str) -> str | None:
        """Read the native table's comment before replacing its definition."""
        catalog, schema, table = target.split(".")
        value = conn.execute(
            text(
                "SELECT comment FROM duckdb_tables() WHERE database_name = :catalog "
                "AND schema_name = :schema AND table_name = :table"
            ),
            {"catalog": catalog, "schema": schema, "table": table},
        ).scalar_one()
        return None if value is None else str(value)
