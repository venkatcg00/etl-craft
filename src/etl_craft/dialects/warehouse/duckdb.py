"""DuckDB warehouse, native tables in one file."""

from __future__ import annotations

from dataclasses import replace

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.engine.interfaces import DBAPIConnection

from etl_craft.config.auth import warehouse_by_key
from etl_craft.core.enums import FailureClass
from etl_craft.core.failures import Failure
from etl_craft.dialects.failure_evidence import driver_error
from etl_craft.dialects.warehouse.base import ReplaceStrategy, SurrogateKey, WarehouseDialect


class DuckDBWarehouse(WarehouseDialect):
    """DuckDB, native tables in one file.

    Only one process may write a DuckDB file at a time, so warehouse access is queued.
    """

    session_fencing = True
    """Embedded: a statement runs inside its own process, so that process ending stops it."""

    def stop_tagged(self, conn: Connection, tag: str) -> None:
        """Nothing runs outside a process; an expired holder's process has stopped writing."""
        return None

    def running_tagged(self, conn: Connection, tag: str) -> int:
        """No other process's statement can be running in an embedded database."""
        return 0

    def script_autocommit(self, engine: Engine, driver: DBAPIConnection) -> None:
        """Keep the native driver's autocommit outside explicit file transactions."""
        return None

    def classify_failure(self, error: BaseException) -> Failure:
        """DuckDB reports SQL and transaction errors as native exception classes."""
        error = driver_error(error)
        failure = super().classify_failure(error)
        names = {base.__name__ for base in type(error).__mro__}
        if names & {
            "TransactionException",
            "IOException",
            "OutOfMemoryException",
            "InterruptException",
        }:
            return replace(failure, kind=FailureClass.TRANSIENT)
        if names & {
            "ParserException",
            "BinderException",
            "CatalogException",
            "PermissionException",
        }:
            return replace(failure, kind=FailureClass.PERMANENT)
        return failure

    spec = warehouse_by_key("duckdb")
    atomic_protection = True
    sqlglot_dialect = "duckdb"
    replace_strategy: ReplaceStrategy = "transactional"
    surrogate_key: SurrogateKey = "sequence"
    single_writer = True
    process_local_queries = True

    def timestamp_text(self, value: str, kind: str) -> str:
        """Normalize aware timestamps explicitly, including after a timezone change."""
        utc = (
            f"({value} AT TIME ZONE 'UTC')"
            if "WITH TIME ZONE" in kind or kind == "TIMESTAMPTZ"
            else f"CAST({value} AS TIMESTAMP)"
        )
        return f"STRFTIME({utc}, '%Y-%m-%dT%H:%M:%S.%f')"

    session_sql = "SET TimeZone = 'UTC'"

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

    def full_column_types(self, conn: Connection, table: str) -> dict[str, str]:
        """DuckDB's information schema retains precision and complete nested types."""
        query, parameters = self.column_metadata_query(conn, table, "column_name, data_type")
        rows = conn.execute(text(query), parameters).all()
        return {str(row[0]).lower(): str(row[1]) for row in rows}
