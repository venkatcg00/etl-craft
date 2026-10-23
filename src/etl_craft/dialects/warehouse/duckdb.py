"""DuckDB warehouse, native tables in one file."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection, CursorResult, Engine
from sqlalchemy.engine.interfaces import DBAPIConnection

from etl_craft.config.auth import warehouse_by_key
from etl_craft.core.enums import FailureClass
from etl_craft.core.failures import Failure
from etl_craft.dialects.failure_evidence import driver_error
from etl_craft.dialects.warehouse.base import ReplaceStrategy, SurrogateKey, WarehouseDialect
from etl_craft.dialects.warehouse.metadata import declarable_source


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

    metadata_types_complete = True
    dml_rowcounts = True

    def describe_select(self, conn: Connection, sql: str) -> list[tuple[str, str]] | None:
        """DESCRIBE a zero-row SELECT, preserving complete nested and decimal types."""
        if self.surrogate_key == "computed" or not declarable_source(sql, self.sqlglot_dialect):
            return None
        rows = conn.execute(text(f"DESCRIBE {self.event_probe_sql(sql, 0)}")).all()
        return [(str(row[0]), str(row[1])) for row in rows]

    def identity_setup_sql(self, target: str) -> tuple[str, ...]:
        """DuckDB requires a sequence because inline SQL identities are not implemented."""
        namespace, table = target.rsplit(".", 1)
        return (f"CREATE OR REPLACE SEQUENCE {namespace}.etl_seq_{table}_row_id START 1",)

    def identity_table_ddl(self, target: str, columns: str, params: Mapping[str, str]) -> str:
        """Declare the key and sequence default together with the table's other columns."""
        namespace, table = target.rsplit(".", 1)
        return (
            f"CREATE TABLE {target} ({columns}, ROW_ID BIGINT PRIMARY KEY "
            f"DEFAULT nextval('{namespace}.etl_seq_{table}_row_id'))"
        )

    def affected_rows(self, result: CursorResult[Any]) -> int:
        """DuckDB returns the exact modified count as its DML result's Count row."""
        return int(result.scalar_one())

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
