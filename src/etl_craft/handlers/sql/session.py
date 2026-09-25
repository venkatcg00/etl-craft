"""One SQL task's work on the warehouse: statements, scratch tables and table shapes.

Every statement goes through ``Session.run``, which logs its step with the row count the driver
reports and how long it took, and the SQL itself at DEBUG. A database error becomes a
``HandlerError`` naming the action, the target and the step, with the failing SQL written to
the attempt's log, so a failed task can be debugged from its log alone.

Counts reported to ``AUD_TASK_RUN_LOG`` never come from a driver's rowcount, which some drivers
do not report per statement; each is its own ``COUNT(*)``, taken before the statement it
describes changes what it counts.
"""

from __future__ import annotations

import contextlib
import logging
import time
from collections.abc import Mapping
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection, CursorResult
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.core.errors import HandlerError
from etl_craft.core.text import qualify, split_object_ref
from etl_craft.dialects.warehouse import WarehouseDialect

logger = logging.getLogger(__name__)

ROW_ID_COLUMN = "ROW_ID"
"""The engine-generated key every table the engine creates gets."""


class Session:
    """A SQL task's connection to the warehouse, and what it knows about the task's target."""

    def __init__(
        self,
        conn: Connection,
        dialect: WarehouseDialect,
        *,
        catalog: str,
        action: str,
        target_object: str,
        task_run_id: int,
        params: Mapping[str, str],
    ) -> None:
        """Work on ``target_object`` (``schema.table``) in ``catalog`` over ``conn``."""
        self.conn = conn
        self.dialect = dialect
        self.action = action
        self.target_object = target_object
        self.task_run_id = task_run_id
        self.params = params
        database, self.schema, self.table = split_object_ref(target_object)
        # A target that names its database is written there; otherwise in the active one.
        self.catalog = database or catalog
        self.target = f"{self.catalog}.{self.schema}.{self.table}"
        self.scratch_tables: list[str] = []

    # Statements

    def run(
        self, sql: str, params: Mapping[str, Any] | None = None, *, step: str
    ) -> CursorResult[Any]:
        """Run one statement, logging it; ``HandlerError`` naming ``step`` when it fails."""
        logger.debug("%s: %s", step, sql)
        started = time.monotonic()
        try:
            result = self.conn.execute(text(sql), dict(params or {}))
        except SQLAlchemyError as error:
            logger.error("%s failed; the statement was:\n%s", step, sql)
            raise self._failure(step, error) from error
        elapsed = time.monotonic() - started
        rows = result.rowcount if result.returns_rows is False and result.rowcount >= 0 else None
        if rows is None:
            logger.info("%s (%.2fs)", step, elapsed)
        else:
            logger.info("%s: %d row(s) (%.2fs)", step, rows, elapsed)
        return result

    def count(self, sql: str, *, step: str) -> int:
        """Return the single integer ``sql`` selects."""
        value = int(self.run(sql, step=step).scalar_one())
        logger.info("%s = %d", step, value)
        return value

    def create_table_as(self, name: str, select_sql: str, *, step: str) -> None:
        """CREATE TABLE ... AS SELECT in the task's dialect and table format."""
        logger.debug("%s: CREATE TABLE %s AS %s", step, name, select_sql)
        started = time.monotonic()
        try:
            self.dialect.create_table_as(self.conn, name, select_sql, self.params)
        except SQLAlchemyError as error:
            logger.error("%s failed; the SELECT was:\n%s", step, select_sql)
            raise self._failure(step, error) from error
        logger.info("%s (%.2fs)", step, time.monotonic() - started)

    def _failure(self, step: str, error: SQLAlchemyError) -> HandlerError:
        """Say which step on which target failed, with the database's own first line."""
        cause = getattr(error, "orig", None) or error
        detail = str(cause).strip().splitlines()[0] if str(cause).strip() else repr(cause)
        return HandlerError(
            f"{self.action} {self.target}: {step} failed: {type(cause).__name__}: {detail}"
        )

    # Names

    def qualify(self, object_ref: str) -> str:
        """Return ``database.schema.table`` for ``schema.table``, in the target's database."""
        return qualify(object_ref, self.catalog)

    def scratch(self, suffix: str) -> str:
        """Name a scratch table for this task run; ``etl_<suffix>_<task_run_id>``.

        Where the session has no default schema, the name is qualified with the target's schema.
        Every scratch table is recorded, so ``sweep`` drops it whatever happens.
        """
        bare = f"etl_{suffix}_{self.task_run_id}"
        name = bare if self.dialect.default_schema else self.qualify(f"{self.schema}.{bare}")
        if name not in self.scratch_tables:
            self.scratch_tables.append(name)
        return name

    def create_scratch(self, name: str, body: str, *, step: str) -> None:
        """Create a scratch table from ``body``, TEMPORARY where the dialect has them."""
        self.run(f"DROP TABLE IF EXISTS {name}", step=f"clear {name}")
        self.run(f"CREATE {self.dialect.scratch_table_keyword()} {name} AS {body}", step=step)

    def drop(self, name: str) -> None:
        """Drop a table if it exists."""
        self.run(f"DROP TABLE IF EXISTS {name}", step=f"drop {name}")

    def sweep(self) -> None:
        """Drop every scratch table, ignoring failures.

        It runs while an error may already be on its way out; a cleanup failure must not
        replace the error worth reading. On a warehouse without rollback (Trino) a scratch table
        left behind would be a real table in the team's schema.
        """
        for name in self.scratch_tables:
            with contextlib.suppress(Exception):
                self.conn.execute(text(f"DROP TABLE IF EXISTS {name}"))

    # Shapes

    def columns(self, name: str) -> list[tuple[str, str]]:
        """Return ``[(column, data_type)]`` of table ``name`` in column order.

        ``name`` is ``catalog.schema.table``, ``schema.table`` or a bare scratch table name,
        which is matched by name alone: scratch names are unique per task run.
        """
        parts = name.split(".")
        table = parts[-1]
        schema = parts[-2] if len(parts) >= 2 else None
        catalog = parts[0] if len(parts) == 3 else None
        where = "lower(table_name) = lower(:table)"
        if schema is not None:
            self.dialect.load_table_metadata(self.conn, schema, table)
            where += " AND lower(table_schema) = lower(:schema)"
        if catalog is not None:
            where += " AND lower(table_catalog) = lower(:catalog)"
        rows = self.run(
            "SELECT column_name, data_type FROM information_schema.columns "
            f"WHERE {where} ORDER BY ordinal_position",
            {"table": table, "schema": schema, "catalog": catalog},
            step=f"read the columns of {name}",
        ).all()
        return [(str(row[0]), str(row[1])) for row in rows]

    def target_columns(self) -> list[tuple[str, str]]:
        """Return the target's columns; empty when it does not exist."""
        return self.columns(self.target)

    def mutation_target(self) -> tuple[str, str]:
        """Return (target clause, qualifier) for a correlated UPDATE or DELETE on the target.

        Trino rejects an alias on UPDATE and DELETE, so there the table name qualifies instead.
        """
        if not self.dialect.mutation_alias:
            return self.target, self.table
        return f"{self.target} t", "t"

    def rename(self, qualified_from: str, qualified_to: str) -> None:
        """Rename a table in the dialect's RENAME syntax."""
        to = qualified_to if self.dialect.qualified_rename else qualified_to.rsplit(".", 1)[-1]
        self.run(
            f"{self.dialect.alter_table_keyword()} {qualified_from} RENAME TO {to}",
            step=f"rename {qualified_from} to {qualified_to}",
        )

    def row_id_insert_parts(self) -> tuple[str, str]:
        """Return the extra (columns, values) an INSERT needs where ROW_ID does not fill itself.

        An identity column or a sequence default fills ROW_ID; Iceberg has neither, so there
        each insert supplies the largest ROW_ID present plus a row number. The base is read
        before the INSERT, since engines disagree about reading the table a statement writes.
        """
        if self.dialect.surrogate_key != "computed":
            return "", ""
        base = self.count(
            f"SELECT COALESCE(MAX({ROW_ID_COLUMN}), 0) FROM {self.target}",
            step="largest ROW_ID",
        )
        # ORDER BY NULL: Databricks rejects a window with no order.
        return (
            f", {ROW_ID_COLUMN}",
            f", {base} + CAST(ROW_NUMBER() OVER (ORDER BY NULL) AS BIGINT)",
        )
