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
import secrets
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
        self.token = secrets.token_hex(3)
        self.publish_hash_version: int | None = None
        self.clear_hash_version = False

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

    def scratch(self, suffix: str, *, persistent: bool = False) -> str:
        """Name a scratch table for this attempt: ``etl_<suffix>_<task_run_id>_<token>``.

        ``token`` is random per session, so no two attempts, of this task or another, share a
        name. A temporary table lives in the session's own namespace and stays unqualified;
        persistent candidates and warehouses without temporary tables use the target's catalog
        and schema. Every scratch
        table is recorded, so ``sweep`` drops it whatever happens.
        """
        bare = f"etl_{suffix}_{self.task_run_id}_{self.token}"
        name = (
            bare
            if self.dialect.temporary_tables and not persistent
            else self.qualify(f"{self.schema}.{bare}")
        )
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

        ``name`` is ``catalog.schema.table``, ``schema.table`` or a bare temporary scratch table
        name, matched by name alone: a scratch name carries a token no other table has.
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

    def column_types(self, name: str) -> dict[str, str]:
        """Read the table's complete types once, with lowercase column keys."""
        logger.debug("read complete column types of %s", name)
        try:
            return self.dialect.full_column_types(self.conn, name)
        except SQLAlchemyError as error:
            raise self._failure(f"read complete column types of {name}", error) from error

    def hash_types(self, name: str) -> dict[str, str]:
        """Return types and declared decimal scales for hashing table ``name``.

        ``name`` is ``catalog.schema.table``, ``schema.table`` or a bare temporary scratch table
        name, matched by name alone: a scratch name carries a token no other table has.
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
            f"SELECT {self.dialect.hash_metadata_columns} FROM information_schema.columns "
            f"WHERE {where} ORDER BY ordinal_position",
            {"table": table, "schema": schema, "catalog": catalog},
            step=f"read the columns of {name}",
        ).all()
        types = {}
        for row in rows:
            kind = row[1].upper()
            if (
                kind in {"DECIMAL", "NUMERIC", "NUMBER"}
                and row[2] is not None
                and row[3] is not None
            ):
                kind = f"DECIMAL({int(row[2])},{int(row[3])})"
            types[row[0].lower()] = kind
        return types

    def hash(self, values: list[str], columns: tuple[str, ...]) -> str:
        """Hash values using the target columns' persisted types and decimal scales."""
        types = self.hash_types(self.target)
        missing = [column for column in columns if column.lower() not in types]
        if missing:
            raise HandlerError(f"{self.target} lacks compare columns {', '.join(missing)}")
        return self.dialect.hash_expression(values, [types[column.lower()] for column in columns])

    def target_columns(self) -> list[tuple[str, str]]:
        """Return the target's columns; empty when it does not exist."""
        return self.columns(self.target)

    def prepare_update_stage(self, stage: str, keys: tuple[str, ...]) -> None:
        """Prepare a stage for joined updates using its dialect's indexes and statistics."""
        for sql in self.dialect.prepare_update_stage(stage, keys):
            self.run(sql, step="prepare the stage for joined updates")

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

        An identity column or a sequence default fills ROW_ID. Targets without a generator
        supply the largest ROW_ID present plus a row number under the target mutation lock.
        The base is read
        before the INSERT, since engines disagree about reading the table a statement writes.
        """
        if self.row_id_generated():
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

    def check_target_format(self) -> None:
        """Refuse a format mismatch before creating or evolving an existing target."""
        try:
            existing = self.dialect.existing_table_format(self.conn, self.target)
        except SQLAlchemyError as error:
            raise self._failure("read the existing table format", error) from error
        if existing != self.dialect.table_format:
            raise HandlerError(
                f"{self.action} {self.target}: existing table format is {existing}, "
                f"but this task resolves to {self.dialect.table_format}; set TABLE_FORMAT="
                f"{existing} or use a different TARGET_OBJECT. Migrate formats explicitly"
            )

    def row_id_generated(self) -> bool:
        """Read the existing target's generator rather than assuming new-table defaults."""
        logger.debug("read the ROW_ID generator of %s", self.target)
        try:
            return self.dialect.row_id_generated(self.conn, self.target)
        except SQLAlchemyError as error:
            raise self._failure("read the ROW_ID generator", error) from error
