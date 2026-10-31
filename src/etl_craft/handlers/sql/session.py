"""One SQL task's work on the warehouse: statements, scratch tables and table shapes.

Every statement goes through ``Session.run``, which logs its step with the row count the driver
reports and how long it took, and the SQL itself at DEBUG. A database error becomes a
``HandlerError`` naming the action, the target and the step, with the failing SQL written to
the attempt's log, so a failed task can be debugged from its log alone.

Counts use exact affected-row results where the dialect guarantees them. Other dialects count
the affected snapshot before mutation; a final target count records the table's full size.
"""

from __future__ import annotations

import contextlib
import logging
import secrets
import time
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from sqlalchemy import event, text
from sqlalchemy.engine import Connection, CursorResult
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.core.enums import SqlAction, TableFormat
from etl_craft.core.errors import HandlerError, SqlGuardError
from etl_craft.core.text import qualify, split_object_ref
from etl_craft.core.uploads import UploadWriter
from etl_craft.dialects.warehouse import WarehouseDialect
from etl_craft.dialects.warehouse.metadata import ddl_tables, projection_names
from etl_craft.engine.repository.offsets import StoredOffset

if TYPE_CHECKING:
    from etl_craft.handlers.sql.protection import Protection

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
        self.protection: Protection | None = None
        self.uploads: tuple[UploadWriter, ...] = ()
        self.bindings: dict[str, object] = {}
        database, self.schema, self.table = split_object_ref(target_object)
        # A target that names its database is written there; otherwise in the active one.
        self.catalog = database or catalog
        self.target = f"{self.catalog}.{self.schema}.{self.table}"
        self.scratch_tables: list[str] = []
        self._temporary_scratch: set[str] = set()
        self.token = secrets.token_hex(3)
        self._scratch_number = 0
        self.publish_hash_version: int | None = None
        self.clear_hash_version = False
        self.publish_view_statement: str | None = None
        self.watermark_to: StoredOffset | None = None
        # An error whose work so far is committed before it is raised: rows set aside over
        # REJECT_LIMIT, while the target is left unwritten.
        self.failure_to_commit: SqlGuardError | None = None
        self._metadata: dict[str, Sequence[tuple[str, str, int | None, int | None, str]]] = {}
        self._types: dict[str, dict[str, str]] = {}
        self._names: dict[str, list[str]] = {}
        self._relations: dict[str, str | None] = {}
        self._generators: dict[str, bool] = {}
        self._formats: dict[str, TableFormat] = {}
        event.listen(conn, "before_cursor_execute", self._invalidate)

    def _invalidate(
        self,
        conn: Connection,
        cursor: object,
        sql: str,
        parameters: object,
        context: object,
        many: bool,
    ) -> None:
        tables = ddl_tables(sql, self.dialect.sqlglot_dialect)
        if tables == set():
            return
        for cache in (
            self._metadata,
            self._types,
            self._names,
            self._relations,
            self._generators,
            self._formats,
        ):
            for name in list(cache):
                if tables is None or name.rsplit(".", 1)[-1] in tables:
                    del cache[name]

    def prime_target(self, related: tuple[str, ...]) -> None:
        """Cache the target and its protection evidence from one catalog read."""
        names = (self.target, *related)
        if not self.dialect.catalog_columns:
            found = self.relation_types(names)
            if 0 not in found:
                self._metadata[self.target.lower()] = ()
            elif self.action not in {SqlAction.CREATE_VIEW, SqlAction.DROP_TABLE}:
                self._shape(self.target)
            return
        query, parameters = self.dialect.task_metadata_query(self.conn, self.target, names)
        rows = self.run(query, parameters, step=f"read the columns of {self.target}").all()
        self._metadata[self.target.lower()] = [
            (str(row[0]), str(row[1]), row[2], row[3], str(row[4]))
            for row in rows
            if row[0] is not None
        ]
        for index, name in enumerate(names):
            self._relations[name.lower()] = rows[0][5 + index] if rows else None

    def select_columns(self, name: str, sql: str) -> list[str]:
        """Use explicit parsed aliases, or ask the warehouse about a dynamic projection."""
        names = projection_names(sql, self.dialect.sqlglot_dialect, self.dialect.identifier_case)
        if names is None:
            query = self.dialect.event_probe_sql(sql, 0)
            names = list(self.run(query, step="read the SELECT's columns").keys())
        self._names[name.lower()] = names
        return names

    def create_hashed_stage(self, name: str, sql: str, compare: tuple[str, ...]) -> str:
        """Keep parsed business names and add only the hash generated by our CTAS."""
        names = self._names[name.lower()]
        hashed = self.hash([f"etl_src.{column}" for column in compare], compare)
        name = self.create_scratch(
            name, self.dialect.hashed_stage(sql, hashed), step="stage the SELECT", immutable=True
        )
        self._names[name.lower()] = [*names, "HASH_KEY"]
        return name

    def describe_select(self, sql: str) -> list[tuple[str, str]] | None:
        """Keep source-shape failures at the same task boundary as materialized staging."""
        try:
            return self.dialect.describe_select(self.conn, sql)
        except SQLAlchemyError as error:
            raise self._failure("stage the SELECT", error) from error

    def _shape(self, name: str) -> Sequence[tuple[str, str, int | None, int | None, str]]:
        key = name.lower()
        if key not in self._metadata:
            started = time.monotonic()
            try:
                found = self.dialect.read_metadata(self.conn, name)
            except SQLAlchemyError as error:
                raise self._failure(f"read the columns of {name}", error) from error
            logger.info("read the columns of %s (%.2fs)", name, time.monotonic() - started)
            self._metadata[key] = found.columns
            if found.generated is not None:
                self._generators[key] = found.generated
            if found.table_format is not None:
                self._formats[key] = found.table_format
        return self._metadata[key]

    # Statements

    def run(
        self, sql: str, params: Mapping[str, Any] | None = None, *, step: str
    ) -> CursorResult[Any]:
        """Run one statement, logging it; ``HandlerError`` naming ``step`` when it fails."""
        logger.debug("%s: %s", step, sql)
        started = time.monotonic()
        try:
            result = self.conn.execute(text(sql), self.bindings | dict(params or {}))
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

    def mutate(self, sql: str, params: Mapping[str, object], *, count_sql: str, step: str) -> int:
        """Use exact DML counts, or count the affected snapshot before unsupported DML."""
        before = None if self.dialect.dml_rowcounts else self.count(count_sql, step=step)
        result = self.run(sql, params, step=step)
        return self.dialect.affected_rows(result) if before is None else before

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

    def create_side_table(self, name: str, select_sql: str, *, step: str) -> None:
        """Create table ``name`` from ``select_sql`` as recovery tables are made.

        It takes the dialect's own table format, never the target's storage settings.
        """
        logger.debug("%s: CREATE TABLE %s AS %s", step, name, select_sql)
        try:
            self.dialect.backup_table(self.conn, name, f"({select_sql}) etl_src")
        except SQLAlchemyError as error:
            logger.error("%s failed; the SELECT was:\n%s", step, select_sql)
            raise self._failure(step, error) from error
        logger.info("%s", step)

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
        """Name a scratch table for this attempt: ``etl_<suffix>_<task_run_id>_<token>_<number>``.

        ``token`` is random per session, so no two attempts, of this task or another, share a
        name. A temporary table lives in the session's own namespace and stays unqualified;
        persistent candidates and warehouses without temporary tables use the target's catalog
        and schema. Every scratch
        table is recorded, so ``sweep`` drops it whatever happens.
        """
        self._scratch_number += 1
        bare = f"etl_{suffix}_{self.task_run_id}_{self.token}_{self._scratch_number}"
        name = (
            bare
            if self.dialect.temporary_tables and not persistent
            else self.qualify(f"{self.schema}.{bare}")
        )
        self.scratch_tables.append(name)
        if self.dialect.temporary_tables and not persistent:
            self._temporary_scratch.add(name)
        return name

    def create_scratch(self, name: str, body: str, *, step: str, immutable: bool = False) -> str:
        """Create a stage; an explicit temporary-table refusal permits persistent storage."""
        actual, ddl = self.dialect.scratch_ddl(name, body, immutable=immutable)
        try:
            self.run(ddl, step=step)
        except HandlerError as error:
            if actual == name or not self.dialect.temporary_stage_unavailable(error.__cause__):
                raise
            logger.info(
                "%s: session temporary tables unavailable; using stage %s", self.target, name
            )
            actual, ddl = self.dialect.scratch_ddl(name, body)
            self.run(ddl, step=step)
        if actual != name:
            self.scratch_tables.remove(name)
            self.scratch_tables.append(actual)
            self._temporary_scratch.add(actual)
        names = projection_names(body, self.dialect.sqlglot_dialect, self.dialect.identifier_case)
        if names is not None:
            self._names[actual.lower()] = names
        return actual

    def drop(self, name: str) -> None:
        """Drop a table if it exists."""
        if name in self._temporary_scratch:
            return
        self.run(f"DROP TABLE IF EXISTS {name}", step=f"drop {name}")
        if name in self.scratch_tables:
            self.scratch_tables.remove(name)

    def sweep(self) -> None:
        """Drop every scratch table, ignoring failures.

        It runs while an error may already be on its way out; a cleanup failure must not
        replace the error worth reading. On a warehouse without rollback (Trino) a scratch table
        left behind would be a real table in the team's schema.
        """
        for name in self.scratch_tables:
            if name in self._temporary_scratch:
                continue
            with contextlib.suppress(Exception):
                self.conn.execute(text(f"DROP TABLE IF EXISTS {name}"))
        event.remove(self.conn, "before_cursor_execute", self._invalidate)

    def add_column(self, name: str, column: str, data_type: str) -> None:
        """Extend cached metadata only after our exact nullable-column DDL succeeds."""
        key = name.lower()
        before = self._metadata.get(key)
        types = self._types.get(key)
        names = self._names.get(key)
        generator, table_format = self._generators.get(key), self._formats.get(key)
        relation = self._relations.get(key)
        self.run(
            self.dialect.add_column_sql(name, column, data_type, target=name == self.target),
            step=f"add column {column} ({data_type})",
        )
        if before is not None:
            self._metadata[key] = [*before, (column, data_type, None, None, "YES")]
        if types is not None:
            self._types[key] = {**types, column.lower(): data_type}
        if names is not None:
            self._names[key] = [*names, column]
        if generator is not None:
            self._generators[key] = generator
        if table_format is not None:
            self._formats[key] = table_format
        if relation is not None:
            self._relations[key] = relation

    # Shapes

    def columns(self, name: str) -> list[tuple[str, str]]:
        """Return ``[(column, data_type)]`` of table ``name`` in column order.

        ``name`` is ``catalog.schema.table``, ``schema.table`` or a bare temporary scratch table
        name, matched by name alone: a scratch name carries a token no other table has.
        """
        if name.lower() in self._names:
            return [(column, "") for column in self._names[name.lower()]]
        return [(str(row[0]), str(row[1])) for row in self._shape(name)]

    def column_types(self, name: str) -> dict[str, str]:
        """Read the table's complete types once, with lowercase column keys."""
        if self.dialect.metadata_types_complete:
            return {column.lower(): kind for column, kind, *_ in self._shape(name)}
        logger.debug("read complete column types of %s", name)
        try:
            key = name.lower()
            if key not in self._types:
                self._types[key] = self.dialect.full_column_types(self.conn, name)
            return dict(self._types[key])
        except SQLAlchemyError as error:
            raise self._failure(f"read complete column types of {name}", error) from error

    def hash_types(self, name: str) -> dict[str, str]:
        """Return types and declared decimal scales for hashing table ``name``.

        ``name`` is ``catalog.schema.table``, ``schema.table`` or a bare temporary scratch table
        name, matched by name alone: a scratch name carries a token no other table has.
        """
        rows = self._shape(name)
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
            raise SqlGuardError(f"{self.target} lacks compare columns {', '.join(missing)}")
        return self.dialect.hash_expression(values, [types[column.lower()] for column in columns])

    def target_relation_type(self) -> str | None:
        """Return the target's information_schema table type, such as VIEW; None when absent."""
        return self.relation_types((self.target,)).get(0)

    def relation_types(self, names: tuple[str, ...]) -> dict[int, str]:
        """Read related objects together without confusing absent and partial evidence."""
        if all(name.lower() in self._relations for name in names):
            return {
                index: kind
                for index, name in enumerate(names)
                if (kind := self._relations[name.lower()]) is not None
            }
        query, parameters = self.dialect.relation_types_query(names)
        rows = self.run(query, parameters, step=f"read relation types of {', '.join(names)}")
        found = {}
        for row in rows.mappings():
            index = int(row["position"])
            found[index] = str(row["table_type"]).upper()
            if storage := row.get("table_format"):
                self._formats[names[index].lower()] = TableFormat(storage)
        self._relations.update({name.lower(): found.get(index) for index, name in enumerate(names)})
        return found

    def not_null_columns(self, name: str) -> set[str]:
        """Return the lower-case names of table ``name``'s columns declared ``NOT NULL``."""
        return {
            str(row[0]).lower()
            for row in self._shape(name)
            if str(row[4]).upper() in {"NO", "FALSE"}
        }

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
        if qualified_from in self.scratch_tables:
            self.scratch_tables.remove(qualified_from)

    def row_id_insert_parts(self) -> tuple[str, str]:
        """Return the extra (columns, values) an INSERT needs where ROW_ID does not fill itself.

        An identity column or a sequence default fills ROW_ID. Targets without a generator
        supply the largest ROW_ID present plus a row number under the target mutation lock.
        The base is read before the INSERT when the dialect cannot read its own write target.
        """
        if self.row_id_generated():
            return "", ""
        base = self.dialect.row_id_base_sql(self.target)
        if base is None:
            base = str(
                self.count(
                    f"SELECT COALESCE(MAX({ROW_ID_COLUMN}), 0) FROM {self.target}",
                    step="largest ROW_ID",
                )
            )
        # ORDER BY NULL: Databricks rejects a window with no order.
        return (
            f", {ROW_ID_COLUMN}",
            f", {base} + CAST(ROW_NUMBER() OVER (ORDER BY NULL) AS BIGINT)",
        )

    def check_target_format(self) -> None:
        """Refuse a format mismatch before creating or evolving an existing target."""
        try:
            key = self.target.lower()
            if key not in self._formats:
                self._formats[key] = self.dialect.existing_table_format(self.conn, self.target)
            existing = self._formats[key]
        except SQLAlchemyError as error:
            raise self._failure("read the existing table format", error) from error
        if existing != self.dialect.spec.table_format:
            raise SqlGuardError(
                f"{self.action} {self.target}: existing table format is {existing}, "
                f"but this task resolves to {self.dialect.spec.table_format}; set TABLE_FORMAT="
                f"{existing} or use a different TARGET_OBJECT. Migrate formats explicitly"
            )

    def row_id_generated(self) -> bool:
        """Read the existing target's generator rather than assuming new-table defaults."""
        logger.debug("read the ROW_ID generator of %s", self.target)
        try:
            key = self.target.lower()
            if key not in self._generators:
                self._generators[key] = self.dialect.row_id_generated(self.conn, self.target)
            return self._generators[key]
        except SQLAlchemyError as error:
            raise self._failure("read the ROW_ID generator", error) from error
