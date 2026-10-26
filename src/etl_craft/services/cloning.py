"""Scheduled or explicit incremental mirrors of retained, project-scoped Engine DB state."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator, Mapping
from contextlib import ExitStack
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import column, inspect, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.types import TypeEngine

from etl_craft.config import ConnectorConfig
from etl_craft.config.managed_engine import managed_cloning_problem
from etl_craft.config.targets import active_catalog, parse_warehouse_url
from etl_craft.core.enums import ActionName, ActionOutcome, CloningScope
from etl_craft.core.errors import CloningError, ConfigurationError, LockTimeoutError
from etl_craft.core.faults import fault_point
from etl_craft.core.text import parse_jdbc_url, qualify, sha256_hex
from etl_craft.dialects.engine import for_engine
from etl_craft.dialects.engine.cloning import INTERNAL, changed_statement
from etl_craft.dialects.engine.metadata import CREDENTIAL_COLUMNS
from etl_craft.dialects.engine.tables import CONFIG_TABLES
from etl_craft.dialects.warehouse import WarehouseDialect
from etl_craft.dialects.warehouse.cloning import replace_keys, write_rows
from etl_craft.dialects.warehouse.copying import MAX_BOUND_VALUES
from etl_craft.engine import locks, resource_locks
from etl_craft.engine.audit import ActionRequest
from etl_craft.engine.cloning import CloneState
from etl_craft.engine.repository.pipelines import current_project_id
from etl_craft.execution.run_types import RunHooks, default_hooks
from etl_craft.handlers.resources import LocalLocker
from etl_craft.warehouse.connection import open_warehouse, warehouse_dialect

logger = logging.getLogger(__name__)

BATCH_ROWS = 1_000
"""How many rows are read from the Engine DB at a time."""

PREFIXES: Mapping[CloningScope, tuple[str, ...]] = {
    CloningScope.CFG: ("CFG_",),
    CloningScope.AUD: ("AUD_",),
    CloningScope.ALL: ("CFG_", "AUD_"),
    CloningScope.NONE: (),
}
"""The table-name prefixes each scope copies."""


@dataclass(frozen=True)
class ClonedTable:
    """One mirror written: its name, the rows copied, whether it was created, columns added."""

    table: str
    mirror: str
    rows: int
    created: bool
    added: tuple[str, ...] = ()


def cloning_problem(config: ConnectorConfig) -> str | None:
    """Return why cloning cannot run with this configuration, or ``None``.

    Nothing connects: this is the Warehouse section, the storage the mirrors need, and whether
    the warehouse schema is the Engine DB's own.
    """
    if problem := managed_cloning_problem(config.engine, config.cloning):
        return problem
    if config.warehouse is None:
        return "Cloning is enabled, but there is no Warehouse section to copy the tables into"
    problem = warehouse_dialect(config).cloning_storage_problem(config.cloning)
    return problem or mirror_schema_problem(config)


def mirror_schema_problem(config: ConnectorConfig) -> str | None:
    """Refuse a warehouse copy into the Engine DB's own physical schema."""
    assert config.warehouse is not None
    engine_profile, warehouse_profile = config.engine, config.warehouse
    if not engine_profile.jdbc_url.startswith("jdbc:postgresql:"):
        return None
    warehouse_url = parse_warehouse_url(warehouse_profile.jdbc_url)
    if not warehouse_url.dialect.startswith("postgresql"):
        return None
    engine_url = parse_jdbc_url(engine_profile.jdbc_url, default_port=5432)
    same_database = (
        (engine_url.host or "").lower(),
        engine_url.port,
        engine_url.database,
    ) == ((warehouse_url.host or "").lower(), warehouse_url.port or 5432, warehouse_url.database)
    if same_database and engine_profile.schema.lower() == warehouse_profile.schema.lower():
        return (
            f"the Warehouse schema {warehouse_profile.schema} is the Engine DB's own schema in the "
            f"same database ({engine_url.host}:{engine_url.port}/{engine_url.database}); cloning "
            "would empty the tables it copies. Point the Warehouse profile's schema elsewhere"
        )
    return None


def clone(engine: Engine, config: ConnectorConfig) -> list[ClonedTable]:
    """Mirror the Engine DB tables of ``Cloning.Scope`` into the warehouse schema.

    ``ConfigurationError`` when the configuration cannot work; a database error fails with the
    table it was copying. Returns nothing when cloning is off or its scope is ``none``.
    """
    cloning = config.cloning
    if not cloning.enabled or cloning.scope == CloningScope.NONE:
        return []
    problem = cloning_problem(config)
    if problem is not None:
        raise ConfigurationError(problem)
    assert config.warehouse is not None
    dialect = warehouse_dialect(config)
    catalog = active_catalog(config)
    schema = config.warehouse.schema
    cloned: list[ClonedTable] = []
    requested = datetime.now(UTC)
    cycle = CloneState(
        sha256_hex(f"{current_project_id()}:{config.warehouse}:{cloning.scope}:cycle".encode()), "*"
    )
    locker = LocalLocker(engine, config)
    waiting = time.monotonic()
    with ExitStack() as held:
        try:
            held.enter_context(locks.CLONE.hold(engine, cloning.lock_wait_seconds))
            warehouse = held.enter_context(open_warehouse(config, locker))
            with warehouse.connect():
                pass
            remaining = max(0.001, cloning.lock_wait_seconds - (time.monotonic() - waiting))
            held.enter_context(
                locks.ResourceLock(locks.CLONE.name, locks.CLONE.key).hold(locker, remaining)
            )
        except LockTimeoutError:
            message = (
                f"clone skipped: lock 'clone' remained held for Cloning.Lock_wait_seconds="
                f"{cloning.lock_wait_seconds}; expected exclusive ownership; "
                "the next scheduled cycle will retry"
            )
            logger.warning(message)
            ActionRequest(
                ActionName.CLONE,
                {"skip": message},
                datetime.now(UTC),
                engine=engine,
                outcome=ActionOutcome.SUCCESS,
                project_id=current_project_id(),
            ).record()
            return []
        if cycle.finished_since(engine, requested):
            logger.info("clone coalesced: another cycle completed while this request waited")
            return []
        cycle = replace(cycle, holder=resource_locks.current_holder(locker.identity))
        cycle.register(engine)
        for table, columns in _engine_tables(engine, config, PREFIXES[cloning.scope]):
            mirror = qualify(f"{schema}.{table}", catalog)
            cloned.append(_clone_table(engine, warehouse, dialect, config, table, mirror, columns))
        cycle.complete(engine)
    return cloned


def run_hooks(config: ConnectorConfig, engine: Engine) -> RunHooks:
    """Compose child admission and finalization hooks without warehouse cloning."""
    from etl_craft.services.operations.child_admission import check_child_start

    return replace(
        default_hooks(config, engine),
        before_admission=lambda conn, run_id: check_child_start(engine, config, conn, run_id),
    )


def _engine_tables(
    engine: Engine, config: ConnectorConfig, prefixes: tuple[str, ...]
) -> list[tuple[str, list[tuple[str, TypeEngine[Any]]]]]:
    """Return each Engine DB table the prefixes name, upper case, with its columns in order."""
    schema = config.engine.schema or None
    inspector = inspect(engine)
    found = []
    for name in sorted(inspector.get_table_names(schema=schema), key=str.upper):
        if (
            name.upper().startswith(prefixes)
            and name.upper() not in INTERNAL | {"CFG_API_TOKENS"}
            and (not name.upper().startswith("CFG_") or name.upper() in CONFIG_TABLES)
        ):
            columns = [
                c
                for c in inspector.get_columns(name, schema=schema)
                if str(c["name"]).upper() not in CREDENTIAL_COLUMNS.get(name.upper(), ())
            ]
            found.append((name.upper(), [(str(c["name"]).upper(), c["type"]) for c in columns]))
    return found


def mirror_type(column_type: TypeEngine[Any], dialect: WarehouseDialect) -> str:
    """Return the warehouse type a mirror gives an Engine DB column."""
    try:
        python_type: type | None = column_type.python_type
    except NotImplementedError:
        python_type = None
    if python_type is bool:
        return "BOOLEAN"
    if python_type is int:
        return "BIGINT"
    if python_type in (float, Decimal):
        return "DECIMAL(38, 10)"
    if python_type is datetime:
        return dialect.audit_column_type("CREATE_DATE")
    if python_type is date:
        return "DATE"
    return dialect.string_type


def _clone_table(
    engine: Engine,
    warehouse: Engine,
    dialect: WarehouseDialect,
    config: ConnectorConfig,
    table: str,
    mirror: str,
    columns: list[tuple[str, TypeEngine[Any]]],
) -> ClonedTable:
    started = time.monotonic()
    names = [name for name, _ in columns]
    identity = sha256_hex(f"{current_project_id()}:{mirror}:{config.warehouse}".encode())
    state = CloneState(
        identity,
        table,
        resource_locks.current_holder("clone"),
        sha256_hex(str([(name, str(kind)) for name, kind in columns]).encode()),
    )
    initialized = state.register(engine) if table.startswith("AUD_") else False
    with engine.connect() as conn:
        keys = [
            str(k).upper()
            for k in inspect(conn).get_pk_constraint(for_engine(conn).stored_name(table))[
                "constrained_columns"
            ]
        ]
    if table.startswith("AUD_") and not keys:
        raise CloningError(
            f"cloning {table} cannot track incremental changes: found no primary key; expected "
            "a package audit-table primary key; add it through an Engine DB migration and retry"
        )
    try:
        created, added = _prepare_mirror(warehouse, dialect, config, mirror, columns)
        if not initialized or created or added:
            with warehouse.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE {mirror}"))
                rows = 0
                for batch in _batches(engine, table, columns):
                    write_rows(conn, mirror, names, batch)
                    rows += len(batch)
            fault_point("clone.after_mirror_commit")
        else:
            rows = _incremental(engine, warehouse, state, (mirror, columns, keys))
        if table.startswith("AUD_"):
            state.complete(engine)
    except SQLAlchemyError as error:
        raise CloningError(
            f"cloning {table} into {mirror} failed: {error}; expected a committed mirror "
            "before acknowledging changes; restore the Engine DB/warehouse and retry clone"
        ) from error
    logger.info(
        "cloned %s into %s: %d row(s) in %.1fs", table, mirror, rows, time.monotonic() - started
    )
    return ClonedTable(table, mirror, rows, created, added)


def _prepare_mirror(
    warehouse: Engine,
    dialect: WarehouseDialect,
    config: ConnectorConfig,
    mirror: str,
    columns: list[tuple[str, TypeEngine[Any]]],
) -> tuple[bool, tuple[str, ...]]:
    names = [name for name, _ in columns]
    types = {name: mirror_type(kind, dialect) for name, kind in columns}
    with warehouse.begin() as conn:
        existing = {name.upper() for name, _ in _mirror_columns(conn, dialect, mirror)}
        if "_ETL_ARCHIVE_ENGINE_ID" in existing:
            raise CloningError(
                f"{mirror}: immutable archive table; expected a replaceable mirror; "
                "choose another cloning schema"
            )
        created = not existing
        added = () if created else tuple(n for n in names if n not in existing)
        if created:
            column_ddl = ", ".join(f"{name} {types[name]}" for name in names)
            ddl = dialect.mirror_table_ddl(mirror, column_ddl, config.cloning)
            conn.execute(text(ddl or f"CREATE TABLE {mirror} ({column_ddl})"))
        else:
            for name in added:
                conn.execute(
                    text(
                        f"{dialect.alter_table_keyword()} {mirror} ADD COLUMN {name} {types[name]}"
                    )
                )
    return created, added


def _incremental(
    engine: Engine,
    warehouse: Engine,
    state: CloneState,
    target: tuple[str, list[tuple[str, TypeEngine[Any]]], list[str]],
) -> int:
    mirror, columns, keys = target
    names = [name for name, _ in columns]
    size = min(BATCH_ROWS, MAX_BOUND_VALUES // max(1, len(keys)))
    after = ""
    rows = 0
    while changes := state.pending(engine, after, size):
        values = [json.loads(key) for key, _ in changes]
        with engine.connect() as source:
            query = changed_statement(
                for_engine(source), state.table_name, names, keys, values
            ).columns(*(column(name, kind) for name, kind in columns))
            batch = [
                tuple(_value(v) for v in row)
                for row in source.execute(query, {"project_id": current_project_id()})
            ]
        with warehouse.begin() as destination:
            replace_keys(destination, mirror, keys, values)
            write_rows(destination, mirror, names, batch)
        fault_point("clone.after_mirror_commit")
        state.acknowledge(engine, changes)
        rows += len(batch)
        after = changes[-1][0]
    return rows


def _batches(
    engine: Engine, table: str, columns: list[tuple[str, TypeEngine[Any]]]
) -> Iterator[list[tuple[object, ...]]]:
    names = [name for name, _ in columns]
    with engine.connect() as conn:
        result = conn.execution_options(stream_results=True, yield_per=BATCH_ROWS).execute(
            for_engine(conn)
            .clone_statement(table, names)
            .columns(*(column(name, kind) for name, kind in columns)),
            {"project_id": current_project_id()},
        )
        for partition in result.partitions(BATCH_ROWS):
            yield [tuple(_value(v) for v in row) for row in partition]


def _value(value: object) -> object:
    return json.dumps(value) if isinstance(value, dict | list) else value


def _mirror_columns(
    conn: Connection, dialect: WarehouseDialect, name: str
) -> list[tuple[str, str]]:
    """Return a mirror's ``[(column, data_type)]``; empty when it does not exist."""
    catalog, schema, table = name.split(".")
    dialect.load_table_metadata(conn, schema, table)
    rows = conn.execute(
        text(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE lower(table_name) = lower(:table) AND lower(table_schema) = lower(:schema) "
            "AND lower(table_catalog) = lower(:catalog) ORDER BY ordinal_position"
        ),
        {"table": table, "schema": schema, "catalog": catalog},
    ).all()
    return [(str(row[0]), str(row[1])) for row in rows]
