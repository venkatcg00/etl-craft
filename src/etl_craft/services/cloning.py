"""Cloning: copying the Engine DB tables into the warehouse after each pipeline run.

With ``Cloning.Enabled``, every run ends by mirroring the Engine DB tables its ``Scope`` names
(``cfg``: the ``CFG_`` tables, ``aud``: the ``AUD_`` tables, ``all``, or ``none``) into the
warehouse profile's ``schema``, so a team can query its pipelines' configuration and history
from inside its own warehouse.

Each mirror has the Engine DB table's name and columns, with portable types: whole numbers
``BIGINT``, other numbers ``DECIMAL(38, 10)``, timestamps the warehouse's timestamp with time
zone, and everything else text; JSON values are copied as their text. A mirror is created when
it is missing, and its rows replaced on every clone, one transaction per table where the
warehouse has them. A mirror whose columns no longer match the Engine DB table, after an
upgrade adds one, is dropped and created again: every row is rewritten anyway.

Cloning writes only the mirrors, and only in the warehouse profile's schema, which must already
exist. It refuses a warehouse schema that is the Engine DB's own, where it would empty the
tables it reads. Two clones never overlap: the second waits, then copies the newer state.

After a run, a failed clone is logged with its cause and does not change how the run ended;
``etl-craft clone`` runs it by hand and fails with the cause (``CloningError``, naming the
table).
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.types import TypeEngine

from etl_craft.config import ConnectorConfig
from etl_craft.config.targets import active_catalog, parse_warehouse_url
from etl_craft.core.enums import CloningScope
from etl_craft.core.errors import CloningError, ConfigurationError
from etl_craft.core.text import parse_jdbc_url, qualify
from etl_craft.dialects.warehouse import WarehouseDialect
from etl_craft.engine import locks
from etl_craft.execution.pipeline import PipelineOutcome, RunHooks, default_hooks
from etl_craft.warehouse.connection import open_warehouse, warehouse_dialect

logger = logging.getLogger(__name__)

BATCH_ROWS = 1_000
"""How many rows are read from the Engine DB and inserted into a mirror at a time."""

PREFIXES: Mapping[CloningScope, tuple[str, ...]] = {
    CloningScope.CFG: ("CFG_",),
    CloningScope.AUD: ("AUD_",),
    CloningScope.ALL: ("CFG_", "AUD_"),
    CloningScope.NONE: (),
}
"""The table-name prefixes each scope copies."""


@dataclass(frozen=True)
class ClonedTable:
    """One mirror written: its name, the rows copied, and whether it was (re)created."""

    table: str
    mirror: str
    rows: int
    created: bool


def cloning_problem(config: ConnectorConfig) -> str | None:
    """Return why cloning cannot run with this configuration, or ``None``.

    Nothing connects: this is the Warehouse section, the storage the mirrors need, and whether
    the warehouse schema is the Engine DB's own.
    """
    if config.warehouse is None:
        return "Cloning is enabled, but there is no Warehouse section to copy the tables into"
    problem = warehouse_dialect(config).cloning_storage_problem(config.cloning)
    return problem or _own_schema_problem(config)


def _own_schema_problem(config: ConnectorConfig) -> str | None:
    assert config.warehouse is not None
    engine_profile, warehouse_profile = config.engine.active, config.warehouse.active
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
    schema = config.warehouse.active.schema
    cloned: list[ClonedTable] = []
    with locks.CLONE.hold(engine), open_warehouse(config, engine) as warehouse:
        for table, columns in _engine_tables(engine, config, PREFIXES[cloning.scope]):
            mirror = qualify(f"{schema}.{table}", catalog)
            cloned.append(_clone_table(engine, warehouse, dialect, config, table, mirror, columns))
    return cloned


def run_hooks(config: ConnectorConfig, engine: Engine) -> RunHooks:
    """Return the hooks a run gets: ``default_hooks``, then a clone once it has ended."""
    hooks = default_hooks(config, engine)
    if not config.cloning.enabled or config.cloning.scope == CloningScope.NONE:
        return hooks
    before = hooks.on_finalized

    def finalized(outcome: PipelineOutcome) -> None:
        if before is not None:
            before(outcome)
        tables = clone(engine, config)
        logger.info(
            "cloned %d table(s), %d row(s), into the warehouse",
            len(tables),
            sum(t.rows for t in tables),
        )

    return replace(hooks, on_finalized=finalized)


def _engine_tables(
    engine: Engine, config: ConnectorConfig, prefixes: tuple[str, ...]
) -> list[tuple[str, list[tuple[str, TypeEngine[Any]]]]]:
    """Return each Engine DB table the prefixes name, upper case, with its columns in order."""
    schema = config.engine.active.schema or None
    inspector = inspect(engine)
    found = []
    for name in sorted(inspector.get_table_names(schema=schema), key=str.upper):
        if name.upper().startswith(prefixes):
            columns = inspector.get_columns(name, schema=schema)
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
    column_ddl = ", ".join(f"{name} {mirror_type(kind, dialect)}" for name, kind in columns)
    insert = text(
        f"INSERT INTO {mirror} ({', '.join(names)}) "
        f"VALUES ({', '.join(f':p{i}' for i in range(len(names)))})"
    )
    rows = 0
    try:
        with warehouse.begin() as conn:
            existing = {name.upper() for name, _ in _mirror_columns(conn, dialect, mirror)}
            created = existing != set(names)
            if existing and created:
                logger.info("%s has columns %s; creating it again", mirror, sorted(existing))
                conn.execute(text(f"DROP TABLE {mirror}"))
            if created:
                ddl = dialect.mirror_table_ddl(mirror, column_ddl, config.cloning)
                conn.execute(text(ddl or f"CREATE TABLE {mirror} ({column_ddl})"))
            else:
                conn.execute(text(f"TRUNCATE TABLE {mirror}"))
            for batch in _batches(engine, table, names):
                conn.execute(insert, [{f"p{i}": v for i, v in enumerate(row)} for row in batch])
                rows += len(batch)
    except SQLAlchemyError as error:
        raise CloningError(f"cloning {table} into {mirror} failed: {error}") from error
    logger.info(
        "cloned %s into %s: %d row(s) in %.1fs%s",
        table,
        mirror,
        rows,
        time.monotonic() - started,
        " (created)" if created else "",
    )
    return ClonedTable(table, mirror, rows, created)


def _batches(engine: Engine, table: str, names: list[str]) -> Iterator[list[tuple[object, ...]]]:
    with engine.connect() as conn:
        result = conn.execution_options(stream_results=True, yield_per=BATCH_ROWS).execute(
            text(f"SELECT {', '.join(names)} FROM {table}")
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
