"""Cloning: merge-style copy of selected Engine DB tables into the warehouse.

Per CLAUDE.md: "a merge-style copy of selected Engine DB tables into the
warehouse, so a team can query engine config/audit from inside their own
warehouse without a separate Postgres connection. Runs after each pipeline
run, only when enabled." This module is that copy mechanism -- the last
piece [Cloning] itself was never more than config plumbing for (see
config.CloningConfig).

[CHOICE] "Merge-style" is implemented as truncate-then-reinsert per table,
not a genuine row-level upsert/delete-tracking merge. A real cross-dialect
MERGE (Engine DB always Postgres, warehouse "any SQLAlchemy-supported
relational engine," per CLAUDE.md's own Architecture section) would need
either a portable UPSERT statement no single ANSI form covers, or per-row
diffing logic duplicated for every possible target dialect -- real
engineering cost with little payoff, since CFG_/AUD_ tables are metadata,
not fact-table-scale data, and the actual stated goal ("so a team can query
engine config/audit from inside their own warehouse") only needs a current,
correct mirror, not incremental change tracking. Flagged as a real,
deliberate scope limit, not silently assumed equivalent to a true merge.

[ADDITION] Column types in the mirrored warehouse table are deliberately
generic (BigInteger/Numeric/Boolean/DateTime/Text), not a byte-for-byte copy
of the Engine DB's own Postgres-specific types -- CFG_PIPELINES.
PIPELINE_PARAMETERS (JSONB) and any array-typed column have no portable
equivalent across "any SQLAlchemy-supported relational engine," so those
values are JSON-serialized into a Text column at insert time instead. This
loses native-type fidelity for those two-or-three columns specifically, but
keeps the whole mechanism dialect-agnostic rather than hand-writing a
per-dialect type map for a small number of edge cases.

[CHOICE] A cloning failure (warehouse unreachable, a genuinely incompatible
target dialect, ...) is caught by the caller (orchestrator.py) and reported
as a warning, never allowed to turn an otherwise-successful pipeline run
into a reported failure -- this is explicitly "special-cased engine-internal
machinery," per CLAUDE.md, secondary to the pipeline's own correctness.

[DEVIATION, 2026-09-20] This module used to carry two ClickHouse-specific
exceptions -- a hand-built CREATE TABLE for its mandatory ENGINE clause, and
an explicit `schema=` for its table-engine reflection. Both are gone with
ClickHouse itself, which is no longer a supported warehouse: per explicit
decision the supported pair is Postgres and DuckDB, both of which take the
plain SQLAlchemy metadata path. The generic mechanism is unchanged and still
never imports a dialect.
"""

from __future__ import annotations

import json

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    MetaData,
    Numeric,
    Table,
    inspect,
    select,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.sql.sqltypes import Text as GenericText
from sqlalchemy.types import TypeEngine

from etl_craft.config import CloningConfig, ConnectorConfig
from etl_craft.sql_actions import ICEBERG_CREATE_PREFIX, table_format_clause
from etl_craft.warehouse import open_warehouse, translate_jdbc_url

# [ADDITION] CLAUDE.md names these table groups ("Scope: cfg | aud | all —
# which table groups mirror into the warehouse") but never enumerates the
# actual tables — this module's own authoritative list, mirroring
# schema.sql's "Config tables" / "Audit tables" summary exactly.
CFG_TABLES: tuple[str, ...] = (
    "CFG_PIPELINES",
    "CFG_PIPELINE_DEPENDENCY",
    "CFG_TASKS",
    "CFG_TASK_DEPENDENCY",
    "CFG_TASK_PARAMETERS",
    "CFG_BUSINESS_RULES",
)
AUD_TABLES: tuple[str, ...] = (
    "AUD_PIPELINES_RUN_LOG",
    "AUD_TASK_RUN_LOG",
    "AUD_BUSINESS_RULES_RUN_LOG",
    "AUD_BUSINESS_RULES_RESULTS",
    "AUD_TASK_OFFSET_TRACKER",
    "AUD_PIPELINE_DEPENDENCY_TRACKER",
    "AUD_TASK_DEPENDENCY_TRACKER",
)


def tables_for_scope(scope: str) -> tuple[str, ...]:
    """Return the table list for `scope` ('cfg', 'aud', or 'all')."""
    if scope == "cfg":
        return CFG_TABLES
    if scope == "aud":
        return AUD_TABLES
    return CFG_TABLES + AUD_TABLES


def _same_database(config: ConnectorConfig) -> bool:
    """Check whether [Postgres] and [Warehouse]'s active profiles resolve to the same database.

    [Bug caught and fixed before shipping, not after] The first version of
    this check compared the *built* Engine objects' own `.url` attributes —
    always blank for both sides, since db.build_engine/warehouse.
    build_warehouse_engine both deliberately use a blank-URL-plus-creator
    pattern (never rendering a secret into a logged/echoed engine URL). Two
    blank URLs always compare equal in some fields and unequal in others by
    accident of which engine happened to be built which way in a given
    caller — in a real integration test built specifically to exercise this
    guard, it silently let a genuinely same-database clone through instead
    of refusing it, hitting Postgres's own real FK-truncate error instead.
    Compares the two profiles' own `jdbc_url` strings instead — config-level
    data that exists regardless of how either Engine object gets built.
    """
    if config.warehouse is None:  # pragma: no cover - callers check first
        return False
    postgres_dialect, postgres_parts = translate_jdbc_url(config.postgres.active.jdbc_url)
    warehouse_dialect, warehouse_parts = translate_jdbc_url(config.warehouse.active.jdbc_url)
    return postgres_dialect == warehouse_dialect and (
        postgres_parts["host"],
        postgres_parts["port"],
        postgres_parts["database"],
    ) == (warehouse_parts["host"], warehouse_parts["port"], warehouse_parts["database"])


def run_cloning_if_enabled(engine: Engine, config: ConnectorConfig) -> None:
    """Clone `config.cloning.scope`'s tables from `engine` into the warehouse, if enabled.

    [ADDITION] Refuses outright if [Warehouse] resolves to the very same
    (host, port, database) as the Engine DB — a real, plausible
    misconfiguration (a team's "warehouse" genuinely is another schema on
    the same Postgres cluster, or a plain copy-paste mistake in
    craft-connector.yml), not a contrived one. Because every mirrored table
    keeps its Engine DB name, cloning onto itself would mean deleting and
    reinserting CFG_/AUD_ tables in place from their own reflection — a
    real, catastrophic risk (concurrent readers would see the table empty
    mid-operation, and anything going wrong between the read and the
    reinsert loses the actual data, not a copy of it), not a redundant
    no-op. Caught here, before anything is touched, rather than left to be
    discovered the first time it happens for real.
    """
    if not config.cloning.enabled:
        return
    if config.warehouse is None:
        raise ValueError(
            "Cloning.Enabled is true but no [Warehouse] is configured — cloning has nowhere "
            "to copy to"
        )
    if _same_database(config):
        raise ValueError(
            "Cloning refused: the active [Warehouse] profile resolves to the same "
            "database as the Engine DB — cloning would destructively truncate-and-"
            "reinsert CFG_/AUD_ tables in place. Point [Warehouse] at a genuinely "
            "separate database, or disable Cloning."
        )
    # [DEVIATION, 2026-09-21, E2-61] Queues behind any task still using a
    # single-writer warehouse rather than failing on its file lock. Cloning
    # runs at a pipeline's finalize point, which under Mode=orchestrator is a
    # separate process from every task — exactly the contention case.
    # Unbounded wait: this is already best-effort (see _run_cloning_best_effort
    # in orchestrator.py), so waiting costs nothing a failure would not.
    with open_warehouse(config, engine) as warehouse_engine:
        for table_name in tables_for_scope(config.cloning.scope):
            _clone_table(
                engine, warehouse_engine, table_name, config.cloning, config.warehouse_table_format
            )


def _generic_type(source_type: TypeEngine) -> TypeEngine:
    """Map a reflected Engine DB (Postgres) column type to a portable, cross-dialect type."""
    python_type = source_type.python_type if hasattr(source_type, "python_type") else None
    if python_type in (int,):
        return BigInteger()
    if python_type in (float,):
        return Numeric()
    if python_type is bool:
        return Boolean()
    try:
        import datetime as _dt

        if python_type in (_dt.datetime, _dt.date):
            return DateTime()
    except (AttributeError, NotImplementedError):
        pass
    return GenericText()


def _reflect(engine: Engine, table_name: str) -> Table:
    # Postgres folds unquoted identifiers to lowercase (see CLAUDE.md's own
    # "Lesson for any future raw-SQL code here") — schema.sql's tables were
    # all created unquoted, so reflection must use the lowercase form.
    return Table(table_name.lower(), MetaData(), autoload_with=engine)


def _ensure_target_table(
    warehouse_engine: Engine, source_table: Table, cloning: CloningConfig, table_format: str
) -> Table:
    target_name = source_table.name
    if inspect(warehouse_engine).has_table(target_name):
        return Table(target_name, MetaData(), autoload_with=warehouse_engine)
    columns = [Column(col.name, _generic_type(col.type)) for col in source_table.columns]

    # [DEVIATION, 2026-09-22, E2-68] CLAUDE.md's invariant is that on any
    # non-Postgres warehouse *every* table the engine creates is an Iceberg
    # table. sql_actions.create_table_as upholds it; this built its mirrors
    # with plain SQLAlchemy DDL, which emits a bare CREATE TABLE and knows
    # nothing about the clause -- so on Databricks and Snowflake the mirror
    # was exactly the silently-unreadable artifact the Snowflake guard exists
    # to prevent. One invariant had two implementations, and only one of them
    # was careful.
    dialect_name = warehouse_engine.dialect.name
    needs_iceberg = table_format == "iceberg" and dialect_name in ICEBERG_CREATE_PREFIX
    if needs_iceberg or table_format_clause(dialect_name, table_format):
        _create_mirror(warehouse_engine, target_name, columns, dialect_name, cloning, table_format)
    else:
        target_metadata = MetaData()
        Table(target_name, target_metadata, *columns)
        target_metadata.create_all(warehouse_engine)
    return Table(target_name, MetaData(), autoload_with=warehouse_engine)


def _create_mirror(
    warehouse_engine: Engine,
    target_name: str,
    columns: list[Column],
    dialect_name: str,
    cloning: CloningConfig,
    table_format: str,
) -> None:
    """Create one mirrored table in the warehouse's configured format, as sql_actions does."""
    type_compiler = warehouse_engine.dialect.type_compiler_instance
    column_ddl = ", ".join(f"{c.name} {type_compiler.process(c.type)}" for c in columns)
    prefix_kind = ICEBERG_CREATE_PREFIX.get(dialect_name) if table_format == "iceberg" else None
    if prefix_kind:
        if not cloning.external_volume or not cloning.base_location:
            raise ValueError(
                f"Cloning to {dialect_name} needs CREATE {prefix_kind} with an EXTERNAL_VOLUME "
                "and BASE_LOCATION — set Cloning.External_volume and Cloning.Base_location in "
                "craft-connector.yml. Refusing rather than mirroring into a non-Iceberg table "
                "nothing else in the lakehouse could read."
            )
        statement = (
            f"CREATE {prefix_kind} {target_name} ({column_ddl}) "
            f"EXTERNAL_VOLUME = '{cloning.external_volume}' CATALOG = 'SNOWFLAKE' "
            f"BASE_LOCATION = '{cloning.base_location}/{target_name}'"
        )
    else:
        clause = table_format_clause(dialect_name, table_format)
        statement = f"CREATE TABLE {target_name} ({column_ddl}) {clause}".rstrip()
    with warehouse_engine.begin() as conn:
        conn.execute(text(statement))


# [ADDITION, 2026-09-20, E2-22] How many rows are held in memory at once
# while mirroring a table. Large enough that the round trips are not the
# bottleneck, small enough that an AUD_ table with millions of rows costs a
# bounded amount of memory rather than all of it.
CLONE_BATCH_ROWS = 5_000


def _serialize_row(row: dict) -> dict:
    return {k: (json.dumps(v) if isinstance(v, (dict, list)) else v) for k, v in row.items()}


def _clone_table(
    engine: Engine,
    warehouse_engine: Engine,
    table_name: str,
    cloning: CloningConfig,
    table_format: str,
) -> None:
    """Mirror one Engine DB table into the warehouse, streaming rather than materializing.

    [DEVIATION, 2026-09-20, E2-22] This used to be
    `rows = [dict(r) for r in conn.execute(select(t)).mappings().all()]` — the
    entire table into a Python list, after every pipeline run. For
    AUD_TASK_RUN_LOG after a year of daily runs that is millions of rows held
    in memory at once, on a box that also has a pipeline to run. Streamed in
    batches now: memory is bounded by CLONE_BATCH_ROWS regardless of table
    size.
    """
    source_table = _reflect(engine, table_name)
    target_table = _ensure_target_table(warehouse_engine, source_table, cloning, table_format)
    qualified_name = (
        f"{target_table.schema}.{target_table.name}" if target_table.schema else target_table.name
    )
    with warehouse_engine.begin() as target_conn:
        # TRUNCATE, not Table.delete() with no predicate. It is already this
        # project's own established "clear a table for a full rewrite" idiom
        # (sql_actions.py's OVERWRITE_TABLE) and both supported warehouses
        # implement it. It was originally chosen because ClickHouse's DELETE
        # compiler refuses an unconditional DELETE outright ("WHERE clause is
        # required"); ClickHouse is gone, but the idiom is the more portable
        # one regardless, so it stays.
        target_conn.execute(text(f"TRUNCATE TABLE {qualified_name}"))
        with engine.connect().execution_options(
            stream_results=True, yield_per=CLONE_BATCH_ROWS
        ) as source_conn:
            for partition in source_conn.execute(select(source_table)).mappings().partitions():
                batch = [_serialize_row(dict(row)) for row in partition]
                if batch:
                    target_conn.execute(target_table.insert(), batch)
