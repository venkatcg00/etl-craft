"""Cloning: merge-style copy of selected Engine DB tables into the Data DB.

Per CLAUDE.md: "a merge-style copy of selected Engine DB tables into the
Data DB, so a team can query engine config/audit from inside their own
warehouse without a separate Postgres connection. Runs after each pipeline
run, only when enabled." This module is that copy mechanism -- the last
piece [Cloning] itself was never more than config plumbing for (see
config.CloningConfig).

[CHOICE] "Merge-style" is implemented as truncate-then-reinsert per table,
not a genuine row-level upsert/delete-tracking merge. A real cross-dialect
MERGE (Engine DB always Postgres, Data DB "any SQLAlchemy-supported
relational engine," per CLAUDE.md's own Architecture section) would need
either a portable UPSERT statement no single ANSI form covers, or per-row
diffing logic duplicated for every possible target dialect -- real
engineering cost with little payoff, since CFG_/AUD_ tables are metadata,
not fact-table-scale data, and the actual stated goal ("so a team can query
engine config/audit from inside their own warehouse") only needs a current,
correct mirror, not incremental change tracking. Flagged as a real,
deliberate scope limit, not silently assumed equivalent to a true merge.

[ADDITION] Column types in the mirrored Data DB table are deliberately
generic (BigInteger/Numeric/Boolean/DateTime/Text), not a byte-for-byte copy
of the Engine DB's own Postgres-specific types -- CFG_PIPELINES.
PIPELINE_PARAMETERS (JSONB) and any array-typed column have no portable
equivalent across "any SQLAlchemy-supported relational engine," so those
values are JSON-serialized into a Text column at insert time instead. This
loses native-type fidelity for those two-or-three columns specifically, but
keeps the whole mechanism dialect-agnostic rather than hand-writing a
per-dialect type map for a small number of edge cases.

[CHOICE] A cloning failure (Data DB unreachable, a genuinely incompatible
target dialect, ...) is caught by the caller (orchestrator.py) and reported
as a warning, never allowed to turn an otherwise-successful pipeline run
into a reported failure -- this is explicitly "special-cased engine-internal
machinery," per CLAUDE.md, secondary to the pipeline's own correctness.

[CHOICE, a real flagged dialect exception, verified directly against a real
ClickHouse target, not assumed] Two genuine ClickHouse-specific gaps
surfaced building this against a real second dialect (the same "prove it
for real" bar warehouse.py's own ClickHouse test already set):
  1. ClickHouse refuses any plain CREATE TABLE with no explicit table
     ENGINE clause -- something no other mainstream dialect this project
     has touched requires. clickhouse-sqlalchemy only accepts that clause
     via its own Engine construct (clickhouse_sqlalchemy.engines.*), which
     this module deliberately never imports -- per CLAUDE.md's Non-goals,
     no third-party dialect is a hard or direct dependency of engine code.
     `_create_clickhouse_table` below is the pragmatic exception instead: a
     hand-built, literal CREATE TABLE with `ENGINE = MergeTree() ORDER BY
     tuple()` (no natural sort key -- this generic mirroring mechanism has
     no basis to pick one), reached only when `data_engine.dialect.name ==
     "clickhouse"` -- a dialect *name* string, not an import.
  2. clickhouse-sqlalchemy's own table-engine reflection resolves the
     target database from `connection.engine.url.database`, not the live
     DBAPI connection's actual database -- which is always blank for
     warehouse.py's own creator-based engines (`create_engine(f"{dialect}
     ://", creator=...)`, deliberately never rendering the password into a
     URL). Every other dialect this project has used resolves its own
     default schema from the live connection itself and needs nothing
     passed; ClickHouse alone needs the real database name passed
     explicitly as `schema=`, resolved once via warehouse.translate_jdbc_url
     the same way warehouse.py itself already parses a profile's JDBC URL.
Both are the same spirit as sql_actions.py's own accepted MD5/TRUNCATE
exceptions: flagged, not silently papered over, and scoped to the one real
dialect this project has actually proven needs them.
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

from etl_craft.config import ConnectorConfig
from etl_craft.warehouse import build_data_engine, translate_jdbc_url

# [ADDITION] CLAUDE.md names these table groups ("Scope: cfg | aud | all —
# which table groups mirror into the Data DB") but never enumerates the
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

# [ADDITION] See this module's own docstring for why ClickHouse specifically
# needs this — a small, literal type-name map for the one raw-SQL fallback
# path, not a general per-dialect type system.
_CLICKHOUSE_TYPE_NAMES: dict[str, str] = {
    "BIGINT": "Int64",
    "NUMERIC": "Float64",
    "BOOLEAN": "UInt8",
    "DATETIME": "DateTime",
    "TEXT": "String",
}


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
    build_data_engine both deliberately use a blank-URL-plus-creator
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
    """Clone `config.cloning.scope`'s tables from `engine` into the Data DB, if enabled.

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
    data_engine = build_data_engine(config)
    try:
        _, parts = translate_jdbc_url(config.warehouse.active.jdbc_url)
        warehouse_database = parts["database"]
        for table_name in tables_for_scope(config.cloning.scope):
            _clone_table(engine, data_engine, table_name, warehouse_database)
    finally:
        data_engine.dispose()


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


def _clickhouse_schema(data_engine: Engine, warehouse_database: str) -> str | None:
    """Resolve the `schema=` reflection needs -- ClickHouse only. See module docstring."""
    return warehouse_database if data_engine.dialect.name == "clickhouse" else None


def _create_clickhouse_table(data_engine: Engine, table_name: str, columns: list[Column]) -> None:
    """Hand-built CREATE TABLE for ClickHouse's mandatory ENGINE clause -- see module docstring."""
    column_defs = ", ".join(
        f"{col.name} Nullable({_CLICKHOUSE_TYPE_NAMES.get(str(col.type), 'String')})"
        for col in columns
    )
    with data_engine.begin() as conn:
        conn.execute(
            text(f"CREATE TABLE {table_name} ({column_defs}) ENGINE = MergeTree() ORDER BY tuple()")
        )


def _ensure_target_table(
    data_engine: Engine, source_table: Table, warehouse_database: str
) -> Table:
    target_name = source_table.name
    schema = _clickhouse_schema(data_engine, warehouse_database)
    if inspect(data_engine).has_table(target_name, schema=schema):
        return Table(target_name, MetaData(), autoload_with=data_engine, schema=schema)
    columns = [Column(col.name, _generic_type(col.type)) for col in source_table.columns]
    if schema is not None:
        _create_clickhouse_table(data_engine, target_name, columns)
    else:
        target_metadata = MetaData()
        Table(target_name, target_metadata, *columns)
        target_metadata.create_all(data_engine)
    return Table(target_name, MetaData(), autoload_with=data_engine, schema=schema)


def _serialize_row(row: dict) -> dict:
    return {k: (json.dumps(v) if isinstance(v, (dict, list)) else v) for k, v in row.items()}


def _clone_table(
    engine: Engine, data_engine: Engine, table_name: str, warehouse_database: str
) -> None:
    source_table = _reflect(engine, table_name)
    with engine.connect() as conn:
        rows = [dict(row) for row in conn.execute(select(source_table)).mappings().all()]

    target_table = _ensure_target_table(data_engine, source_table, warehouse_database)
    qualified_name = (
        f"{target_table.schema}.{target_table.name}" if target_table.schema else target_table.name
    )
    with data_engine.begin() as conn:
        # TRUNCATE, not Table.delete() with no predicate: ClickHouse's own
        # DELETE compiler refuses an unconditional DELETE outright ("WHERE
        # clause is required") -- verified directly, not assumed. TRUNCATE
        # is already this project's own established "clear a table for a
        # full rewrite" idiom (sql_actions.py's OVERWRITE_TABLE) and every
        # dialect touched so far, Postgres included, supports it.
        conn.execute(text(f"TRUNCATE TABLE {qualified_name}"))
        if rows:
            conn.execute(target_table.insert(), [_serialize_row(row) for row in rows])
