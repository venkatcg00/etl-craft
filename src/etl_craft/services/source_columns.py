"""Source definitions, warehouse seeding and schema drift checks."""

from __future__ import annotations

from collections.abc import Iterable

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from sqlglot import exp
from sqlglot.errors import SqlglotError

from etl_craft.config import ConnectorConfig
from etl_craft.config.targets import active_catalog
from etl_craft.core.enums import Handler, RefreshType, SqlAction
from etl_craft.core.errors import MetadataError
from etl_craft.core.text import is_safe_identifier, is_safe_object_ref, qualify
from etl_craft.engine.repository.source_columns import SourceColumn, fetch_source_columns
from etl_craft.engine.repository.tasks import fetch_task_parameters
from etl_craft.engine.repository.validation import fetch_active_tasks
from etl_craft.handlers.sql.spec import resolve_select
from etl_craft.warehouse.connection import open_warehouse, warehouse_dialect


def definition_problem(column: SourceColumn, dialect: str | None) -> str | None:
    """Check definitions from either CSV files or project migrations."""
    if not is_safe_object_ref(column.table_name):
        return (
            f"TABLE_NAME={column.table_name!r}: expected schema.table or "
            f"database.schema.table; correct the source definition"
        )
    if not is_safe_identifier(column.column_name):
        return (
            f"COLUMN_NAME={column.column_name!r}: expected an unquoted SQL identifier; "
            f"correct the source definition"
        )
    try:
        kind = exp.DataType.build(column.data_type, dialect=dialect)
        if kind.this == exp.DataType.Type.UNKNOWN:
            raise ValueError("unknown type")
    except (SqlglotError, ValueError) as error:
        return (
            f"DATA_TYPE={column.data_type!r}: expected a SQL type for "
            f"{dialect or 'the warehouse'} ({error}); correct the source definition"
        )
    return None


def nullable(value: object) -> str | None:
    """Normalize the warehouse's nullability without asserting an unavailable value."""
    return {"YES": "Y", "TRUE": "Y", "NO": "N", "FALSE": "N"}.get(str(value).upper())


def warehouse_kind(conn: Connection, config: ConnectorConfig, table: str) -> str | None:
    """Classify the warehouse's relation kind through its existing metadata query."""
    query, params = warehouse_dialect(config).relation_type_query(
        qualify(table, active_catalog(config))
    )
    value = conn.execute(text(query), params).scalar_one_or_none()
    name = str(value).upper().replace("_", " ")
    if name.endswith("VIEW"):
        return "VIEW"
    if name.endswith("TABLE") or name in ("MANAGED", "EXTERNAL"):
        return "TABLE"
    return None


def warehouse_columns(conn: Connection, config: ConnectorConfig, table: str) -> list[SourceColumn]:
    """Read an existing table's columns in order, including full declared SQL types."""
    if not is_safe_object_ref(table):
        raise MetadataError(
            f"TABLE_NAME={table!r}: expected schema.table or database.schema.table; correct "
            f"the source definition"
        )
    dialect = warehouse_dialect(config)
    qualified = qualify(table, active_catalog(config))
    query, params = dialect.column_metadata_query(
        conn, qualified, "column_name, ordinal_position, is_nullable"
    )
    rows = conn.execute(text(query), params).all()
    if not rows:
        return []
    types = dialect.full_column_types(conn, qualified)
    kind = warehouse_kind(conn, config, table)
    return [
        SourceColumn(
            table.lower(),
            str(row[0]).lower(),
            types[str(row[0]).lower()],
            position,
            nullable(row[2]),
            kind,
        )
        for position, row in enumerate(rows, 1)
    ]


def _type(value: str, dialect: str | None) -> exp.DataType:
    kind = exp.DataType.build(value, dialect=dialect)
    if dialect == "snowflake":
        if kind.this in (
            exp.DataType.Type.INT,
            exp.DataType.Type.BIGINT,
            exp.DataType.Type.SMALLINT,
            exp.DataType.Type.TINYINT,
        ):
            return exp.DataType.build("NUMBER(38,0)", dialect=dialect)
        if kind.this == exp.DataType.Type.DECIMAL and not kind.expressions:
            return exp.DataType.build("NUMBER(38,0)", dialect=dialect)
    if kind.this == exp.DataType.Type.DECIMAL and len(kind.expressions) == 1:
        kind.append("expressions", exp.DataTypeParam(this=exp.Literal.number(0)))
    return exp.DataType.build(kind.sql(dialect=dialect), dialect=dialect)


def _same_type(declared: str, deployed: str, dialect: str | None) -> bool:
    before, after = _type(declared, dialect), _type(deployed, dialect)
    if (
        dialect == "snowflake"
        and before.this == exp.DataType.Type.TIMESTAMP
        and after.this
        in (
            exp.DataType.Type.TIMESTAMPNTZ,
            exp.DataType.Type.TIMESTAMPTZ,
            exp.DataType.Type.TIMESTAMPLTZ,
        )
    ):
        before.set("this", after.this)
    if (
        before.this == after.this
        and not before.expressions
        and before.this
        in (
            exp.DataType.Type.VARCHAR,
            exp.DataType.Type.TEXT,
            exp.DataType.Type.CHAR,
            exp.DataType.Type.TIMESTAMP,
            exp.DataType.Type.TIMESTAMPNTZ,
            exp.DataType.Type.TIMESTAMPTZ,
            exp.DataType.Type.TIMESTAMPLTZ,
        )
    ):
        after.set("expressions", [])
    return before == after


def drift(
    definitions: Iterable[SourceColumn], actual: Iterable[SourceColumn], dialect: str | None
) -> list[str]:
    """Name every added, missing or differently typed column; missing tables are ignored."""
    expected = {column.column_name.lower(): column for column in definitions}
    found = {column.column_name.lower(): column for column in actual}
    if not found:
        return []
    messages = [
        f"warehouse adds column {name} ({found[name].data_type}); add it to columns.csv"
        for name in sorted(found.keys() - expected.keys())
    ]
    messages += [
        f"warehouse lacks column {name} ({expected[name].data_type}); restore it or update "
        f"columns.csv"
        for name in sorted(expected.keys() - found.keys())
    ]
    for name in sorted(expected.keys() & found.keys()):
        declared, deployed = expected[name].is_nullable, found[name].is_nullable
        if declared is not None and declared != deployed:
            messages.append(
                f"column {name} has warehouse nullability {deployed or 'unavailable'}, "
                f"declared {declared}; restore its constraint or update columns.csv"
            )
        before, after = expected[name].data_type, found[name].data_type
        if not _same_type(before, after, dialect):
            messages.append(
                f"column {name} has warehouse type {after}, defined as {before}; restore its "
                f"type or update columns.csv"
            )
    for column in expected.values():
        actual_column = next(iter(found.values()))
        if column.object_kind is not None and column.object_kind != actual_column.object_kind:
            messages.append(
                f"warehouse object kind {actual_column.object_kind or 'unavailable'}, "
                f"declared {column.object_kind}; restore the object or update tables.csv"
            )
            break
    return messages


def seed_sources(engine: Engine, config: ConnectorConfig) -> list[SourceColumn]:
    """Seed external reads and script targets that exist; keep metadata for missing tables."""
    from etl_craft.services.lineage import referenced_tables, table_name

    if config.warehouse is None:
        with engine.connect() as conn:
            return fetch_source_columns(conn)
    dialect = warehouse_dialect(config).sqlglot_dialect
    catalog = active_catalog(config)
    sources: set[str] = set()
    managed: set[str] = set()
    with engine.connect() as conn:
        defined = fetch_source_columns(conn)
        for task in fetch_active_tasks(conn):
            params = fetch_task_parameters(conn, task.task_id)
            target = (params.get("TARGET_OBJECT") or "").strip()
            if task.handler == Handler.SQL:
                if (params.get("SQL_ACTION") or "").upper() == SqlAction.DROP_TABLE:
                    continue
                if target and (params.get("SQL_ACTION") or "").upper() != SqlAction.DELETE_ROWS:
                    managed.add(table_name(target.split("."), catalog))
                select, _ = resolve_select(
                    config, params, pipeline_run_id=0, refresh_type=RefreshType.FULL
                )
                sources.update(referenced_tables(select, dialect=dialect, catalog=catalog))
            elif task.handler == Handler.PYTHON and target:
                sources.add(table_name(target.split("."), catalog))
    sources.update(table_name(column.table_name.split("."), catalog) for column in defined)
    seeded = list(defined)
    tables = sources - managed
    if not tables:
        return seeded
    with open_warehouse(config, engine) as warehouse, warehouse.connect() as conn:
        for table in sorted(tables):
            columns = warehouse_columns(conn, config, table)
            if columns:
                seeded = [
                    column
                    for column in seeded
                    if table_name(column.table_name.split("."), catalog) != table
                ]
                seeded.extend(columns)
    return sorted(seeded, key=lambda column: (column.table_name, column.column_position))
