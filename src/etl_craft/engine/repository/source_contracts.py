"""Lossless conversion of legacy source definitions into scoped object contracts."""

import re
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection
from sqlglot import exp
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

from etl_craft.core.actor import current_actor
from etl_craft.core.enums import YesNo
from etl_craft.core.errors import MigrationError
from etl_craft.core.text import is_safe_identifier, is_safe_object_ref
from etl_craft.engine.repository.normalization import ObjectIdentity, object_identity

LEGACY_COLUMNS = (
    "SOURCE_COLUMN_ID",
    "TABLE_NAME",
    "COLUMN_NAME",
    "DATA_TYPE",
    "COLUMN_POSITION",
    "ACTIVE_FLAG",
    "CREATED_BY",
    "CREATE_DATE",
    "UPDATED_BY",
    "UPDATED_DATE",
)


def legacy_sources(
    conn: Connection, catalog: str | None, dialect: str | None
) -> list[dict[str, Any]]:
    """Check all legacy rows before any upgrade changes their schema or ledger."""
    inspector = inspect(conn)
    name = "cfg_source_columns" if conn.dialect.name == "postgresql" else "CFG_SOURCE_COLUMNS"
    if not inspector.has_table(name):
        return []
    columns = {column["name"].upper() for column in inspector.get_columns(name)}
    if columns - set(LEGACY_COLUMNS):
        raise MigrationError(
            f"CFG_SOURCE_COLUMNS has custom columns {sorted(columns - set(LEGACY_COLUMNS))}; "
            "move their data into an explicit project contract before conversion"
        )
    if conn.dialect.name == "sqlite":
        indexes = [
            row.name
            for row in conn.exec_driver_sql("PRAGMA index_list(CFG_SOURCE_COLUMNS)")
            if row.origin != "pk"
        ]
    else:
        indexes = [index["name"] for index in inspector.get_indexes(name)]
    custom_indexes = [
        index
        for index in indexes
        if index not in ("ux_source_column_active", "ux_source_position_active")
    ]
    dependents = [
        view
        for view in inspector.get_view_names()
        if view.upper() != "AUD_METADATA_CHANGES"
        and re.search(
            r"\bCFG_SOURCE_COLUMNS\b", inspector.get_view_definition(view) or "", re.IGNORECASE
        )
    ]
    for table in inspector.get_table_names():
        for fk in inspector.get_foreign_keys(table):
            if str(fk["referred_table"]).upper() == "CFG_SOURCE_COLUMNS":
                dependents.append(f"{table}.{fk['name'] or 'foreign key'}")
    if conn.dialect.name == "sqlite":
        triggers = conn.execute(
            text(
                "SELECT name AS name FROM sqlite_schema WHERE type='trigger' "
                "AND upper(tbl_name)='CFG_SOURCE_COLUMNS'"
            )
        ).scalars()
    else:
        triggers = conn.execute(
            text(
                "SELECT tgname AS name FROM pg_trigger WHERE tgrelid='cfg_source_c"
                "olumns'::regclass "
                "AND NOT tgisinternal"
            )
        ).scalars()
    custom_triggers = [
        trigger
        for trigger in triggers
        if not trigger.startswith(
            (
                "trg_actor_guard_cfg_source_columns",
                "trg_truncate_cfg_source_columns",
                "trg_audit_cfg_source_columns",
                "trg_changes_cfg_source_columns",
            )
        )
    ]
    if custom_indexes or custom_triggers or dependents:
        raise MigrationError(
            f"CFG_SOURCE_COLUMNS has project schema objects: indexes {custom_indexes}, "
            f"triggers {custom_triggers}, dependents {dependents}; move their definitions "
            "to the normalized contract with the previous package before conversion"
        )
    rows = [
        dict(row)
        for row in conn.execute(
            text(
                "SELECT "
                + ", ".join(f"{name} AS {name.lower()}" for name in LEGACY_COLUMNS)
                + " FROM CFG_SOURCE_COLUMNS ORDER BY SOURCE_COLUMN_ID"
            )
        ).mappings()
    ]
    keys: dict[tuple[object, ...], list[int]] = defaultdict(list)
    positions: dict[tuple[object, ...], list[int]] = defaultdict(list)
    problems = []
    for row in rows:
        if not is_safe_object_ref(row["table_name"]) or not is_safe_identifier(row["column_name"]):
            problems.append(
                f"SOURCE_COLUMN_ID={row['source_column_id']}: legacy source names "
                f"{row['table_name']!r}, {row['column_name']!r} require unquoted "
                "SQL identifiers supported by the source consumers"
            )
            continue
        try:
            identity = object_identity(row["table_name"], catalog, dialect)
        except ValueError as error:
            problems.append(f"SOURCE_COLUMN_ID={row['source_column_id']}: {error}")
            continue
        column = normalize_identifiers(exp.to_identifier(row["column_name"]), dialect=dialect).name
        row["identity"], row["resolved_column"] = identity, column
        keys[(*identity, column)].append(row["source_column_id"])
        if row["active_flag"] == YesNo.YES:
            positions[(*identity, row["column_position"])].append(row["source_column_id"])
    for kind, groups in (("identity", keys), ("active column position", positions)):
        for key, ids in groups.items():
            if len(ids) > 1:
                problems.append(f"{kind} {key!r}: SOURCE_COLUMN_IDs {ids}")
    if problems:
        raise MigrationError(
            "source contract conversion cannot run: "
            + "; ".join(problems)
            + "; repair these rows with the previous installed package before upgrading; "
            "reactivate existing identities rather than insert inactive duplicates"
        )
    return rows


def convert_sources(conn: Connection, rows: list[dict[str, Any]]) -> None:
    """Preserve source IDs, stamps and immutable evidence in the migration transaction."""
    high_water = int(
        conn.execute(
            text(
                "SELECT COALESCE(MAX(source_id),0) AS high_water FROM ("
                "SELECT MAX(SOURCE_COLUMN_ID) AS source_id FROM CFG_SOURCE_COLUMNS UNION ALL "
                "SELECT MAX(SOURCE_COLUMN_ID) AS source_id FROM AUD_CFG_SOURCE_COLUMNS_HISTORY) ids"
            )
        ).scalar_one()
    )
    if conn.dialect.name == "sqlite":
        high_water = max(
            high_water,
            int(
                conn.execute(
                    text(
                        "SELECT COALESCE(MAX(seq),0) AS high_water FROM sqlite_sequence "
                        "WHERE upper(name)='CFG_SOURCE_COLUMNS'"
                    )
                ).scalar_one()
            ),
        )
    else:
        sequence = conn.execute(
            text(
                "SELECT pg_get_serial_sequence('cfg_source_columns','source_column_id') AS sequence"
            )
        ).scalar_one()
        last, called = conn.execute(
            text(f"SELECT last_value AS last_value, is_called AS is_called FROM {sequence}")
        ).one()
        high_water = max(high_water, int(last) if called else int(last) - 1)
    conn.exec_driver_sql("DROP VIEW IF EXISTS AUD_METADATA_CHANGES")
    conn.execute(
        text(
            "INSERT INTO AUD_SOURCE_COLUMNS_BASELINE ("
            + ", ".join(LEGACY_COLUMNS)
            + ") SELECT "
            + ", ".join(LEGACY_COLUMNS)
            + " FROM CFG_SOURCE_COLUMNS"
        )
    )
    grouped: dict[ObjectIdentity, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row["identity"]].append(row)
    now = datetime.now(UTC)
    actor = current_actor()
    table_ids = {}
    for identity, columns in grouped.items():
        table_ids[identity] = conn.execute(
            text(
                "INSERT INTO CFG_TABLES (CATALOG_NAME, SCHEMA_NAME, TABLE_NAME, ACTIVE_FLAG, "
                "CREATED_BY, CREATE_DATE, UPDATED_BY, UPDATED_DATE) "
                "VALUES (:catalog, :schema, :table, :active, :actor, :now, :actor, :now) "
                "RETURNING TABLE_ID AS table_id"
            ),
            dict(zip(("catalog", "schema", "table"), identity, strict=True))
            | {
                "active": YesNo.of(any(r["active_flag"] == YesNo.YES for r in columns)),
                "actor": actor.name,
                "now": now,
            },
        ).scalar_one()
    override = " OVERRIDING SYSTEM VALUE" if conn.dialect.name == "postgresql" else ""
    for row in rows:
        conn.execute(
            text(
                "INSERT INTO CFG_COLUMNS (COLUMN_ID, TABLE_ID, COLUMN_NAME, DATA_TYPE, "
                "COLUMN_POSITION, ACTIVE_FLAG, CREATED_BY, CREATE_DATE, UPDATED_BY, UPDATED_DATE)"
                + override
                + " VALUES (:source_column_id, :table_id, :resolved_column, :data_type, "
                ":column_position, :active_flag, :created_by, :create_date, "
                ":updated_by, :updated_date)"
            ),
            row | {"table_id": table_ids[row["identity"]]},
        )
    if conn.dialect.name == "postgresql":
        conn.execute(
            text(
                "SELECT setval(pg_get_serial_sequence('cfg_columns','column_id'), :value, :called)"
            ),
            {"value": max(1, high_water), "called": high_water > 0},
        )
    elif high_water:
        changed = conn.execute(
            text("UPDATE sqlite_sequence SET seq=:value WHERE name='CFG_COLUMNS'"),
            {"value": high_water},
        ).rowcount
        if not changed:
            conn.execute(
                text("INSERT INTO sqlite_sequence (name,seq) VALUES ('CFG_COLUMNS',:value)"),
                {"value": high_water},
            )
    history_names = [
        name
        for name in inspect(conn).get_table_names()
        if name.upper().startswith("AUD_CFG_") and name.upper().endswith("_HISTORY")
    ]
    maximum = (
        "(SELECT COALESCE(MAX(m),0) FROM ("
        + " UNION ALL ".join(f"SELECT MAX(CHANGE_ID) AS m FROM {name}" for name in history_names)
        + ") changes)"
    )
    for table in ("CFG_TABLES", "CFG_COLUMNS"):
        name = table.lower() if conn.dialect.name == "postgresql" else table
        names = [column["name"] for column in inspect(conn).get_columns(name)]
        listed = ", ".join(names)
        # The baseline begins at conversion; older exports use the untouched legacy evidence.
        conn.execute(
            text(
                f"INSERT INTO AUD_{table}_HISTORY (CHANGE_ID, VALID_FROM, VALID_TO, CURRENT_FLAG, "
                f"OPERATION, ACTOR, ACTOR_KIND, MIGRATION, {listed}) SELECT "
                f"{maximum} "
                f"+ ROW_NUMBER() OVER (ORDER BY {names[0]}) "
                + ", :now, :end, 'Y', 'INSERT', :actor, :kind, "
                "'engine/0028_source_contracts.sql', "
                f"{listed} FROM {table}"
            ),
            {
                "now": now,
                "end": datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC),
                "actor": actor.name,
                "kind": actor.kind.value,
            },
        )
    if conn.dialect.name == "postgresql":
        conn.execute(text(f"SELECT setval('etl_craft_change_id', {maximum} + 1, false)"))
    conn.exec_driver_sql("DROP TABLE CFG_SOURCE_COLUMNS")


def resolve_source_identity(
    catalog_name: str | None,
    schema_name: str,
    table_name: str,
    catalog: str | None,
    dialect: str | None,
) -> ObjectIdentity:
    """Resolve the unquoted identifier components supported by source consumers."""
    parts = [part for part in (catalog_name, schema_name, table_name) if part is not None]
    if not all(is_safe_identifier(part) for part in parts):
        raise ValueError(f"source components {parts!r}: expected separate unquoted SQL identifiers")
    return object_identity(".".join(parts), catalog, dialect)


def resolve_project_sources(
    conn: Connection, catalog: str | None, dialect: str | None
) -> list[tuple[ObjectIdentity, ObjectIdentity]]:
    """Resolve current source identities once before committing a project migration."""
    name = "cfg_tables" if conn.dialect.name == "postgresql" else "CFG_TABLES"
    if not inspect(conn).has_table(name):
        return []
    plans = []
    changes = []
    keys: dict[ObjectIdentity, list[int]] = defaultdict(list)
    for row in conn.execute(
        text(
            "SELECT TABLE_ID AS table_id, CATALOG_NAME AS catalog_name, "
            "SCHEMA_NAME AS schema_name, "
            "TABLE_NAME AS table_name FROM CFG_TABLES"
        )
    ):
        try:
            identity = resolve_source_identity(
                row.catalog_name, row.schema_name, row.table_name, catalog, dialect
            )
        except ValueError as error:
            raise MigrationError(f"CFG_TABLES TABLE_ID={row.table_id}: {error}") from error
        keys[identity].append(row.table_id)
        before = (row.catalog_name, row.schema_name, row.table_name)
        if identity != before:
            changes.append((before, identity))
            plans.append(
                dict(zip(("catalog", "schema", "table"), identity, strict=True))
                | {"id": row.table_id}
            )
    duplicates = {key: ids for key, ids in keys.items() if len(ids) > 1}
    if duplicates:
        raise MigrationError(
            f"CFG_TABLES has ambiguous resolved identities {duplicates}; "
            "use one existing parent per warehouse object"
        )
    for plan in plans:
        conn.execute(
            text(
                "UPDATE CFG_TABLES SET CATALOG_NAME=:catalog, SCHEMA_NAME=:schema, "
                "TABLE_NAME=:table "
                "WHERE TABLE_ID=:id"
            ),
            plan,
        )
    return changes
