"""Lossless conversion of legacy source definitions into scoped object contracts."""

import re
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import inspect
from sqlalchemy.engine import Connection
from sqlglot import exp
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

from etl_craft.core.actor import current_actor
from etl_craft.core.enums import YesNo
from etl_craft.core.errors import MigrationError
from etl_craft.core.text import is_safe_identifier, is_safe_object_ref
from etl_craft.dialects.engine import for_engine
from etl_craft.engine.queries import statement
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
    inspector, engine_dialect = inspect(conn), for_engine(conn)
    name = engine_dialect.stored_name("CFG_SOURCE_COLUMNS")
    if not inspector.has_table(name):
        return []
    columns = {column["name"].upper() for column in inspector.get_columns(name)}
    if columns - set(LEGACY_COLUMNS):
        raise MigrationError(
            f"CFG_SOURCE_COLUMNS has custom columns {sorted(columns - set(LEGACY_COLUMNS))}; "
            "move their data into an explicit project contract before conversion"
        )
    indexes = engine_dialect.index_names(conn, "CFG_SOURCE_COLUMNS")
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
    triggers = engine_dialect.table_triggers(conn, "CFG_SOURCE_COLUMNS")
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
        dict(row) for row in conn.execute(statement(conn, "source_contract_legacy_rows")).mappings()
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
        conn.execute(statement(conn, "source_contract_select_cfg_source_columns")).scalar_one()
    )
    engine_dialect = for_engine(conn)
    high_water = max(high_water, engine_dialect.legacy_source_id_high_water(conn))
    conn.execute(statement(conn, "drop_metadata_changes_view"))
    conn.execute(statement(conn, "source_contract_save_baseline"))
    grouped: dict[ObjectIdentity, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row["identity"]].append(row)
    now = datetime.now(UTC)
    actor = current_actor()
    table_ids = {}
    for identity, columns in grouped.items():
        table_ids[identity] = conn.execute(
            statement(conn, "source_contract_insert_cfg_tables"),
            dict(zip(("catalog", "schema", "table"), identity, strict=True))
            | {
                "active": YesNo.of(any(r["active_flag"] == YesNo.YES for r in columns)),
                "actor": actor.name,
                "now": now,
            },
        ).scalar_one()
    for row in rows:
        conn.execute(
            statement(conn, "source_contract_insert_column"),
            row | {"table_id": table_ids[row["identity"]]},
        )
    engine_dialect.set_column_id_high_water(conn, high_water)
    # The baseline begins at conversion; older exports use the untouched legacy evidence.
    engine_dialect.record_config_baseline(
        conn,
        ("CFG_TABLES", "CFG_COLUMNS"),
        migration="engine/0028_source_contracts.sql",
        actor=actor,
    )
    conn.execute(statement(conn, "source_contract_drop_legacy"))


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
    if not inspect(conn).has_table(for_engine(conn).stored_name("CFG_TABLES")):
        return []
    plans = []
    changes = []
    keys: dict[ObjectIdentity, list[int]] = defaultdict(list)
    query = (
        "source_contract_select_cfg_tables"
        if "project_id"
        in {name.lower() for name in for_engine(conn).column_names(conn, "CFG_TABLES")}
        else "source_contract_select_cfg_tables_legacy"
    )
    for row in conn.execute(statement(conn, query)):
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
            statement(conn, "source_contract_update_cfg_tables"),
            plan,
        )
    return changes
