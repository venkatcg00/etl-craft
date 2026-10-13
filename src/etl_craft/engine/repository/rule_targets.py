"""Convert rule targets to scoped table references without rewriting earlier evidence."""

import re
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection

from etl_craft.core.actor import current_actor
from etl_craft.core.errors import MigrationError
from etl_craft.core.text import is_safe_object_ref
from etl_craft.engine.repository.normalization import object_identity
from etl_craft.engine.repository.source_contracts import resolve_project_sources


def legacy_rule_targets(
    conn: Connection, catalog: str | None, dialect: str | None
) -> list[dict[str, Any]]:
    """Refuse unsupported legacy targets before any pending upgrade runs."""
    name = "cfg_business_rules" if conn.dialect.name == "postgresql" else "CFG_BUSINESS_RULES"
    inspector = inspect(conn)
    columns = {column["name"].upper() for column in inspector.get_columns(name)}
    if "TARGET_TABLE" not in columns:
        return []
    expected = {
        "BUSINESS_RULE_ID",
        "BUSINESS_RULE_NAME",
        "PIPELINE_ID",
        "TASK_ID",
        "BUSINESS_RULE_SQL",
        "BUSINESS_RULE_TYPE",
        "BUSINESS_RULE_KEY_COLUMN",
        "TARGET_TABLE",
        "SEQUENCE_NUMBER",
        "ACTIVE_FLAG",
        "CREATED_BY",
        "CREATE_DATE",
        "UPDATED_BY",
        "UPDATED_DATE",
    }
    extra = columns - expected
    dependents = [
        view
        for view in inspector.get_view_names()
        if view.upper() != "AUD_METADATA_CHANGES"
        and re.search(r"\bCFG_BUSINESS_RULES\b", inspector.get_view_definition(view) or "", re.I)
        and re.search(r"\bTARGET_TABLE\b", inspector.get_view_definition(view) or "", re.I)
    ]
    if conn.dialect.name == "sqlite":
        objects = conn.execute(
            text(
                "SELECT name AS name, type AS kind, sql AS sql FROM sqlite_schema "
                "WHERE upper(tbl_name)='CFG_BUSINESS_RULES' AND type IN ('trigger','index')"
            )
        ).all()
        dependents += [
            row.name
            for row in objects
            if row.sql
            and (
                (
                    row.kind == "trigger"
                    and not row.name.startswith(
                        ("trg_audit_cfg_business_rules", "trg_actor_guard_cfg_business_rules")
                    )
                )
                or (re.search(r"\bTARGET_TABLE\b", row.sql, re.I) and row.kind == "index")
            )
        ]
    else:
        dependents += [
            row.name
            for row in conn.execute(
                text(
                    "SELECT indexname AS name,indexdef AS definition FROM pg_indexes "
                    "WHERE schemaname=current_schema() AND tablename='cfg_business_rules'"
                )
            )
            if re.search(r"\btarget_table\b", row.definition, re.I)
        ]
        triggers = conn.execute(
            text(
                "SELECT tgname AS name FROM pg_trigger "
                "WHERE tgrelid='cfg_business_rules'::regclass AND NOT tgisinternal"
            )
        ).scalars()
        dependents += [
            trigger
            for trigger in triggers
            if not trigger.startswith(
                (
                    "trg_actor_guard_cfg_business_rules",
                    "trg_truncate_cfg_business_rules",
                    "trg_audit_cfg_business_rules",
                    "trg_changes_cfg_business_rules",
                )
            )
        ]
    if extra or dependents:
        raise MigrationError(
            f"CFG_BUSINESS_RULES has custom columns {sorted(extra)} or target-dependent "
            f"schema objects {dependents}; move their data/definitions to the scoped contract "
            "with the previous package before upgrading"
        )
    rows = [
        dict(row)
        for row in conn.execute(
            text(
                "SELECT BUSINESS_RULE_ID AS business_rule_id, TARGET_TABLE AS target_table "
                "FROM CFG_BUSINESS_RULES ORDER BY BUSINESS_RULE_ID"
            )
        ).mappings()
    ]
    for row in rows:
        value = row["target_table"]
        if not is_safe_object_ref(value):
            raise MigrationError(
                f"CFG_BUSINESS_RULES BUSINESS_RULE_ID={row['business_rule_id']} "
                f"TARGET_TABLE={value!r}: expected schema.table or catalog.schema.table "
                "with unquoted SQL identifiers; correct it using the previous package "
                "before upgrading"
            )
        row["identity"] = object_identity(value, catalog, dialect)
    return rows


def convert_rule_targets(
    conn: Connection, rows: list[dict[str, Any]], catalog: str | None, dialect: str | None
) -> None:
    """Keep rule IDs/stamps and close each old version once, within the DDL transaction."""
    resolve_project_sources(conn, catalog, dialect)
    suspend_config_capture(conn, "CFG_BUSINESS_RULES")
    for row in rows:
        params = dict(zip(("catalog", "schema", "table"), row["identity"], strict=True))
        table_id = conn.execute(
            text(
                "SELECT TABLE_ID AS table_id FROM CFG_TABLES WHERE SCHEMA_NAME=:schema "
                "AND TABLE_NAME=:table AND (CATALOG_NAME=:catalog OR "
                "(CATALOG_NAME IS NULL AND :catalog IS NULL))"
            ),
            params,
        ).scalar_one_or_none()
        if table_id is None:
            table_id = conn.execute(
                text(
                    "INSERT INTO CFG_TABLES (CATALOG_NAME,SCHEMA_NAME,TABLE_NAME) "
                    "VALUES (:catalog,:schema,:table) RETURNING TABLE_ID AS table_id"
                ),
                params,
            ).scalar_one()
        conn.execute(
            text("UPDATE CFG_BUSINESS_RULES SET TARGET_TABLE_ID=:table WHERE BUSINESS_RULE_ID=:id"),
            {"table": table_id, "id": row["business_rule_id"]},
        )
    record_conversion(
        conn, "CFG_BUSINESS_RULES", "BUSINESS_RULE_ID", "engine/0029_rule_target_contracts.sql"
    )


def suspend_config_capture(conn: Connection, table: str) -> None:
    """Suspend stamps and version capture while a contract changes representation."""
    quote = conn.dialect.identifier_preparer.quote
    name = table.lower() if conn.dialect.name == "postgresql" else table
    if conn.dialect.name == "sqlite":
        triggers = conn.execute(
            text(
                "SELECT name AS name FROM sqlite_schema WHERE type='trigger' AND "
                "upper(tbl_name)=upper(:name)"
            ),
            {"name": name},
        ).scalars()
    else:
        triggers = conn.execute(
            text(
                "SELECT tgname AS name FROM pg_trigger "
                "WHERE tgrelid=to_regclass(:name) "
                "AND NOT tgisinternal"
            ),
            {"name": name},
        ).scalars()
    for trigger in list(triggers):
        if trigger.startswith((f"trg_audit_{table.lower()}", f"trg_changes_{table.lower()}")):
            suffix = f" ON {name}" if conn.dialect.name == "postgresql" else ""
            conn.exec_driver_sql(f"DROP TRIGGER {quote(trigger)}{suffix}")


def record_conversion(conn: Connection, table: str, primary: str, migration: str) -> None:
    """Close current versions and record unchanged stamps with the new representation."""
    now = datetime.now(UTC)
    actor = current_actor()
    quote = conn.dialect.identifier_preparer.quote
    name = table.lower() if conn.dialect.name == "postgresql" else table
    conn.execute(
        text(
            f"UPDATE AUD_{table}_HISTORY SET VALID_TO=:now,CURRENT_FLAG='N' "
            f"WHERE CURRENT_FLAG='Y' AND {primary} IN "
            f"(SELECT {primary} FROM {table})"
        ),
        {"now": now},
    )
    history_names = [
        table
        for table in inspect(conn).get_table_names()
        if table.upper().startswith("AUD_CFG_") and table.upper().endswith("_HISTORY")
    ]
    maximum = (
        "(SELECT COALESCE(MAX(m),0) FROM ("
        + " UNION ALL ".join(f"SELECT MAX(CHANGE_ID) AS m FROM {table}" for table in history_names)
        + ") changes)"
    )
    names = [column["name"] for column in inspect(conn).get_columns(name)]
    listed = ", ".join(quote(column) for column in names)
    conn.execute(
        text(
            f"INSERT INTO AUD_{table}_HISTORY (CHANGE_ID,VALID_FROM,VALID_TO,"
            "CURRENT_FLAG,OPERATION,ACTOR,ACTOR_KIND,MIGRATION,"
            f"{listed}) SELECT {maximum}+ROW_NUMBER() OVER (ORDER BY {primary}),"
            ":now,:end,'Y','UPDATE',:actor,:kind,:migration,"
            f"{listed} FROM {table}"
        ),
        {
            "migration": migration,
            "now": now,
            "end": datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC),
            "actor": actor.name,
            "kind": actor.kind.value,
        },
    )
    if conn.dialect.name == "postgresql":
        conn.execute(text(f"SELECT setval('etl_craft_change_id', {maximum}+1, false)"))
    conn.exec_driver_sql("DROP VIEW IF EXISTS AUD_METADATA_CHANGES")
