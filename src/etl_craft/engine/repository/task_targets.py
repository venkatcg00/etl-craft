"""Move task targets into scoped references while retaining their parameter history."""

from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection

from etl_craft.core.errors import MigrationError
from etl_craft.core.text import is_safe_object_ref
from etl_craft.engine.repository.normalization import object_identity
from etl_craft.engine.repository.rule_targets import record_conversion, suspend_config_capture
from etl_craft.engine.repository.source_contracts import resolve_project_sources

MIGRATION = "engine/0030_task_target_contracts.sql"


def legacy_task_targets(
    conn: Connection, catalog: str | None, dialect: str | None
) -> list[dict[str, Any]]:
    """Refuse ambiguous targets or custom rebuild objects before any upgrade runs."""
    inspector = inspect(conn)
    task_name = "cfg_tasks" if conn.dialect.name == "postgresql" else "CFG_TASKS"
    if "TARGET_TABLE_ID" in {column["name"].upper() for column in inspector.get_columns(task_name)}:
        return []
    name = "cfg_task_parameters" if conn.dialect.name == "postgresql" else "CFG_TASK_PARAMETERS"
    expected = {
        "TASK_PARAMETER_ID",
        "TASK_ID",
        "PARAMETER_NAME",
        "PARAMETER_VALUE",
        "ACTIVE_FLAG",
        "CREATED_BY",
        "CREATE_DATE",
        "UPDATED_BY",
        "UPDATED_DATE",
    }
    extra = {column["name"].upper() for column in inspector.get_columns(name)} - expected
    dependents = []
    if conn.dialect.name == "sqlite":
        dependents = [
            row.name
            for row in conn.execute(
                text(
                    "SELECT name AS name FROM sqlite_schema WHERE type='trigger' "
                    "AND upper(tbl_name) IN ('CFG_TASKS','CFG_TASK_PARAMETERS')"
                )
            )
            if not row.name.startswith(("trg_audit_cfg_", "trg_actor_guard_cfg_"))
        ]
    else:
        dependents = [
            name
            for name in conn.execute(
                text(
                    "SELECT tgname AS name FROM pg_trigger WHERE NOT tgisinternal "
                    "AND tgrelid IN ('cfg_tasks'::regclass,'cfg_task_parameters'::regclass)"
                )
            ).scalars()
            if not name.startswith(
                ("trg_actor_guard_cfg_", "trg_truncate_cfg_", "trg_audit_cfg_", "trg_changes_cfg_")
            )
        ]
    if extra or dependents:
        raise MigrationError(
            f"CFG_TASK_PARAMETERS has custom columns {sorted(extra)} or task/parameter triggers "
            f"{dependents}; move their data/definitions to the scoped contract with the previous "
            "package before upgrading"
        )
    rows = [
        dict(row)
        for row in conn.execute(
            text(
                "SELECT TASK_PARAMETER_ID AS parameter_id,TASK_ID AS "
                "task_id,PARAMETER_NAME AS name,"
                "PARAMETER_VALUE AS value,ACTIVE_FLAG AS active FROM CFG_TASK_PARAMETERS "
                "WHERE upper(trim(PARAMETER_NAME))='TARGET_OBJECT' ORDER BY TASK_PARAMETER_ID"
            )
        ).mappings()
    ]
    seen = set()
    for row in rows:
        if (
            row["name"] != "TARGET_OBJECT"
            or row["task_id"] in seen
            or row["value"] is None
            or not is_safe_object_ref(row["value"])
        ):
            raise MigrationError(
                f"CFG_TASK_PARAMETERS TASK_PARAMETER_ID={row['parameter_id']} "
                f"{row['name']!r}={row['value']!r}: expected exactly one TARGET_OBJECT per task, "
                "named exactly TARGET_OBJECT, with schema.table or catalog.schema.table using "
                "unquoted SQL identifiers; correct it using the previous package before upgrading"
            )
        seen.add(row["task_id"])
        row["identity"] = object_identity(row["value"], catalog, dialect)
    return rows


def convert_task_targets(
    conn: Connection, rows: list[dict[str, Any]], catalog: str | None, dialect: str | None
) -> None:
    """Keep task stamps and parameter deletion evidence in the same DDL transaction."""
    resolve_project_sources(conn, catalog, dialect)
    suspend_config_capture(conn, "CFG_TASKS")
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
            text(
                "UPDATE CFG_TASKS SET TARGET_TABLE_ID=:target,TARGET_ACTIVE_FLAG=:active "
                "WHERE TASK_ID=:task"
            ),
            {"target": table_id, "active": row["active"], "task": row["task_id"]},
        )
    record_conversion(conn, "CFG_TASKS", "TASK_ID", MIGRATION)
    conn.execute(text("DELETE FROM CFG_TASK_PARAMETERS WHERE PARAMETER_NAME='TARGET_OBJECT'"))
