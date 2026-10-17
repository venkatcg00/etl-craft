"""Convert rule and task targets to scoped table references without rewriting earlier evidence.

The rule and task conversions share one path; a ``TargetContract`` holds what differs.
"""

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection

from etl_craft.core.actor import current_actor
from etl_craft.core.errors import MigrationError
from etl_craft.core.text import is_safe_object_ref
from etl_craft.engine.repository.normalization import object_identity
from etl_craft.engine.repository.source_contracts import resolve_project_sources

Rows = list[dict[str, Any]]


@dataclass(frozen=True)
class TargetContract:
    """What one legacy target representation is checked, read and converted with."""

    table: str
    primary: str
    migration: str
    legacy_column: tuple[str, str]
    """The table and column whose presence (or, for tasks, absence) marks it unconverted."""
    legacy_when_present: bool
    expected_table: str
    expected: frozenset[str]
    trigger_tables: tuple[str, ...]
    sqlite_triggers_kept: tuple[str, ...]
    postgres_triggers_kept: tuple[str, ...]
    target_column: str | None
    """A column whose views and indexes count as dependents, or ``None``."""
    dependents_message: str
    rows_sql: str
    check_row: Callable[[dict[str, Any], set[Any]], None]
    value_key: str
    update_sql: str
    update_params: Callable[[dict[str, Any], int], dict[str, Any]]
    after_sql: str | None = None


def _check_rule_row(row: dict[str, Any], seen: set[Any]) -> None:
    value = row["target_table"]
    if not is_safe_object_ref(value):
        raise MigrationError(
            f"CFG_BUSINESS_RULES BUSINESS_RULE_ID={row['business_rule_id']} "
            f"TARGET_TABLE={value!r}: expected schema.table or catalog.schema.table "
            "with unquoted SQL identifiers; correct it using the previous package "
            "before upgrading"
        )


def _check_task_row(row: dict[str, Any], seen: set[Any]) -> None:
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


_STAMPS = frozenset({"ACTIVE_FLAG", "CREATED_BY", "CREATE_DATE", "UPDATED_BY", "UPDATED_DATE"})

RULE_TARGETS = TargetContract(
    table="CFG_BUSINESS_RULES",
    primary="BUSINESS_RULE_ID",
    migration="engine/0029_rule_target_contracts.sql",
    legacy_column=("CFG_BUSINESS_RULES", "TARGET_TABLE"),
    legacy_when_present=True,
    expected_table="CFG_BUSINESS_RULES",
    expected=_STAMPS
    | {
        "BUSINESS_RULE_ID",
        "BUSINESS_RULE_NAME",
        "PIPELINE_ID",
        "TASK_ID",
        "BUSINESS_RULE_SQL",
        "BUSINESS_RULE_TYPE",
        "BUSINESS_RULE_KEY_COLUMN",
        "TARGET_TABLE",
        "SEQUENCE_NUMBER",
    },
    trigger_tables=("CFG_BUSINESS_RULES",),
    sqlite_triggers_kept=("trg_audit_cfg_business_rules", "trg_actor_guard_cfg_business_rules"),
    postgres_triggers_kept=(
        "trg_actor_guard_cfg_business_rules",
        "trg_truncate_cfg_business_rules",
        "trg_audit_cfg_business_rules",
        "trg_changes_cfg_business_rules",
    ),
    target_column="TARGET_TABLE",
    dependents_message="CFG_BUSINESS_RULES has custom columns {extra} or target-dependent "
    "schema objects {dependents}",
    rows_sql="SELECT BUSINESS_RULE_ID AS business_rule_id, TARGET_TABLE AS target_table "
    "FROM CFG_BUSINESS_RULES ORDER BY BUSINESS_RULE_ID",
    check_row=_check_rule_row,
    value_key="target_table",
    update_sql="UPDATE CFG_BUSINESS_RULES SET TARGET_TABLE_ID=:table WHERE BUSINESS_RULE_ID=:id",
    update_params=lambda row, table_id: {"table": table_id, "id": row["business_rule_id"]},
)

TASK_TARGETS = TargetContract(
    table="CFG_TASKS",
    primary="TASK_ID",
    migration="engine/0030_task_target_contracts.sql",
    legacy_column=("CFG_TASKS", "TARGET_TABLE_ID"),
    legacy_when_present=False,
    expected_table="CFG_TASK_PARAMETERS",
    expected=_STAMPS | {"TASK_PARAMETER_ID", "TASK_ID", "PARAMETER_NAME", "PARAMETER_VALUE"},
    trigger_tables=("CFG_TASKS", "CFG_TASK_PARAMETERS"),
    sqlite_triggers_kept=("trg_audit_cfg_", "trg_actor_guard_cfg_"),
    postgres_triggers_kept=(
        "trg_actor_guard_cfg_",
        "trg_truncate_cfg_",
        "trg_audit_cfg_",
        "trg_changes_cfg_",
    ),
    target_column=None,
    dependents_message="CFG_TASK_PARAMETERS has custom columns {extra} or task/parameter "
    "triggers {dependents}",
    rows_sql="SELECT TASK_PARAMETER_ID AS parameter_id,TASK_ID AS "
    "task_id,PARAMETER_NAME AS name,"
    "PARAMETER_VALUE AS value,ACTIVE_FLAG AS active FROM CFG_TASK_PARAMETERS "
    "WHERE upper(trim(PARAMETER_NAME))='TARGET_OBJECT' ORDER BY TASK_PARAMETER_ID",
    check_row=_check_task_row,
    value_key="value",
    update_sql="UPDATE CFG_TASKS SET TARGET_TABLE_ID=:target,TARGET_ACTIVE_FLAG=:active "
    "WHERE TASK_ID=:task",
    update_params=lambda row, table_id: {
        "target": table_id,
        "active": row["active"],
        "task": row["task_id"],
    },
    after_sql="DELETE FROM CFG_TASK_PARAMETERS WHERE PARAMETER_NAME='TARGET_OBJECT'",
)


def _name(conn: Connection, table: str) -> str:
    return table.lower() if conn.dialect.name == "postgresql" else table


def legacy_targets(
    conn: Connection, contract: TargetContract, catalog: str | None, dialect: str | None
) -> Rows:
    """Refuse unsupported legacy targets before any pending upgrade runs."""
    inspector = inspect(conn)
    marker_table, marker_column = contract.legacy_column
    present = marker_column in {
        column["name"].upper() for column in inspector.get_columns(_name(conn, marker_table))
    }
    if present != contract.legacy_when_present:
        return []
    columns = {
        column["name"].upper()
        for column in inspector.get_columns(_name(conn, contract.expected_table))
    }
    extra = columns - contract.expected
    dependents = _dependents(conn, contract)
    if extra or dependents:
        found = contract.dependents_message.format(extra=sorted(extra), dependents=dependents)
        raise MigrationError(
            f"{found}; move their data/definitions to the scoped contract with the previous "
            "package before upgrading"
        )
    rows = [dict(row) for row in conn.execute(text(contract.rows_sql)).mappings()]
    seen: set[Any] = set()
    for row in rows:
        contract.check_row(row, seen)
        row["identity"] = object_identity(row[contract.value_key], catalog, dialect)
    return rows


def _dependents(conn: Connection, contract: TargetContract) -> list[str]:
    """Return the views, indexes and triggers a conversion would break."""
    if conn.dialect.name == "sqlite":
        return _target_views(conn, contract) + _sqlite_dependents(conn, contract)
    return _target_views(conn, contract) + _postgres_dependents(conn, contract)


def _target_views(conn: Connection, contract: TargetContract) -> list[str]:
    if contract.target_column is None:
        return []
    inspector = inspect(conn)
    return [
        view
        for view in inspector.get_view_names()
        if view.upper() != "AUD_METADATA_CHANGES"
        and re.search(rf"\b{contract.table}\b", inspector.get_view_definition(view) or "", re.I)
        and re.search(
            rf"\b{contract.target_column}\b", inspector.get_view_definition(view) or "", re.I
        )
    ]


def _sqlite_dependents(conn: Connection, contract: TargetContract) -> list[str]:
    kinds = "('trigger','index')" if contract.target_column else "('trigger')"
    tables = ",".join(f"'{table}'" for table in contract.trigger_tables)
    objects = conn.execute(
        text(
            "SELECT name AS name, type AS kind, sql AS sql FROM sqlite_schema "
            f"WHERE upper(tbl_name) IN ({tables}) AND type IN {kinds}"
        )
    ).all()
    found = []
    for row in objects:
        if row.kind == "trigger":
            if contract.target_column and not row.sql:
                continue
            if not row.name.startswith(contract.sqlite_triggers_kept):
                found.append(row.name)
        elif row.sql and re.search(rf"\b{contract.target_column}\b", row.sql, re.I):
            found.append(row.name)
    return found


def _postgres_dependents(conn: Connection, contract: TargetContract) -> list[str]:
    found = []
    if contract.target_column is not None:
        found += [
            row.name
            for row in conn.execute(
                text(
                    "SELECT indexname AS name,indexdef AS definition FROM pg_indexes "
                    "WHERE schemaname=current_schema() AND tablename=:table"
                ),
                {"table": contract.table.lower()},
            )
            if re.search(rf"\b{contract.target_column}\b", row.definition, re.I)
        ]
    relations = ",".join(f"'{table.lower()}'::regclass" for table in contract.trigger_tables)
    triggers = conn.execute(
        text(
            "SELECT tgname AS name FROM pg_trigger "
            f"WHERE NOT tgisinternal AND tgrelid IN ({relations})"
        )
    ).scalars()
    return found + [
        trigger for trigger in triggers if not trigger.startswith(contract.postgres_triggers_kept)
    ]


def convert_targets(
    conn: Connection,
    contract: TargetContract,
    rows: Rows,
    catalog: str | None,
    dialect: str | None,
) -> None:
    """Keep ids and stamps and close each old version once, within the DDL transaction."""
    resolve_project_sources(conn, catalog, dialect)
    suspend_config_capture(conn, contract.table)
    for row in rows:
        table_id = _table_id(conn, row["identity"])
        conn.execute(text(contract.update_sql), contract.update_params(row, table_id))
    record_conversion(conn, contract.table, contract.primary, contract.migration)
    if contract.after_sql is not None:
        conn.execute(text(contract.after_sql))


def _table_id(conn: Connection, identity: tuple[str | None, str, str]) -> int:
    """Return the ``CFG_TABLES`` id of ``identity``, adding the table when it is missing."""
    params = dict(zip(("catalog", "schema", "table"), identity, strict=True))
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
    return int(table_id)


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
