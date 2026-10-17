"""Convert rule and task targets to scoped table references without rewriting earlier evidence.

The rule and task conversions share one path; a ``TargetContract`` holds what differs.
"""

import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from sqlalchemy import inspect
from sqlalchemy.engine import Connection

from etl_craft.core.actor import current_actor
from etl_craft.core.errors import MigrationError
from etl_craft.core.text import is_safe_object_ref
from etl_craft.dialects.engine import for_engine
from etl_craft.engine.queries import statement
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
    triggers_kept: tuple[str, ...]
    """Prefixes of the triggers etl-craft itself creates, which a conversion recreates."""
    target_column: str | None
    """A column whose views and indexes count as dependents, or ``None``."""
    dependents_message: str
    rows_query: str
    check_row: Callable[[dict[str, Any], set[Any]], None]
    value_key: str
    update_query: str
    update_params: Callable[[dict[str, Any], int], dict[str, Any]]
    after_query: str | None = None


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
    triggers_kept=(
        "trg_actor_guard_cfg_business_rules",
        "trg_truncate_cfg_business_rules",
        "trg_audit_cfg_business_rules",
        "trg_changes_cfg_business_rules",
    ),
    target_column="TARGET_TABLE",
    dependents_message="CFG_BUSINESS_RULES has custom columns {extra} or target-dependent "
    "schema objects {dependents}",
    rows_query="target_contract_rule_rows",
    check_row=_check_rule_row,
    value_key="target_table",
    update_query="target_contract_update_rule",
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
    triggers_kept=(
        "trg_actor_guard_cfg_",
        "trg_truncate_cfg_",
        "trg_audit_cfg_",
        "trg_changes_cfg_",
    ),
    target_column=None,
    dependents_message="CFG_TASK_PARAMETERS has custom columns {extra} or task/parameter "
    "triggers {dependents}",
    rows_query="target_contract_task_rows",
    check_row=_check_task_row,
    value_key="value",
    update_query="target_contract_update_task",
    update_params=lambda row, table_id: {
        "target": table_id,
        "active": row["active"],
        "task": row["task_id"],
    },
    after_query="target_contract_delete_task_targets",
)


def legacy_targets(
    conn: Connection, contract: TargetContract, catalog: str | None, dialect: str | None
) -> Rows:
    """Refuse unsupported legacy targets before any pending upgrade runs."""
    inspector, stored = inspect(conn), for_engine(conn).stored_name
    marker_table, marker_column = contract.legacy_column
    present = marker_column in {
        column["name"].upper() for column in inspector.get_columns(stored(marker_table))
    }
    if present != contract.legacy_when_present:
        return []
    columns = {
        column["name"].upper() for column in inspector.get_columns(stored(contract.expected_table))
    }
    extra = columns - contract.expected
    dependents = _dependents(conn, contract)
    if extra or dependents:
        found = contract.dependents_message.format(extra=sorted(extra), dependents=dependents)
        raise MigrationError(
            f"{found}; move their data/definitions to the scoped contract with the previous "
            "package before upgrading"
        )
    rows = [dict(row) for row in conn.execute(statement(conn, contract.rows_query)).mappings()]
    seen: set[Any] = set()
    for row in rows:
        contract.check_row(row, seen)
        row["identity"] = object_identity(row[contract.value_key], catalog, dialect)
    return rows


def _dependents(conn: Connection, contract: TargetContract) -> list[str]:
    """Return the views, indexes and triggers a conversion would break."""
    dialect = for_engine(conn)
    found = _target_views(conn, contract)
    if contract.target_column is not None:
        found += [
            name
            for name, definition in dialect.table_indexes(conn, contract.table)
            if re.search(rf"\b{contract.target_column}\b", definition, re.I)
        ]
    return found + [
        trigger
        for table in contract.trigger_tables
        for trigger in dialect.table_triggers(conn, table)
        if not trigger.startswith(contract.triggers_kept)
    ]


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
        conn.execute(statement(conn, contract.update_query), contract.update_params(row, table_id))
    record_conversion(conn, contract.table, contract.primary, contract.migration)
    if contract.after_query is not None:
        conn.execute(statement(conn, contract.after_query))


def _table_id(conn: Connection, identity: tuple[str | None, str, str]) -> int:
    """Return the ``CFG_TABLES`` id of ``identity``, adding the table when it is missing."""
    params = dict(zip(("catalog", "schema", "table"), identity, strict=True))
    table_id = conn.execute(
        statement(conn, "target_contract_select_cfg_tables"),
        params,
    ).scalar_one_or_none()
    if table_id is None:
        table_id = conn.execute(
            statement(conn, "target_contract_insert_cfg_tables"),
            params,
        ).scalar_one()
    return int(table_id)


def suspend_config_capture(conn: Connection, table: str) -> None:
    """Suspend stamps and version capture while a contract changes representation."""
    dialect = for_engine(conn)
    for trigger in dialect.table_triggers(conn, table):
        if trigger.startswith((f"trg_audit_{table.lower()}", f"trg_changes_{table.lower()}")):
            dialect.drop_trigger(conn, trigger, table)


def record_conversion(conn: Connection, table: str, primary: str, migration: str) -> None:
    """Close current versions and record unchanged stamps with the new representation."""
    for_engine(conn).record_config_conversion(
        conn, table, primary, migration=migration, actor=current_actor()
    )
