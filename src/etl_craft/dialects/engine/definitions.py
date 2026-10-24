"""SQL projections of admitted metadata; all other queries retain their live tables."""

import re

from etl_craft.core.definitions import Definition

# Only execution-definition readers opt in; permission and capacity queries stay live.
QUERIES = frozenset(
    {
        "pipeline_graph_tasks",
        "pipeline_steps",
        "pipeline_task_dependencies",
        "task_parameters",
        "task_codes",
        "task_execution_detail",
        "pipeline_detail",
        "pipeline_handlers",
        "pipeline_gate_edges",
        "task_gate_edges",
        "sql_task_state",
        "target_tasks",
        "task_run_context",
        "business_rules",
        "business_rules_for_task",
        "task_columns",
        "table_columns",
        "target_columns",
        "rule_definition",
        "transition_insert_task_run",
        "task_code_by_id",
        "task_id_by_code",
        "pipeline_id_by_code",
        "transition_queue_attempt",
        "task_rule_counts",
        "task_statuses_for_run",
        "failure_watch_messages",
    }
)


def projection(sql: str, definition: Definition, *, postgres: bool, name: str = "") -> str:
    """Shadow only referenced config tables with typed, immutable rows."""
    if name == "sql_task_state":
        start = sql.index("WITH origin AS (") + len("WITH origin AS (")
        end = sql.index("), writers AS (")
        # Target identity is admitted; other writers remain live protection evidence.
        return sql[:start] + projection(sql[start:end], definition, postgres=postgres) + sql[end:]
    names = set(re.findall(r"\bCFG_[A-Z_]+\b", sql, re.IGNORECASE))
    ctes = []
    for name in sorted(names):
        name = name.upper()
        if name not in definition.columns:
            continue
        # Images without project fields belong to the migrated default project.
        legacy_project = name in {"CFG_PIPELINES", "CFG_TABLES"} and (
            "project_id" not in definition.columns[name]
        )
        if postgres:
            rows = f"CAST(:_admitted_metadata AS jsonb)->'{name}'->'rows'"
            if legacy_project:
                rows = (
                    f"(SELECT jsonb_agg(value || jsonb_build_object('project_id',1)) "
                    f"FROM jsonb_array_elements({rows}))"
                )
            query = f"SELECT * FROM jsonb_populate_recordset(NULL::{name}, {rows})"
        else:
            fields = ",".join(
                f"json_extract(entry.value,'$.{column}') AS {column}"
                for column in definition.columns[name]
            )
            if legacy_project:
                fields += ",1 AS project_id"
            query = f"SELECT {fields} FROM json_each(:_admitted_metadata,'$.{name}.rows') entry"
        ctes.append(f"{name} AS ({query})")
    if not ctes:
        return sql
    prefix = "WITH " + ",".join(ctes)
    return (
        prefix + "," + sql.lstrip()[4:]
        if sql.lstrip().upper().startswith("WITH ")
        else prefix + " " + sql
    )
