"""Explain the resolved query catalog with non-NULL, typed representative bindings."""

import re
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime

from sqlalchemy import bindparam, text

from etl_craft.core.enums import ApiRole, Permission

UTILITIES = {
    "controller_listen": "LISTEN has no query plan.",
    "controller_unlisten": "UNLISTEN has no query plan.",
    "drop_metadata_changes_view": "DROP VIEW has no query plan.",
    "source_contract_drop_legacy": "DROP TABLE has no query plan.",
}
INTEGER_NAMES = frozenset(
    {
        "id",
        "count",
        "sequence",
        "offset",
        "length",
        "key",
        "pipeline",
        "project",
        "target",
        "task",
        "version_number",
        "revision",
        "pid",
        "exit_code",
        "looks",
        "limit",
        "column_position",
        "current_only",
        "version",
    }
)
MOMENT = datetime(2026, 1, 1, tzinfo=UTC)
VALUES = {
    "lock_key": 8241012,
    "batch_id": "batch",
    "body": "{}",
    "destination": "destination",
    "bundle_id": "a" * 64,
    "connections": "a" * 64,
    "approved": "Y",
    "use_bundles": False,
    "use_refresh": False,
    "refresh_required": False,
    "missing_only": False,
    "passed": True,
    "snapshot": "{}",
    "digests": '["' + "a" * 64 + '"]',
    "refresh_snapshot": "{}",
    "event_wait": False,
    "manifest": "{}",
    "mirror_id": "plan-mirror",
    "row_key": "[1]",
    "signature": "schema",
    "version": "version",
    **dict.fromkeys(
        (
            "warehouse_limit",
            "local_cpu",
            "local_memory_mb",
            "local_slots",
            "python_slots",
            "warehouse_slots",
            "python_cpu",
            "warehouse_cpu",
            "python_memory_mb",
            "warehouse_memory_mb",
            "cpu_free",
            "memory_free_mb",
            "cpu_capacity",
            "memory_capacity_mb",
            "cpu",
            "memory_mb",
        ),
        1,
    ),
    "capacity_source": "MEASURED",
    "seconds": 60,
    "python_free": True,
    "warehouse_free": True,
    "report": "{}",
    "node_id": "node_1",
    "force": "N",
    "rerun": "N",
    "active": "Y",
    "active_flag": "Y",
    "backfill": "N",
    "exclusive": "Y",
    "kind": "SYSTEM",
    "actor_kind": "SYSTEM",
    "requested_by_kind": "SYSTEM",
    "ended_by_kind": "SYSTEM",
    "status": "SUCCESS",
    "role": "operator",
    "grantee_role": ApiRole.VIEWER,
    "permission": Permission.CAN_VIEW,
    "max_running_tasks": 8,
    "retryable": True,
    "stopped": False,
    "overwrite": False,
    "forever": datetime(9999, 1, 1, tzinfo=UTC),
    "lease": MOMENT,
    "before": MOMENT,
    "deadline": MOMENT,
    "started": MOMENT,
    "ended": MOMENT,
    "now": MOMENT,
    "since": MOMENT,
    "run_date": MOMENT.date(),
    "date": MOMENT.date(),
    "cutoff": MOMENT,
    "create_date": MOMENT,
    "end_date": MOMENT,
    "updated_date": MOMENT,
    "arguments": "{}",
    "evidence": "{}",
    "entries": '[{"key":"one","report_id":"11111111-1111-1111-8111-111111111111"}]',
    "observations": "{}",
    "resources": '[{"lock_key": 1, "resource_name": "plan", "confirmed": null}]',
    "task_run_ids": "[1,2]",
    "attempt_ids": "[1,2]",
    "sso_group_ids": "[]",
    "code": "PLAN_1",
    "pipeline_code": "PLAN_1",
    "task_code": "task_1",
    "catalog": "",
    "catalog_name": "",
    "target_catalog_name": "",
    "project_only": True,
    "schema": "plan",
    "schema_name": "plan",
    "target_schema_name": "plan",
    "table": "table_1",
    "table_name": "table_1",
    "target_table_name": "table_1",
}


def bindings(sql, name=""):
    overrides = {
        "access_list": {"project": "default"},
        "window_reserve": {"high": MOMENT},
        "connection_reservation_insert": {
            "pool": 10,
            "sessions": 2,
            "tasks": 0,
            "backend": 1,
            "backend_started": MOMENT,
            "birth": 1.0,
            "expires": MOMENT,
        },
        "connection_reservation_heartbeat": {
            "expires": MOMENT,
            "backend": 1,
            "backend_started": MOMENT,
        },
        "latest_finished_pipeline_run": {"ended_by": MOMENT},
        "pipeline_run_history": {"before": 1000000000},
        "task_run_history": {"before": 1000000000},
        "target_contract_update_rule": {"table": 1},
        "lineage_cache_edges": {"confirmed_keys": [(1, "plan")]},
        "clear_rule_results": {"keys": ["key_1"]},
        "table_rule_result": {"passed": "Y"},
        "control_event_probes": {"keys": [1, 2]},
        "agent_list": {"statuses": ["ACTIVE", "DRAINING"]},
        "insert_task_documentation": {"version": 1},
        "save_target_hash_version": {"version": 1},
        "item_report": {"report_id": "plan-report"},
        "item_latest": {"key": "one"},
        "item_insert": {"key": "one", "number": 1, "report_id": "plan-report"},
        "event_consumed_keys": {"keys": ["key_1", "key_2"]},
    }.get(name, {})
    parameters = text(sql).compile().params
    values = {}
    for name in parameters:
        if name in overrides:
            values[name] = overrides[name]
        elif name in VALUES:
            values[name] = VALUES[name]
        elif (
            name in INTEGER_NAMES
            or name.endswith(("_id", "_count", "_number", "_slots"))
            or name == "rows_written"
        ):
            values[name] = 1
        elif name.endswith("_at") or name in {"wait_until", "not_before"}:
            values[name] = MOMENT
        else:
            values[name] = "plan"
    statement = text("EXPLAIN (FORMAT JSON) " + sql)
    for name in re.findall(r"\bIN\s+:(\w+)", sql, re.I):
        statement = statement.bindparams(bindparam(name, expanding=True))
        values[name] = overrides.get(name, ["aud_task_attempts"] if name == "names" else [1, 2])
    return statement, values


def migration_query(name):
    return (
        name.endswith("_legacy")
        or name.startswith(("config_history_", "source_contract_", "target_contract_"))
        or name
        in {
            "account_legacy_tokens",
            "recorded_hash_version_names",
            "recorded_rule_result_names",
            "recorded_view_statement_names",
        }
    )


@contextmanager
def migration_schema(conn):
    """Shadow only shapes used before identity enforcement and column normalization."""
    transaction = conn.begin_nested()
    try:
        for table in (
            "cfg_tables",
            "cfg_columns",
            "cfg_api_tokens",
            "cfg_business_rules",
            "aud_target_hash_version",
            "aud_target_view_statement",
            "aud_business_rules_results",
        ):
            conn.exec_driver_sql(
                f"CREATE TEMP TABLE {table} (LIKE public.{table} INCLUDING ALL EXCLUDING IDENTITY)"
            )
            conn.exec_driver_sql(f"INSERT INTO {table} SELECT * FROM public.{table}")
        for table, column in (
            ("cfg_api_tokens", "role"),
            ("cfg_business_rules", "target_table"),
            ("aud_target_hash_version", "target_object"),
            ("aud_target_view_statement", "target_object"),
            ("aud_business_rules_results", "target_table"),
        ):
            conn.exec_driver_sql(f"ALTER TABLE {table} ADD COLUMN {column} VARCHAR")
        conn.exec_driver_sql(
            "CREATE TEMP TABLE CFG_SOURCE_COLUMNS "
            "(LIKE public.AUD_SOURCE_COLUMNS_BASELINE INCLUDING ALL)"
        )
        conn.exec_driver_sql(
            "INSERT INTO CFG_SOURCE_COLUMNS SELECT * FROM public.AUD_SOURCE_COLUMNS_BASELINE"
        )
        conn.exec_driver_sql(
            "CREATE INDEX plan_legacy_pipeline_code ON CFG_PIPELINES (PIPELINE_CODE)"
        )
        conn.exec_driver_sql("ANALYZE")
        yield
    finally:
        transaction.rollback()


def catalog_plans(conn, dialect):
    names = dialect.query_names() - UTILITIES.keys()
    for historical in (False, True):
        with migration_schema(conn) if historical else nullcontext():
            for name in sorted(name for name in names if migration_query(name) == historical):
                sql = (
                    dialect.clone_statement("CFG_PIPELINES", ["PIPELINE_ID"]).text
                    if name == "clone_project_scope"
                    else dialect.query(name)
                )
                statement, values = bindings(sql, name)
                with conn.begin_nested():
                    plan = conn.execute(statement, values).scalar_one()[0]["Plan"]
                yield name, plan


def full_scans(plan, partial_indexes=frozenset(), limited=False):
    """Find heap-wide reads and unbounded walks of whole indexes."""
    kind = plan["Node Type"]
    if kind == "Seq Scan" or (
        kind in {"Index Scan", "Index Only Scan"}
        and "Index Cond" not in plan
        and not limited
        and plan.get("Index Name") not in partial_indexes
    ):
        yield plan
    for child in plan.get("Plans", []):
        bounded = kind == "Limit" or (limited and kind in {"Result", "Subquery Scan"})
        yield from full_scans(child, partial_indexes, bounded)


REGISTRY = "Reads the agent registry: one row per agent token, bounded by the tokens issued."
FULL_READS = {
    (
        "refresh_direct_projects",
        "cfg_pipelines",
    ): "Relates committed direct edits to every configured pipeline's owning project.",
    (
        "refresh_doctor",
        "cfg_pipelines",
    ): "Lists refresh readiness for every pipeline in the selected project.",
    (
        "retention_aud_pipeline_refreshes_count",
        "aud_pipeline_refreshes",
    ): "Counts the complete eligible refresh category for an explicit retention snapshot.",
    (
        "retention_bundle_candidates_count",
        "aud_project_bundles",
    ): "Counts obsolete bundles for explicit retention; references use point reads.",
    ("metrics_runs", "cfg_pipelines"): "Groups current runs across all configured projects.",
    ("metrics_gates", "cfg_pipelines"): "Groups current waits across all configured projects.",
    (
        "audit_project_pipeline_ids",
        "cfg_pipelines",
    ): "Collects all project pipeline identities to filter retained audit history.",
    (
        "clone_project_scope",
        "cfg_pipelines",
    ): "Exports every pipeline in the selected project to the warehouse mirror.",
    (
        "clone_projects",
        "aud_project_bundles",
    ): "Enumerates every project whose current connection bundle was approved.",
    (
        "config_rows_pipelines",
        "cfg_pipelines",
    ): "Enumerates all configuration rows for plan, apply and export.",
    (
        "config_rows_tasks",
        "cfg_pipelines",
    ): "Enumerates all configuration rows for plan, apply and export.",
    (
        "config_rows_task_parameters",
        "cfg_pipelines",
    ): "Enumerates all configuration rows for plan, apply and export.",
    (
        "config_rows_task_dependency",
        "cfg_pipelines",
    ): "Enumerates all configuration rows for plan, apply and export.",
    (
        "config_rows_pipeline_dependency",
        "cfg_pipelines",
    ): "Enumerates all configuration rows for plan, apply and export.",
    (
        "config_rows_business_rules",
        "cfg_pipelines",
    ): "Enumerates all configuration rows for plan, apply and export.",
    ("agent_host_lock", "aud_agents"): REGISTRY,
    ("agent_list", "aud_agents"): REGISTRY,
    ("connection_budget_agents", "aud_agents"): REGISTRY,
    ("agent_usage", "aud_agents"): REGISTRY,
    ("placement_agents", "aud_agents"): REGISTRY,
    ("reconcile_attempts", "aud_agents"): REGISTRY,
    ("transition_expire_agents", "aud_agents"): REGISTRY,
    ("active_pipeline_codes", "cfg_pipelines"): "Enumerates every active pipeline definition.",
    (
        "active_pipeline_parameters",
        "cfg_pipelines",
    ): "Enumerates parameters across active pipelines.",
    ("active_pipelines", "cfg_pipelines"): "Loads every active pipeline definition.",
    (
        "catalog_pipeline_edges",
        "cfg_pipelines",
    ): "Enumerates pipeline definitions on both ends of the selected project's catalog edges.",
    (
        "active_schedule_definitions",
        "cfg_pipelines",
    ): "Lists schedules across all active pipelines.",
    ("active_tasks", "cfg_pipelines"): "Lists tasks across all active pipelines.",
    (
        "active_task_dependency_edges",
        "cfg_pipelines",
    ): "Builds the complete active dependency graph.",
    (
        "active_pipeline_dependency_edges",
        "cfg_pipelines",
    ): "Builds the complete active dependency graph.",
    (
        "all_pipeline_dependency_edges",
        "cfg_pipelines",
    ): "Validates the complete pipeline dependency graph.",
    ("catalog_interventions", "cfg_pipelines"): "Builds bounded history for every active pipeline.",
    ("catalog_tasks", "cfg_pipelines"): "Builds the catalog for every active pipeline.",
    (
        "catalog_task_runs",
        "cfg_pipelines",
    ): "Builds bounded task history for every active pipeline.",
    ("catalog_pipeline_runs", "cfg_pipelines"): "Builds bounded history for every active pipeline.",
    (
        "catalog_metadata_versions",
        "cfg_pipelines",
    ): "Reports current versions for every catalog pipeline.",
    ("catalog_pipelines", "cfg_pipelines"): "Lists every active pipeline in the catalog.",
    (
        "catalog_consumption",
        "cfg_pipelines",
    ): "Explains bounded history across all active pipelines.",
    ("catalog_rules", "cfg_pipelines"): "Lists rules across all active pipelines.",
    ("catalog_lineage_cache", "cfg_pipelines"): "Builds complete active-pipeline lineage.",
    ("controller_active_runs", "cfg_pipelines"): "Finds active definitions for the working set.",
    ("controller_schedules", "cfg_pipelines"): "Checks the schedule of every active pipeline.",
    ("metadata_codes", "cfg_pipelines"): "Loads the complete identifier-to-code map.",
    ("permission_pipelines", "cfg_pipelines"): "Lists all pipelines for permission evaluation.",
    ("sql_tasks", "cfg_pipelines"): "Validates SQL tasks across all active pipelines.",
    ("rehash_tasks", "cfg_pipelines"): "Finds all merge targets across active pipelines.",
    ("catalog_lineage_cache", "aud_column_lineage"): "Loads the complete current lineage cache.",
    (
        "recorded_hash_version_names",
        "aud_target_hash_version",
    ): "Normalizes every legacy target name once during migration.",
    (
        "recorded_rule_result_names",
        "aud_business_rules_results",
    ): "Normalizes every legacy target name once during migration.",
    (
        "recorded_view_statement_names",
        "aud_target_view_statement",
    ): "Normalizes every legacy target name once during migration.",
}

FULL_READS.update(
    {
        (
            "business_rule_legacy_sql",
            "cfg_business_rules",
        ): "Converts every stored legacy predicate once during the rule-query upgrade.",
        (
            "team_tables",
            "aud_team_tables",
        ): "Lists every claimed team table once for doctor's unowned-table check.",
        (
            "business_rule_duplicate_flags",
            "aud_business_rules_results",
        ): "Checks all active flags before upgrades; no legacy duplicate can be ignored.",
    }
)

# Exact dry-run counts visit the complete eligible category during explicit maintenance.
FULL_READS.update(
    {
        (
            "retention_aud_gate_waits_count",
            "aud_gate_waits",
        ): "Counts all eligible rows for the explicit retention snapshot.",
        (
            "retention_aud_dependency_consumption_count",
            "aud_dependency_consumption",
        ): "Counts all eligible rows for the explicit retention snapshot.",
        (
            "retention_aud_gate_decisions_count",
            "aud_gate_decisions",
        ): "Counts all eligible rows for the explicit retention snapshot.",
        (
            "retention_aud_run_interventions_count",
            "aud_run_interventions",
        ): "Counts all eligible rows for the explicit retention snapshot.",
        (
            "retention_aud_pipeline_pauses_count",
            "aud_pipeline_pauses",
        ): "Counts all eligible rows for the explicit retention snapshot.",
        (
            "retention_aud_actions_count",
            "aud_actions",
        ): "Counts all eligible rows for the explicit retention snapshot.",
        (
            "retention_aud_control_requests_count",
            "aud_control_requests",
        ): "Counts all eligible rows for the explicit retention snapshot.",
        (
            "retention_aud_task_items_count",
            "aud_task_items",
        ): "Counts all eligible rows for the explicit retention snapshot.",
        (
            "retention_aud_business_rules_results_count",
            "aud_business_rules_results",
        ): "Counts all eligible rows for the explicit retention snapshot.",
        (
            "retention_aud_business_rules_run_log_count",
            "aud_business_rules_run_log",
        ): "Counts all eligible rows for the explicit retention snapshot.",
        (
            "retention_aud_attempt_acknowledgements_count",
            "aud_attempt_acknowledgements",
        ): "Counts all eligible rows for the explicit retention snapshot.",
        (
            "retention_aud_task_attempts_count",
            "aud_task_attempts",
        ): "Counts all eligible rows for the explicit retention snapshot.",
        (
            "retention_aud_task_run_log_count",
            "aud_task_run_log",
        ): "Counts all eligible rows for the explicit retention snapshot.",
        (
            "retention_aud_pipelines_run_log_count",
            "aud_pipelines_run_log",
        ): "Counts all eligible rows for the explicit retention snapshot.",
        (
            "retention_aud_cfg_business_rules_history_count",
            "aud_cfg_business_rules_history",
        ): "Counts all eligible rows for the explicit retention snapshot.",
        (
            "retention_aud_cfg_columns_history_count",
            "aud_cfg_columns_history",
        ): "Counts all eligible rows for the explicit retention snapshot.",
        (
            "retention_aud_cfg_pipelines_history_count",
            "aud_cfg_pipelines_history",
        ): "Counts all eligible rows for the explicit retention snapshot.",
        (
            "retention_aud_cfg_pipeline_dependency_history_count",
            "aud_cfg_pipeline_dependency_history",
        ): "Counts all eligible rows for the explicit retention snapshot.",
        (
            "retention_aud_cfg_pipeline_grants_history_count",
            "aud_cfg_pipeline_grants_history",
        ): "Counts all eligible rows for the explicit retention snapshot.",
        (
            "retention_aud_cfg_projects_history_count",
            "aud_cfg_projects_history",
        ): "Counts all eligible rows for the explicit retention snapshot.",
        (
            "retention_aud_cfg_tables_history_count",
            "aud_cfg_tables_history",
        ): "Counts all eligible rows for the explicit retention snapshot.",
        (
            "retention_aud_cfg_tasks_history_count",
            "aud_cfg_tasks_history",
        ): "Counts all eligible rows for the explicit retention snapshot.",
        (
            "retention_aud_cfg_task_dependency_history_count",
            "aud_cfg_task_dependency_history",
        ): "Counts all eligible rows for the explicit retention snapshot.",
        (
            "retention_aud_cfg_task_parameters_history_count",
            "aud_cfg_task_parameters_history",
        ): "Counts all eligible rows for the explicit retention snapshot.",
        (
            "retention_logs_count",
            "aud_task_attempts",
        ): "Counts all eligible attempt logs for explicit retention.",
        (
            "retention_tick",
            "aud_retention_ticks",
        ): "One daily completion cursor per project, independent of run volume.",
    }
)

FULL_READS.update(
    {
        (
            "archive_origin",
            "aud_engine_identity",
        ): "One deployment identity, independent of history volume.",
        ("archive_pending", "aud_retention_archives"): "At most one pending snapshot per project.",
        ("archive_finish", "aud_retention_archives"): "At most one pending snapshot per project.",
    }
)
