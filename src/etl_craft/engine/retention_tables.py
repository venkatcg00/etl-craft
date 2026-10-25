"""Project-owned history with explicit retention queries; live state is never a candidate."""

from dataclasses import dataclass


@dataclass(frozen=True)
class HistoryTable:
    """The shared delete/count query pair and the configuration period that enables it."""

    table: str
    query: str
    period: str


TABLES = (
    HistoryTable("AUD_GATE_WAITS", "retention_aud_gate_waits", "run_history_days"),
    HistoryTable(
        "AUD_DEPENDENCY_CONSUMPTION", "retention_aud_dependency_consumption", "consumption_days"
    ),
    HistoryTable("AUD_GATE_DECISIONS", "retention_aud_gate_decisions", "consumption_days"),
    HistoryTable("AUD_RUN_INTERVENTIONS", "retention_aud_run_interventions", "interventions_days"),
    HistoryTable("AUD_PIPELINE_PAUSES", "retention_aud_pipeline_pauses", "interventions_days"),
    HistoryTable("AUD_ACTIONS", "retention_aud_actions", "run_history_days"),
    HistoryTable("AUD_CONTROL_REQUESTS", "retention_aud_control_requests", "run_history_days"),
    HistoryTable("AUD_TASK_ITEMS", "retention_aud_task_items", "run_history_days"),
    HistoryTable(
        "AUD_BUSINESS_RULES_RESULTS", "retention_aud_business_rules_results", "run_history_days"
    ),
    HistoryTable(
        "AUD_BUSINESS_RULES_RUN_LOG", "retention_aud_business_rules_run_log", "run_history_days"
    ),
    HistoryTable(
        "AUD_ATTEMPT_ACKNOWLEDGEMENTS", "retention_aud_attempt_acknowledgements", "run_history_days"
    ),
    HistoryTable("AUD_TASK_ATTEMPTS", "retention_aud_task_attempts", "run_history_days"),
    HistoryTable("AUD_TASK_RUN_LOG", "retention_aud_task_run_log", "run_history_days"),
    HistoryTable("AUD_PIPELINES_RUN_LOG", "retention_aud_pipelines_run_log", "run_history_days"),
    HistoryTable(
        "AUD_CFG_BUSINESS_RULES_HISTORY",
        "retention_aud_cfg_business_rules_history",
        "config_history_days",
    ),
    HistoryTable(
        "AUD_CFG_COLUMNS_HISTORY", "retention_aud_cfg_columns_history", "config_history_days"
    ),
    HistoryTable(
        "AUD_CFG_PIPELINES_HISTORY", "retention_aud_cfg_pipelines_history", "config_history_days"
    ),
    HistoryTable(
        "AUD_CFG_PIPELINE_DEPENDENCY_HISTORY",
        "retention_aud_cfg_pipeline_dependency_history",
        "config_history_days",
    ),
    HistoryTable(
        "AUD_CFG_PIPELINE_GRANTS_HISTORY",
        "retention_aud_cfg_pipeline_grants_history",
        "config_history_days",
    ),
    HistoryTable(
        "AUD_CFG_PROJECTS_HISTORY", "retention_aud_cfg_projects_history", "config_history_days"
    ),
    HistoryTable(
        "AUD_CFG_TABLES_HISTORY", "retention_aud_cfg_tables_history", "config_history_days"
    ),
    HistoryTable("AUD_CFG_TASKS_HISTORY", "retention_aud_cfg_tasks_history", "config_history_days"),
    HistoryTable(
        "AUD_CFG_TASK_DEPENDENCY_HISTORY",
        "retention_aud_cfg_task_dependency_history",
        "config_history_days",
    ),
    HistoryTable(
        "AUD_CFG_TASK_PARAMETERS_HISTORY",
        "retention_aud_cfg_task_parameters_history",
        "config_history_days",
    ),
)
