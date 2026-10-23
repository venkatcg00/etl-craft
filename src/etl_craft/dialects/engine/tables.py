"""Own exactly the named package tables; other CFG_ tables belong to the team."""

CONFIG_TABLES = frozenset(
    {
        "CFG_POOLS",
        "CFG_PIPELINES",
        "CFG_PIPELINE_DEPENDENCY",
        "CFG_TASKS",
        "CFG_TASK_DEPENDENCY",
        "CFG_TASK_PARAMETERS",
        "CFG_BUSINESS_RULES",
        "CFG_ACCOUNTS",
        "CFG_GROUPS",
        "CFG_GROUP_MEMBERS",
        "CFG_PIPELINE_GRANTS",
        "CFG_API_TOKENS",
        "CFG_TABLES",
        "CFG_COLUMNS",
    }
)

AUDIT_TABLES = frozenset(
    {
        "AUD_PIPELINES_RUN_LOG",
        "AUD_TASK_RUN_LOG",
        "AUD_RUN_INTERVENTIONS",
        "AUD_PIPELINE_PAUSES",
        "AUD_BUSINESS_RULES_RUN_LOG",
        "AUD_BUSINESS_RULES_RESULTS",
        "AUD_TASK_OFFSET_TRACKER",
        "AUD_COLUMN_LINEAGE",
        "AUD_LINEAGE_VERSIONS",
        "AUD_LINEAGE_VERSION_EDGES",
        "AUD_DOCS_PUBLICATION",
        "AUD_TASK_DOCUMENTATION",
        "AUD_DEPENDENCY_CONSUMPTION",
        "AUD_TASK_ATTEMPTS",
        "AUD_RESOURCE_LOCKS",
        "AUD_AGENTS",
        "AUD_AGENT_COMMANDS",
        "AUD_ATTEMPT_LOG_CHUNKS",
        "AUD_GATE_DECISIONS",
        "AUD_ACTIONS",
        "AUD_CFG_POOLS_HISTORY",
        "AUD_CFG_PIPELINES_HISTORY",
        "AUD_CFG_PIPELINE_DEPENDENCY_HISTORY",
        "AUD_CFG_TASKS_HISTORY",
        "AUD_CFG_TASK_DEPENDENCY_HISTORY",
        "AUD_CFG_TASK_PARAMETERS_HISTORY",
        "AUD_CFG_BUSINESS_RULES_HISTORY",
        "AUD_TARGET_HASH_VERSION",
        "AUD_TARGET_VIEW_STATEMENT",
        "AUD_CONTROLLERS",
        "AUD_GATE_WAITS",
        "AUD_TASK_EVENTS",
        "AUD_TASK_ITEMS",
        "AUD_EVENT_FILES",
        "AUD_EVENT_KEYS",
        "AUD_SOURCE_COLUMNS_BASELINE",
        "AUD_CFG_TABLES_HISTORY",
        "AUD_CFG_COLUMNS_HISTORY",
        "AUD_CFG_SOURCE_COLUMNS_HISTORY",
        "AUD_CFG_API_TOKENS_HISTORY",
        "AUD_CFG_ACCOUNTS_HISTORY",
        "AUD_CFG_GROUPS_HISTORY",
        "AUD_CFG_GROUP_MEMBERS_HISTORY",
        "AUD_CFG_PIPELINE_GRANTS_HISTORY",
        "AUD_ATTEMPT_ACKNOWLEDGEMENTS",
        "AUD_CONTROL_REQUESTS",
        "AUD_METADATA_CHANGES",
    }
)


LEGACY_CONFIG_TABLES = frozenset({"CFG_SOURCE_COLUMNS"})
OWNED_CONFIG_TABLES = CONFIG_TABLES | LEGACY_CONFIG_TABLES


def team_config(table: str, *, legacy: bool = False) -> bool:
    """Recognize a team table without inspecting its columns or values."""
    owned = OWNED_CONFIG_TABLES if legacy else CONFIG_TABLES
    return table.upper().startswith("CFG_") and table.upper() not in owned


def retired_history(table: str) -> bool:
    """Recognize a historical team table name; callers verify its installed history guards."""
    name = table.upper()
    return name.startswith("AUD_CFG_") and name.endswith("_HISTORY") and name not in AUDIT_TABLES


LEGACY_AUDIT_TABLES = frozenset(
    {"AUD_PIPELINE_DEPENDENCY_TRACKER", "AUD_TASK_DEPENDENCY_TRACKER", "AUD_OVERSEERS"}
)


def validate_audit_table(table: str, triggers: set[str]) -> None:
    """Refuse a new team audit table without mistaking retained history for team data."""
    from etl_craft.core.errors import MetadataError

    name = table.upper()
    if not name.startswith("AUD_") or name in AUDIT_TABLES | LEGACY_AUDIT_TABLES:
        return
    known = {
        f"trg_actor_guard_{table.lower()}",
        f"trg_actor_guard_{table.lower()}_insert",
        f"trg_frozen_{table.lower()}_insert",
    }
    if retired_history(table) and known & triggers:
        return
    raise MetadataError(
        f"{table}: AUD_ is reserved for etl-craft audit records; rename this team "
        f"table outside AUD_ before migrating"
    )
