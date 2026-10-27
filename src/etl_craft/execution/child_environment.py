"""One explicit environment allowlist for local and agent task interpreters."""

import os

from etl_craft.config import ConnectorConfig

PROCESS_VARIABLES = frozenset(
    {
        "PATH",
        "PYTHONPATH",
        "HOME",
        "LANG",
        "LANGUAGE",
        "TZ",
        "TMPDIR",
        "TMP",
        "TEMP",
        "USER",
        "LOGNAME",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
        "SYSTEMROOT",
        "COVERAGE_PROCESS_START",
        "COVERAGE_RCFILE",
        "COVERAGE_FILE",
        "ETL_CRAFT_FAULT",
    }
)


def child_environment(config: ConnectorConfig) -> dict[str, str]:
    """Keep process basics and configured connection variables, never deployment tokens."""
    names = PROCESS_VARIABLES | {
        setting.variable
        for setting in config.settings
        if setting.variable is not None
        and (
            setting.where.startswith(("Engine.", "Warehouse."))
            or (setting.where.startswith("Orchestration.") and ".Email." in setting.where)
        )
    }
    names |= {p.secret_var for p in (config.engine, config.warehouse) if p is not None}
    return {
        key: value
        for key, value in os.environ.items()
        if (key in names or key.startswith("LC_"))
        and key != "ETL_CRAFT_TOKEN"
        and key != "ETL_CRAFT_ATTEMPT_OWNER"
    } | {"PYTHONUNBUFFERED": "1"}
