"""Configuration orchestration."""

from __future__ import annotations

from pathlib import Path

from etl_craft.config.loader_helpers import Profiled, flag, whole_number
from etl_craft.config.model import ExecutionLimits
from etl_craft.core.enums import GatePolicy, RetryOn, enum_value
from etl_craft.core.errors import ConfigurationError
from etl_craft.core.text import is_metadata_code

ORCHESTRATION_KEYS = frozenset(
    {
        "Api_address",
        "Log_dir",
        "Task_timeout_seconds",
        "Shutdown_grace_seconds",
        "Engine_db_outage_seconds",
        "Local_python_slots",
        "Local_warehouse_slots",
        "Timezone",
        "Project",
        "Db_pool_size",
        "Enforce_sla",
        "Gate_wait_minutes",
        "Retries",
        "Retry_on",
        "Email_recipients",
        "Allow_schedule",
        "Dependency_gates",
        "Email",
    }
)


def parse_project(settings: Profiled, path: Path) -> str:
    """Read the project code used when a command does not pass ``--project``."""
    code = settings.text("Project") or "default"
    if not is_metadata_code(code):
        raise ConfigurationError(
            f"{path}: Orchestration.Project={code!r}; expected a letter followed by at most "
            "127 letters, numbers or underscores; correct the project code"
        )
    return code


def parse_dependency_gates(orchestration: Profiled, path: Path) -> GatePolicy:
    """Read the dependency policy."""
    raw = orchestration.text("Dependency_gates")
    if raw is None:
        return GatePolicy.ENFORCE
    policy = enum_value(
        GatePolicy,
        raw,
        ConfigurationError(
            f"{path}: Orchestration.Dependency_gates must be ENFORCE, WARN or OFF, got {raw!r}"
            f"{orchestration.resolver.hint(orchestration.where('Dependency_gates'))}"
        ),
    )
    return policy


def parse_limits(settings: Profiled, path: Path) -> ExecutionLimits:
    """Read execution limits with their defaults and bounds."""
    defaults = ExecutionLimits()

    def number(key: str, *, low: int = 0) -> int:
        """``Orchestration.<key>``, defaulting to the limit field of the same lowercase name."""
        return int(whole_number(settings, key, getattr(defaults, key.lower()), path, minimum=low))

    if "Max_parallel_tasks" in settings.settings:
        raise ConfigurationError(
            f"{path}: Orchestration.Max_parallel_tasks={settings.value('Max_parallel_tasks')!r}; "
            "expected per-pipeline limits in config/pipelines.csv. Copy this value to "
            "MAX_PARALLEL_TASKS for each pipeline that needs it, then remove "
            "Orchestration.Max_parallel_tasks from craft-connector.yml"
        )
    return ExecutionLimits(
        task_timeout_seconds=number("Task_timeout_seconds"),
        shutdown_grace_seconds=number("Shutdown_grace_seconds"),
        engine_db_outage_seconds=number("Engine_db_outage_seconds"),
        enforce_sla=flag(settings, "Enforce_sla", False, path),
        gate_wait_minutes=number("Gate_wait_minutes"),
        local_python_slots=whole_number(settings, "Local_python_slots", None, path, minimum=1),
        local_warehouse_slots=whole_number(
            settings, "Local_warehouse_slots", None, path, minimum=1
        ),
    )


def parse_retry_on(settings: Profiled, path: Path) -> RetryOn:
    """Read the project policy; reject a typo rather than broadening retries."""
    raw = settings.text("Retry_on")
    if raw is None:
        return RetryOn.TRANSIENT
    return enum_value(
        RetryOn,
        raw,
        ConfigurationError(
            f"{path}: Orchestration.Retry_on={raw!r}; expected "
            f"{', '.join(RetryOn)}; set Retry_on to one of these values"
        ),
    )
