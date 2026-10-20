"""Configuration orchestration."""

from __future__ import annotations

from pathlib import Path

from etl_craft.config.loader_helpers import Profiled, flag, whole_number
from etl_craft.config.model import ExecutionLimits
from etl_craft.core.enums import GatePolicy, RetryOn, enum_value
from etl_craft.core.errors import ConfigurationError

ORCHESTRATION_KEYS = frozenset(
    {
        "Api_address",
        "Log_dir",
        "Task_timeout_seconds",
        "Max_parallel_tasks",
        "Shutdown_grace_seconds",
        "Engine_db_outage_seconds",
        "Local_ingestion_slots",
        "Local_warehouse_slots",
        "Timezone",
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

    return ExecutionLimits(
        task_timeout_seconds=number("Task_timeout_seconds"),
        max_parallel_tasks=number("Max_parallel_tasks", low=1),
        shutdown_grace_seconds=number("Shutdown_grace_seconds"),
        engine_db_outage_seconds=number("Engine_db_outage_seconds"),
        enforce_sla=flag(settings, "Enforce_sla", False, path),
        gate_wait_minutes=number("Gate_wait_minutes"),
        local_ingestion_slots=whole_number(
            settings, "Local_ingestion_slots", None, path, minimum=1
        ),
        local_warehouse_slots=whole_number(
            settings, "Local_warehouse_slots", None, path, minimum=1
        ),
        max_parallel_tasks_written=settings.value("Max_parallel_tasks") is not None,
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
