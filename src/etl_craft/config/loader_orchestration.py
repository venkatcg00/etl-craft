"""Configuration orchestration."""

from __future__ import annotations

from pathlib import Path

from etl_craft.config.loader_helpers import Profiled, flag, string_list, whole_number
from etl_craft.config.model import DagDefaults, ExecutionLimits
from etl_craft.core.enums import GatePolicy, Mode, enum_value
from etl_craft.core.errors import ConfigurationError

ORCHESTRATION_KEYS = frozenset(
    {
        "Api_address",
        "Mode",
        "Name",
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
        "Global_dag",
        "Catchup",
        "Tags",
        "Retries",
        "Retry_delay_minutes",
        "Depends_on_past",
        "Email_on_failure",
        "Email_recipients",
        "Allow_schedule",
        "Dependency_gates",
        "Email",
    }
)


def parse_mode(orchestration: Profiled, path: Path) -> Mode:
    """Read the required orchestration mode."""
    return enum_value(
        Mode,
        orchestration.text("Mode") or "",
        ConfigurationError(
            f"{path}: Orchestration.Mode must be local or remote, got "
            f"{orchestration.settings.get('Mode')!r}"
            f"{orchestration.resolver.hint(orchestration.where('Mode'))}"
        ),
    )


def parse_dependency_gates(orchestration: Profiled, mode: Mode, path: Path) -> GatePolicy:
    """Read the dependency policy and refuse overrides in remote mode."""
    raw = orchestration.text("Dependency_gates")
    if raw is None:
        return GatePolicy.ENFORCE
    policy = enum_value(
        GatePolicy,
        raw,
        ConfigurationError(
            f"{path}: Orchestration.Dependency_gates must be enforce, warn or off, got {raw!r}"
            f"{orchestration.resolver.hint(orchestration.where('Dependency_gates'))}"
        ),
    )
    if mode == Mode.REMOTE and policy != GatePolicy.ENFORCE:
        raise ConfigurationError(
            f"{path}: Orchestration.Dependency_gates is {policy}, but Mode is remote: in remote "
            "mode etl-craft checks no dependency gate (the orchestrator's sensors do), so there "
            "is nothing to relax. Remove Dependency_gates from the remote profile, or bypass the "
            "sensor in the orchestrator"
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
    )


def parse_dag_defaults(settings: Profiled, path: Path) -> DagDefaults:
    """Read optional DAG defaults for exported orchestrator configuration."""
    return DagDefaults(
        global_dag=flag(settings, "Global_dag", False, path),
        catchup=flag(settings, "Catchup", None, path),
        tags=string_list(settings, "Tags", path),
        retries=whole_number(settings, "Retries", None, path),
        retry_delay_minutes=whole_number(settings, "Retry_delay_minutes", None, path),
        depends_on_past=flag(settings, "Depends_on_past", None, path),
        email_on_failure=flag(settings, "Email_on_failure", None, path),
        email_recipients=string_list(settings, "Email_recipients", path),
        allow_schedule=flag(settings, "Allow_schedule", True, path),
    )
