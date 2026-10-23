"""Local placement uses measured machine capacity and the same requests as agent claims."""

from collections.abc import Mapping
from dataclasses import dataclass

from sqlalchemy.engine import Connection, Engine

from etl_craft.config import ConnectorConfig
from etl_craft.config.sizing import ceilings
from etl_craft.core.enums import DeploymentProfile, Handler
from etl_craft.core.errors import MetadataError
from etl_craft.core.placement import ResourceRequest
from etl_craft.core.resources import CONTROLLER_CPUS, CONTROLLER_MEMORY, MIB, Sizing, discover, size
from etl_craft.engine.repository.tasks import TaskExecutionDetail
from etl_craft.execution.attempt_types import ChildOptions, TaskOutcome
from etl_craft.execution.pools import AttemptSpec


@dataclass(frozen=True)
class LocalCapacity:
    """One measured local allocation, shared by a pool's claims and its process ceiling."""

    sizing: Sizing
    cpu: float
    memory_mb: int

    def parameters(self, config: ConnectorConfig) -> dict[str, object]:
        """Claim limits bind into the existing ownership update without another DB read."""
        return {
            "warehouse_limit": config.max_warehouse_queries,
            "local_cpu": self.cpu,
            "local_memory_mb": self.memory_mb,
            "local_slots": min(
                self.sizing.task_slots.value,
                self.sizing.python_slots.value + self.sizing.warehouse_slots.value,
            ),
            "python_slots": self.sizing.python_slots.value,
            "warehouse_slots": self.sizing.warehouse_slots.value,
        }


def local_capacity(config: ConnectorConfig) -> LocalCapacity:
    """Keep a machine-wide measured cap even when each pipeline has its own limit."""
    allocation = discover()
    sizing = size(allocation, config.deployment or DeploymentProfile.CONTROLLER, ceilings(config))
    cpu = max(0, (allocation.cpus.value or 0) - (sizing.reserve_cpus or 0) - CONTROLLER_CPUS)
    memory = max(
        0, (allocation.memory.value or 0) - (sizing.reserve_memory or 0) - CONTROLLER_MEMORY
    )
    return LocalCapacity(sizing, cpu, int(memory / MIB))


def require_capacity(
    request: ResourceRequest, cpu: float, memory_mb: int, task: str, pool: str
) -> None:
    """Refuse a task that cannot fit even when the selected allocation is empty."""
    if request.cpu > cpu or request.memory_mb > memory_mb:
        raise MetadataError(
            f"task {task}: needs CPU_REQUEST={request.cpu:g}, "
            f"MEMORY_REQUEST_MB={request.memory_mb}; "
            f"largest allocation in {pool} has {cpu:g} CPU and {memory_mb} MB; expected one "
            "allocation that fits both requests. Reduce the requests or add capacity to this pool"
        )


def check_admission(
    config: ConnectorConfig, detail: TaskExecutionDetail, params: Mapping[str, str]
) -> str | None:
    """Validate resource and pool metadata before creating any run state."""
    from etl_craft.core.placement import resource_request
    from etl_craft.engine.placement import check_pool

    task = f"{detail.pipeline_code}.{detail.task_code}"
    check_pool(detail.pool, detail.pool_active, task)
    request = resource_request(detail.handler, params, task)
    if (
        config.deployment != DeploymentProfile.CONTROLLER_AGENTS
        and detail.handler != Handler.EVENT_WAIT
    ):
        measured = local_capacity(config)
        require_capacity(request, measured.cpu, measured.memory_mb, task, "local pool")
    return (
        f"QUEUED in pool {detail.pool}: needs {request.memory_mb} MB and {request.cpu:g} CPU; "
        "waiting for an eligible agent and free slot. Add capacity or adjust task requests; "
        "no controller fallback"
        if config.deployment == DeploymentProfile.CONTROLLER_AGENTS
        else None
    )


def wait_for_attempt(
    engine: Engine, config: ConnectorConfig, spec: AttemptSpec, child: ChildOptions
) -> TaskOutcome:
    """Wait for the same atomic admission used by local pool workers."""
    import threading

    from etl_craft.core.actor import current_actor
    from etl_craft.core.enums import RunStatus
    from etl_craft.engine import transitions
    from etl_craft.execution.attempt_types import TaskOutcome
    from etl_craft.execution.runner import try_execute_attempt

    cancel = child.cancel or threading.Event()
    while True:
        outcome = try_execute_attempt(engine, config, spec, child)
        if outcome is not None:
            return outcome
        if cancel.wait(0.1):
            with engine.begin() as conn:
                transitions.cancel_attempt(
                    conn,
                    spec.attempt_id,
                    current_actor(),
                    error_message="cancelled while waiting for capacity",
                )
            return TaskOutcome(RunStatus.CANCELLED, "cancelled while queued", spec.task_run_id)


def local_headroom(conn: Connection, config: ConnectorConfig) -> dict[str, object]:
    """Record current host reservations only after a local claim found no capacity."""
    import socket

    from etl_craft.engine.queries import statement

    measured = local_capacity(config)
    used = conn.execute(
        statement(conn, "placement_local_usage"), {"host": socket.gethostname()}
    ).one()
    return {
        "capacity_label": "local pool",
        "cpu_free": max(0, measured.cpu - float(used.cpu_used)),
        "memory_free_mb": max(0, measured.memory_mb - int(used.memory_used_mb)),
        "cpu_capacity": measured.cpu,
        "memory_capacity_mb": measured.memory_mb,
        "python_free_slots": max(
            0,
            min(
                measured.sizing.python_slots.value - used.python_used,
                measured.sizing.task_slots.value - used.slots_used,
            ),
        ),
        "warehouse_free_slots": max(
            0,
            min(
                measured.sizing.warehouse_slots.value - used.warehouse_used,
                measured.sizing.task_slots.value - used.slots_used,
            ),
        ),
        "python_capacity_slots": measured.sizing.python_slots.value,
        "warehouse_capacity_slots": measured.sizing.warehouse_slots.value,
    }
