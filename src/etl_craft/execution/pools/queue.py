"""The explicit cluster provider queues attempts and never starts a local task process."""

import threading

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.actor import current_actor
from etl_craft.core.enums import AttemptStatus, RunStatus, SlotKind
from etl_craft.core.errors import ConfigurationError, RunStateError
from etl_craft.dialects.engine import for_engine
from etl_craft.engine import transitions
from etl_craft.engine.notifications import agent_channel, pool_channel
from etl_craft.engine.queries import statement
from etl_craft.engine.resource_locks import notify
from etl_craft.execution.attempt_types import ChildOptions, TaskOutcome
from etl_craft.execution.pools import (
    AttemptSpec,
    Capacity,
    ExecutionHandle,
    HandleState,
    HandleStatus,
)
from etl_craft.warehouse.fencing import cluster_problem


class QueuePool:
    """Observe durable attempts independently of controller and API process lifetimes."""

    name = "queue"
    status_reads_engine = True

    def __init__(self, engine: Engine, config: ConnectorConfig) -> None:
        """Refuse unsupported storage before admitting cluster work."""
        if not for_engine(engine).supports_api_replicas:
            raise ConfigurationError(
                "CONTROLLER_AGENTS requires PostgreSQL; choose a PostgreSQL Engine DB"
            )
        if problem := cluster_problem(config):
            raise ConfigurationError(problem)
        self.engine, self.config = engine, config

    def capacity(self) -> Capacity:
        """Admission queues work even with no registered agent; claims enforce actual capacity."""
        slots: dict[str, int] = dict.fromkeys(SlotKind, 2**31 - 1)
        return Capacity(slots, slots)

    def submit(self, spec: AttemptSpec) -> ExecutionHandle:
        """Notify the admitted pool without changing the attempt's recorded state."""
        with self.engine.begin() as conn:
            notify(conn, pool_channel(spec.pool))
        return ExecutionHandle(self.name, spec.attempt_id)

    def status(self, handle: ExecutionHandle) -> HandleStatus:
        """Read the exact attempt; QUEUED is pending, never a reason for local fallback."""
        with self.engine.connect() as conn:
            row = conn.execute(
                statement(conn, "agent_work"), {"attempt_id": handle.attempt_id}
            ).one_or_none()
        if row is None:
            return HandleStatus(handle, HandleState.UNKNOWN)
        if row.status in {AttemptStatus.QUEUED, AttemptStatus.CLAIMED, AttemptStatus.RUNNING}:
            return HandleStatus(handle, HandleState.RUNNING)
        waiting = row.status == AttemptStatus.HANDED_OFF
        status = RunStatus.SUCCESS if row.status == AttemptStatus.SUCCESS else RunStatus.FAILED
        return HandleStatus(
            handle,
            HandleState.ENDED,
            TaskOutcome(
                status, row.error_message or str(row.status), row.task_run_id, waiting=waiting
            ),
        )

    def cancel(self, handle: ExecutionHandle, grace_seconds: float) -> None:
        """Request remote cancellation; only the owner acknowledges stopping its child."""
        with self.engine.begin() as conn:
            row = conn.execute(
                statement(conn, "agent_work"), {"attempt_id": handle.attempt_id}
            ).one_or_none()
            if row is None:
                return
            if row.status == AttemptStatus.QUEUED:
                transitions.cancel_attempt(
                    conn,
                    handle.attempt_id,
                    current_actor(),
                    error_message="cancelled before agent claim",
                )
            else:
                conn.execute(
                    statement(conn, "agent_cancel"),
                    {
                        "attempt_id": handle.attempt_id,
                        "now": conn.execute(statement(conn, "agent_now")).scalar_one(),
                    },
                )
            if row.owner_id is not None:
                notify(conn, agent_channel(int(row.owner_id)))

    def reconcile(self) -> list[HandleStatus]:
        """Lease reconciliation owns lost attempts; this provider never adopts a process."""
        return []

    def close(self) -> None:
        """Leave durable attempts for the next controller; agents own their execution."""


def wait_attempt(
    engine: Engine, config: ConnectorConfig, spec: AttemptSpec, child: ChildOptions
) -> TaskOutcome:
    """Wait for remote work without ever creating a controller-host task process."""
    pool = QueuePool(engine, config)
    handle = pool.submit(spec)
    cancel = child.cancel or threading.Event()
    while True:
        status = pool.status(handle)
        if status.state is HandleState.ENDED and status.outcome is not None:
            return status.outcome
        if status.state is HandleState.UNKNOWN:
            raise RunStateError(
                f"attempt {spec.attempt_id}: queued attempt disappeared; inspect the Engine DB"
            )
        if cancel.wait(0.1):
            pool.cancel(handle, child.kill_grace_seconds)


def restore(engine: Engine, run_id: int) -> dict[int, ExecutionHandle]:
    """Attach observation to durable attempts after a controller restart; never adopt a lease."""
    with engine.connect() as conn:
        return {
            int(row.task_id): ExecutionHandle("queue", int(row.attempt_id))
            for row in conn.execute(statement(conn, "queue_live_run"), {"run_id": run_id})
        }
