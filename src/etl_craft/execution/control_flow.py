"""Agent acknowledgements, graceful draining and scoped process aborts."""

from __future__ import annotations

import json
import socket

import psutil
from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.actor import SYSTEM_ACTOR, acting_as, current_actor
from etl_craft.core.enums import AttemptStatus, ControlAction, ControlStatus, RunStatus
from etl_craft.engine import controls, transitions
from etl_craft.engine.queries import statement
from etl_craft.execution.leases import process_start
from etl_craft.warehouse.connection import warehouse_dialect

ACTIVE_ATTEMPTS = frozenset({AttemptStatus.QUEUED, AttemptStatus.CLAIMED, AttemptStatus.RUNNING})


def process_local_queries(config: ConnectorConfig) -> bool:
    """Whether ending the task process also ends every engine-owned warehouse query."""
    return config.warehouse is None or warehouse_dialect(config).process_local_queries


def process_ended(attempt: controls.ControlAttempt) -> bool:
    """Prove a local birth ended; unreadable identity is unknown, never exit evidence."""
    if attempt.host != socket.gethostname() or attempt.pid is None or attempt.process_start is None:
        return False
    birth = process_start(attempt.pid)
    if birth is not None:
        return birth != attempt.process_start
    try:
        return psutil.Process(attempt.pid).status() == psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return True
    except psutil.AccessDenied:
        return False


def abort_requested(engine: Engine, run_id: int, task_id: int) -> bool:
    """Whether an agent must terminate this task, never another task's process."""
    with engine.connect() as conn:
        fences = controls.blocked(conn, run_id)
    return ControlAction.ABORT in (fences.get(None), fences.get(task_id))


@acting_as(SYSTEM_ACTOR)
def advance(engine: Engine, run_id: int | None = None, *, observed: bool = False) -> None:
    """Observe pinned work and finish controls only when actual exit/query evidence exists."""
    with engine.connect() as conn:
        rows = conn.execute(statement(conn, "control_pending"), {"run_id": run_id}).all()
    for row in rows:
        with engine.begin() as conn:
            controls.lock_run(conn, row.pipeline_run_id)
            current = controls.get(conn, row.control_id)
            if current.status == ControlStatus.COMPLETED:
                continue
            attempts = controls.affected(conn, tuple(json.loads(row.attempt_ids)))
            for attempt in attempts:
                if attempt.status == AttemptStatus.QUEUED:
                    transitions.cancel_attempt(
                        conn,
                        attempt.attempt_id,
                        current_actor(),
                        error_message=f"control_id={row.control_id}: {row.action}",
                    )
                    controls.observe_exit(conn, attempt.attempt_id, queries_ended=True)
            attempts = controls.affected(conn, current.attempt_ids)
            waiting = [
                a.attempt_id
                for a in attempts
                if a.process_ended_at is None or a.queries_ended_at is None
            ]
            if waiting and not observed:
                continue
            if waiting:
                controls.update(
                    conn,
                    row.control_id,
                    ControlStatus.ACKNOWLEDGED,
                    f"waiting for process/query endings of attempts {waiting}; "
                    "a request or expired lease is not completion evidence",
                )
                continue
            if row.task_id is None:
                lease = conn.execute(
                    statement(conn, "run_lease"), {"row_id": row.pipeline_run_id}
                ).one()
                # A live supervisor observes the same durable fence; no process is adopted.
                if lease.status in {RunStatus.QUEUED, RunStatus.IN_PROGRESS}:
                    transitions.cancel_pipeline_run(conn, row.pipeline_run_id)
            controls.update(
                conn,
                row.control_id,
                ControlStatus.COMPLETED,
                f"{row.action} completed; affected agent processes and "
                "engine-owned warehouse queries have ended; successful evidence is kept",
            )


def after_exit(engine: Engine, run_id: int) -> None:
    """Let an independently started task acknowledge controls without a pipeline scheduler."""
    advance(engine, run_id, observed=True)


@acting_as(SYSTEM_ACTOR)
def reconcile_endings(
    engine: Engine,
    config: ConnectorConfig,
    attempt_ids: tuple[int, ...],
    *,
    pipeline_id: int | None = None,
    task_id: int | None = None,
) -> None:
    """Record physical endings after reconciliation, retaining unknown server query status."""
    with engine.begin() as conn:
        pending = set(attempt_ids)
        rows = conn.execute(statement(conn, "control_pending"), {"run_id": None}).all()
        for row in rows:
            request = controls.get(conn, row.control_id)
            if pipeline_id is None or request.pipeline_id == pipeline_id:
                pending.update(
                    a.attempt_id
                    for a in request.attempts
                    if task_id is None or a.task_id == task_id
                )
        for attempt in controls.affected(conn, tuple(pending)):
            if attempt.status not in ACTIVE_ATTEMPTS and process_ended(attempt):
                controls.observe_exit(
                    conn, attempt.attempt_id, queries_ended=process_local_queries(config)
                )
    advance(engine, observed=True)
