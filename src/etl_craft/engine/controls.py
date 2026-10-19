"""Durable scoped control records and the admission fence shared by every agent."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import ClassVar

from sqlalchemy import bindparam
from sqlalchemy.engine import Connection

from etl_craft.core.enums import ControlAcknowledgement, ControlAction, ControlStatus, RunStatus
from etl_craft.core.errors import ResourceNotFoundError, RunRefusedError
from etl_craft.core.time import as_utc
from etl_craft.engine.queries import statement


@dataclass(frozen=True)
class ControlAttempt:
    """The process identity and observed query/agent endings of an affected attempt."""

    attempt_id: int
    status: str
    host: str | None
    pid: int | None
    process_start: str | None
    process_ended_at: datetime | None
    queries_ended_at: datetime | None
    owner_id: str | None
    task_id: int
    handler: str


@dataclass(frozen=True)
class ControlView:
    """One accepted request and its actual acknowledgements, independently observable."""

    SCHEMA: ClassVar[str] = "etl-craft/control/1"
    control_id: int
    request_key: str
    pipeline_run_id: int
    pipeline_id: int
    pipeline_code: str
    task_id: int | None
    task_code: str | None
    action: ControlAction
    status: ControlStatus
    attempt_ids: tuple[int, ...]
    requested_by: str
    requested_by_kind: str
    reason: str
    requested_at: datetime
    acknowledged_at: datetime | None
    completed_at: datetime | None
    message: str
    run_status: RunStatus
    attempts: tuple[ControlAttempt, ...]
    execution_run_id: int | None = None


def lock_run(conn: Connection, run_id: int) -> None:
    """Serialize admission and control changes against the exact same run row."""
    conn.execute(statement(conn, "control_lock_run"), {"run_id": run_id})


def blocked(conn: Connection, run_id: int) -> dict[int | None, ControlAction]:
    """Return each scope whose latest command retains a dispatch fence."""
    rows = conn.execute(statement(conn, "control_blocked"), {"run_id": run_id})
    return {row.task_id: ControlAction(row.action) for row in rows}


def assert_dispatch(conn: Connection, run_id: int, task_id: int) -> None:
    """Refuse even force/retry admissions while this task or its run is held/stopped."""
    lock_run(conn, run_id)
    fences = blocked(conn, run_id)
    action = fences.get(None) or fences.get(task_id)
    if action is not None:
        raise RunRefusedError(
            f"pipeline_run_id={run_id}, task_id={task_id}: dispatch is fenced by {action}; "
            "inspect the control request and release the selected scope after it completes"
        )


def guard_task(conn: Connection, task_run_id: int) -> None:
    """Fence queuing under a bound task summary."""
    row = conn.execute(
        statement(conn, "control_task_context"), {"task_run_id": task_run_id}
    ).one_or_none()
    if row is None:
        return
    assert_dispatch(conn, row.pipeline_run_id, row.task_id)


def guard_attempt(conn: Connection, attempt_id: int) -> None:
    """Fence the queued-to-owned transition as well as initial queue creation."""
    row = conn.execute(
        statement(conn, "control_attempt_context"), {"attempt_id": attempt_id}
    ).one_or_none()
    if row is None:
        return
    assert_dispatch(conn, row.pipeline_run_id, row.task_id)


def affected(conn: Connection, attempt_ids: tuple[int, ...]) -> tuple[ControlAttempt, ...]:
    """Read pinned attempts; never replace a request's attempts with a newer retry."""
    if not attempt_ids:
        return ()
    query = statement(conn, "control_attempts").bindparams(bindparam("attempt_ids", expanding=True))
    result = []
    for row in conn.execute(query, {"attempt_ids": attempt_ids}):
        values = dict(row._mapping)
        for key in ("process_ended_at", "queries_ended_at"):
            if values[key] is not None:
                values[key] = as_utc(values[key])
        result.append(ControlAttempt(**values))
    return tuple(result)


def get(conn: Connection, control_id: int) -> ControlView:
    """Read a request and its observed evidence, including after its agent disconnects."""
    row = conn.execute(statement(conn, "control_get"), {"control_id": control_id}).one_or_none()
    if row is None:
        raise ResourceNotFoundError(
            f"control_id={control_id}: no request exists; use the id returned by the control action"
        )
    values = dict(row._mapping)
    values["attempt_ids"] = tuple(json.loads(values["attempt_ids"]))
    values["attempts"] = affected(conn, values["attempt_ids"])
    for key in ("requested_at", "acknowledged_at", "completed_at"):
        if values[key] is not None:
            values[key] = as_utc(values[key])
    values["action"] = ControlAction(values["action"])
    values["status"] = ControlStatus(values["status"])
    values["run_status"] = RunStatus(values["run_status"])
    return ControlView(**values)


def observe_exit(conn: Connection, attempt_id: int, *, queries_ended: bool) -> None:
    """Record physical process exit only after the owning supervisor observed it."""
    kinds = [ControlAcknowledgement.PROCESS]
    if queries_ended:
        kinds.append(ControlAcknowledgement.QUERIES)
    for kind in kinds:
        acknowledge(conn, attempt_id, kind)


def acknowledge(conn: Connection, attempt_id: int, kind: ControlAcknowledgement) -> None:
    """Append one observation without overwriting an earlier acknowledgement."""
    conn.execute(
        statement(conn, "control_observe_exit"),
        {"attempt_id": attempt_id, "kind": kind, "now": datetime.now(UTC)},
    )


def update(conn: Connection, control_id: int, status: ControlStatus, message: str) -> None:
    """Advance a request without rewriting its original actor, reason, scope or attempts."""
    from etl_craft.engine import transitions

    now = datetime.now(UTC)
    transitions.update_control(
        conn,
        {
            "control_id": control_id,
            "status": status,
            "message": message,
            "now": now,
            "completed_at": now if status == ControlStatus.COMPLETED else None,
        },
    )


def scope_attempts(
    conn: Connection, run_id: int, task_id: int | None
) -> tuple[ControlAttempt, ...]:
    """Read candidate agents before pinning the exact affected attempt identities."""
    rows = conn.execute(
        statement(conn, "control_scope_attempts"), {"run_id": run_id, "task_id": task_id}
    )
    return tuple(ControlAttempt(**dict(row._mapping)) for row in rows)


def permits_paused(conn: Connection, run_id: int) -> bool:
    """Check one-run permission without changing the definition pause."""
    return bool(
        conn.execute(statement(conn, "control_permits_paused"), {"run_id": run_id}).scalar()
    )
