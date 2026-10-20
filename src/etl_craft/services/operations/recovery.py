"""Prepare exact recovery and restart contexts without resetting shared checkpoints."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy.engine import Connection

from etl_craft.core.enums import AttemptStatus, ControlAction, RunStatus
from etl_craft.core.errors import RunRefusedError
from etl_craft.engine import controls, run_scopes, runlog, transitions
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.dependencies import (
    fetch_pipeline_dependency_edges,
    fetch_pipeline_graph,
)
from etl_craft.engine.repository.tasks import resolve_task_id
from etl_craft.services.operations.context import OperationContext

if TYPE_CHECKING:
    from etl_craft.services.operations.requests import ControlRequest

RECOVERY_ACTIONS = frozenset({ControlAction.RECOVER, ControlAction.RESTART})
IMMEDIATE_ACTIONS = (
    frozenset({ControlAction.HOLD, ControlAction.RELEASE, ControlAction.RUN_PAUSED})
    | RECOVERY_ACTIONS
)


def check_recovery(
    conn: Connection, selected: runlog.RunIdentity, action: ControlAction, task_id: int | None
) -> None:
    """Require ended owners and evidence before reopening or replacing their work."""
    run_id = selected.pipeline_run_id
    lease = conn.execute(statement(conn, "run_lease"), {"row_id": run_id}).one()
    pending = (
        conn.execute(statement(conn, "control_unfinished"), {"run_id": run_id, "task_id": task_id})
        .scalars()
        .all()
    )
    attempts = controls.scope_attempts(conn, run_id, task_id)
    if (
        lease.owner_id is not None
        or pending
        or any(
            a.status in {AttemptStatus.QUEUED, AttemptStatus.CLAIMED, AttemptStatus.RUNNING}
            or a.process_ended_at is None
            or a.queries_ended_at is None
            for a in attempts
        )
    ):
        raise RunRefusedError(
            f"run_id={run_id}: {action} requires ended ownership and process/query evidence; "
            f"owner={lease.owner_id!r}, pending_controls={pending}. "
            "Stop and acknowledge the exact work, then reconcile expired ownership; "
            "unknown server queries require their warehouse adapter."
        )
    if action == ControlAction.RESTART:
        if selected.status not in {
            RunStatus.SUCCESS,
            RunStatus.FAILED,
            RunStatus.CANCELLED,
            RunStatus.SKIPPED,
        }:
            raise RunRefusedError(
                f"run_id={run_id} is {selected.status}; RESTART needs a terminal source. "
                f"Stop it first, or RECOVER unfinished work."
            )
        restart_selection(conn, selected, task_id)
    elif selected.status in {RunStatus.SUCCESS, RunStatus.SKIPPED, RunStatus.QUEUED}:
        raise RunRefusedError(
            f"run_id={run_id} is {selected.status}; RECOVER needs unfinished or failed "
            f"work. Start its queued run or use RESTART for a new context."
        )
    elif (
        task_id is not None
        and selected.status != RunStatus.IN_PROGRESS
        and run_scopes.fetch_selection(conn, run_id) is None
    ):
        raise RunRefusedError(
            f"run_id={run_id}: task-only recovery cannot reopen a terminal "
            f"whole-pipeline context without admitting its siblings. Recover the whole "
            f"run, use an isolated task RESTART, or explicitly rerun the task."
        )
    elif task_id is not None and None in controls.blocked(conn, run_id):
        raise RunRefusedError(
            f"run_id={run_id}: the whole run is fenced; task recovery cannot release "
            f"another scope. Recover or release the run first."
        )


def restart_selection(
    conn: Connection, selected: runlog.RunIdentity, task_id: int | None
) -> run_scopes.TaskSelection | None:
    """Enforce the same isolation rules as any manual selection."""
    stored = run_scopes.fetch_selection(conn, selected.pipeline_run_id)
    if task_id is None:
        if stored is not None:
            stored.exclusive(conn)
        return stored
    scope = run_scopes.TaskSelection(
        (task_id,), False if stored is None else stored.ignore_dependencies
    )
    pipeline_id = (
        conn.execute(statement(conn, "transition_row_run"), {"row_id": selected.pipeline_run_id})
        .one()
        .pipeline_id
    )
    if fetch_pipeline_dependency_edges(conn, pipeline_id) and not scope.ignore_dependencies:
        raise RunRefusedError(
            f"task_id={task_id}: pipeline dependencies cannot be inherited by an "
            f"isolated restart. Restart the whole run, or use an explicit standalone "
            f"dependency override with a reason."
        )
    run_scopes.selected_graph(fetch_pipeline_graph(conn, pipeline_id), scope)
    scope.exclusive(conn)
    return scope


def prepare_recovery(
    conn: Connection,
    ctx: OperationContext,
    selected: runlog.RunIdentity,
    request: ControlRequest,
    control_id: int,
) -> None:
    """Commit the new context or reopen the old one with its control in one transaction."""
    if request.action == ControlAction.RECOVER:
        if selected.status != RunStatus.IN_PROGRESS:
            transitions.reopen_run(conn, selected.pipeline_run_id, ctx.actor, reason=request.reason)
        return
    row = conn.execute(
        statement(conn, "transition_row_run"), {"row_id": selected.pipeline_run_id}
    ).one()
    scope = restart_selection(
        conn,
        selected,
        None
        if request.task_code is None
        else resolve_task_id(conn, row.pipeline_id, request.task_code),
    )
    key = f"control-restart:{control_id}"
    if scope is None:
        transitions.create_run(
            conn,
            row.pipeline_id,
            ctx.actor,
            run_date=selected.run_date,
            run_key=key,
            status=RunStatus.QUEUED,
        )
    else:
        run_scopes.create_manual_run(
            conn, row.pipeline_id, ctx.actor, scope, key, selected.run_date
        )


def control_effect(action: ControlAction) -> str:
    """Describe data effects in both capability previews and durable acceptance."""
    if action == ControlAction.RESTART:
        return (
            "Create a new run; keep source history, current offsets/watermarks and "
            "human locks. Actions such as APPEND may append again; inspect handler "
            "effects before execution."
        )
    if action == ControlAction.RECOVER:
        return (
            "Resume the same run; keep SUCCESS/SKIPPED, committed checkpoints, consumer "
            "decisions and human locks."
        )
    if action == ControlAction.RUN_PAUSED:
        return "Permit this exact run while automatic execution remains paused."
    return "Act only on the selected scope; preserve successful attempts and human data locks."
