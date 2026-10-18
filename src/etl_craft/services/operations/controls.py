"""Actor-scoped run/task holds, graceful stops, aborts and durable request inspection."""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy.engine import Connection

from etl_craft.core.enums import (
    AttemptStatus,
    ControlAction,
    ControlStatus,
    Handler,
    Mode,
    RunStatus,
)
from etl_craft.core.errors import LockTimeoutError, RunRefusedError, RunStateError, UsageError
from etl_craft.engine import controls, locks, run_scopes, runlog
from etl_craft.engine.controls import ControlView
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.repository.tasks import (
    fetch_task_codes,
    fetch_task_execution_detail,
    fetch_task_parameters,
    resolve_task_id,
)
from etl_craft.execution import control_flow
from etl_craft.services.operations.context import OperationContext, operation


@dataclass(frozen=True)
class ControlRequest:
    """One exact run or task scope; repeated keys retain the original request identity."""

    pipeline_code: str
    action: ControlAction
    reason: str
    selector: runlog.RunSelector = runlog.ACTIVE_RUN
    task_code: str | None = None
    request_key: str | None = None

    def __post_init__(self) -> None:
        """Validate the action boundary before changing any admission or attempt state."""
        if not isinstance(self.action, ControlAction):
            raise UsageError(
                f"control action={self.action!r}; expected a ControlAction enum; "
                "use HOLD, RELEASE, STOP or ABORT"
            )
        if not self.reason.strip():
            raise UsageError("control reason is empty; give a nonblank reason for this action")
        if self.request_key is not None and (
            not self.request_key.strip() or len(self.request_key) > 128
        ):
            raise UsageError("control request_key must contain 1 to 128 characters; use a new UUID")


def request_control(ctx: OperationContext, request: ControlRequest) -> ControlView:
    """Commit the dispatch fence first; return acceptance separately from worker completion."""
    command = {ControlAction.HOLD: "pause", ControlAction.RELEASE: "resume"}.get(
        request.action, "cancel"
    )
    args = {
        "pipeline_code": request.pipeline_code,
        "task_code": request.task_code,
        "action": request.action,
        "reason": request.reason,
        "run_id": request.selector.run_id,
        "run_key": request.selector.run_key,
        "request_key": request.request_key,
    }
    with operation(ctx, command, args):
        if ctx.config.mode != Mode.LOCAL:
            raise RunRefusedError(
                f"{request.pipeline_code}: legacy external-orchestrator mode has no owned "
                "worker control adapter; stop/hold the exact task in that orchestrator. "
                "Native controls require a local owning worker."
            )
        with _event_locks(ctx, request) as run_id, ctx.engine.begin() as conn:
            pipeline_id = resolve_pipeline_id(conn, request.pipeline_code)
            controls.lock_run(conn, run_id)
            selected = runlog.select_run(conn, pipeline_id, runlog.RunSelector(run_id=run_id))
            task_id = (
                None
                if request.task_code is None
                else resolve_task_id(conn, pipeline_id, request.task_code)
            )
            if task_id is not None:
                run_scopes.check_task(conn, run_id, task_id)
            existing = _existing(conn, request, run_id, task_id)
            if existing is not None:
                return controls.get(conn, existing)
            if selected.status not in {RunStatus.QUEUED, RunStatus.IN_PROGRESS}:
                raise RunStateError(
                    f"run_id={run_id} is {selected.status}; controls need a non-terminal run. "
                    "Start a new run or inspect the completed request instead."
                )
            attempts = tuple(
                a
                for a in controls.scope_attempts(conn, run_id, task_id)
                if a.status in control_flow.ACTIVE_ATTEMPTS
                or a.status == AttemptStatus.LOST
                or a.process_ended_at is None
                or a.queries_ended_at is None
            )
            _check_query_control(ctx, request, attempts)
            control_id = _insert(conn, ctx, request, run_id, task_id, attempts)
        control_flow.advance(ctx.engine, run_id)
        with ctx.engine.connect() as conn:
            return controls.get(conn, control_id)


def _existing(
    conn: Connection, request: ControlRequest, run_id: int, task_id: int | None
) -> int | None:
    """Idempotency keys cannot silently change their scope or action."""
    if request.request_key is not None:
        keyed = conn.execute(
            statement(conn, "control_by_key"), {"request_key": request.request_key}
        ).one_or_none()
        if keyed is not None:
            if (keyed.pipeline_run_id, keyed.task_id, keyed.action) != (
                run_id,
                task_id,
                request.action,
            ):
                raise RunStateError(
                    f"request_key={request.request_key!r}: control_id={keyed.control_id} "
                    "belongs to another scope/action; reuse its original inputs or use a new key"
                )
            return int(keyed.control_id)
    latest = conn.execute(
        statement(conn, "control_latest"), {"run_id": run_id, "task_id": task_id}
    ).one_or_none()
    if latest is not None:
        if latest.action == request.action:
            return int(latest.control_id)
        if latest.status != ControlStatus.COMPLETED and (
            request.action in {ControlAction.HOLD, ControlAction.RELEASE}
            or (latest.action == ControlAction.ABORT and request.action == ControlAction.STOP)
        ):
            raise RunRefusedError(
                f"control_id={latest.control_id} is {latest.status}; inspect its process/query "
                "acknowledgements before changing this fence; an expired lease is not completion"
            )
    return None


def _check_query_control(
    ctx: OperationContext, request: ControlRequest, attempts: tuple[controls.ControlAttempt, ...]
) -> None:
    if request.action == ControlAction.STOP and not control_flow.process_local_queries(ctx.config):
        unknown = [
            a.attempt_id
            for a in attempts
            if a.handler == Handler.PYTHON and a.status != AttemptStatus.QUEUED
        ]
        if unknown:
            raise RunRefusedError(
                f"{request.pipeline_code}: Python attempts {unknown} cannot acknowledge all "
                "server-side script queries; scoped STOP needs a query adapter. "
                "Use definition pause to stop new dispatch while the script finishes."
            )
    if request.action != ControlAction.ABORT:
        return
    unsafe = [
        a.attempt_id
        for a in attempts
        if a.status != AttemptStatus.QUEUED and not control_flow.process_local_queries(ctx.config)
    ]
    if unsafe:
        raise RunRefusedError(
            f"{request.pipeline_code}: attempts {unsafe} belong to a server-side "
            "warehouse deployment "
            "without a cancellation acknowledgement adapter. ABORT is refused before any "
            "signal or status change; use graceful STOP to let those queries finish."
        )


def _insert(
    conn: Connection,
    ctx: OperationContext,
    request: ControlRequest,
    run_id: int,
    task_id: int | None,
    attempts: tuple[controls.ControlAttempt, ...],
) -> int:
    now = datetime.now(UTC)
    immediate = request.action in {ControlAction.HOLD, ControlAction.RELEASE}
    status = ControlStatus.COMPLETED if immediate else ControlStatus.REQUESTED
    inserted = conn.execute(
        statement(conn, "control_insert"),
        {
            "request_key": request.request_key or str(uuid4()),
            "run_id": run_id,
            "task_id": task_id,
            "action": request.action,
            "status": status,
            "attempt_ids": json.dumps([] if immediate else [a.attempt_id for a in attempts]),
            "actor": ctx.actor.name,
            "actor_kind": ctx.actor.kind,
            "reason": request.reason,
            "now": now,
            "acknowledged_at": now if immediate else None,
            "completed_at": now if immediate else None,
            "message": (
                f"{request.action}: dispatch fence committed; running attempts continue"
                if immediate
                else f"{request.action} requested; waiting for actual worker/query endings"
            ),
        },
    ).scalar_one_or_none()
    if inserted is not None:
        return int(inserted)
    existing = _existing(conn, request, run_id, task_id)
    if existing is not None:
        return existing
    raise RunStateError(
        f"request_key={request.request_key!r} conflicts with an existing request; "
        "inspect the original control or submit a new unique key"
    )


def get_control(ctx: OperationContext, control_id: int) -> ControlView:
    """Read durable evidence without turning observation into a control or a guessed outcome."""
    with operation(ctx, "status", {"control_id": control_id}), ctx.engine.connect() as conn:
        return controls.get(conn, control_id)


@contextmanager
def _event_locks(ctx: OperationContext, request: ControlRequest) -> Iterator[int]:
    """Finish any active probe under its existing lock before fencing its scope."""
    with ctx.engine.connect() as conn:
        pipeline_id = resolve_pipeline_id(conn, request.pipeline_code)
        selected = runlog.select_run(conn, pipeline_id, request.selector)
        scope = run_scopes.fetch_selection(conn, selected.pipeline_run_id)
        tasks = fetch_task_codes(conn, pipeline_id)
        event_ids = [
            task_id
            for task_id, code in tasks.items()
            if (request.task_code is None or code == request.task_code)
            and (scope is None or task_id in scope.task_ids)
            and runlog.fetch_task_run_status(conn, task_id, selected.pipeline_run_id)
            not in {RunStatus.SUCCESS, RunStatus.SKIPPED}
            and fetch_task_execution_detail(conn, task_id).handler == Handler.EVENT_WAIT
        ]
        for task_id in event_ids:
            if (
                request.action in {ControlAction.STOP, ControlAction.ABORT}
                and fetch_task_parameters(conn, task_id).get("EVENT_SQL")
                and not control_flow.process_local_queries(ctx.config)
            ):
                raise RunRefusedError(
                    f"task_id={task_id}: EVENT_SQL uses server-side queries without a durable "
                    "query acknowledgement adapter; native scoped controls are unavailable. "
                    "Use definition pause to stop new dispatch, "
                    "or wait for a successful event check."
                )
    try:
        with ExitStack() as stack:
            for task_id in sorted(event_ids):
                stack.enter_context(locks.event(task_id).hold(ctx.engine, wait_seconds=0.1))
            yield selected.pipeline_run_id
    except LockTimeoutError as error:
        raise RunRefusedError(
            f"{request.pipeline_code}: an event probe is active; no control was accepted. "
            "Retry after its query finishes; an active probe cannot be acknowledged as stopped."
        ) from error
