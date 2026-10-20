"""Actor-scoped run/task holds, graceful stops, aborts and durable request inspection."""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import ClassVar
from uuid import uuid4

from sqlalchemy.engine import Connection

from etl_craft.core.enums import (
    DEFINITION_CONTROLS,
    ActionName,
    AttemptStatus,
    ControlAction,
    ControlStatus,
    Handler,
    RunStatus,
)
from etl_craft.core.errors import (
    EtlCraftError,
    LockTimeoutError,
    RunRefusedError,
    RunStateError,
    UsageError,
)
from etl_craft.engine import controls, locks, run_scopes, runlog, transitions
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
from etl_craft.execution.manual import select_tasks
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.models import OperationResult
from etl_craft.services.operations.recovery import (
    IMMEDIATE_ACTIONS,
    RECOVERY_ACTIONS,
    check_recovery,
    control_effect,
    prepare_recovery,
)
from etl_craft.services.operations.requests import BulkControlRequest, ControlId, ControlRequest


def control(ctx: OperationContext, request: ControlRequest) -> ControlView | OperationResult:
    """Pause or resume a definition, or request a scoped control of one run or task."""
    if request.action in DEFINITION_CONTROLS:
        from etl_craft.services.operations.pipelines import set_pause

        if request.run_id is not None or request.run_key is not None or request.task_code:
            raise UsageError(
                f"{request.action} applies to the pipeline definition; omit `run_id`, `run_key` "
                "and `task_code`"
            )
        return set_pause(ctx, request)
    return request_control(ctx, request)


def request_control(ctx: OperationContext, request: ControlRequest) -> ControlView:
    """Commit the dispatch fence first; return acceptance separately from agent completion."""
    if request.action == ControlAction.RUN_PAUSED and request.selector == runlog.ACTIVE_RUN:
        return _new_paused(ctx, request)
    with control_scope(ctx, request) as run_id, ctx.engine.begin() as conn:
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
        attempts = check_control(conn, ctx, request, selected, task_id)
        control_id = _insert(conn, ctx, request, run_id, task_id, attempts)
        if request.action in RECOVERY_ACTIONS:
            prepare_recovery(conn, ctx, selected, request, control_id)
    control_flow.advance(ctx.engine, run_id)
    with ctx.engine.connect() as conn:
        return controls.get(conn, control_id)


def check_control(
    conn: Connection,
    ctx: OperationContext,
    request: ControlRequest,
    selected: runlog.RunIdentity,
    task_id: int | None,
) -> tuple[controls.ControlAttempt, ...]:
    """Share state/capability decisions between previews and accepted requests."""
    run_id = selected.pipeline_run_id
    if request.action in RECOVERY_ACTIONS:
        check_recovery(conn, selected, request.action, task_id)
    else:
        if request.action == ControlAction.RUN_PAUSED and task_id is not None:
            raise UsageError(
                "RUN_PAUSED grants one run permission; omit task_code to keep the "
                "definition pause intact"
            )
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
    if request.action in {ControlAction.HOLD, ControlAction.RELEASE}:
        pending = (
            conn.execute(
                statement(conn, "control_unfinished"), {"run_id": run_id, "task_id": task_id}
            )
            .scalars()
            .all()
        )
        if pending:
            raise RunRefusedError(
                f"run_id={run_id}: pending controls={pending}; inspect process/query "
                f"acknowledgements before changing this fence"
            )
    _check_query_control(ctx, request, attempts)
    return attempts


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
        if latest.action == request.action and not (
            request.action in RECOVERY_ACTIONS and request.request_key is not None
        ):
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
    immediate = request.action in IMMEDIATE_ACTIONS
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
                f"{request.action}: {control_effect(request.action)} "
                "Preparation committed; inspect execution_run_id separately from execution outcome."
                if immediate
                else f"{request.action} requested; waiting for actual agent/query endings"
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


def get_control(ctx: OperationContext, request: ControlId) -> ControlView:
    """Read durable evidence without turning observation into a control or a guessed outcome."""
    with ctx.engine.connect() as conn:
        return controls.get(conn, request.control_id)


@contextmanager
def control_scope(ctx: OperationContext, request: ControlRequest) -> Iterator[int]:
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


def _new_paused(ctx: OperationContext, request: ControlRequest) -> ControlView:
    """Create a keyed one-off context atomically with its definition-pause permission."""
    if request.request_key is None:
        raise UsageError(
            "RUN_PAUSED without a run selector creates a context; "
            "supply `request_key` for repeatable creation"
        )
    key = request.request_key
    lock = locks.EngineLock("control-context", locks.advisory_key(f"control-context:{key}"))
    with lock.hold(ctx.engine), ctx.engine.begin() as conn:
        pipeline_id = resolve_pipeline_id(conn, request.pipeline_code)
        task_id = (
            None
            if request.task_code is None
            else resolve_task_id(conn, pipeline_id, request.task_code)
        )
        keyed = conn.execute(statement(conn, "control_by_key"), {"request_key": key}).one_or_none()
        if keyed is not None:
            original = controls.get(conn, keyed.control_id)
            if (original.pipeline_id, original.task_id, original.action) != (
                pipeline_id,
                task_id,
                request.action,
            ):
                raise RunStateError(
                    f"request_key={key!r}: another scope/action already owns it; "
                    "reuse its original inputs or choose a new key"
                )
            return original
        run_key = f"control-paused:{key}"
        logical_date = runlog.today(ctx.config.timezone)
        if request.task_code is None:
            run_id = transitions.create_run(
                conn,
                pipeline_id,
                ctx.actor,
                run_key=run_key,
                run_date=logical_date,
                status=RunStatus.QUEUED,
            )
        else:
            scope = select_tasks(conn, pipeline_id, request.task_code)
            run_id = run_scopes.create_manual_run(
                conn, pipeline_id, ctx.actor, scope, run_key, logical_date
            )
        control_id = _insert(conn, ctx, request, run_id, task_id, ())
        return controls.get(conn, control_id)


@dataclass(frozen=True)
class BulkOutcome:
    """Keep the requested identity even when this object refuses its action."""

    pipeline_code: str
    task_code: str | None
    request_key: str
    action: ControlAction
    control: ControlView | OperationResult | None
    error: str | None


@dataclass(frozen=True)
class BulkControlView:
    """Inspect each object independently; a partial failure never rolls back other objects."""

    SCHEMA: ClassVar[str] = "etl-craft/bulk-controls/1"
    outcomes: tuple[BulkOutcome, ...]
    message: str


def request_bulk_controls(ctx: OperationContext, request: BulkControlRequest) -> BulkControlView:
    """Invoke the ordinary audited control once per object; one refusal never stops the rest."""
    from etl_craft.services.operations.actions import invoke

    outcomes = []
    for selected in request.requests:
        control, error = None, None
        try:
            done = invoke(ctx, ActionName.CONTROL, selected)
            assert isinstance(done, (ControlView, OperationResult))
            control = done
        except EtlCraftError as refused:
            error = str(refused)
        outcomes.append(
            BulkOutcome(
                selected.pipeline_code,
                selected.task_code,
                selected.request_key or "",
                selected.action,
                control,
                error,
            )
        )
    failures = sum(o.error is not None for o in outcomes)
    return BulkControlView(
        tuple(outcomes),
        f"{len(outcomes) - failures} accepted; {failures} refused; "
        "inspect each durable control for completion",
    )
