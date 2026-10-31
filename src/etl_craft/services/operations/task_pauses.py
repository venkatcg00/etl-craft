"""Standing task pause changes, shared by CLI, API and bulk controls."""

from datetime import UTC, datetime

from etl_craft.core.enums import ControlAction, RunStatus
from etl_craft.core.errors import RunStateError, UsageError
from etl_craft.core.time import as_utc
from etl_craft.engine import standing_pauses
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.repository.tasks import resolve_task_id
from etl_craft.engine.stewardship import refuse_bypass
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.models import OperationResult
from etl_craft.services.operations.requests import ControlRequest


def change(ctx: OperationContext, request: ControlRequest) -> OperationResult:
    """Close and open versions in one transaction, without an observable unpaused window."""
    now = datetime.now(UTC)
    if request.then is not None:
        raise UsageError(
            f"task {request.pipeline_code}.{request.task_code}: "
            f"--then={str(request.then)!r} is refused; "
            "tasks have no schedule ticks; omit --then to release held runs"
        )
    if request.expires_at is not None and as_utc(request.expires_at) <= now:
        raise UsageError(
            f"--expires-at={request.expires_at.isoformat()!r}; expected a future ISO-8601 "
            "timestamp with an offset; supply a future timestamp"
        )
    with ctx.engine.begin() as conn:
        pipeline_id = resolve_pipeline_id(conn, request.pipeline_code)
        standing_pauses.lock(conn, pipeline_id)
        assert request.task_code is not None
        previous = next(
            (
                pause
                for pause in standing_pauses.current(conn, pipeline_id)
                if pause.task_code == request.task_code
            ),
            None,
        )
        task_id = (
            previous.task_id
            if previous is not None
            else resolve_task_id(conn, pipeline_id, request.task_code)
        )
        if request.action == ControlAction.PAUSE:
            refuse_bypass(conn, pipeline_id, task_id, request.outcome, "standing task pause")
        active = previous is not None and previous.active(now)
        identity = f"{request.pipeline_code}.{request.task_code}"
        if request.action == ControlAction.PAUSE:
            if active and not request.replace:
                assert previous is not None
                raise RunStateError(
                    f"{identity}: already paused by {previous.describe()}; use --replace "
                    "to close that version and open a replacement atomically"
                )
            if request.replace and not active:
                raise RunStateError(
                    f"{identity}: --replace found no active task pause (absent or expired); "
                    "omit --replace to create a pause"
                )
        elif not active:
            raise RunStateError(
                f"{identity}: no active task pause (absent or expired); expected an active "
                "pause to resume; inspect status before resuming"
            )
        parameters = {
            "pipeline_id": pipeline_id,
            "task_id": task_id,
            "outcome": request.outcome,
            "expires_at": request.expires_at,
            "now": now,
            "actor": ctx.actor.name,
            "actor_kind": ctx.actor.kind,
            "reason": request.reason,
        }
        if previous is not None:
            conn.execute(
                statement(conn, "standing_pause_close"),
                {**parameters, "pause_id": previous.pause_id},
            )
        if request.action == ControlAction.PAUSE:
            pause_id = conn.execute(
                statement(conn, "standing_pause_insert"), parameters
            ).scalar_one()
            message = (
                f"{identity}: standing pause {pause_id}; actor={ctx.actor.name}, "
                f"outcome={request.outcome or 'hold'}, "
                f"expiry={request.expires_at or 'until resume'}, "
                f"reason={request.reason!r}"
            )
        else:
            message = f"{identity}: resumed; task pause and outcome removed; held runs released"
    return OperationResult(RunStatus.SUCCESS, message, pipeline_id, request.pipeline_code)
