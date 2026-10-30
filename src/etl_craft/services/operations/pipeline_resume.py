"""Explicit schedule resume choices, committed with pause closure and created runs."""

from collections import deque
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from sqlalchemy.engine import Connection

from etl_craft.core.enums import InterventionAction, ResumeChoice, RunStatus, TriggerKind, YesNo
from etl_craft.core.errors import RunStateError, UsageError
from etl_craft.engine import runlog, standing_pauses
from etl_craft.engine.queries import statement
from etl_craft.engine.repository import pauses
from etl_craft.engine.repository.intervention_rows import record_intervention
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.run_creation import RunCreation, create
from etl_craft.engine.schedule_ticks import cursor, schedule
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.requests import ControlRequest


def resume(ctx: OperationContext, request: ControlRequest) -> str:
    """Serialize choice, new queued work and the durable next-tick boundary."""
    now = datetime.now(UTC)
    with ctx.engine.begin() as conn:
        pipeline_id = resolve_pipeline_id(conn, request.pipeline_code)
        standing_pauses.lock(conn, pipeline_id)
        pause = pauses.fetch_open_pause(conn, pipeline_id)
        if pause is None:
            raise RunStateError(
                f"{request.pipeline_code}: not paused; inspect status before resuming"
            )
        zone, ticks, skip_ticks = _plan(conn, ctx, request, pipeline_id, now)
        created = []
        if request.then == ResumeChoice.RUN_NOW:
            created.append(
                create(conn, pipeline_id, ctx.actor, RunCreation(status=RunStatus.QUEUED))
            )
        elif request.then == ResumeChoice.CATCH_UP:
            assert zone is not None
            for tick in ticks:
                key = "schedule:" + tick.isoformat()
                found = conn.execute(
                    statement(conn, "scheduled_tick_exists"),
                    {"pipeline_id": pipeline_id, "run_key": key},
                ).scalar_one_or_none()
                if found is None:
                    created.append(
                        create(
                            conn,
                            pipeline_id,
                            ctx.actor,
                            RunCreation(
                                run_date=tick.astimezone(zone).date(),
                                trigger_kind=TriggerKind.SCHEDULE,
                                run_key=key,
                                status=RunStatus.SKIPPED if skip_ticks else RunStatus.QUEUED,
                            ),
                        )
                    )
        for run_id in created:
            record_intervention(
                conn,
                pipeline_id=pipeline_id,
                pipeline_run_id=run_id,
                action=InterventionAction.NEW_RUN,
                reason=f"resume then={request.then}: {request.reason}",
                requested_by=ctx.actor.name,
            )
        reason = request.reason
        if request.then is not None:
            reason += f"; then={request.then}; runs created={created}"
        pauses.close_pause(conn, pipeline_id, ctx.actor.name, reason, now=now)
        active = runlog.fetch_active_pipeline_run_id(conn, pipeline_id)
    message = f"{request.pipeline_code}: resumed"
    if request.then is not None:
        message += f"; then={request.then}; runs created={created}"
    if active is not None:
        message += (
            f"; `etl-craft run --pipeline-code {request.pipeline_code}` goes on with "
            f"pipeline_run_id={active}"
        )
    return message


def _plan(
    conn: Connection,
    ctx: OperationContext,
    request: ControlRequest,
    pipeline_id: int,
    now: datetime,
) -> tuple[ZoneInfo | None, list[datetime], bool]:
    """Preview retained ticks and refuse missing or meaningless resume choices."""
    row = conn.execute(
        statement(conn, "controller_schedules"),
        {
            "refresh_required": bool(ctx.engine.get_execution_options().get("refresh_required")),
            "pipeline_id": pipeline_id,
        },
    ).one_or_none()
    ticks = []
    zone = None
    if row is not None and row.run_schedule is not None:
        zone, start = cursor(row._mapping, row.project_timezone, now)
        plan = schedule(conn, row._mapping, zone)
        retained: deque[datetime] = deque(
            maxlen=row.max_catchup_runs if row.catchup == YesNo.YES else 1
        )
        tick = plan.next_tick(start)
        while tick.instant <= now:
            if tick.skipped is None:
                retained.append(tick.instant)
            tick = plan.next_tick(tick.instant)
        ticks = list(retained)
        if request.then is None:
            raise UsageError(
                f"{request.pipeline_code}: scheduled resume requires "
                "--then run-now|catch-up|wait; "
                f"missed ticks retained by CATCHUP/MAX_CATCHUP_RUNS: "
                f"{', '.join(t.isoformat() for t in ticks) or '(none)'}; "
                f"catch-up would create {len(ticks)} run(s); choose --then"
            )
    elif request.then is not None:
        raise UsageError(
            f"{request.pipeline_code}: --then={str(request.then)!r} is refused: pipeline is "
            "unscheduled and has no schedule ticks; omit --then"
        )
    return zone, ticks, bool(row is not None and row.pending and row.overlap_policy == "SKIP")
