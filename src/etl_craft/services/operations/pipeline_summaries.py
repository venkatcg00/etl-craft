"""Each pipeline's next scheduled run and newest runs, read for many pipelines at once."""

from collections.abc import Sequence
from dataclasses import replace
from datetime import datetime

from sqlalchemy.engine import Connection

from etl_craft.core.enums import RunStatus, SlaStatus
from etl_craft.core.errors import EtlCraftError
from etl_craft.engine.queries import statement
from etl_craft.engine.schedule_ticks import cursor, schedule
from etl_craft.services.operations.models import PipelineView, RecentRun

HORIZON_DAYS = 366
"""How far ahead a next scheduled run is looked for."""


def summarize(
    conn: Connection, views: Sequence[PipelineView], now: datetime, refresh_required: bool
) -> tuple[PipelineView, ...]:
    """Add the next scheduled run and the five newest runs to each view."""
    if not views:
        return ()
    recent: dict[int, list[RecentRun]] = {}
    for row in conn.execute(statement(conn, "pipeline_recent_runs")):
        recent.setdefault(row.pipeline_id, []).append(
            RecentRun(
                row.pipeline_run_id,
                RunStatus(row.status),
                None if row.sla_status is None else SlaStatus(row.sla_status),
            )
        )
    ticks = _next_runs(
        conn, {v.pipeline_id for v in views if v.paused is None}, now, refresh_required
    )
    return tuple(
        replace(
            view,
            next_run=ticks.get(view.pipeline_id, (None, None))[0],
            schedule_problem=ticks.get(view.pipeline_id, (None, None))[1],
            recent_runs=tuple(recent.get(view.pipeline_id, ())),
        )
        for view in views
    )


def _next_runs(
    conn: Connection, pipeline_ids: set[int], now: datetime, refresh_required: bool
) -> dict[int, tuple[datetime | None, str | None]]:
    """Find each scheduled pipeline's next running tick the way the controller does."""
    found: dict[int, tuple[datetime | None, str | None]] = {}
    rows = conn.execute(
        statement(conn, "controller_schedules"),
        {"pipeline_id": None, "refresh_required": refresh_required},
    )
    for row in rows:
        if row.run_schedule is None or row.pipeline_id not in pipeline_ids:
            continue
        try:
            zone, start = cursor(row._mapping, row.project_timezone, now)
            tick = schedule(conn, row._mapping, zone).first_run(max(start, now), HORIZON_DAYS)
        except EtlCraftError as error:
            found[row.pipeline_id] = (None, str(error))
            continue
        found[row.pipeline_id] = (
            (tick.instant, None)
            if tick is not None
            else (None, f"no run in the next {HORIZON_DAYS} days; check the schedule and calendar")
        )
    return found
