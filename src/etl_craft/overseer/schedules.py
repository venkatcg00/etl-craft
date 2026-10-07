"""Create durable schedule ticks, retaining only each definition's next deadline."""

from __future__ import annotations

import threading
from collections import deque
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy.engine import Engine

from etl_craft.core.actor import Actor, ActorKind, acting_as
from etl_craft.core.cron import next_after, timezone
from etl_craft.core.enums import InterventionAction
from etl_craft.core.errors import MetadataError
from etl_craft.engine import transitions
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.interventions import record_intervention
from etl_craft.execution.leases import as_utc
from etl_craft.services.operations import OperationContext

SCHEDULE_ACTOR = Actor("schedule", ActorKind.SCHEDULE)


class Schedules:
    """Cache one future tick per active definition; history is the restart cursor."""

    def __init__(self) -> None:
        """Start with no deadlines until the Engine DB has been read."""
        self.deadlines: dict[int, tuple[tuple[object, ...], datetime]] = {}

    def refresh(
        self, ctx: OperationContext, now: datetime, stop: threading.Event | None = None
    ) -> None:
        """Record missed ticks and enqueue due work in short transactions."""
        with ctx.engine.connect() as conn:
            rows = conn.execute(statement(conn, "overseer_schedules")).all()
        active = {r.pipeline_id for r in rows if r.run_schedule is not None}
        self.deadlines = {p: v for p, v in self.deadlines.items() if p in active}
        for row in rows:
            if stop is not None and stop.is_set():
                return
            if row.run_schedule is None:
                continue
            signature = (
                row.run_schedule,
                row.schedule_timezone,
                row.schedule_start_date,
                row.last_run_key,
            )
            cached = self.deadlines.get(row.pipeline_id)
            if cached is not None and cached[0] == signature and cached[1] > now:
                continue
            try:
                zone = timezone(
                    row.schedule_timezone
                    if row.schedule_timezone is not None
                    else ctx.config.timezone
                )
                if row.schedule_start_date is not None:
                    start = date.fromisoformat(str(row.schedule_start_date))
                    cursor = datetime.combine(start, time(), zone).astimezone(UTC) - timedelta(
                        microseconds=1
                    )
                else:
                    cursor = as_utc(row.create_date) if row.create_date is not None else now
            except ValueError as error:
                raise MetadataError(
                    f"{row.pipeline_code}: SCHEDULE_START_DATE={row.schedule_start_date!r}; "
                    "expected YYYY-MM-DD; correct the schedule"
                ) from error
            if row.last_run_key is not None:
                try:
                    previous = datetime.fromisoformat(row.last_run_key.removeprefix("schedule:"))
                    if previous.utcoffset() != timedelta(0) or not row.last_run_key.startswith(
                        "schedule:"
                    ):
                        raise ValueError("expected a UTC schedule key")
                    cursor = max(cursor, previous)
                except ValueError as error:
                    raise MetadataError(
                        f"{row.pipeline_code}: scheduled RUN_KEY={row.last_run_key!r}; "
                        "expected schedule:<UTC ISO instant>; repair the historical key"
                    ) from error
            tick = next_after(row.run_schedule, cursor, zone)
            retained: deque[datetime] = deque()
            count = row.max_catchup_runs if row.catchup == "Y" else 1
            with acting_as(SCHEDULE_ACTOR):
                while tick <= now:
                    if stop is not None and stop.is_set():
                        return
                    if len(retained) == count:
                        _create_tick(
                            ctx.engine,
                            row.pipeline_id,
                            zone,
                            retained.popleft(),
                            "missed while no overseer was running",
                        )
                    retained.append(tick)
                    tick = next_after(row.run_schedule, tick, zone)
                for instant in retained:
                    if stop is not None and stop.is_set():
                        return
                    _create_tick(
                        ctx.engine,
                        row.pipeline_id,
                        zone,
                        instant,
                        "previous run still active"
                        if row.pending and row.overlap_policy == "SKIP"
                        else None,
                    )
            self.deadlines[row.pipeline_id] = (signature, tick)


def _create_tick(
    engine: Engine, pipeline_id: int, zone: ZoneInfo, instant: datetime, reason: str | None
) -> None:
    """Commit one tick and its reason without holding the writer lock across catch-up."""
    with engine.begin() as conn:
        key = "schedule:" + instant.isoformat()
        existing = conn.execute(
            statement(conn, "scheduled_tick_exists"),
            {"pipeline_id": pipeline_id, "run_key": key},
        ).scalar_one_or_none()
        if existing is not None:
            return
        status = "SKIPPED" if reason else "QUEUED"
        run_id = transitions.create_run(
            conn,
            pipeline_id,
            SCHEDULE_ACTOR,
            run_date=instant.astimezone(zone).date(),
            trigger_kind="SCHEDULE",
            run_key=key,
            status=status,
        )
        record_intervention(
            conn,
            pipeline_id=pipeline_id,
            pipeline_run_id=run_id,
            action=InterventionAction.NEW_RUN,
            reason=reason or "scheduled tick",
            requested_by=SCHEDULE_ACTOR.name,
            to_status=status,
        )
