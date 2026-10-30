"""Create durable schedule ticks, retaining only each definition's next deadline."""

from __future__ import annotations

import threading
from collections import deque
from datetime import datetime

from sqlalchemy.engine import Engine

from etl_craft.controller.catalog import CatalogSchedule
from etl_craft.core.actor import acting_as, current_actor
from etl_craft.core.calendars import Tick
from etl_craft.core.enums import InterventionAction, RunStatus, TriggerKind, YesNo
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.intervention_rows import record_intervention
from etl_craft.engine.run_creation import RunCreation, create
from etl_craft.engine.schedule_ticks import cursor as schedule_cursor
from etl_craft.engine.schedule_ticks import schedule as pipeline_schedule
from etl_craft.services.operations import OperationContext


class Schedules:
    """Cache one future tick per active definition; history is the restart cursor."""

    def __init__(self) -> None:
        """Start with no deadlines until the Engine DB has been read."""
        self.catalog = CatalogSchedule()
        self.deadlines: dict[int, tuple[tuple[object, ...], datetime]] = {}

    def refresh(
        self, ctx: OperationContext, now: datetime, stop: threading.Event | None = None
    ) -> None:
        """Record missed ticks and enqueue due work in short transactions."""
        self.catalog.refresh(ctx, now, stop)
        if not ctx.config.allow_schedule:
            return
        with ctx.engine.connect() as conn:
            rows = conn.execute(
                statement(conn, "controller_schedules"),
                {
                    "pipeline_id": None,
                    "refresh_required": bool(
                        ctx.engine.get_execution_options().get("refresh_required")
                    ),
                },
            ).all()
        active = {r.pipeline_id for r in rows if r.run_schedule is not None}
        self.deadlines = {p: v for p, v in self.deadlines.items() if p in active}
        with ctx.engine.connect() as conn:
            paused = set(conn.execute(statement(conn, "schedule_paused_ids")).scalars())
        for row in rows:
            if stop is not None and stop.is_set():
                return
            if row.run_schedule is None or row.pipeline_id in paused:
                continue
            zone, cursor = schedule_cursor(row._mapping, row.project_timezone, now)
            with ctx.engine.connect() as conn:
                plan = pipeline_schedule(conn, row._mapping, zone)
            signature = (plan, row.schedule_start_date, row.last_run_key, row.resumed_at)
            cached = self.deadlines.get(row.pipeline_id)
            if cached is not None and cached[0] == signature and cached[1] > now:
                continue
            tick = plan.next_tick(cursor)
            retained: deque[Tick] = deque()
            count = row.max_catchup_runs if row.catchup == YesNo.YES else 1
            with acting_as(ctx.actor):
                while tick.instant <= now:
                    if stop is not None and stop.is_set():
                        return
                    if tick.skipped is not None:
                        _create_tick(ctx.engine, row.pipeline_id, tick, tick.skipped)
                    else:
                        if len(retained) == count:
                            _create_tick(
                                ctx.engine,
                                row.pipeline_id,
                                retained.popleft(),
                                "missed while no controller was running",
                            )
                        retained.append(tick)
                    tick = plan.next_tick(tick.instant)
                for kept in retained:
                    if stop is not None and stop.is_set():
                        return
                    _create_tick(
                        ctx.engine,
                        row.pipeline_id,
                        kept,
                        "previous run still active"
                        if row.pending and row.overlap_policy == "SKIP"
                        else None,
                    )
            self.deadlines[row.pipeline_id] = (signature, tick.instant)


def _create_tick(engine: Engine, pipeline_id: int, tick: Tick, reason: str | None) -> None:
    """Commit one tick and its reason without holding the writer lock across catch-up."""
    with engine.begin() as conn:
        key = "schedule:" + tick.instant.isoformat()
        existing = conn.execute(
            statement(conn, "scheduled_tick_exists"),
            {"pipeline_id": pipeline_id, "run_key": key},
        ).scalar_one_or_none()
        if existing is not None:
            return
        status = RunStatus.SKIPPED if reason else RunStatus.QUEUED
        run_id = create(
            conn,
            pipeline_id,
            current_actor(),
            RunCreation(
                run_date=tick.run_date,
                trigger_kind=TriggerKind.SCHEDULE,
                run_key=key,
                status=status,
            ),
        )
        record_intervention(
            conn,
            pipeline_id=pipeline_id,
            pipeline_run_id=run_id,
            action=InterventionAction.NEW_RUN,
            reason=reason or "scheduled tick",
            requested_by=current_actor().name,
            to_status=status,
        )
