"""Clones share maintenance ticks and never backlog missed intervals."""

import threading
from dataclasses import replace
from datetime import UTC, datetime, timedelta

from etl_craft.config import CloningConfig
from etl_craft.controller.cloning import clone_due
from etl_craft.core.actor import Actor, ActorKind
from etl_craft.core.enums import ApiRole
from etl_craft.services.operations import OperationContext
from fixtures.metadata import add_pipeline


def test_schedule_coalesces_missed_ticks_and_stops_without_running(engine_db, monkeypatch):
    from etl_craft.controller import cloning

    engine = engine_db.engine
    with engine.begin() as conn:
        add_pipeline(conn, "P")
    config = replace(engine_db.config, cloning=CloningConfig(enabled=True, every_minutes=15))
    ctx = OperationContext(engine, config, Actor("service", ActorKind.SERVICE), role=ApiRole.ADMIN)
    calls = []
    monkeypatch.setattr(cloning, "invoke", lambda selected, *_: calls.append(selected))
    now = datetime(2026, 1, 1, tzinfo=UTC)
    deadlines = {}
    stop = threading.Event()
    clone_due(ctx, now, stop, deadlines)
    clone_due(ctx, now + timedelta(minutes=1), stop, deadlines)
    assert len(calls) == 1
    clone_due(ctx, now + timedelta(minutes=90), stop, deadlines)
    assert len(calls) == 2
    assert deadlines[1] == now + timedelta(minutes=105)
    stop.set()
    clone_due(ctx, now + timedelta(minutes=120), stop, deadlines)
    assert len(calls) == 2
    assert calls[0].actor.kind == ActorKind.SYSTEM


def test_maintenance_admits_only_one_worker_and_coalesces_requests(engine_db, monkeypatch):
    from etl_craft.controller import retention

    reached, release = threading.Event(), threading.Event()
    calls = []

    def work(*args):
        calls.append(args[1])
        reached.set()
        assert release.wait(timeout=10)

    ctx = OperationContext(engine_db.engine, engine_db.config, Actor("service", ActorKind.SERVICE))
    monkeypatch.setattr(retention, "maintain", work)
    schedule = retention.RetentionSchedule()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    stop = threading.Event()
    try:
        schedule.refresh(ctx, now, stop)
        assert reached.wait(timeout=10)
        for minutes in range(1, 100):
            schedule.refresh(ctx, now + timedelta(minutes=minutes), stop)
        assert len(calls) == 1
    finally:
        release.set()
        schedule.close()
