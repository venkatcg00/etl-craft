"""Catalog ticks survive controller restarts without holding up task supervision."""

import threading
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from etl_craft.controller.leadership import leadership
from etl_craft.controller.schedules import Schedules
from etl_craft.core.actor import Actor, ActorKind
from etl_craft.core.enums import ActionOutcome, ApiRole
from etl_craft.core.errors import ExitCode, MetadataError
from etl_craft.engine import audit
from etl_craft.services.operations import OperationContext, documents

BASE = datetime(2026, 1, 2, 10, tzinfo=UTC)


@pytest.fixture
def catalog_clock(monkeypatch):
    clock = [BASE]

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0].astimezone(tz)

    monkeypatch.setattr(audit, "datetime", Clock)
    return clock


@pytest.fixture
def catalog_context(engine_db, tmp_path):
    config = replace(
        engine_db.config,
        config_path=tmp_path / "craft-connector.yml",
        docs_site=replace(engine_db.config.docs_site, schedule="@hourly"),
    )
    return OperationContext(
        engine_db.engine, config, Actor("catalog-service", ActorKind.SERVICE), role=ApiRole.ADMIN
    )


def rows(ctx):
    with ctx.engine.connect() as conn:
        return conn.execute(
            text(
                "SELECT ACTOR AS actor, ACTOR_KIND AS kind, OUTCOME AS outcome, EXIT_CODE AS code, "
                "STARTED_AT AS requested_at "
                "FROM AUD_ACTIONS WHERE COMMAND='generate-docs' ORDER BY ACTION_ID"
            )
        ).all()


def tick(schedules, ctx, clock, now):
    clock[0] = now
    schedules.refresh(ctx, now)


def test_catalog_builds_once_per_tick_across_restart_and_leadership_change(
    catalog_context, catalog_clock
):
    ctx, clock = catalog_context, catalog_clock
    first = Schedules()
    with leadership(ctx.engine):
        try:
            tick(first, ctx, clock, BASE)
            assert first.catalog.build is None
            tick(first, ctx, clock, BASE + timedelta(hours=1))
            first.catalog.build.result(timeout=10)
            tick(first, ctx, clock, clock[0])
        finally:
            first.catalog.close()
    second = Schedules()
    with leadership(ctx.engine):
        try:
            tick(second, ctx, clock, clock[0])
            assert second.catalog.build is None
            tick(second, ctx, clock, BASE + timedelta(hours=2))
            second.catalog.build.result(timeout=10)
        finally:
            second.catalog.close()
    records = rows(ctx)
    assert [row.outcome for row in records] == [
        ActionOutcome.REQUESTED,
        ActionOutcome.SUCCESS,
        ActionOutcome.REQUESTED,
        ActionOutcome.SUCCESS,
    ]
    assert records[0].requested_at == records[1].requested_at
    assert records[2].requested_at == records[3].requested_at
    assert {row.actor for row in records} == {ctx.actor.name}
    assert {row.kind for row in records} == {ActorKind.SERVICE}
    assert [row.code for row in records if row.outcome == ActionOutcome.SUCCESS] == [0, 0]
    assert (ctx.config.project_dir / "catalog" / "index.html").exists()


@pytest.mark.parametrize(
    "problem, code",
    [
        (
            MetadataError("catalog fixture: expected valid metadata; repair the task"),
            ExitCode.METADATA,
        ),
        (
            OSError("catalog fixture: expected writable folder; repair the task"),
            ExitCode.UNEXPECTED,
        ),
    ],
)
def test_failed_catalog_is_audited_and_next_tick_retries(
    catalog_context, catalog_clock, monkeypatch, caplog, problem, code
):
    ctx, clock = catalog_context, catalog_clock
    schedules = Schedules()
    original = documents.build_catalog

    def fail(*args, **kwargs):
        raise problem

    try:
        tick(schedules, ctx, clock, BASE)
        monkeypatch.setattr(documents, "build_catalog", fail)
        tick(schedules, ctx, clock, BASE + timedelta(hours=1))
        with pytest.raises(type(problem), match="repair the task"):
            schedules.catalog.build.result(timeout=10)
        monkeypatch.setattr(documents, "build_catalog", original)
        tick(schedules, ctx, clock, BASE + timedelta(hours=2))
        schedules.catalog.build.result(timeout=10)
    finally:
        schedules.catalog.close()
    assert [row.outcome for row in rows(ctx)] == [
        ActionOutcome.REQUESTED,
        ActionOutcome.FAILURE,
        ActionOutcome.REQUESTED,
        ActionOutcome.SUCCESS,
    ]
    assert rows(ctx)[1].code == code
    assert f"catalog folder={ctx.config.project_dir / 'catalog'}" in caplog.text
    assert "repair the task" in caplog.text


def test_busy_catalog_skips_ticks_and_does_not_block_supervision(
    catalog_context, catalog_clock, monkeypatch
):
    ctx, clock = catalog_context, catalog_clock
    entered, release = threading.Event(), threading.Event()
    original = documents.build_catalog

    def blocked(*args, **kwargs):
        entered.set()
        assert release.wait(timeout=10), "test did not release the catalog worker"
        return original(*args, **kwargs)

    monkeypatch.setattr(documents, "build_catalog", blocked)
    schedules = Schedules()
    try:
        tick(schedules, ctx, clock, BASE)
        tick(schedules, ctx, clock, BASE + timedelta(hours=1))
        assert entered.wait(timeout=10)
        first = schedules.catalog.build
        tick(schedules, ctx, clock, BASE + timedelta(hours=2))
        assert schedules.catalog.build is first
        assert [row.outcome for row in rows(ctx)] == [ActionOutcome.REQUESTED]
        release.set()
        first.result(timeout=10)
        tick(schedules, ctx, clock, clock[0])
        assert len(rows(ctx)) == 2
        tick(schedules, ctx, clock, BASE + timedelta(hours=3))
        schedules.catalog.build.result(timeout=10)
    finally:
        release.set()
        schedules.catalog.close()
    assert len(rows(ctx)) == 4


def test_manual_build_is_the_durable_schedule_cursor(catalog_context, catalog_clock):
    ctx, clock = catalog_context, catalog_clock
    schedules = Schedules()
    try:
        tick(schedules, ctx, clock, BASE)
        clock[0] = BASE + timedelta(hours=1)
        documents.generate_docs(ctx, with_warehouse=False, output=None, strict=False)
        tick(schedules, ctx, clock, clock[0])
        assert schedules.catalog.build is None
        assert len(rows(ctx)) == 2
    finally:
        schedules.catalog.close()


def test_restart_does_not_replay_missed_catalog_ticks(catalog_context, catalog_clock):
    ctx, clock = catalog_context, catalog_clock
    documents.generate_docs(ctx, with_warehouse=False, output=None, strict=False)
    schedules = Schedules()
    try:
        tick(schedules, ctx, clock, BASE + timedelta(hours=5, minutes=30))
        assert schedules.catalog.build is None
        tick(schedules, ctx, clock, BASE + timedelta(hours=6))
        schedules.catalog.build.result(timeout=10)
    finally:
        schedules.catalog.close()
    assert len(rows(ctx)) == 4


def test_disabled_and_stopping_catalog_preserve_the_last_build(catalog_context, catalog_clock):
    ctx, clock = catalog_context, catalog_clock
    schedules = Schedules()
    try:
        tick(schedules, ctx, clock, BASE)
        tick(schedules, ctx, clock, BASE + timedelta(hours=1))
        schedules.catalog.build.result(timeout=10)
        previous = rows(ctx)
        assert len(previous) == 2
        disabled = replace(
            ctx, config=replace(ctx.config, docs_site=replace(ctx.config.docs_site, schedule=None))
        )
        schedules.refresh(disabled, BASE + timedelta(hours=2))
        stop = threading.Event()
        stop.set()
        schedules.refresh(ctx, BASE + timedelta(hours=2), stop)
        assert rows(ctx) == previous
    finally:
        schedules.catalog.close()
