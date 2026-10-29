"""Bounded cursors freeze before SQL and survive retries without releasing lost writers."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from etl_craft.core.actor import current_actor
from etl_craft.core.enums import AttemptStatus, OffsetType, RunStatus
from etl_craft.core.errors import RunRefusedError, StaleTransitionError
from etl_craft.engine import timestamp_windows as windows
from etl_craft.engine import transitions as tr
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.offsets import StoredOffset, fetch_task_offset
from etl_craft.engine.runlog import RunSelector
from etl_craft.engine.task_store import LocalTaskStore, TaskIdentity
from etl_craft.execution.interventions import reset_watermark
from etl_craft.handlers.resources import LocalLocker
from fixtures.metadata import add_pipeline, add_task
from fixtures.races import two_at_once


@pytest.fixture
def bounded(engine_db, monkeypatch):
    engine, config = engine_db.engine, engine_db.config
    clock = [(datetime.now(UTC) + timedelta(seconds=1)).replace(microsecond=123456)]
    original = windows.statement

    def controlled(conn, name):
        if name == "engine_clock":
            return text("SELECT :now AS now").bindparams(now=clock[0])
        return original(conn, name)

    monkeypatch.setattr(windows, "statement", controlled)
    with engine.begin() as conn:
        source = add_pipeline(conn, "SOURCE")
        reader = add_pipeline(conn, "READER")
        source_task = add_task(conn, source, "write", "SQL", SQL_ACTION="SCD1_MERGE")
        reader_task = add_task(
            conn,
            reader,
            "read",
            "SQL",
            SQL_ACTION="APPEND_TABLE",
            TARGET_OBJECT="s.dm",
            SELECT_QUERY="SELECT changed FROM s.ods WHERE changed >= $$watermark "
            "AND changed < $$watermark_max",
            WATERMARK_TYPE="TIMESTAMP",
            WATERMARK_COLUMN="changed",
            WATERMARK_START="2000-01-01T00:00:00+00:00",
            WATERMARK_SOURCE_PIPELINES='["SOURCE"]',
        )
        source_run = tr.create_run(conn, source, current_actor())
        reader_run = tr.create_run(conn, reader, current_actor())
        source_summary = tr.find_or_create_task_run(conn, source_task, source_run).task_run_id
        reader_summary = tr.find_or_create_task_run(conn, reader_task, reader_run).task_run_id
    definition = windows.WindowDefinition(
        StoredOffset(OffsetType.TIMESTAMP, "2000-01-01T00:00:00+00:00"), ("SOURCE",)
    )

    def start(summary):
        with engine.begin() as conn:
            attempt = tr.queue_attempt(conn, summary, current_actor())
            tr.claim_attempt(
                conn,
                attempt,
                current_actor(),
                owner="owner",
                lease_expires_at=clock[0] + timedelta(hours=1),
            )
            tr.start_attempt(conn, attempt, current_actor(), owner="owner")
        return attempt

    def store(attempt):
        return LocalTaskStore(
            engine,
            config,
            TaskIdentity(reader, reader_task, reader_run, reader_summary, attempt, "owner"),
            LocalLocker(engine, config),
        )

    return (
        engine,
        clock,
        source_summary,
        reader_summary,
        reader_task,
        reader_run,
        definition,
        start,
        store,
    )


def test_a_window_freezes_before_sql_and_success_commits_its_high(bounded):
    engine, clock, source, summary, task, _, definition, start, store = bounded
    source_start = clock[0]
    source_attempt = start(source)
    clock[0] += timedelta(seconds=1)
    attempt = start(summary)
    first = store(attempt).window(definition)
    assert first.high == source_start
    with engine.begin() as conn:
        before = fetch_task_offset(conn, task)
        assert before.pending_task_run_id == summary
        assert before.pending_upper_bound == first.high
        assert before.offset_value == definition.initial.offset_value
        row = conn.execute(statement(conn, "window_range"), {"task_run_id": summary}).one()
        assert row.watermark_to == first.high.isoformat()
        tr.finish_attempt(
            conn, source_attempt, AttemptStatus.SUCCESS, current_actor(), owner="owner"
        )
        tr.finish_attempt(conn, attempt, AttemptStatus.FAILED, current_actor(), owner="owner")
    clock[0] += timedelta(seconds=1)
    retry = start(summary)
    assert store(retry).window(definition) == first
    with engine.begin() as conn:
        with pytest.raises(StaleTransitionError):
            tr.finish_attempt(
                conn,
                retry,
                AttemptStatus.SUCCESS,
                current_actor(),
                owner="owner",
                offset=StoredOffset(OffsetType.TIMESTAMP, clock[0].isoformat()),
            )
        with pytest.raises(StaleTransitionError, match="invalid timestamp offset"):
            tr.finish_attempt(
                conn,
                retry,
                AttemptStatus.SUCCESS,
                current_actor(),
                owner="owner",
                offset=StoredOffset(OffsetType.TIMESTAMP, "invalid"),
            )
        tr.finish_attempt(
            conn,
            retry,
            AttemptStatus.SUCCESS,
            current_actor(),
            owner="owner",
            offset=StoredOffset(OffsetType.TIMESTAMP, first.high.isoformat()),
        )
    with engine.connect() as conn:
        after = fetch_task_offset(conn, task)
        assert after.offset_value == first.high.isoformat()
        assert after.pending_task_run_id is None and after.pending_upper_bound is None


def test_clock_rollback_refuses_source_starts_and_pending_resets(engine_db, bounded):
    engine, clock, source, summary, _, run, definition, start, store = bounded
    attempt = start(summary)
    store(attempt).window(definition)
    with pytest.raises(RunRefusedError, match="active attempt"):
        reset_watermark(
            engine,
            engine_db.config,
            "READER",
            "read",
            "2000-01-01",
            "test",
            selector=RunSelector(run_id=run),
        )
    with engine.begin() as conn:
        tr.finish_attempt(conn, attempt, AttemptStatus.FAILED, current_actor(), owner="owner")
    with pytest.raises(RunRefusedError, match="pending timestamp window"):
        reset_watermark(
            engine,
            engine_db.config,
            "READER",
            "read",
            "2000-01-01",
            "test",
            selector=RunSelector(run_id=run),
        )
    clock[0] -= timedelta(seconds=1)
    with pytest.raises(RunRefusedError, match=r"Engine DB clock.*behind.*restore"):
        start(source)


def test_source_start_and_admission_share_one_fence(bounded):
    engine, clock, source, summary, _, _, definition, start, store = bounded
    attempt = start(summary)
    clock[0] += timedelta(microseconds=123456)

    def start_source():
        return start(source)

    def admission():
        return store(attempt).window(definition)

    source_attempt, window = two_at_once(start_source, admission, at=(engine, "begin"))
    with engine.connect() as conn:
        assert window.high <= windows.recorded_start(conn, source, source_attempt)
        assert window.high.microsecond != 0


def test_backfill_range_survives_retry_without_a_live_cursor(bounded):
    engine, clock, _, summary, task, _, definition, start, store = bounded
    attempt = start(summary)
    definition = replace(definition, backfill=True)
    window = store(attempt).window(definition)
    with engine.begin() as conn:
        assert fetch_task_offset(conn, task) is None
        tr.finish_attempt(conn, attempt, AttemptStatus.FAILED, current_actor(), owner="owner")
    clock[0] += timedelta(seconds=1)
    assert store(start(summary)).window(definition) == window


def test_lost_windows_release_only_after_confirmed_abandonment(engine_db, bounded):
    from etl_craft.core.enums import ActionName, ApiRole, ControlAction
    from etl_craft.engine import controls
    from etl_craft.services.operations import OperationContext
    from fixtures.actions import act

    engine, _, _, summary, task, run, definition, start, store = bounded
    attempt = start(summary)
    window = store(attempt).window(definition)
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET LEASE_EXPIRES_AT=:expired WHERE ATTEMPT_ID=:id"),
            {"expired": datetime.now(UTC) - timedelta(seconds=1), "id": attempt},
        )
        tr.lose_attempt(conn, attempt, current_actor(), owner="owner")
        assert fetch_task_offset(conn, task).pending_task_run_id == summary
    ctx = OperationContext(engine, engine_db.config, current_actor(), role=ApiRole.ADMIN)
    act(
        ctx,
        ActionName.CONTROL,
        pipeline_code="READER",
        action=ControlAction.ABORT,
        run_id=run,
        reason="abandon uncertain window",
    )
    with engine.begin() as conn:
        assert fetch_task_offset(conn, task).pending_task_run_id == summary
        controls.observe_exit(conn, attempt, queries_ended=True)
    from etl_craft.execution.control_flow import advance

    advance(engine, run, observed=True)
    with engine.connect() as conn:
        after = fetch_task_offset(conn, task)
        assert after.pending_task_run_id is None
        assert after.offset_value == definition.initial.offset_value
        evidence = conn.execute(statement(conn, "window_range"), {"task_run_id": summary}).one()
        assert evidence.watermark_to == window.high.isoformat()


def test_pending_conflicts_and_successful_reruns_do_not_change_a_frozen_window(bounded):
    engine, clock, _, summary, task, _, definition, start, store = bounded
    attempt = start(summary)
    first = store(attempt).window(definition)
    with engine.begin() as conn:
        pipeline = conn.execute(
            text("SELECT PIPELINE_ID AS pipeline_id FROM CFG_TASKS WHERE TASK_ID=:id"), {"id": task}
        ).scalar_one()
        queued = tr.create_run(conn, pipeline, current_actor(), status=RunStatus.QUEUED)
        sibling = tr.find_or_create_task_run(conn, task, queued).task_run_id
        with pytest.raises(RunRefusedError, match=r"pending timestamp window belongs.*resume"):
            windows.admit_window(conn, task, sibling, clock[0], definition)
        tr.finish_attempt(
            conn,
            attempt,
            AttemptStatus.SUCCESS,
            current_actor(),
            owner="owner",
            offset=StoredOffset(OffsetType.TIMESTAMP, first.high.isoformat()),
        )
    clock[0] += timedelta(seconds=1)
    assert store(start(summary)).window(definition) == first


def test_a_failed_success_transaction_keeps_the_pending_pair_and_running_attempt(
    bounded, monkeypatch
):
    from etl_craft.core.errors import InjectedFaultError

    engine, _, _, summary, task, _, definition, start, store = bounded
    attempt = start(summary)
    window = store(attempt).window(definition)
    monkeypatch.setenv("ETL_CRAFT_FAULT", "script.after_offset")
    with pytest.raises(InjectedFaultError), engine.begin() as conn:
        tr.finish_attempt(
            conn,
            attempt,
            AttemptStatus.SUCCESS,
            current_actor(),
            owner="owner",
            offset=StoredOffset(OffsetType.TIMESTAMP, window.high.isoformat()),
        )
    with engine.connect() as conn:
        after = fetch_task_offset(conn, task)
        assert after.offset_value == definition.initial.offset_value
        assert after.pending_task_run_id == summary and after.pending_upper_bound == window.high
        row = conn.execute(statement(conn, "transition_row_attempt"), {"row_id": attempt}).one()
        assert row.status == AttemptStatus.RUNNING


def test_a_range_report_cannot_replace_the_frozen_pending_pair(bounded):
    engine, clock, _, summary, _, _, definition, start, store = bounded
    attempt = start(summary)
    window = store(attempt).window(definition)
    with pytest.raises(RunRefusedError, match="differs from admitted"):
        store(attempt).watermark(
            window.low.isoformat(), (clock[0] + timedelta(seconds=1)).isoformat()
        )
    with pytest.raises(RunRefusedError, match="expected the admitted ISO timestamps"):
        store(attempt).watermark("invalid", window.high.isoformat())
    with engine.connect() as conn:
        row = conn.execute(statement(conn, "window_range"), {"task_run_id": summary}).one()
        assert row.watermark_to == window.high.isoformat()


@pytest.mark.parametrize(
    "ending",
    [AttemptStatus.LOST, AttemptStatus.FAILED, AttemptStatus.TIMED_OUT, AttemptStatus.CANCELLED],
)
def test_uncertain_source_endings_keep_the_fence_until_writers_are_confirmed_stopped(
    bounded, ending
):
    from etl_craft.engine import controls

    engine, clock, source, summary, task, run, definition, start, store = bounded
    source_start = clock[0]
    source_attempt = start(source)
    with engine.begin() as conn:
        if ending == AttemptStatus.LOST:
            conn.execute(
                text("UPDATE AUD_TASK_ATTEMPTS SET LEASE_EXPIRES_AT=:expired WHERE ATTEMPT_ID=:id"),
                {"expired": datetime.now(UTC) - timedelta(seconds=1), "id": source_attempt},
            )
            tr.lose_attempt(conn, source_attempt, current_actor(), owner="owner")
        else:
            tr.finish_attempt(conn, source_attempt, ending, current_actor(), owner="owner")
    clock[0] += timedelta(seconds=1)
    attempt = start(summary)
    first = store(attempt).window(definition)
    assert first.high == source_start
    with engine.begin() as conn:
        controls.observe_exit(conn, source_attempt, queries_ended=True)
        tr.finish_attempt(
            conn,
            attempt,
            AttemptStatus.SUCCESS,
            current_actor(),
            owner="owner",
            offset=StoredOffset(OffsetType.TIMESTAMP, first.high.isoformat()),
        )
        pipeline = conn.execute(
            text("SELECT PIPELINE_ID AS pipeline_id FROM CFG_TASKS WHERE TASK_ID=:id"), {"id": task}
        ).scalar_one()
        tr.finish_run(conn, run, RunStatus.SUCCESS, current_actor())
        next_run = tr.create_run(conn, pipeline, current_actor())
        next_summary = tr.find_or_create_task_run(conn, task, next_run).task_run_id
    clock[0] += timedelta(seconds=1)
    next_attempt = start(next_summary)
    previous = store(attempt)
    next_store = replace(
        previous,
        identity=replace(
            previous.identity,
            pipeline_run_id=next_run,
            task_run_id=next_summary,
            attempt_id=next_attempt,
        ),
    )
    assert next_store.window(definition).high == clock[0]
