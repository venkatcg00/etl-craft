"""Automatic retries retain attempt history and release worker slots while delayed."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from etl_craft.core.actor import current_actor
from etl_craft.core.errors import StaleTransitionError
from etl_craft.engine import transitions
from etl_craft.execution.gates import Clock
from etl_craft.execution.pipeline import run_pipeline
from etl_craft.execution.retries import refresh_retries
from etl_craft.execution.runner import run_task
from fixtures.metadata import add_dependency, add_pipeline, add_task, start_run
from integration.execution.test_pipeline import CHILD, statuses
from integration.execution.test_pipeline import (
    child_can_import_fixtures as child_can_import_fixtures,
)
from integration.execution.test_pipeline import config as config


def attempts(engine, run):
    with engine.connect() as conn:
        return conn.execute(
            text(
                "SELECT a.ATTEMPT_ID AS attempt_id, a.TASK_RUN_ID AS task_run_id, "
                "a.ATTEMPT_NUMBER AS attempt_number, a.STATUS AS status, a.RETRYABLE AS retryable, "
                "a.OWNER_ID AS owner_id, a.NOT_BEFORE AS not_before FROM AUD_TASK_ATTEMPTS a "
                "JOIN AUD_TASK_RUN_LOG r ON r.TASK_RUN_ID=a.TASK_RUN_ID "
                "WHERE r.PIPELINE_RUN_ID=:run ORDER BY a.ATTEMPT_ID"
            ),
            {"run": run},
        ).all()


@pytest.mark.parametrize(("single", "force"), [(False, False), (True, False), (False, True)])
def test_two_failures_then_success_and_no_early_failure_alert(engine_db, config, single, force):
    engine = engine_db.engine
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(
            conn,
            pipeline,
            "load",
            BEHAVIOUR="retry",
            RETRIES="2",
            RETRY_DELAY_SECONDS="1" if force else "0",
            RETRY_BACKOFF="1",
        )
        if single:
            run = start_run(conn, pipeline)
        else:
            after = add_task(conn, pipeline, "after", BEHAVIOUR="succeed")
            alert = add_task(conn, pipeline, "alert", BEHAVIOUR="succeed")
            add_dependency(conn, pipeline, after, task)
            add_dependency(conn, pipeline, alert, task, "FAILURE")
    outcome = (
        run_task(engine, config, "P", "load", child=CHILD)
        if single
        else run_pipeline(engine, config, "P", child=CHILD, force=force, clock=Clock())
    )
    assert outcome.status == "SUCCESS"
    if not single:
        run = outcome.pipeline_run_id
        assert statuses(engine, run)["after"][0] == "SUCCESS"
        assert statuses(engine, run)["alert"][0] == ("SUCCESS" if force else "SKIPPED")
    rows = [r for r in attempts(engine, run) if r.attempt_number <= 3]
    load = [r for r in rows if r.task_run_id == rows[0].task_run_id]
    assert [r.status for r in load] == ["FAILED", "FAILED", "SUCCESS"]
    assert len({r.owner_id for r in load}) == 3
    assert len({r.task_run_id for r in load}) == 1


def test_sql_guard_never_retries(engine_db, config):
    engine = engine_db.engine
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        add_task(conn, pipeline, "load", BEHAVIOUR="guard", RETRIES="2", RETRY_DELAY_SECONDS="0")
    outcome = run_pipeline(engine, config, "P", child=CHILD)
    rows = attempts(engine, outcome.pipeline_run_id)
    assert outcome.status == "FAILED"
    assert len(rows) == 1 and rows[0].status == "FAILED" and not rows[0].retryable


def test_delayed_retry_is_persisted_and_cannot_be_claimed_early(engine_db, config):
    engine = engine_db.engine
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "load", RETRIES="2", RETRY_DELAY_SECONDS="60")
        run = start_run(conn, pipeline)
        summary = transitions.find_or_create_task_run(conn, task, run).task_run_id
        first = transitions.queue_attempt(conn, summary, current_actor())
        transitions.claim_attempt(
            conn,
            first,
            current_actor(),
            owner="first",
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
        )
        transitions.finish_attempt(conn, first, "FAILED", current_actor(), owner="first")
    waiting, exhausted = refresh_retries(engine, config, run, {task})
    assert task in waiting and not exhausted and waiting[task] > datetime.now(UTC)
    assert refresh_retries(engine, config, run, {task}) == (waiting, exhausted)
    rows = attempts(engine, run)
    assert [r.status for r in rows] == ["FAILED", "QUEUED"]
    with engine.begin() as conn:
        with pytest.raises(StaleTransitionError):
            transitions.claim_attempt(
                conn,
                rows[-1].attempt_id,
                current_actor(),
                owner="early",
                lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
            )
        with pytest.raises(StaleTransitionError):
            transitions.queue_attempt(conn, summary, current_actor(), previous_attempt_id=first)


def test_lost_attempt_retries_with_a_new_owner(engine_db, config):
    engine = engine_db.engine
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(
            conn, pipeline, "load", RETRIES="1", RETRY_DELAY_SECONDS="0", BEHAVIOUR="succeed"
        )
        run = start_run(conn, pipeline)
        summary = transitions.find_or_create_task_run(conn, task, run).task_run_id
        first = transitions.queue_attempt(conn, summary, current_actor())
        transitions.claim_attempt(
            conn,
            first,
            current_actor(),
            owner="lost-worker",
            lease_expires_at=datetime.now(UTC) - timedelta(seconds=1),
        )
        transitions.lose_attempt(conn, first, current_actor(), owner="lost-worker")
    outcome = run_task(engine, config, "P", "load", child=CHILD)
    rows = attempts(engine, run)
    assert outcome.status == "SUCCESS"
    assert [r.status for r in rows] == ["LOST", "SUCCESS"]
    assert rows[1].owner_id != rows[0].owner_id
    assert rows[1].task_run_id == rows[0].task_run_id


def test_delayed_retry_does_not_take_the_only_worker_slot(engine_db, config):
    import time
    from dataclasses import replace

    from etl_craft.execution.pipeline import pipeline_steps

    engine = engine_db.engine
    config = replace(config, limits=replace(config.limits, max_parallel_tasks=1))
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "delayed", RETRIES="1", RETRY_DELAY_SECONDS="3600")
        add_task(conn, pipeline, "independent", BEHAVIOUR="succeed")
        run = start_run(conn, pipeline)
        summary = transitions.find_or_create_task_run(conn, task, run).task_run_id
        first = transitions.queue_attempt(conn, summary, current_actor())
        transitions.claim_attempt(
            conn,
            first,
            current_actor(),
            owner="failed-worker",
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
        )
        transitions.finish_attempt(conn, first, "FAILED", current_actor(), owner="failed-worker")
    steps = pipeline_steps(engine, config, "P", child=CHILD)
    deadline = time.monotonic() + 10
    try:
        while statuses(engine, run).get("independent", (None,))[0] != "SUCCESS":
            assert time.monotonic() < deadline
            next(steps)
            time.sleep(0.01)
    finally:
        steps.close()
    rows = attempts(engine, run)
    assert len([r for r in rows if r.status == "QUEUED"]) == 1
    waiting, _ = refresh_retries(engine, config, run, {task})
    assert waiting[task] > datetime.now(UTC)
    assert len(attempts(engine, run)) == 3
    resumed = pipeline_steps(engine, config, "P", child=CHILD)
    try:
        assert next(resumed) > 3500
        assert refresh_retries(engine, config, run, {task})[0] == waiting
        assert len(attempts(engine, run)) == 3
    finally:
        resumed.close()


@pytest.mark.parametrize(
    "behaviour,timeout,expected", [("fail", "0", "FAILED"), ("sleep", "1", "TIMED_OUT")]
)
def test_retry_budget_is_exhausted_for_failure_and_timeout(
    engine_db, config, behaviour, timeout, expected
):
    engine = engine_db.engine
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        add_task(
            conn,
            pipeline,
            "load",
            BEHAVIOUR=behaviour,
            RETRIES="1",
            RETRY_DELAY_SECONDS="0",
            TASK_TIMEOUT_SECONDS=timeout,
        )
    outcome = run_pipeline(engine, config, "P", child=CHILD)
    assert outcome.status == "FAILED"
    assert [r.status for r in attempts(engine, outcome.pipeline_run_id)] == [expected, expected]


def test_remote_orchestrator_retains_retry_control(engine_db, config):
    from dataclasses import replace

    from etl_craft.core.enums import Mode

    engine = engine_db.engine
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        add_task(conn, pipeline, "load", BEHAVIOUR="fail", RETRIES="3", RETRY_DELAY_SECONDS="0")
        run = start_run(conn, pipeline)
    outcome = run_task(engine, replace(config, mode=Mode.REMOTE), "P", "load", child=CHILD)
    assert outcome.status == "FAILED"
    assert len(attempts(engine, run)) == 1


def test_cancelled_attempt_never_retries(engine_db, config):
    engine = engine_db.engine
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "load", RETRIES="3", RETRY_DELAY_SECONDS="0")
        run = start_run(conn, pipeline)
        summary = transitions.find_or_create_task_run(conn, task, run).task_run_id
        attempt = transitions.queue_attempt(conn, summary, current_actor())
        transitions.cancel_attempt(
            conn, attempt, current_actor(), error_message="operator cancelled"
        )
    assert refresh_retries(engine, config, run, {task}) == ({}, set())
    assert len(attempts(engine, run)) == 1
