"""Inspection reads exact identities on both Engine DBs without writing audit or admission state."""

import re
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import event, text

from etl_craft.core.actor import current_actor
from etl_craft.core.errors import MetadataError, RunStateError
from etl_craft.engine import transitions
from etl_craft.engine.repository import trackers
from etl_craft.engine.runlog import RunSelector
from etl_craft.services.operations import OperationContext, to_json
from etl_craft.services.operations.status import explain_task, pipeline_status
from fixtures.metadata import (
    add_dependency,
    add_pipeline,
    add_pipeline_dependency,
    add_task,
    start_run,
)


def test_status_includes_unrun_tasks_and_transitive_failure_blocks(engine_db):
    engine = engine_db.engine
    ctx = OperationContext(engine, engine_db.config, current_actor())
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        first = add_task(conn, pipeline, "first")
        after = add_task(conn, pipeline, "after")
        last = add_task(conn, pipeline, "last")
        add_dependency(conn, pipeline, after, first)
        add_dependency(conn, pipeline, last, after)
        run = start_run(conn, pipeline)
        summary = transitions.find_or_create_task_run(conn, first, run).task_run_id
        attempt = transitions.queue_attempt(conn, summary, current_actor())
        transitions.claim_attempt(
            conn,
            attempt,
            current_actor(),
            owner="worker",
            lease_expires_at=datetime.now(UTC) + timedelta(minutes=1),
        )
        transitions.finish_attempt(
            conn,
            attempt,
            "FAILED",
            current_actor(),
            owner="worker",
            error_message="first line\nsecond line",
        )
    statements = []

    def record(conn, cursor, query, parameters, context, many):
        statements.append(re.sub(r"--[^\n]*", "", query).strip().split()[0].upper())

    event.listen(engine, "before_cursor_execute", record)
    try:
        status = pipeline_status(ctx, "P")
        assert [task.task_code for task in status.tasks] == ["after", "first", "last"]
        assert status.blocked_by_failures == ("after", "last")
        assert status.tasks[1].error == "first line"
        assert status.tasks[1].attempts == 1
        assert status.tasks[0].explanation.task is None
        doc = to_json(status)
        assert doc["run"]["pipeline_run_id"] == run
        assert doc["tasks"][1]["explanation"]["task"]["attempts"][0]["retryable"] is True
        assert explain_task(ctx, "P", "after").state == "blocked by failure"
        with pytest.raises(MetadataError):
            explain_task(ctx, "P", "missing")
    finally:
        event.remove(engine, "before_cursor_execute", record)
    assert not {"INSERT", "UPDATE", "DELETE"} & set(statements)
    with pytest.raises(RunStateError):
        pipeline_status(ctx, "P", selector=RunSelector(run_id=run + 99))


def test_explain_retains_recorded_cross_pipeline_decisions_and_retry_due(engine_db):
    engine = engine_db.engine
    ctx = OperationContext(engine, engine_db.config, current_actor())
    due = datetime.now(UTC) + timedelta(minutes=1)
    with engine.begin() as conn:
        upstream = add_pipeline(conn, "UP")
        upstream_task = add_task(conn, upstream, "load")
        up_run = start_run(conn, upstream)
        up_summary = transitions.find_or_create_task_run(conn, upstream_task, up_run).task_run_id
        transitions.finish_task_run(conn, up_summary, status="SUCCESS")
        transitions.finalize_pipeline_run(conn, up_run, "SUCCESS")
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "load")
        dependency = add_dependency(conn, pipeline, task, upstream_task, upstream_pipeline=upstream)
        pipeline_dependency = add_pipeline_dependency(conn, pipeline, upstream)
        run = start_run(conn, pipeline)
        trackers.record_decisions(
            conn,
            run,
            (
                trackers.GateDecision(
                    pipeline_dependency,
                    False,
                    trackers.FinishedRun(up_run, "SUCCESS", True, pipeline_run_id=up_run),
                    "SATISFIED",
                    "Recorded pipeline admission.",
                ),
            ),
        )
        summary = transitions.find_or_create_task_run(conn, task, run).task_run_id
        attempt = transitions.queue_attempt(conn, summary, current_actor(), not_before=due)
        trackers.record_decisions(
            conn,
            run,
            (
                trackers.GateDecision(
                    dependency,
                    True,
                    trackers.FinishedRun(up_summary, "SUCCESS", True, pipeline_run_id=up_run),
                    "SATISFIED",
                    "Recorded task admission.",
                ),
            ),
            attempt_id=attempt,
        )
    done = explain_task(ctx, "P", "load")
    assert done.state == "retry scheduled"
    assert abs((done.retry_at - due).total_seconds()) < 0.001
    assert done.dependencies[0].recorded and done.dependencies[0].met
    assert done.dependencies[0].selected_task_run_id == up_summary
    assert done.dependencies[0].reason == "Recorded task admission."
    assert done.pipeline_dependencies[0].recorded
    assert done.pipeline_dependencies[0].reason == "Recorded pipeline admission."
    assert done.dependencies[0].status == "SUCCESS"


def test_gate_waits_are_visible_and_conditional_dependencies_use_the_required_count(engine_db):
    from etl_craft.engine.queries import statement

    engine = engine_db.engine
    ctx = OperationContext(engine, engine_db.config, current_actor())
    now = datetime.now(UTC)
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        first = add_task(conn, pipeline, "first")
        second = add_task(conn, pipeline, "second")
        task = add_task(conn, pipeline, "any")
        add_dependency(conn, pipeline, task, first)
        add_dependency(conn, pipeline, task, second)
        conn.execute(
            text("UPDATE CFG_TASKS SET RUN_CONDITION='ANY' WHERE TASK_ID=:task"), {"task": task}
        )
        run = start_run(conn, pipeline)
        summary = transitions.find_or_create_task_run(conn, first, run).task_run_id
        transitions.finish_task_run(conn, summary, status="SUCCESS")
        for waiting_task in (None, second):
            conn.execute(
                statement(conn, "save_gate_wait"),
                {
                    "pipeline_run_id": run,
                    "task_id": waiting_task,
                    "first_check_at": now,
                    "next_check_at": now + timedelta(seconds=1),
                    "looks": 1,
                    "wait_until": now + timedelta(minutes=5),
                },
            )
    done = explain_task(ctx, "P", "second")
    assert done.state == "waiting on a gate"
    assert {wait.task_id for wait in done.gate_waits} == {None, second}
    with engine.begin() as conn:
        conn.execute(
            statement(conn, "finish_gate_wait"),
            {"pipeline_run_id": run, "task_id": None, "looks": 1},
        )
    done = explain_task(ctx, "P", "any")
    assert done.run_condition == "ANY" and done.required_count == 1
    assert [dependency.met for dependency in done.dependencies] == [True, False]
    assert done.state == "not run"


@pytest.mark.parametrize("policy", ["enforce", "warn", "off"])
def test_pending_cross_pipeline_gates_follow_runtime_policy(engine_db, policy):
    from dataclasses import replace

    from etl_craft.core.enums import GatePolicy

    engine = engine_db.engine
    ctx = OperationContext(
        engine, replace(engine_db.config, dependency_gates=GatePolicy(policy)), current_actor()
    )
    with engine.begin() as conn:
        upstream = add_pipeline(conn, "UP")
        upstream_task = add_task(conn, upstream, "load")
        old_run = start_run(conn, upstream)
        old_task = transitions.find_or_create_task_run(conn, upstream_task, old_run).task_run_id
        transitions.finish_task_run(conn, old_task, status="SUCCESS")
        transitions.finalize_pipeline_run(conn, old_run, "SUCCESS")
        live_run = start_run(conn, upstream)
        live_task = transitions.find_or_create_task_run(conn, upstream_task, live_run).task_run_id
        live_attempt = transitions.queue_attempt(conn, live_task, current_actor())
        transitions.claim_attempt(
            conn,
            live_attempt,
            current_actor(),
            owner="worker",
            lease_expires_at=datetime.now(UTC) + timedelta(minutes=1),
        )
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "load")
        add_dependency(conn, pipeline, task, upstream_task, upstream_pipeline=upstream)
        add_pipeline_dependency(conn, pipeline, upstream)
        start_run(conn, pipeline)
    done = explain_task(ctx, "P", "load")
    gates = (*done.dependencies, *done.pipeline_dependencies)
    assert all(not gate.recorded for gate in gates)
    assert all(gate.met == (policy == "off") for gate in gates)
    assert done.state == ("not run" if policy == "off" else "waiting on a gate")
    if policy != "off":
        assert all(gate.status == "IN-PROGRESS" for gate in gates)
        assert all(gate.selected_pipeline_run_id is None for gate in gates)
    with engine.begin() as conn:
        transitions.finish_attempt(conn, live_attempt, "FAILED", current_actor(), owner="worker")
        transitions.finalize_pipeline_run(conn, live_run, "FAILED")
    done = explain_task(ctx, "P", "load")
    assert all(
        gate.met == (policy != "enforce")
        for gate in (*done.dependencies, *done.pipeline_dependencies)
    )


def test_backfill_explanation_assumes_success_but_never_failure(engine_db):
    engine = engine_db.engine
    ctx = OperationContext(engine, engine_db.config, current_actor())
    with engine.begin() as conn:
        upstream = add_pipeline(conn, "UP")
        upstream_task = add_task(conn, upstream, "load")
        pipeline = add_pipeline(conn, "P")
        success = add_task(conn, pipeline, "success")
        failure = add_task(conn, pipeline, "failure")
        add_dependency(conn, pipeline, success, upstream_task, upstream_pipeline=upstream)
        add_dependency(
            conn, pipeline, failure, upstream_task, upstream_pipeline=upstream, kind="FAILURE"
        )
        add_pipeline_dependency(conn, pipeline, upstream)
        start_run(conn, pipeline, backfill=True)
    assert explain_task(ctx, "P", "success").state == "not run"
    why = explain_task(ctx, "P", "failure")
    assert why.state == "waiting on a gate"
    assert not why.dependencies[0].met
    assert why.pipeline_dependencies[0].met
