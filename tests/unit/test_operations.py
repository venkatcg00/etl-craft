"""Service contracts on SQLite without warehouses, network services or task subprocesses."""

import json
import logging
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, date, datetime, timedelta

import pytest
import yaml
from sqlalchemy import event, text

from etl_craft.cli import main
from etl_craft.config import load_config
from etl_craft.core.actor import Actor, ActorKind, acting_as, current_actor
from etl_craft.core.enums import RunStatus
from etl_craft.core.errors import MetadataError, RunStateError, UsageError
from etl_craft.engine import transitions
from etl_craft.engine.audit import command_request, register_engine
from etl_craft.engine.runlog import RunSelector
from etl_craft.execution import pipeline as execution
from etl_craft.execution import runner
from etl_craft.services.operations import (
    OperationContext,
    backfills,
    inspect,
    pipelines,
    runs,
    tasks,
    to_json,
)
from etl_craft.services.operations.models import GraphView
from etl_craft.services.operations.requests import RunRequest
from etl_craft.services.operations.snapshots import run_view, task_run_view
from fixtures.engine_db import apply_schema, sqlite_engine_db
from fixtures.metadata import add_pipeline, add_task, start_run

pytestmark = pytest.mark.unit
P = "P"


@pytest.fixture(autouse=True)
def restore_logger():
    logger = logging.getLogger("etl_craft")
    handlers, level = list(logger.handlers), logger.level
    yield
    logger.handlers[:] = handlers
    logger.setLevel(level)


@pytest.fixture
def ctx(tmp_path, monkeypatch):
    db = sqlite_engine_db(tmp_path)
    apply_schema(db.engine)
    path = tmp_path / "craft-connector.yml"
    path.write_text(
        yaml.safe_dump(
            {
                "Secrets": {"Source_type": "environment"},
                "Orchestration": {"Mode": "local"},
                "Engine": {"dev": {"jdbc_url": "jdbc:sqlite:engine.db", "schema": "main"}},
            },
            sort_keys=False,
        )
    )
    monkeypatch.setenv("ETL_CRAFT_ACTOR", "api-user")
    monkeypatch.setenv("ETL_CRAFT_ACTOR_KIND", "HUMAN")
    config = load_config(path)
    try:
        yield OperationContext(db.engine, config, Actor("api-user", ActorKind.HUMAN))
    finally:
        db.engine.dispose()


def seed(ctx, *, active=False, task=False):
    with ctx.engine.begin() as conn:
        pipeline_id = add_pipeline(conn, "P")
        task_id = add_task(conn, pipeline_id, "load", "PYTHON") if task else None
        pipeline_run_id = start_run(conn, pipeline_id) if active else None
        task_run_id = None
        if task_id is not None and pipeline_run_id is not None:
            actor = current_actor()
            task_run_id = transitions.find_or_create_task_run(
                conn, task_id, pipeline_run_id
            ).task_run_id
            attempt = transitions.queue_attempt(conn, task_run_id, actor)
            transitions.claim_attempt(
                conn,
                attempt,
                actor,
                owner="seed-owner",
                lease_expires_at=datetime.now(UTC) + timedelta(minutes=1),
            )
            transitions.finish_attempt(conn, attempt, "FAILED", actor, owner="seed-owner")
    return pipeline_id, pipeline_run_id, task_id, task_run_id


def action_count(ctx):
    with ctx.engine.connect() as conn:
        return conn.execute(text("SELECT COUNT(*) FROM AUD_ACTIONS")).scalar_one()


def test_run_lifecycle_has_one_request_per_call_and_restores_actor(ctx):
    pipeline_id, _, _, _ = seed(ctx)
    parent = Actor("caller", ActorKind.SYSTEM)
    with acting_as(parent):
        started = runs.execute_run(
            ctx,
            RunRequest(
                P,
                init_only=True,
                run_date=date(2026, 10, 1),
                selector=RunSelector(run_key="api:one"),
            ),
        )
        assert current_actor() == parent
    assert action_count(ctx) == 1
    assert started.run.pipeline_id == pipeline_id
    assert started.run.run_key == "api:one"
    assert started.run.started_by == "api-user"
    document = to_json(started)
    assert document["schema"] == "etl-craft/operation/1"
    assert document["run"]["schema"] == "etl-craft/run/1"
    assert document["run"]["run_date"] == "2026-10-01"
    assert document["status"] == "IN-PROGRESS"
    assert document["run"]["start_date"].endswith("+00:00")
    with pytest.raises(FrozenInstanceError):
        started.run.status = "FAILED"
    ended = runs.finalize_run(ctx, P, selector=RunSelector(run_key="api:one"))
    assert ended.run.pipeline_run_id == started.run.pipeline_run_id
    assert ended.run.status == "SUCCESS"
    assert ended.run.ended_by == "etl-craft"
    assert ended.run.ended_by_kind == "SYSTEM"
    assert action_count(ctx) == 2


def test_trigger_skip_and_stand_in_use_their_returned_run_ids(ctx):
    seed(ctx)
    triggered = runs.trigger_run(ctx, P, reason="operator request")
    skipped = runs.skip_run(ctx, P, "planned maintenance")
    stand_in = runs.stand_in_run(ctx, P, "SUCCESS", "source verified")
    ids = [result.run.pipeline_run_id for result in (triggered, skipped, stand_in)]
    assert len(set(ids)) == 3
    assert [result.run.status for result in (triggered, skipped, stand_in)] == [
        "SUCCESS",
        "SKIPPED",
        "SUCCESS",
    ]
    assert inspect.run_history(ctx, P, all_runs=True).entries[0].pipeline_run_id == ids[-1]
    assert action_count(ctx) == 3
    doc = to_json(inspect.audit(ctx, P))
    assert all(action["actor"] == "api-user" for action in doc["actions"])
    assert doc["actions"][0]["arguments"]["reason"] == "operator request"


def test_marking_failed_is_a_successful_action_with_a_failed_run(ctx):
    _, run_id, _, _ = seed(ctx, active=True)
    done = runs.mark(ctx, P, "FAILED", "verified", selector=RunSelector(run_id=run_id))
    assert done.status == "SUCCESS"
    assert done.run.status == "FAILED"
    assert done.run.pipeline_run_id == run_id
    assert action_count(ctx) == 1


def test_task_mark_preserves_failed_attempt_history_and_canonical_ids(ctx):
    pipeline_id, run_id, task_id, task_run_id = seed(ctx, active=True, task=True)
    done = runs.mark(
        ctx, P, "SUCCESS", "verified", task_code="load", rows=7, selector=RunSelector(run_id=run_id)
    )
    assert done.task.task_run_id == task_run_id
    assert done.task.pipeline_run_id == run_id
    assert done.task.pipeline_id == pipeline_id
    assert done.task.task_id == task_id
    assert done.task.status == "SUCCESS"
    assert done.task.target_count == 7
    assert done.task.attempts[0].status == "FAILED"
    assert done.task.attempts[0].task_run_id == task_run_id
    assert done.task.attempts[0].pipeline_run_id == run_id
    assert done.task.attempts[0].pipeline_id == pipeline_id
    doc = to_json(inspect.run_history(ctx, P, "load", selector=RunSelector(run_id=run_id)))
    assert doc["entries"][0]["attempts"][0]["schema"] == "etl-craft/attempt/1"
    assert "run_id" not in doc["entries"][0]
    assert "T" in doc["changes"][0]["requested_at"]
    assert action_count(ctx) == 1


def test_cancel_pause_resume_and_reconcile_do_not_invent_executions(ctx):
    _, run_id, _, _ = seed(ctx, active=True)
    paused = pipelines.set_pause(ctx, P, "maintenance", verb="pause")
    assert paused.pipeline.paused.paused_by == "api-user"
    assert paused.run is None and paused.task is None
    assert "T" in to_json(paused)["pipeline"]["paused"]["paused_at"]
    assert to_json(paused)["pipeline"]["paused"]["pipeline_code"] == "P"
    refused = runs.trigger_run(ctx, P)
    assert refused.run is None and refused.task is None
    task = tasks.run_task(ctx, P, "not_started")
    assert task.status == "SKIPPED" and task.task is None
    resumed = pipelines.set_pause(ctx, P, "ready", verb="resume")
    assert resumed.pipeline.paused is None
    cancelled = runs.cancel_run(ctx, P, "stop", selector=RunSelector(run_id=run_id))
    assert cancelled.status == "SUCCESS" and cancelled.run.status == "CANCELLED"
    report = runs.reconcile_runs(ctx, P)
    assert report.lost_attempt_ids == () and report.released_pipeline_run_ids == ()


def test_backfill_preserves_dates_ids_and_one_action(ctx):
    pipeline_id, _, _, _ = seed(ctx)
    done = runs.execute_run(
        ctx,
        RunRequest(
            P,
            backfill=(date(2026, 10, 1), date(2026, 10, 2)),
            reason="repair",
        ),
    )
    assert done.stopped is None and done.status == "SUCCESS"
    assert [result.run.run_date for result in done.runs] == [date(2026, 10, 1), date(2026, 10, 2)]
    assert len({result.run.pipeline_run_id for result in done.runs}) == 2
    assert all(
        result.run.pipeline_id == pipeline_id and result.run.backfill for result in done.runs
    )
    assert to_json(done)["schema"] == "etl-craft/backfill/1"
    assert action_count(ctx) == 1


def test_backfill_pause_has_a_stopped_outcome_without_a_run(ctx):
    seed(ctx)
    pipelines.set_pause(ctx, P, "maintenance", verb="pause")
    done = backfills.run_backfill(ctx, P, date(2026, 10, 1), date(2026, 10, 2), "repair")
    assert done.stopped is not None and done.stopped.run is None
    assert done.runs == (done.stopped,)


def test_task_result_uses_the_returned_task_id_not_another_pipelines_active_run(ctx, monkeypatch):
    pipeline_id, run_id, task_id, task_run_id = seed(ctx, active=True, task=True)
    with ctx.engine.begin() as conn:
        other = add_pipeline(conn, "OTHER")
        start_run(conn, other)

    def run_bound(engine, config, code, task_code, **options):
        assert current_actor() == ctx.actor
        assert options["selector"].run_id == run_id
        assert options["override"].reason == "verified"
        return runner.TaskOutcome(RunStatus.FAILED, "load failed", task_run_id)

    monkeypatch.setattr(runner, "run_task", run_bound)
    done = runs.execute_run(
        ctx,
        RunRequest(
            P,
            task_code="load",
            ignore_dependencies=True,
            reason="verified",
            selector=RunSelector(run_id=run_id),
            run_date=inspect.run_history(ctx, P).entries[0].run_date,
        ),
    )
    assert done.run.pipeline_id == pipeline_id
    assert done.run.pipeline_run_id == run_id
    assert done.task.task_run_id == task_run_id
    assert done.task.task_id == task_id


@pytest.mark.parametrize("method", ["force_task", "rerun_task"])
def test_task_overrides_return_the_selected_run_and_stored_summary(ctx, monkeypatch, method):
    _, run_id, _, task_run_id = seed(ctx, active=True, task=True)

    def override(engine, config, code, task_code, reason=None, **options):
        assert options["selector"].run_id == run_id
        return execution.PipelineOutcome(RunStatus.FAILED, "still failed", run_id)

    monkeypatch.setattr(execution, method, override)
    keywords = {"selector": RunSelector(run_id=run_id)}
    if method == "rerun_task":
        keywords["reason"] = "verified"
    done = getattr(tasks, method)(ctx, P, "load", **keywords)
    assert done.task.task_run_id == task_run_id
    assert done.run.pipeline_run_id == run_id
    assert done.status == done.task.status == "FAILED"


def test_wrong_pipeline_and_wrong_date_are_refused_without_changing_history(ctx):
    pipeline_id, run_id, _, _ = seed(ctx, active=True)
    with ctx.engine.begin() as conn:
        other = add_pipeline(conn, "OTHER")
        other_run = start_run(conn, other)
    before = to_json(inspect.run_history(ctx, P, all_runs=True))
    with pytest.raises(RunStateError):
        runs.finalize_run(ctx, P, selector=RunSelector(run_id=other_run))
    with pytest.raises(UsageError, match="a run's date cannot change"):
        runs.finalize_run(ctx, P, selector=RunSelector(run_id=run_id), run_date=date(2000, 1, 1))
    assert to_json(inspect.run_history(ctx, P, all_runs=True)) == before
    with ctx.engine.connect() as conn:
        with pytest.raises(RunStateError):
            run_view(conn, pipeline_id, other_run)
        with pytest.raises(UsageError):
            task_run_view(conn, pipeline_id)


def test_refused_api_request_keeps_its_actor_and_does_not_leak_scope(ctx):
    parent = current_actor()
    with pytest.raises(MetadataError):
        runs.initialize_run(ctx, "MISSING")
    assert current_actor() == parent
    with ctx.engine.connect() as conn:
        row = conn.execute(text("SELECT ACTOR, OUTCOME, ARGUMENTS FROM AUD_ACTIONS")).one()
    assert tuple(row)[:2] == ("api-user", "REQUESTED")
    assert json.loads(row[2])["pipeline_code"] == "MISSING"


def test_nested_requests_with_different_actors_are_distinct(ctx):
    seed(ctx)
    outer = Actor("outer", ActorKind.SYSTEM)
    with acting_as(outer), command_request("run", {"pipeline_code": "P"}):
        register_engine(ctx.engine)
        runs.initialize_run(ctx, P)
        assert current_actor() == outer
    actions = inspect.audit(ctx, P).actions
    assert [(a.actor, a.kind) for a in actions] == [("outer", "SYSTEM"), ("api-user", "HUMAN")]


@pytest.mark.parametrize(
    "command,service,function,extra",
    [
        ("run", runs, "execute_run", ["--init-only"]),
        ("mark", runs, "mark", ["--status", "FAILED", "--reason", "verified"]),
        ("cancel", runs, "cancel_run", ["--reason", "stop"]),
        ("pause", pipelines, "set_pause", ["--reason", "maintenance"]),
        ("resume", pipelines, "set_pause", ["--reason", "ready"]),
        ("reconcile", runs, "reconcile_runs", []),
        ("graph", inspect, "pipeline_graph", []),
        ("steps", inspect, "pipeline_steps", []),
        ("history", inspect, "run_history", ["--all"]),
        ("audit", inspect, "audit", []),
        ("list", inspect, "list_pipelines", []),
    ],
)
def test_cli_json_is_the_service_document_with_one_action(
    ctx, capsys, monkeypatch, command, service, function, extra
):
    seed(ctx, active=True, task=command in {"steps", "history"})
    if command == "resume":
        pipelines.set_pause(ctx, P, "maintenance", verb="pause")
    before_actions = action_count(ctx)
    observed = []
    original = getattr(service, function)

    def capture(*args, **kwargs):
        done = original(*args, **kwargs)
        observed.append(done)
        return done

    monkeypatch.setattr(service, function, capture)
    arguments = ["--config", str(ctx.config.config_path), command, "--format", "json"]
    if command != "list":
        arguments.extend(["--pipeline_code", "P"])
    code = main([*arguments, *extra])
    output = capsys.readouterr()
    assert code == 0, output.err
    assert len(observed) == 1
    assert json.loads(output.out) == to_json(observed[0])
    mutating = command in {"run", "mark", "cancel", "pause", "resume", "reconcile"}
    assert action_count(ctx) == before_actions + int(mutating)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"force": True, "init_only": True},
        {"ignore_dependencies": True},
        {"rerun": True, "ignore_dependencies": True, "task_code": "load"},
        {"force": True, "rerun": True, "task_code": "load"},
        {"with_downstream": True},
        {"skip": True, "task_code": "load"},
        {"reason": "unused"},
        {"backfill": (date.today(), date.today()), "skip": True},
        {"skip": True, "selector": RunSelector(run_id=1)},
        {"run_date": date.today(), "skip": True},
        {"task_code": "load", "init_only": True},
        {"task_code": ""},
    ],
)
def test_invalid_requests_are_rejected_before_opening_resources(kwargs):
    with pytest.raises(UsageError):
        RunRequest(P, **kwargs)


def test_serializer_returns_fresh_mappings_and_refuses_nondocuments():
    view = GraphView(1, "P", (("load",),), (), {"load": (("extract", "SUCCESS"),)}, ())
    document = to_json(view)
    document["depends_on"]["load"][0][0] = "changed"
    assert view.depends_on["load"][0][0] == "extract"
    for obj in (None, "text", date.today(), GraphView):
        with pytest.raises(UsageError):
            to_json(obj)
    bad = replace(view, depends_on={1: ()})
    with pytest.raises(UsageError, match="keys must be strings"):
        to_json(bad)
    bad = replace(view, depends_on={"bad": object()})
    with pytest.raises(UsageError, match="cannot serialize object"):
        to_json(bad)


def test_read_operations_leave_audit_actions_unchanged_and_keep_empty_documents(ctx):
    seed(ctx)
    assert to_json(inspect.list_pipelines(ctx))["pipelines"][0]["pipeline_id"] > 0
    assert to_json(inspect.run_history(ctx, P, all_runs=True))["entries"] == []
    assert inspect.pipeline_graph(ctx, P).waves == ()
    assert inspect.audit(ctx, P, since=datetime(2099, 1, 1, tzinfo=UTC)).actions == ()
    assert action_count(ctx) == 0


def test_json_numbers_must_be_finite():
    view = GraphView(1, "P", (), (), {"bad": float("inf")}, ())
    with pytest.raises(UsageError, match="finite"):
        to_json(view)


def test_missing_override_reason_is_refused_before_a_task_runs(ctx):
    seed(ctx, active=True, task=True)
    before = to_json(inspect.run_history(ctx, P, "load"))
    with pytest.raises(UsageError, match="needs a --reason"):
        runs.execute_run(ctx, RunRequest(P, task_code="load", ignore_dependencies=True))
    assert to_json(inspect.run_history(ctx, P, "load")) == before
    assert action_count(ctx) == 1


@pytest.mark.parametrize("view", ["list", "history"])
def test_inspection_reads_do_not_grow_per_pipeline_or_run(ctx, view):
    seed(ctx)
    queries = []

    def record(conn, cursor, statement, parameters, context, executemany):
        if statement.lstrip().upper().startswith("SELECT"):
            queries.append(statement)

    event.listen(ctx.engine, "before_cursor_execute", record)
    try:
        if view == "history":
            runs.stand_in_run(ctx, P, "SUCCESS", "verified")
        queries.clear()
        first = (
            inspect.list_pipelines(ctx)
            if view == "list"
            else inspect.run_history(ctx, P, all_runs=True)
        )
        baseline = len(queries)
        for number in range(1, 5):
            if view == "list":
                with ctx.engine.begin() as conn:
                    add_pipeline(conn, f"P{number}")
            else:
                runs.stand_in_run(ctx, P, "SUCCESS", "verified")
        queries.clear()
        expanded = (
            inspect.list_pipelines(ctx)
            if view == "list"
            else inspect.run_history(ctx, P, all_runs=True)
        )
        assert len(queries) == baseline
        field = "pipelines" if view == "list" else "entries"
        assert len(getattr(first, field)) == 1
        assert len(getattr(expanded, field)) == 5
    finally:
        event.remove(ctx.engine, "before_cursor_execute", record)
