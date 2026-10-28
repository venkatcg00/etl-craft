"""Durable child admission, immutable handoff and explicit trigger generations."""

import json
from datetime import UTC, date, datetime, timedelta

import pytest
from sqlalchemy import text

from etl_craft.core.actor import Actor, ActorKind
from etl_craft.core.enums import ActionName, ApiRole, AttemptStatus, Handler, RunStatus
from etl_craft.engine import transitions
from etl_craft.engine.runlog import RunSelector
from etl_craft.execution import leases
from etl_craft.execution.attempt_types import ChildOptions, TaskRun
from etl_craft.execution.runner import admit_attempt, execute_attempt
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.actions import invoke
from fixtures.auth import service_token
from fixtures.metadata import add_pipeline, add_task


@pytest.fixture
def trigger_scene(metadata_project):
    return lambda **params: build_scene(metadata_project, **params)


def build_scene(
    metadata_project, *, task_type="ETL", handler=Handler.RUN_PIPELINE, other_task=False, **params
):
    project = metadata_project
    service_token(project.engine)
    with project.engine.begin() as conn:
        parent = add_pipeline(conn, "PARENT")
        child = add_pipeline(conn, "CHILD")
        task = add_task(
            conn,
            parent,
            "trigger",
            handler,
            **{"TRIGGER_PIPELINE_CODE": "CHILD", **params},
        )
        conn.execute(
            text("UPDATE CFG_TASKS SET TASK_TYPE=:kind WHERE TASK_ID=:id"),
            {"id": task, "kind": task_type},
        )
        for name, value in params.items():
            if value is None:
                conn.execute(
                    text(
                        "UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE=NULL "
                        "WHERE TASK_ID=:id AND PARAMETER_NAME=:name"
                    ),
                    {"id": task, "name": name},
                )
        if other_task:
            add_task(conn, parent, "other", Handler.PYTHON)
        run = transitions.create_run(
            conn, parent, Actor("test-admin", ActorKind.SERVICE), run_date=date(2026, 9, 2)
        )
    ctx = OperationContext(
        project.engine, project.config, Actor("test-admin", ActorKind.SERVICE), role=ApiRole.ADMIN
    )
    return project, ctx, child, task, run


def check_trigger_waits(engine, run_id, now):
    from etl_craft.execution.trigger_waits import check_trigger_waits as check

    return check(engine, run_id, now)


def owned(scene, *, rerun=False, decisions=()):
    from etl_craft.services.operations.triggers import TriggerRequest

    project, ctx, _, task, run = scene
    spec = admit_attempt(
        project.engine,
        project.config,
        TaskRun(
            pipeline_code="PARENT",
            task_code="trigger",
            task_id=task,
            pipeline_run_id=run,
            force=False,
            decisions=decisions,
        ),
        rerun=rerun,
    )
    owner = leases.owner_id()
    with project.engine.begin() as conn:
        transitions.claim_attempt(
            conn,
            spec.attempt_id,
            ctx.actor,
            owner=owner,
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
        )
    return TriggerRequest(task_run_id=spec.task_run_id, attempt_id=spec.attempt_id, owner=owner)


@pytest.mark.parametrize(
    "ending", [RunStatus.SUCCESS, RunStatus.SKIPPED, RunStatus.FAILED, RunStatus.CANCELLED]
)
def test_wait_handoff_and_each_child_ending(trigger_scene, ending):
    trigger_scene = trigger_scene()
    project, ctx, _, _, run = trigger_scene
    request = owned(trigger_scene)
    result = invoke(ctx, ActionName.TRIGGER_PIPELINE, request)
    with project.engine.begin() as conn:
        attempt = conn.execute(
            text("SELECT STATUS AS status FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"),
            {"id": request.attempt_id},
        ).one()
        assert attempt.status == AttemptStatus.HANDED_OFF
        child = conn.execute(
            text(
                "SELECT STATUS AS status, STARTED_BY AS started_by, "
                "TRIGGERED_BY_TASK_RUN_ID AS task_run_id FROM AUD_PIPELINES_RUN_LOG "
                "WHERE PIPELINE_RUN_ID=:id"
            ),
            {"id": result.pipeline_run_id},
        ).one()
        assert child.status == RunStatus.QUEUED
        assert child.started_by == ctx.actor.name
        assert child.task_run_id == request.task_run_id
        transitions.admit_run(conn, result.pipeline_run_id, ctx.actor)
        transitions.finalize_pipeline_run(conn, result.pipeline_run_id, ending)
    assert check_trigger_waits(project.engine, run, datetime.now(UTC)) == []
    with project.engine.connect() as conn:
        summary = conn.execute(
            text("SELECT STATUS AS status FROM AUD_TASK_RUN_LOG WHERE TASK_RUN_ID=:id"),
            {"id": request.task_run_id},
        ).one()
        assert summary.status == ending


def test_real_attempt_frees_its_slot_after_handoff(trigger_scene):
    trigger_scene = trigger_scene()
    project, ctx, _, task, run = trigger_scene
    from etl_craft.services.operations.pipeline_refresh import RefreshInput

    refresh = invoke(ctx, ActionName.PIPELINE_REFRESH, RefreshInput(pipeline_code="CHILD"))
    assert not refresh.failed, refresh
    spec = admit_attempt(
        project.engine,
        project.config,
        TaskRun(
            pipeline_code="PARENT",
            task_code="trigger",
            task_id=task,
            pipeline_run_id=run,
            force=False,
        ),
    )
    outcome = execute_attempt(project.engine, project.config, spec, ChildOptions())
    assert outcome.waiting
    assert outcome.status == RunStatus.IN_PROGRESS
    assert check_trigger_waits(project.engine, run, datetime.now(UTC))


def test_detach_ends_only_after_child_is_committed(trigger_scene):
    trigger_scene = trigger_scene(TRIGGER_MODE="DETACH")
    project, ctx, _, _task, run = trigger_scene
    request = owned(trigger_scene)
    done = invoke(ctx, ActionName.TRIGGER_PIPELINE, request)
    assert done.status == RunStatus.SUCCESS
    with project.engine.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT STATUS AS status FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID=:id"
                ),
                {"id": done.pipeline_run_id},
            ).scalar_one()
            == RunStatus.QUEUED
        )
        assert (
            conn.execute(
                text("SELECT STATUS AS status FROM AUD_TASK_RUN_LOG WHERE TASK_RUN_ID=:id"),
                {"id": request.task_run_id},
            ).scalar_one()
            == RunStatus.SUCCESS
        )
        assert (
            conn.execute(
                text("SELECT COUNT(*) AS n FROM AUD_GATE_WAITS WHERE PIPELINE_RUN_ID=:id"),
                {"id": run},
            ).scalar_one()
            == 0
        )


def test_timeout_leaves_child_running(trigger_scene):
    trigger_scene = trigger_scene(TRIGGER_TIMEOUT_MINUTES="1")
    project, ctx, _, _task, run = trigger_scene
    request = owned(trigger_scene)
    child = invoke(ctx, ActionName.TRIGGER_PIPELINE, request)
    assert check_trigger_waits(project.engine, run, datetime.now(UTC) + timedelta(minutes=2)) == []
    with project.engine.connect() as conn:
        error = conn.execute(
            text("SELECT ERROR_MESSAGE AS message FROM AUD_TASK_RUN_LOG WHERE TASK_RUN_ID=:id"),
            {"id": request.task_run_id},
        ).scalar_one()
        assert "child keeps running" in error and str(child.pipeline_run_id) in error
        assert (
            conn.execute(
                text(
                    "SELECT STATUS AS status FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID=:id"
                ),
                {"id": child.pipeline_run_id},
            ).scalar_one()
            == RunStatus.QUEUED
        )


def test_explicit_rerun_retains_summary_and_creates_new_child(trigger_scene):
    trigger_scene = trigger_scene()
    project, ctx, _, _, run = trigger_scene
    first_request = owned(trigger_scene)
    first = invoke(ctx, ActionName.TRIGGER_PIPELINE, first_request)
    with project.engine.begin() as conn:
        transitions.admit_run(conn, first.pipeline_run_id, ctx.actor)
        transitions.finalize_pipeline_run(conn, first.pipeline_run_id, RunStatus.SUCCESS)
    check_trigger_waits(project.engine, run, datetime.now(UTC))
    second_request = owned(trigger_scene, rerun=True)
    second = invoke(ctx, ActionName.TRIGGER_PIPELINE, second_request)
    assert second_request.task_run_id == first_request.task_run_id
    assert second.pipeline_run_id != first.pipeline_run_id
    with project.engine.connect() as conn:
        generation = conn.execute(
            text(
                "SELECT TRIGGER_GENERATION AS generation FROM AUD_TASK_RUN_LOG "
                "WHERE TASK_RUN_ID=:id"
            ),
            {"id": first_request.task_run_id},
        ).scalar_one()
        assert generation == 1


def test_mark_waiting_task_closes_wait_without_cancelling_child(trigger_scene):
    trigger_scene = trigger_scene()
    from etl_craft.services.operations.requests import MarkRequest

    project, ctx, _, _, run = trigger_scene
    child = invoke(ctx, ActionName.TRIGGER_PIPELINE, owned(trigger_scene))
    invoke(
        ctx,
        ActionName.MARK,
        MarkRequest(
            pipeline_code="PARENT",
            run_id=run,
            task_code="trigger",
            status=RunStatus.SKIPPED,
            reason="operator chose to proceed",
        ),
    )
    assert check_trigger_waits(project.engine, run, datetime.now(UTC)) == []
    with project.engine.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT STATUS AS status FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID=:id"
                ),
                {"id": child.pipeline_run_id},
            ).scalar_one()
            == RunStatus.QUEUED
        )


@pytest.mark.parametrize("mode", ["WAIT", "DETACH"])
@pytest.mark.parametrize("action", ["STOP", "ABORT"])
def test_parent_stop_only_cancels_wait_children(trigger_scene, mode, action):
    trigger_scene = trigger_scene(TRIGGER_MODE=mode)
    from etl_craft.core.enums import ControlAction
    from etl_craft.services.operations.requests import ControlRequest

    project, ctx, _, _task, run = trigger_scene
    request = owned(trigger_scene)
    child = invoke(ctx, ActionName.TRIGGER_PIPELINE, request)
    from etl_craft.engine import controls

    with project.engine.begin() as conn:
        controls.observe_exit(conn, request.attempt_id, queries_ended=True)
    invoke(
        ctx,
        ActionName.CONTROL,
        ControlRequest(
            pipeline_code="PARENT", run_id=run, action=ControlAction(action), reason="stop parent"
        ),
    )
    with project.engine.connect() as conn:
        status = conn.execute(
            text("SELECT STATUS AS status FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID=:id"),
            {"id": child.pipeline_run_id},
        ).scalar_one()
        assert status == (RunStatus.CANCELLED if mode == "WAIT" else RunStatus.QUEUED)

    from etl_craft.execution.trigger_waits import settle_ended_parent_waits

    settle_ended_parent_waits(project.engine, datetime.now(UTC))
    with project.engine.connect() as conn:
        summary = conn.execute(
            text("SELECT STATUS AS status FROM AUD_TASK_RUN_LOG WHERE TASK_RUN_ID=:id"),
            {"id": request.task_run_id},
        ).scalar_one()
        assert summary == (RunStatus.CANCELLED if mode == "WAIT" else RunStatus.SUCCESS)


def test_interrupted_admission_rolls_back_child_and_handoff(trigger_scene, monkeypatch):
    from etl_craft.services.operations import triggers

    scene = trigger_scene()
    project, ctx, _, _, _ = scene
    request = owned(scene)

    def fail(point):
        if point == "trigger.after_child":
            raise OSError("agent interrupted before handoff")

    with monkeypatch.context() as patch:
        patch.setattr(triggers, "fault_point", fail)
        with pytest.raises(OSError, match="interrupted"):
            invoke(ctx, ActionName.TRIGGER_PIPELINE, request)
    with project.engine.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) AS n FROM AUD_PIPELINES_RUN_LOG WHERE "
                    "TRIGGERED_BY_TASK_RUN_ID=:id"
                ),
                {"id": request.task_run_id},
            ).scalar_one()
            == 0
        )
    invoke(ctx, ActionName.TRIGGER_PIPELINE, request)
    with project.engine.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) AS n FROM AUD_PIPELINES_RUN_LOG WHERE "
                    "TRIGGERED_BY_TASK_RUN_ID=:id"
                ),
                {"id": request.task_run_id},
            ).scalar_one()
            == 1
        )


def test_paused_child_is_refused_with_remedy(trigger_scene):
    from etl_craft.core.errors import RunRefusedError
    from etl_craft.engine.repository.pauses import record_pause

    scene = trigger_scene()
    project, ctx, child, _, _ = scene
    request = owned(scene)
    with project.engine.begin() as conn:
        record_pause(conn, child, ctx.actor.name, "maintenance")
    with pytest.raises(RunRefusedError, match=r"maintenance.*resume.*mark"):
        invoke(ctx, ActionName.TRIGGER_PIPELINE, request)


@pytest.mark.parametrize("mode", ["WAIT", "DETACH"])
@pytest.mark.parametrize("resume", [False, True])
def test_controller_runs_child_and_resumes_parent(trigger_scene, mode, resume):
    import threading
    import time
    from concurrent.futures import ThreadPoolExecutor
    from dataclasses import replace

    from etl_craft.controller.supervisor import serve
    from etl_craft.core.actor import acting_as

    scene = trigger_scene(TRIGGER_MODE=mode)
    project, ctx, child, _task, _ = scene
    with project.engine.begin() as conn:
        add_task(
            conn, child, "work", Handler.PYTHON, SCRIPT_FILE="python_scripts/sales/fetch_orders.py"
        )
    (project.config.python_scripts_dir / "sales/fetch_orders.py").write_text(
        "from etl_craft.scripting import ScriptResult\ndef run(task):\n    return ScriptResult(1)\n"
    )
    # One agent slot proves a parent wait cannot hold the only slot needed by its child.
    ctx = replace(
        ctx,
        config=replace(project.config, limits=replace(project.config.limits, max_parallel_tasks=1)),
    )
    if resume:
        invoke(ctx, ActionName.TRIGGER_PIPELINE, owned(scene))
    stop = threading.Event()

    def supervise():
        with acting_as(ctx.actor):
            serve(ctx, stop)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(supervise)
        try:
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                with project.engine.connect() as conn:
                    rows = (
                        conn.execute(
                            text(
                                "SELECT STATUS AS status FROM AUD_PIPELINES_RUN_LOG ORDER BY "
                                "PIPELINE_RUN_ID"
                            )
                        )
                        .scalars()
                        .all()
                    )
                if len(rows) == 2 and all(status == RunStatus.SUCCESS for status in rows):
                    break
                if future.done():
                    future.result()
                stop.wait(0.05)
            else:
                pytest.fail(f"controller did not finish parent and child: {rows}")
        finally:
            stop.set()
            future.result(timeout=10)


@pytest.mark.parametrize("target", ["MISSING", "PARENT", "INACTIVE"])
def test_validate_refuses_invalid_targets(trigger_scene, target):
    from etl_craft.services.validate import validate

    scene = trigger_scene()
    project, _, _, task, _ = scene
    with project.engine.begin() as conn:
        inactive = add_pipeline(conn, "INACTIVE")
        conn.execute(
            text("UPDATE CFG_PIPELINES SET ACTIVE_FLAG='N' WHERE PIPELINE_ID=:id"), {"id": inactive}
        )
        conn.execute(
            text(
                "UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE=:target WHERE "
                "TASK_ID=:id AND PARAMETER_NAME='TRIGGER_PIPELINE_CODE'"
            ),
            {"id": task, "target": target},
        )
    report = validate(project.engine, project.config)
    assert any(f.status.value == "FAIL" and target in f.message for f in report.findings)


@pytest.mark.parametrize("dependency", [False, True])
def test_validate_refuses_trigger_and_dependency_cycles(trigger_scene, dependency):
    from etl_craft.services.validate import validate
    from fixtures.metadata import add_pipeline_dependency

    scene = trigger_scene()
    project, _, child, _, _ = scene
    with project.engine.begin() as conn:
        parent = conn.execute(
            text("SELECT PIPELINE_ID AS id FROM CFG_PIPELINES WHERE PIPELINE_CODE='PARENT'")
        ).scalar_one()
        if dependency:
            add_pipeline_dependency(conn, child, parent)
        else:
            add_task(conn, child, "back", Handler.RUN_PIPELINE, TRIGGER_PIPELINE_CODE="PARENT")
    report = validate(project.engine, project.config)
    assert any(
        "cycle" in f.message
        and "PARENT" in f.message
        and "CHILD" in f.message
        and "remove" in f.message
        for f in report.findings
    )


def test_run_links_status_and_catalog_share_exact_identities(trigger_scene, tmp_path):
    from etl_craft.services.catalog.collect import collect_catalog
    from etl_craft.services.catalog.site import write_site
    from etl_craft.services.operations.requests import RunFilter, RunId

    scene = trigger_scene()
    project, ctx, _, _, run = scene
    request = owned(scene)
    child = invoke(ctx, ActionName.TRIGGER_PIPELINE, request)
    parent_view = invoke(ctx, ActionName.RUN_DETAIL, RunId(run_id=run))
    child_view = invoke(ctx, ActionName.RUN_DETAIL, RunId(run_id=child.pipeline_run_id))
    assert parent_view.child_run_ids == (child.pipeline_run_id,)
    assert child_view.parent_run_id == run
    assert child_view.triggered_by_task_run_id == request.task_run_id
    status = invoke(ctx, ActionName.STATUS, RunFilter(pipeline_code="PARENT", run_id=run))
    assert status.tasks[0].explanation.state == "waiting on child"
    assert str(child.pipeline_run_id) in status.tasks[0].explanation.next_action
    catalog = collect_catalog(project.engine, project.config)
    assert [link.child_run_id for link in catalog.trigger_links] == [child.pipeline_run_id]
    folder = tmp_path / "catalog"
    write_site(catalog, project.config, folder)
    overview = (folder / "dags.html").read_text()
    assert "→ WAIT →" in overview and "PARENT.trigger" in overview
    run_html = (folder / "runs" / f"{run}.html").read_text()
    assert f"{child.pipeline_run_id}.html" in run_html
    child_html = (folder / "runs" / f"{child.pipeline_run_id}.html").read_text()
    assert f"{run}.html" in child_html


@pytest.mark.parametrize(
    "name,value,mode",
    [
        ("TRIGGER_TIMEOUT_MINUTES", "0", "WAIT"),
        ("TRIGGER_TIMEOUT_MINUTES", "-1", "WAIT"),
        ("TRIGGER_TIMEOUT_MINUTES", "nan", "WAIT"),
        ("TRIGGER_TIMEOUT_MINUTES", "inf", "WAIT"),
        ("TRIGGER_TIMEOUT_MINUTES", "abc", "WAIT"),
        ("TRIGGER_TIMEOUT_MINUTES", "NONE", "DETACH"),
        ("TRIGGER_MODE", "wait", None),
        ("TRIGGER_MODE", "OTHER", None),
        ("TRIGGER_PIPELINE_CODE", "", None),
        ("TRIGGER_PIPELINE_CODE", None, None),
        ("TRIGGER_MODE", None, None),
        ("TRIGGER_TIMEOUT_MINUTES", None, "WAIT"),
    ],
)
def test_invalid_trigger_parameters_fail_validation_and_admission(trigger_scene, name, value, mode):
    from etl_craft.core.errors import MetadataError
    from etl_craft.services.validate import validate

    params = {name: value}
    if mode:
        params["TRIGGER_MODE"] = mode
    scene = trigger_scene(**params)
    project, ctx, _, _task, _ = scene
    report = validate(project.engine, project.config)
    assert any(
        name in finding.message and "expected" in finding.message for finding in report.findings
    )
    with pytest.raises(MetadataError, match=name):
        invoke(ctx, ActionName.TRIGGER_PIPELINE, owned(scene))


def test_http_run_details_expose_parent_and_child_links(trigger_scene):
    from fastapi.testclient import TestClient

    from etl_craft.api.app import create_app

    scene = trigger_scene()
    project, ctx, _, _, run = scene
    token = service_token(project.engine, name="reader")
    request = owned(scene)
    child = invoke(ctx, ActionName.TRIGGER_PIPELINE, request)
    with TestClient(create_app(ctx)) as client:
        headers = {"Authorization": f"Bearer {token}"}
        parent = client.get(f"/api/v1/projects/default/runs/{run}", headers=headers)
        nested = client.get(
            f"/api/v1/projects/default/runs/{child.pipeline_run_id}", headers=headers
        )
    assert parent.status_code == nested.status_code == 200
    assert parent.json()["child_run_ids"] == [child.pipeline_run_id]
    assert nested.json()["parent_run_id"] == run
    assert nested.json()["triggered_by_task_run_id"] == request.task_run_id
    assert nested.json()["run_date"] == "2026-09-02"


@pytest.mark.parametrize(
    "policy,retries", [("TRANSIENT", False), ("TRANSIENT_AND_UNKNOWN", False), ("ANY", True)]
)
def test_child_failure_is_permanent_for_captured_retry_policy(trigger_scene, policy, retries):
    from etl_craft.execution.retries import refresh_retries

    scene = trigger_scene(RETRIES="1", RETRY_DELAY_SECONDS="0", RETRY_ON=policy)
    project, ctx, _, task, run = scene
    request = owned(scene)
    child = invoke(ctx, ActionName.TRIGGER_PIPELINE, request)
    with project.engine.begin() as conn:
        transitions.admit_run(conn, child.pipeline_run_id, ctx.actor)
        transitions.finalize_pipeline_run(conn, child.pipeline_run_id, RunStatus.FAILED)
    check_trigger_waits(project.engine, run, datetime.now(UTC))
    waiting, exhausted = refresh_retries(project.engine, project.config, run, {task})
    assert (task in waiting) == retries
    if retries:
        again = invoke(ctx, ActionName.TRIGGER_PIPELINE, owned(scene))
        assert again.pipeline_run_id == child.pipeline_run_id
        check_trigger_waits(project.engine, run, datetime.now(UTC))
        assert task in refresh_retries(project.engine, project.config, run, {task})[1]
    else:
        assert task in exhausted


def test_child_queues_behind_an_explicit_existing_run(trigger_scene):
    scene = trigger_scene()
    project, ctx, child, _, _ = scene
    with project.engine.begin() as conn:
        active = transitions.create_run(conn, child, ctx.actor)
    started = invoke(ctx, ActionName.TRIGGER_PIPELINE, owned(scene))
    assert started.pipeline_run_id != active
    with project.engine.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT STATUS AS status FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID=:id"
                ),
                {"id": started.pipeline_run_id},
            ).scalar_one()
            == RunStatus.QUEUED
        )


def test_marked_failed_wait_does_not_automatically_relaunch(trigger_scene):
    from etl_craft.core.enums import RetryOn
    from etl_craft.execution.retries import refresh_retries
    from etl_craft.services.operations.requests import MarkRequest

    scene = trigger_scene(RETRIES="1", RETRY_ON=RetryOn.ANY)
    project, ctx, _, task, run = scene
    invoke(ctx, ActionName.TRIGGER_PIPELINE, owned(scene))
    invoke(
        ctx,
        ActionName.MARK,
        MarkRequest(
            pipeline_code="PARENT",
            run_id=run,
            task_code="trigger",
            status=RunStatus.FAILED,
            reason="operator ending the wait",
        ),
    )
    assert refresh_retries(project.engine, project.config, run, {task}) == ({}, {task})


def test_marking_another_task_preserves_skipped_trigger_history(trigger_scene):
    from etl_craft.services.operations.requests import MarkRequest

    scene = trigger_scene(other_task=True)
    project, ctx, _, _, run = scene
    request = owned(scene)
    child = invoke(ctx, ActionName.TRIGGER_PIPELINE, request)
    with project.engine.begin() as conn:
        transitions.admit_run(conn, child.pipeline_run_id, ctx.actor)
        transitions.finalize_pipeline_run(conn, child.pipeline_run_id, RunStatus.SKIPPED)
    check_trigger_waits(project.engine, run, datetime.now(UTC))
    invoke(
        ctx,
        ActionName.MARK,
        MarkRequest(
            pipeline_code="PARENT",
            run_id=run,
            task_code="other",
            status=RunStatus.SUCCESS,
            reason="manual completion",
        ),
    )
    with project.engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT STATUS AS status FROM AUD_TASK_RUN_LOG WHERE TASK_RUN_ID=:id"),
                {"id": request.task_run_id},
            ).scalar_one()
            == RunStatus.SKIPPED
        )


@pytest.mark.parametrize("ending", [RunStatus.SUCCESS, RunStatus.FAILED])
def test_wait_completion_consumes_only_admitted_dependency_snapshot(trigger_scene, ending):
    from etl_craft.core.enums import GateOutcome
    from etl_craft.engine.repository import trackers
    from fixtures.metadata import add_dependency, upstream_run

    scene = trigger_scene()
    project, ctx, _, task, run = scene
    with project.engine.begin() as conn:
        parent = conn.execute(
            text("SELECT PIPELINE_ID AS id FROM CFG_PIPELINES WHERE PIPELINE_CODE='PARENT'")
        ).scalar_one()
        upstream = add_pipeline(conn, "UPSTREAM")
        feed = add_task(conn, upstream, "feed", Handler.PYTHON)
        upstream_id, rows = upstream_run(conn, upstream, {feed: (RunStatus.SUCCESS, 1)})
        edge = add_dependency(conn, parent, task, feed, upstream_pipeline=upstream)
    selected = trackers.FinishedRun(
        rows[feed], RunStatus.SUCCESS, True, pipeline_run_id=upstream_id
    )
    decision = trackers.GateDecision(
        edge, True, selected, GateOutcome.SATISFIED, "admitted upstream"
    )
    request = owned(scene, decisions=(decision,))
    child = invoke(ctx, ActionName.TRIGGER_PIPELINE, request)
    with project.engine.begin() as conn:
        assert (
            conn.execute(
                text("SELECT COUNT(*) AS n FROM AUD_DEPENDENCY_CONSUMPTION WHERE TASK_ID=:id"),
                {"id": task},
            ).scalar_one()
            == 0
        )
        upstream_run(conn, upstream, {feed: (RunStatus.SUCCESS, 1)})
        transitions.admit_run(conn, child.pipeline_run_id, ctx.actor)
        transitions.finalize_pipeline_run(conn, child.pipeline_run_id, ending)
    check_trigger_waits(project.engine, run, datetime.now(UTC))
    with project.engine.connect() as conn:
        consumed = conn.execute(
            text(
                "SELECT CONSUMED_PIPELINE_RUN_ID AS upstream_run_id, "
                "CONSUMED_TASK_RUN_ID AS task_run_id "
                "FROM AUD_DEPENDENCY_CONSUMPTION WHERE TASK_ID=:id"
            ),
            {"id": task},
        ).all()
        assert bool(consumed) == (ending == RunStatus.SUCCESS)
        if consumed:
            assert consumed[0].upstream_run_id == upstream_id
            assert consumed[0].task_run_id == rows[feed]


def test_trigger_task_requires_etl_type(trigger_scene):
    from etl_craft.core.errors import MetadataError
    from etl_craft.services.validate import validate

    scene = trigger_scene(task_type="INGESTION")
    project, ctx, _, _task, _ = scene
    request = owned(scene)
    assert any(
        "TASK_TYPE" in finding.message and "ETL" in finding.message
        for finding in validate(project.engine, project.config).findings
    )
    with pytest.raises(MetadataError, match="TASK_TYPE"):
        invoke(ctx, ActionName.TRIGGER_PIPELINE, request)


@pytest.mark.parametrize("ending", [RunStatus.FAILED, RunStatus.CANCELLED])
def test_child_failure_details_transfer_without_aggregating_counts(trigger_scene, ending):
    scene = trigger_scene()
    project, ctx, child, _, run = scene
    request = owned(scene)
    result = invoke(ctx, ActionName.TRIGGER_PIPELINE, request)
    with project.engine.begin() as conn:
        work = add_task(conn, child, "work", Handler.PYTHON)
        transitions.admit_run(conn, result.pipeline_run_id, ctx.actor)
        child_task = transitions.find_or_create_task_run(conn, work, result.pipeline_run_id)
        transitions.finish_task_run(
            conn,
            child_task.task_run_id,
            status=RunStatus.FAILED,
            error_message="source feed rejected the batch",
        )
        transitions.finalize_pipeline_run(conn, result.pipeline_run_id, ending)
    check_trigger_waits(project.engine, run, datetime.now(UTC))
    with project.engine.connect() as conn:
        summary = conn.execute(
            text(
                "SELECT STATUS AS status, ERROR_MESSAGE AS message, "
                "SOURCE_COUNT AS source_count, TARGET_COUNT AS target_count "
                "FROM AUD_TASK_RUN_LOG WHERE TASK_RUN_ID=:id"
            ),
            {"id": request.task_run_id},
        ).one()
        assert summary.status == ending
        assert "work" in summary.message and "source feed rejected the batch" in summary.message
        assert summary.source_count is None and summary.target_count is None


@pytest.mark.parametrize("missing", [False, True])
def test_trigger_request_refuses_missing_or_other_handler_attempt(trigger_scene, missing):
    from dataclasses import replace

    from etl_craft.core.errors import ResourceNotFoundError, UsageError

    scene = trigger_scene(handler=Handler.RUN_PIPELINE if missing else Handler.PYTHON)
    project, ctx, _, _, _ = scene
    request = owned(scene)
    if missing:
        request = replace(request, task_run_id=request.task_run_id + 1000)
    error = ResourceNotFoundError if missing else UsageError
    message = "outside this project or missing" if missing else "expected an existing RUN_PIPELINE"
    with pytest.raises(error, match=message):
        invoke(ctx, ActionName.TRIGGER_PIPELINE, request)
    with project.engine.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) AS n FROM AUD_PIPELINES_RUN_LOG "
                    "WHERE TRIGGER_KIND='PIPELINE_TASK'"
                )
            ).scalar_one()
            == 0
        )


@pytest.mark.parametrize("late", [False, True])
def test_resumed_wait_uses_child_ending_time_for_deadline(trigger_scene, late):
    from etl_craft.core.time import as_utc

    scene = trigger_scene()
    project, ctx, _, _, run = scene
    request = owned(scene)
    child = invoke(ctx, ActionName.TRIGGER_PIPELINE, request)
    with project.engine.begin() as conn:
        transitions.admit_run(conn, child.pipeline_run_id, ctx.actor)
        transitions.finalize_pipeline_run(conn, child.pipeline_run_id, RunStatus.SUCCESS)
        ended = as_utc(
            conn.execute(
                text(
                    "SELECT END_DATE AS ended FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID=:id"
                ),
                {"id": child.pipeline_run_id},
            ).scalar_one()
        )
        conn.execute(
            text("UPDATE AUD_GATE_WAITS SET WAIT_UNTIL=:deadline WHERE PIPELINE_RUN_ID=:id"),
            {"id": run, "deadline": ended + timedelta(seconds=-1 if late else 1)},
        )
    check_trigger_waits(project.engine, run, ended + timedelta(minutes=1))
    with project.engine.connect() as conn:
        status = conn.execute(
            text("SELECT STATUS AS status FROM AUD_TASK_RUN_LOG WHERE TASK_RUN_ID=:id"),
            {"id": request.task_run_id},
        ).scalar_one()
        assert status == (RunStatus.FAILED if late else RunStatus.SUCCESS)


def test_explicit_rerun_of_ended_parent_keeps_controller_wait_open(trigger_scene):
    from etl_craft.services.operations.requests import RunRequest

    scene = trigger_scene()
    project, ctx, _, _, run = scene
    from etl_craft.services.operations.pipeline_refresh import RefreshInput

    checked = invoke(ctx, ActionName.PIPELINE_REFRESH, RefreshInput(pipeline_code="CHILD"))
    assert not checked.failed, checked
    first = invoke(ctx, ActionName.TRIGGER_PIPELINE, owned(scene))
    with project.engine.begin() as conn:
        transitions.admit_run(conn, first.pipeline_run_id, ctx.actor)
        transitions.finalize_pipeline_run(conn, first.pipeline_run_id, RunStatus.SUCCESS)
    check_trigger_waits(project.engine, run, datetime.now(UTC))
    with project.engine.begin() as conn:
        transitions.finalize_pipeline_run(conn, run, RunStatus.SUCCESS)
    result = invoke(
        ctx,
        ActionName.RUN,
        RunRequest(
            pipeline_code="PARENT",
            task_code="trigger",
            run_id=run,
            rerun=True,
            reason="launch the child again",
        ),
    )
    assert result.status == RunStatus.IN_PROGRESS
    with project.engine.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT STATUS AS status FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID=:id"
                ),
                {"id": run},
            ).scalar_one()
            == RunStatus.IN_PROGRESS
        )
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) AS n FROM AUD_PIPELINES_RUN_LOG "
                    "WHERE TRIGGERED_BY_TASK_RUN_ID IS NOT NULL"
                )
            ).scalar_one()
            == 2
        )


@pytest.mark.parametrize("invalid", ["actor", "lease"])
def test_trigger_admission_refuses_unowned_or_expired_request_atomically(trigger_scene, invalid):
    from dataclasses import replace

    from etl_craft.core.errors import PermissionDeniedError, StaleTransitionError

    scene = trigger_scene()
    project, ctx, _, _, _ = scene
    request = owned(scene)
    if invalid == "actor":
        ctx = replace(ctx, actor=Actor("other-admin", ActorKind.SERVICE))
        error = PermissionDeniedError
    else:
        with project.engine.begin() as conn:
            conn.execute(
                text("UPDATE AUD_TASK_ATTEMPTS SET LEASE_EXPIRES_AT=:expired WHERE ATTEMPT_ID=:id"),
                {"id": request.attempt_id, "expired": datetime.now(UTC) - timedelta(minutes=1)},
            )
        error = StaleTransitionError
    with pytest.raises(error):
        invoke(ctx, ActionName.TRIGGER_PIPELINE, request)
    with project.engine.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) AS n FROM AUD_PIPELINES_RUN_LOG "
                    "WHERE TRIGGER_KIND='PIPELINE_TASK'"
                )
            ).scalar_one()
            == 0
        )


def test_inactive_trigger_parameters_do_not_create_phantom_cycles(trigger_scene):
    from etl_craft.services.validate import validate

    scene = trigger_scene()
    project, _, _, task, _ = scene
    with project.engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE='PARENT', ACTIVE_FLAG='N' "
                "WHERE TASK_ID=:id AND PARAMETER_NAME='TRIGGER_PIPELINE_CODE'"
            ),
            {"id": task},
        )
    findings = validate(project.engine, project.config).findings
    assert any("TRIGGER_PIPELINE_CODE" in finding.message for finding in findings)
    assert not any("cycle" in finding.message for finding in findings)


@pytest.mark.parametrize("disabled", ["account", "group", "grant", "other-group"])
def test_retained_claims_recheck_current_restrictions_before_each_child(trigger_scene, disabled):
    from etl_craft.core.errors import PermissionDeniedError, ResourceNotFoundError
    from fixtures.actions import act

    scene = trigger_scene()
    project, ctx, _, _, parent = scene
    group = act(ctx, ActionName.GROUP_ADD, name="team").groups[0]
    other = act(ctx, ActionName.GROUP_ADD, name="other").groups[0]
    with project.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_ACCOUNTS SET ROLE='viewer' WHERE ACCOUNT_NAME='test-admin'"))
        conn.execute(
            text("UPDATE AUD_PIPELINES_RUN_LOG SET SSO_GROUP_IDS=:ids WHERE PIPELINE_RUN_ID=:id"),
            {"ids": f"[{group.group_id},{other.group_id}]", "id": parent},
        )
        conn.execute(
            text(
                "INSERT INTO CFG_PIPELINE_GRANTS (GROUP_ID, PIPELINE_CODE, PERMISSION) "
                "VALUES (:id, 'CHILD', 'CAN_RUN')"
            ),
            {"id": group.group_id},
        )
        target = {
            "other-group": "CFG_GROUPS",
            "account": "CFG_ACCOUNTS",
            "group": "CFG_GROUPS",
            "grant": "CFG_PIPELINE_GRANTS",
        }[disabled]
        condition = f" WHERE GROUP_ID={other.group_id}" if disabled == "other-group" else ""
        conn.execute(text(f"UPDATE {target} SET ACTIVE_FLAG='N'{condition}"))
    request = owned(scene)
    expected = ResourceNotFoundError if disabled in ("account", "grant") else PermissionDeniedError
    message = (
        r"project 'default' is missing or not permitted"
        if disabled in ("account", "grant")
        else r"test-admin.*CHILD.*operator.*(permission|group grant)"
    )
    with pytest.raises(expected, match=message):
        invoke(ctx, ActionName.TRIGGER_PIPELINE, request)
    with project.engine.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) AS n FROM AUD_PIPELINES_RUN_LOG "
                    "WHERE TRIGGERED_BY_TASK_RUN_ID=:id"
                ),
                {"id": request.task_run_id},
            ).scalar_one()
            == 0
        )


def test_children_and_grandchildren_copy_the_admitted_group_ids(trigger_scene):
    from fixtures.actions import act

    scene = trigger_scene()
    project, ctx, child, _, parent = scene
    group = act(ctx, ActionName.GROUP_ADD, name="team").groups[0]
    with project.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_ACCOUNTS SET ROLE='viewer' WHERE ACCOUNT_NAME='test-admin'"))
        conn.execute(
            text("UPDATE AUD_PIPELINES_RUN_LOG SET SSO_GROUP_IDS=:ids WHERE PIPELINE_RUN_ID=:id"),
            {"ids": f"[{group.group_id}]", "id": parent},
        )
        grand = add_pipeline(conn, "GRANDCHILD")
        task = add_task(
            conn, child, "grand", Handler.RUN_PIPELINE, TRIGGER_PIPELINE_CODE="GRANDCHILD"
        )
        for code in ("CHILD", "GRANDCHILD"):
            conn.execute(
                text(
                    "INSERT INTO CFG_PIPELINE_GRANTS (GROUP_ID, PIPELINE_CODE, PERMISSION) "
                    "VALUES (:id, :code, 'CAN_RUN')"
                ),
                {"id": group.group_id, "code": code},
            )
    first = invoke(ctx, ActionName.TRIGGER_PIPELINE, owned(scene))
    with project.engine.begin() as conn:
        transitions.admit_run(conn, first.pipeline_run_id, ctx.actor)
    second_scene = (project, ctx, grand, task, first.pipeline_run_id)
    second = invoke(ctx, ActionName.TRIGGER_PIPELINE, owned(second_scene))
    with project.engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT SSO_GROUP_IDS AS ids FROM AUD_PIPELINES_RUN_LOG "
                "WHERE PIPELINE_RUN_ID IN (:child, :grand)"
            ),
            {"child": first.pipeline_run_id, "grand": second.pipeline_run_id},
        ).all()
        assert len(rows) == 2 and all(json.loads(row.ids) == [group.group_id] for row in rows)


@pytest.mark.parametrize("disabled", ["account", "group", "grant", "other-group"])
def test_queued_child_rechecks_permission_when_it_reaches_admission(trigger_scene, disabled):
    from etl_craft.execution.pipeline import run_pipeline
    from etl_craft.services.cloning import run_hooks
    from fixtures.actions import act

    scene = trigger_scene()
    project, ctx, _, _, parent = scene
    group = act(ctx, ActionName.GROUP_ADD, name="team").groups[0]
    other = act(ctx, ActionName.GROUP_ADD, name="other").groups[0]
    with project.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_ACCOUNTS SET ROLE='viewer' WHERE ACCOUNT_NAME='test-admin'"))
        conn.execute(
            text("UPDATE AUD_PIPELINES_RUN_LOG SET SSO_GROUP_IDS=:ids WHERE PIPELINE_RUN_ID=:id"),
            {"ids": f"[{group.group_id},{other.group_id}]", "id": parent},
        )
        conn.execute(
            text(
                "INSERT INTO CFG_PIPELINE_GRANTS (GROUP_ID, PIPELINE_CODE, PERMISSION) "
                "VALUES (:id, 'CHILD', 'CAN_RUN')"
            ),
            {"id": group.group_id},
        )
    child = invoke(ctx, ActionName.TRIGGER_PIPELINE, owned(scene))
    with project.engine.begin() as conn:
        target = {
            "other-group": "CFG_GROUPS",
            "account": "CFG_ACCOUNTS",
            "group": "CFG_GROUPS",
            "grant": "CFG_PIPELINE_GRANTS",
        }[disabled]
        condition = f" WHERE GROUP_ID={other.group_id}" if disabled == "other-group" else ""
        conn.execute(text(f"UPDATE {target} SET ACTIVE_FLAG='N'{condition}"))
    outcome = run_pipeline(
        project.engine,
        project.config,
        "CHILD",
        selector=RunSelector(run_id=child.pipeline_run_id),
        hooks=run_hooks(project.config, project.engine),
    )
    assert outcome.status == RunStatus.CANCELLED
    with project.engine.connect() as conn:
        reason = conn.execute(
            text("SELECT REASON AS reason FROM AUD_RUN_INTERVENTIONS WHERE PIPELINE_RUN_ID=:id"),
            {"id": child.pipeline_run_id},
        ).scalar_one()
        if disabled in ("account", "grant"):
            assert "project 'default' is missing or not permitted" in reason
        else:
            assert "test-admin" in reason and "CHILD" in reason and "operator" in reason


def test_admitted_trigger_edge_cannot_close_a_new_child_cycle(trigger_scene):
    from etl_craft.core.errors import MetadataError

    scene = trigger_scene()
    project, ctx, child, task, _ = scene
    with project.engine.begin() as conn:
        add_pipeline(conn, "OTHER")
        conn.execute(
            text(
                "UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE='OTHER' WHERE "
                "TASK_ID=:id AND PARAMETER_NAME='TRIGGER_PIPELINE_CODE'"
            ),
            {"id": task},
        )
        add_task(conn, child, "return", Handler.RUN_PIPELINE, TRIGGER_PIPELINE_CODE="PARENT")
    with pytest.raises(MetadataError, match=r"cycle.*acyclic.*remove"):
        invoke(ctx, ActionName.TRIGGER_PIPELINE, owned(scene))
    with project.engine.connect() as conn:
        assert (
            conn.execute(text("SELECT count(*) AS n FROM AUD_PIPELINES_RUN_LOG")).scalar_one() == 1
        )
