"""Standing controls retain version history and validate before modifying it."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.enums import ActionName, ApiRole, ControlAction, RunStatus
from etl_craft.core.errors import RunStateError, UsageError
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.requests import ControlRequest
from fixtures.metadata import add_pipeline, add_task


@pytest.fixture
def paused_task(engine_db):
    with engine_db.engine.begin() as conn:
        add_task(conn, add_pipeline(conn, "P"), "T", "EMAIL_ALERT")
    ctx = OperationContext(engine_db.engine, engine_db.config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
    request = ControlRequest(
        pipeline_code="P",
        task_code="T",
        action=ControlAction.PAUSE,
        reason="operator test",
        outcome=RunStatus.SUCCESS,
    )
    return ctx, request


def test_task_pause_replacement_is_explicit_and_keeps_both_versions(paused_task):
    ctx, request = paused_task
    invoke(ctx, ActionName.CONTROL, request)
    with pytest.raises(
        RunStateError, match=r"actor=.*outcome=SUCCESS.*expiry=.*reason=.*--replace"
    ):
        invoke(ctx, ActionName.CONTROL, request)
    invoke(ctx, ActionName.CONTROL, replace(request, replace=True, outcome=RunStatus.FAILED))
    with ctx.engine.connect() as conn:
        versions = conn.execute(
            text(
                "SELECT OUTCOME AS outcome,RESUMED_AT AS resumed FROM AUD_PIPELINE_PAUSES "
                "ORDER BY PIPELINE_PAUSE_ID"
            )
        ).all()
    assert [v.outcome for v in versions] == [RunStatus.SUCCESS, RunStatus.FAILED]
    assert versions[0].resumed is not None and versions[1].resumed is None


def test_task_pause_replace_requires_an_active_pause(paused_task):
    ctx, request = paused_task
    with pytest.raises(RunStateError, match="--replace found no active task pause"):
        invoke(ctx, ActionName.CONTROL, replace(request, replace=True))


@pytest.mark.parametrize("expiry", [datetime(2026, 1, 1), datetime(2000, 1, 1, tzinfo=UTC)])
def test_task_pause_expiry_requires_a_future_timestamp_with_offset(paused_task, expiry):
    ctx, request = paused_task
    with pytest.raises(UsageError, match=r"ISO-8601.*offset"):
        invoke(ctx, ActionName.CONTROL, replace(request, expires_at=expiry))


def test_task_resume_refuses_schedule_choice_and_removes_the_outcome(paused_task):
    ctx, request = paused_task
    invoke(ctx, ActionName.CONTROL, request)
    resume = replace(request, action=ControlAction.RESUME, outcome=None)
    with pytest.raises(UsageError, match="tasks have no schedule ticks"):
        invoke(ctx, ActionName.CONTROL, replace(resume, then="wait"))
    invoke(ctx, ActionName.CONTROL, resume)
    with pytest.raises(RunStateError, match="no active task pause"):
        invoke(ctx, ActionName.CONTROL, resume)


@pytest.mark.parametrize("outcome", [RunStatus.SUCCESS, RunStatus.FAILED, RunStatus.SKIPPED])
def test_standing_pause_outcome_creates_no_attempt_or_data(cli_project, outcome):
    project = cli_project
    code, output = project.run(
        "pause",
        "--pipeline-code",
        "P",
        "--task-code",
        "load",
        "--outcome",
        outcome,
        "--reason",
        "maintenance",
    )
    assert code == 0, output
    code, output = project.run("run", "--pipeline-code", "P")
    assert code == (1 if outcome == RunStatus.FAILED else 0), output
    with project.engine.connect() as conn:
        task = conn.execute(
            text(
                "SELECT STATUS AS status,ROWS_WRITTEN AS rows_written,ERROR_MESSAGE AS message "
                "FROM AUD_TASK_RUN_LOG"
            )
        ).one()
        assert task.status == outcome
        assert not task.rows_written
        assert "by standing pause" in task.message
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS")).scalar_one() == 0
        version = conn.execute(
            text(
                "SELECT ITEM_KEY AS version FROM AUD_RUN_INTERVENTIONS WHERE ACTION='PAUSE_APPLIED'"
            )
        ).scalar_one()
        assert version == "1"


def test_replacing_pause_changes_only_later_admissions_and_keeps_earlier_runs_held(paused_task):
    from etl_craft.engine import standing_pauses, transitions

    ctx, request = paused_task
    with ctx.engine.begin() as conn:
        pipeline = conn.execute(text("SELECT PIPELINE_ID FROM CFG_PIPELINES")).scalar_one()
        task = conn.execute(text("SELECT TASK_ID FROM CFG_TASKS")).scalar_one()
        before = transitions.create_run(conn, pipeline, ctx.actor, status=RunStatus.QUEUED)
        transitions.admit_run(conn, before, ctx.actor)
        transitions.finish_run(conn, before, RunStatus.SUCCESS, ctx.actor)
    invoke(ctx, ActionName.CONTROL, request)
    with ctx.engine.begin() as conn:
        first = transitions.create_run(conn, pipeline, ctx.actor)
        transitions.finish_run(conn, first, RunStatus.SUCCESS, ctx.actor)
    invoke(ctx, ActionName.CONTROL, replace(request, replace=True, outcome=RunStatus.FAILED))
    with ctx.engine.begin() as conn:
        later = transitions.create_run(conn, pipeline, ctx.actor)
        now = datetime.now(UTC)
        old_pause, held = standing_pauses.at_task(conn, pipeline, before, task, now)
        assert held and old_pause.outcome == RunStatus.FAILED
        pinned, held = standing_pauses.at_task(conn, pipeline, first, task, now)
        assert not held and pinned.pause_id == 1 and pinned.outcome == RunStatus.SUCCESS
        pinned, held = standing_pauses.at_task(conn, pipeline, later, task, now)
        assert not held and pinned.pause_id == 2 and pinned.outcome == RunStatus.FAILED
    invoke(ctx, ActionName.CONTROL, replace(request, action=ControlAction.RESUME, outcome=None))
    with ctx.engine.begin() as conn:
        assert standing_pauses.at_task(conn, pipeline, before, task, now) == (None, False)


@pytest.fixture
def scheduled_pause(paused_task, monkeypatch):
    from etl_craft.services.operations import pipeline_resume

    ctx, request = paused_task
    with ctx.engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE CFG_PIPELINES SET RUN_SCHEDULE='@daily',CATCHUP='Y',MAX_CATCHUP_RUNS=2,"
                "SCHEDULE_START_DATE='2026-11-01'"
            )
        )

    class FixedTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 11, 4, 12, tzinfo=UTC)

    monkeypatch.setattr(pipeline_resume, "datetime", FixedTime)
    pause = replace(request, task_code=None, outcome=None)
    invoke(ctx, ActionName.CONTROL, pause)
    return ctx, replace(pause, action=ControlAction.RESUME)


@pytest.mark.parametrize(("choice", "expected"), [("run-now", 1), ("catch-up", 2), ("wait", 0)])
def test_scheduled_resume_creates_only_the_chosen_runs(scheduled_pause, choice, expected):
    from etl_craft.controller.schedules import Schedules

    ctx, request = scheduled_pause
    result = invoke(ctx, ActionName.CONTROL, replace(request, then=choice))
    assert "runs created=" in result.message
    with ctx.engine.connect() as conn:
        assert (
            conn.execute(text("SELECT COUNT(*) FROM AUD_PIPELINES_RUN_LOG")).scalar_one()
            == expected
        )
        reason = conn.execute(text("SELECT RESUME_REASON FROM AUD_PIPELINE_PAUSES")).scalar_one()
        assert f"then={choice}" in reason and "runs created=" in reason
    Schedules().refresh(ctx, datetime(2026, 11, 4, 12, tzinfo=UTC))
    with ctx.engine.connect() as conn:
        assert (
            conn.execute(text("SELECT COUNT(*) FROM AUD_PIPELINES_RUN_LOG")).scalar_one()
            == expected
        )


def test_scheduled_resume_requires_choice_and_keeps_pause_when_refused(scheduled_pause):
    ctx, request = scheduled_pause
    with pytest.raises(UsageError, match=r"run-now\|catch-up\|wait.*catch-up would create 2"):
        invoke(ctx, ActionName.CONTROL, request)
    with ctx.engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT COUNT(*) FROM AUD_PIPELINE_PAUSES WHERE RESUMED_AT IS NULL")
            ).scalar_one()
            == 1
        )


def test_unscheduled_resume_refuses_then(paused_task):
    ctx, request = paused_task
    pause = replace(request, task_code=None, outcome=None)
    invoke(ctx, ActionName.CONTROL, pause)
    with pytest.raises(UsageError, match=r"unscheduled.*omit --then"):
        invoke(ctx, ActionName.CONTROL, replace(pause, action=ControlAction.RESUME, then="wait"))


def test_pause_expiry_releases_held_runs_without_rewriting_history(paused_task):
    from etl_craft.engine import standing_pauses, transitions

    ctx, request = paused_task
    expires = datetime.now(UTC) + timedelta(hours=1)
    invoke(ctx, ActionName.CONTROL, replace(request, outcome=None, expires_at=expires))
    with ctx.engine.begin() as conn:
        pipeline = conn.execute(text("SELECT PIPELINE_ID FROM CFG_PIPELINES")).scalar_one()
        task = conn.execute(text("SELECT TASK_ID FROM CFG_TASKS")).scalar_one()
        run = transitions.create_run(conn, pipeline, ctx.actor)
        assert standing_pauses.at_task(conn, pipeline, run, task, expires - timedelta(seconds=1))[1]
        assert standing_pauses.at_task(conn, pipeline, run, task, expires) == (None, False)
        assert conn.execute(text("SELECT RESUMED_AT FROM AUD_PIPELINE_PAUSES")).scalar_one() is None


def test_pause_and_run_concurrently_capture_one_consistent_version(paused_task):
    from etl_craft.engine import standing_pauses, transitions
    from fixtures.races import two_at_once

    ctx, request = paused_task
    with ctx.engine.connect() as conn:
        pipeline = conn.execute(text("SELECT PIPELINE_ID FROM CFG_PIPELINES")).scalar_one()
        task = conn.execute(text("SELECT TASK_ID FROM CFG_TASKS")).scalar_one()

    def admit():
        with ctx.engine.begin() as conn:
            return transitions.create_run(conn, pipeline, ctx.actor)

    def pause():
        return invoke(ctx, ActionName.CONTROL, request)

    _, run = two_at_once(pause, admit, at=(ctx.engine, "begin"))
    with ctx.engine.begin() as conn:
        pinned = conn.execute(
            text("SELECT ITEM_KEY FROM AUD_RUN_INTERVENTIONS WHERE ACTION='PAUSE_ADMISSION'")
        ).scalar_one_or_none()
        pause, held = standing_pauses.at_task(conn, pipeline, run, task, datetime.now(UTC))
    assert pause is not None
    assert held == (pinned is None)
    assert pinned is None or pinned == str(pause.pause_id)


def test_standing_pause_api_uses_grants_and_refuses_cross_project(paused_task):
    from fastapi.testclient import TestClient

    from etl_craft.api.app import create_app
    from etl_craft.services.operations.actions import ACTIONS, action_path
    from fixtures.actions import act

    ctx, _ = paused_task
    viewer = act(ctx, ActionName.TOKEN_CREATE, name="pause-viewer", role=ApiRole.VIEWER)
    runner = act(ctx, ActionName.TOKEN_CREATE, name="pause-runner", role=ApiRole.VIEWER)
    for name, permission in (("pause-viewer", "CAN_VIEW"), ("pause-runner", "CAN_RUN")):
        act(
            ctx,
            ActionName.ACCESS_GRANT,
            project="default",
            account=name,
            permission=permission,
            reason="role matrix",
        )
    client = TestClient(create_app(ctx))
    route = action_path(ACTIONS[ActionName.CONTROL]).format(project="default")
    body = {"pipeline_code": "P", "task_code": "T", "action": "PAUSE", "reason": "maintenance"}
    assert client.post(route, json=body).status_code == 401
    for token, expected in ((viewer.token, 403), (runner.token, 200)):
        response = client.post(route, json=body, headers={"Authorization": "Bearer " + token})
        assert response.status_code == expected, response.text
    body["action"] = "RESUME"
    for token, expected in ((viewer.token, 403), (runner.token, 200)):
        response = client.post(route, json=body, headers={"Authorization": "Bearer " + token})
        assert response.status_code == expected, response.text
    for expiry in ("2026-12-01T12:00:00", "2000-01-01T12:00:00+00:00"):
        invalid = {**body, "action": "PAUSE", "expires_at": expiry}
        response = client.post(
            route, json=invalid, headers={"Authorization": "Bearer " + runner.token}
        )
        assert response.status_code == 400, response.text
        assert expiry in response.text and "offset" in response.text
    with ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_PIPELINES SET RUN_SCHEDULE='@daily',SCHEDULE_START_DATE=:today"),
            {"today": datetime.now(UTC).date().isoformat()},
        )
    pipeline_body = {"pipeline_code": "P", "action": "PAUSE", "reason": "maintenance"}
    headers = {"Authorization": "Bearer " + runner.token}
    assert client.post(route, json=pipeline_body, headers=headers).status_code == 200
    pipeline_body["action"] = "RESUME"
    refused = client.post(route, json=pipeline_body, headers=headers)
    assert refused.status_code == 400
    assert "run-now|catch-up|wait" in refused.text
    assert (
        client.post(route, json={**pipeline_body, "then": "wait"}, headers=headers).status_code
        == 200
    )
    hidden = route.replace("/default/", "/hidden/")
    missing = client.post(hidden, json=body, headers=headers)
    assert missing.status_code == 404
    act(ctx, ActionName.PROJECT_ADD, project="hidden", reason="isolation test")
    with ctx.engine.begin() as conn:
        hidden_id = conn.execute(
            text("SELECT PROJECT_ID FROM CFG_PROJECTS WHERE PROJECT_CODE='hidden'")
        ).scalar_one()
        hidden_pipeline = add_pipeline(conn, "P", project_id=hidden_id)
        add_task(conn, hidden_pipeline, "T", "EMAIL_ALERT")
    response = client.post(hidden, json=body, headers=headers)
    assert response.status_code == 404, response.text
    assert response.json() == missing.json()
    with ctx.engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT COUNT(*) FROM AUD_PIPELINE_PAUSES WHERE PIPELINE_ID=:id"),
                {"id": hidden_pipeline},
            ).scalar_one()
            == 0
        )


def test_preexisting_run_holds_only_the_paused_task_then_executes_after_resume(cli_project):
    project = cli_project
    root = project.config.project_dir
    (project.config.python_scripts_dir / "load.py").write_text(
        "from pathlib import Path\nfrom etl_craft.scripting import ScriptResult\n"
        "def run(task):\n"
        f"    Path({str(root)!r}, task.task_code).write_text('ran')\n"
        "    return ScriptResult(1)\n"
    )
    with project.engine.begin() as conn:
        add_task(conn, project.pipeline_id, "other", "PYTHON", SCRIPT_FILE="python_scripts/load.py")
    code, output = project.run("pipelines", "refresh")
    assert code == 0, output
    code, output = project.run("run", "--pipeline-code", "P", "--init-only")
    assert code == 0, output
    code, output = project.run(
        "pause",
        "--pipeline-code",
        "P",
        "--task-code",
        "load",
        "--outcome",
        "SUCCESS",
        "--reason",
        "hold older run",
    )
    assert code == 0, output
    parent = project.start("run", "--pipeline-code", "P")
    try:
        project.wait_for(
            "SELECT r.STATUS FROM AUD_TASK_RUN_LOG r JOIN CFG_TASKS t ON t.TASK_ID=r.TASK_ID "
            "WHERE t.TASK_CODE='other'",
            expected="SUCCESS",
        )
        assert (root / "other").read_text() == "ran"
        assert not (root / "load").exists()
        code, output = project.run("status", "--pipeline-code", "P")
        assert code == 0 and "standing pause" in output and "hold older run" in output
        code, output = project.run(
            "resume", "--pipeline-code", "P", "--task-code", "load", "--reason", "maintenance done"
        )
        assert code == 0, output
        assert parent.wait() == 0, parent.output
        assert (root / "load").read_text() == "ran"
    finally:
        parent.close()


def test_supplied_success_has_no_data_and_no_fresh_cross_pipeline_output(cli_project):
    from etl_craft.core.enums import DependencyType
    from etl_craft.engine.repository import trackers
    from etl_craft.execution.gates import judge

    project = cli_project
    (project.config.python_scripts_dir / "load.py").write_text(
        "from sqlalchemy import text\nfrom etl_craft.scripting import ScriptResult\n"
        "def run(task):\n"
        "    with task.warehouse() as engine, engine.begin() as conn:\n"
        "        conn.execute(text('CREATE TABLE standing_proof AS SELECT 1 AS id'))\n"
        "    return ScriptResult(1)\n"
    )
    code, output = project.run("pipelines", "refresh")
    assert code == 0, output
    code, output = project.run(
        "pause",
        "--pipeline-code",
        "P",
        "--task-code",
        "load",
        "--outcome",
        "SUCCESS",
        "--reason",
        "no source data",
    )
    assert code == 0, output
    assert project.run("run", "--pipeline-code", "P")[0] == 0
    with project.engine.connect() as conn:
        pipeline = trackers.fetch_latest_finished_pipeline_run(
            conn, project.pipeline_id, datetime.now(UTC)
        )
        task = trackers.fetch_latest_finished_task_run(conn, project.task_id)
    import duckdb

    with duckdb.connect(str(project.config.project_dir / "warehouse.duckdb")) as warehouse:
        assert warehouse.execute(
            "SELECT COUNT(*) AS count FROM information_schema.tables "
            "WHERE table_name='standing_proof'"
        ).to_arrow_table().to_pylist() == [{"count": 0}]
    assert pipeline is not None and not pipeline.has_data and pipeline.revision == 0
    assert task is not None and not task.has_data and task.revision == 0
    for upstream in (pipeline, task):
        assert judge(DependencyType.SUCCESS, upstream, None)[0] is None
        assert judge(DependencyType.HAS_DATA, upstream, None)[0] is None


@pytest.mark.parametrize("override", ["--rerun", "--ignore-dependencies"])
def test_explicit_dependency_overrides_never_resume_a_standing_pause(cli_project, override):
    project = cli_project
    assert project.run("run", "--pipeline-code", "P", "--init-only")[0] == 0
    assert (
        project.run(
            "pause", "--pipeline-code", "P", "--task-code", "load", "--reason", "maintenance"
        )[0]
        == 0
    )
    code, output = project.run(
        "run",
        "--pipeline-code",
        "P",
        "--task-code",
        "load",
        override,
        "--reason",
        "dependency override",
    )
    assert "standing pause" in output, (code, output)
    with project.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS")).scalar_one() == 0
        assert (
            conn.execute(text("SELECT STATUS FROM AUD_PIPELINES_RUN_LOG")).scalar_one()
            == RunStatus.IN_PROGRESS
        )


def test_real_repair_after_supplied_success_publishes_a_fresh_revision(cli_project):
    from etl_craft.engine.repository import trackers

    project = cli_project
    assert (
        project.run(
            "pause",
            "--pipeline-code",
            "P",
            "--task-code",
            "load",
            "--outcome",
            "SUCCESS",
            "--reason",
            "maintenance",
        )[0]
        == 0
    )
    assert project.run("run", "--pipeline-code", "P")[0] == 0
    with project.engine.connect() as conn:
        run = conn.execute(text("SELECT PIPELINE_RUN_ID FROM AUD_PIPELINES_RUN_LOG")).scalar_one()
    assert (
        project.run(
            "resume", "--pipeline-code", "P", "--task-code", "load", "--reason", "repair now"
        )[0]
        == 0
    )
    code, output = project.run(
        "run",
        "--pipeline-code",
        "P",
        "--task-code",
        "load",
        "--run-id",
        str(run),
        "--rerun",
        "--reason",
        "produce actual output",
    )
    assert code == 0, output
    with project.engine.connect() as conn:
        finished = trackers.fetch_latest_finished_pipeline_run(
            conn, project.pipeline_id, datetime.now(UTC)
        )
        task = trackers.fetch_latest_finished_task_run(conn, project.task_id)
    assert finished.revision == task.revision == 1
    assert finished.has_data and task.has_data


@pytest.mark.parametrize(
    ("outcome", "condition", "runs_child"),
    [
        (RunStatus.SUCCESS, "SUCCESS", True),
        (RunStatus.FAILED, "FAILURE", True),
        (RunStatus.SKIPPED, "ALWAYS", True),
        (RunStatus.SUCCESS, "HAS_DATA", False),
    ],
)
def test_standing_outcomes_drive_dependencies_without_retrying_the_paused_task(
    cli_project,
    outcome,
    condition,
    runs_child,
):
    from fixtures.metadata import add_dependency

    project = cli_project
    marker = project.config.project_dir / "child-ran"
    (project.config.python_scripts_dir / "child.py").write_text(
        "from pathlib import Path\nfrom etl_craft.scripting import ScriptResult\n"
        "def run(task):\n"
        f"    Path({str(marker)!r}).write_text('ran')\n"
        "    return ScriptResult(1)\n"
    )
    with project.engine.begin() as conn:
        child = add_task(
            conn, project.pipeline_id, "child", "PYTHON", SCRIPT_FILE="python_scripts/child.py"
        )
        add_dependency(conn, project.pipeline_id, child, project.task_id, condition)
        conn.execute(
            text(
                "INSERT INTO CFG_TASK_PARAMETERS (TASK_ID,PARAMETER_NAME,PARAMETER_VALUE) "
                "VALUES (:task,'RETRIES','3')"
            ),
            {"task": project.task_id},
        )
    code, output = project.run("pipelines", "refresh")
    assert code == 0, output
    assert (
        project.run(
            "pause",
            "--pipeline-code",
            "P",
            "--task-code",
            "load",
            "--outcome",
            outcome,
            "--reason",
            "dependency test",
        )[0]
        == 0
    )
    project.run("run", "--pipeline-code", "P")
    assert marker.exists() == runs_child
    with project.engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT TASK_ID AS task_id,STATUS AS status,ATTEMPT_COUNT AS attempts "
                "FROM AUD_TASK_RUN_LOG"
            )
        ).all()
    statuses = {row.task_id: row for row in rows}
    assert statuses[project.task_id].status == outcome
    assert statuses[project.task_id].attempts == 0
    assert statuses[child].status == (RunStatus.SUCCESS if runs_child else RunStatus.SKIPPED)


def test_replacement_never_exposes_an_unpaused_window(paused_task):
    from sqlalchemy.engine import Connection

    from etl_craft.engine.connection import read_snapshot
    from fixtures.races import paused_after

    ctx, request = paused_task
    invoke(ctx, ActionName.CONTROL, request)

    def closing(_conn, query, *args, **kwargs):
        return query.get_execution_options().get("etl_craft_query") == "standing_pause_close"

    def replacement():
        return invoke(
            ctx, ActionName.CONTROL, replace(request, replace=True, outcome=RunStatus.FAILED)
        )

    with (
        paused_after(replacement, owner=Connection, name="execute", when=closing),
        read_snapshot(ctx.engine) as conn,
    ):
        assert (
            conn.execute(
                text("SELECT PIPELINE_PAUSE_ID FROM AUD_PIPELINE_PAUSES WHERE RESUMED_AT IS NULL")
            ).scalar_one()
            == 1
        )
    with read_snapshot(ctx.engine) as conn:
        assert (
            conn.execute(
                text("SELECT PIPELINE_PAUSE_ID FROM AUD_PIPELINE_PAUSES WHERE RESUMED_AT IS NULL")
            ).scalar_one()
            == 2
        )


def test_non_tty_scheduled_resume_requires_then_without_closing_pause(cli_project):
    project = cli_project
    with project.engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_PIPELINES SET RUN_SCHEDULE='@daily',SCHEDULE_START_DATE=:today"),
            {"today": datetime.now(UTC).date().isoformat()},
        )
    assert project.run("pipelines", "refresh")[0] == 0
    assert project.run("pause", "--pipeline-code", "P", "--reason", "maintenance")[0] == 0
    code, output = project.run("resume", "--pipeline-code", "P", "--reason", "maintenance over")
    assert code == UsageError.exit_code, output
    assert "--then run-now|catch-up|wait" in output
    assert "catch-up would create" in output
    with project.engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT COUNT(*) FROM AUD_PIPELINE_PAUSES WHERE RESUMED_AT IS NULL")
            ).scalar_one()
            == 1
        )
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_PIPELINES_RUN_LOG")).scalar_one() == 0


def test_queued_run_keeps_its_pause_version_when_later_admitted(paused_task):
    from etl_craft.engine import standing_pauses, transitions

    ctx, request = paused_task
    invoke(ctx, ActionName.CONTROL, request)
    with ctx.engine.begin() as conn:
        pipeline = conn.execute(text("SELECT PIPELINE_ID FROM CFG_PIPELINES")).scalar_one()
        task = conn.execute(text("SELECT TASK_ID FROM CFG_TASKS")).scalar_one()
        queued = transitions.create_run(conn, pipeline, ctx.actor, status=RunStatus.QUEUED)
    invoke(ctx, ActionName.CONTROL, replace(request, replace=True, outcome=RunStatus.FAILED))
    with ctx.engine.begin() as conn:
        transitions.admit_run(conn, queued, ctx.actor)
        control, held = standing_pauses.at_task(conn, pipeline, queued, task, datetime.now(UTC))
        assert not held
        assert control.pause_id == 1 and control.outcome == RunStatus.SUCCESS


def test_metadata_graph_and_config_reads_show_the_standing_pause(cli_project):
    from etl_craft.services.operations.config_files import ConfigDirectory
    from etl_craft.services.operations.requests import PipelineInput

    project = cli_project
    ctx = OperationContext(project.engine, project.config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
    invoke(
        ctx,
        ActionName.CONTROL,
        ControlRequest(
            pipeline_code="P",
            task_code="load",
            action=ControlAction.PAUSE,
            outcome=RunStatus.SUCCESS,
            reason="visible maintenance",
        ),
    )
    for action in (ActionName.PIPELINE, ActionName.GRAPH):
        view = invoke(ctx, action, PipelineInput(pipeline_code="P"))
        assert len(view.task_pauses) == 1
        pause = view.task_pauses[0]
        assert pause.task_code == "load" and pause.outcome == RunStatus.SUCCESS
        assert pause.reason == "visible maintenance" and pause.actor == SYSTEM_ACTOR.name
    folder = project.config.project_dir / "config"
    folder.mkdir()
    (folder / "pipelines.csv").write_text("PIPELINE_CODE,ACTIVE_FLAG\nP,Y\n")
    for action in (ActionName.CONFIG_PLAN, ActionName.CONFIG_APPLY):
        result = invoke(ctx, action, ConfigDirectory(config_dir=folder))
        assert not result.failed
        assert len(result.task_pauses) == 1
        assert "visible maintenance" in result.task_pauses[0].describe()


def test_resume_catchup_preserves_skip_overlap_policy(scheduled_pause):
    from etl_craft.engine import transitions

    ctx, request = scheduled_pause
    with ctx.engine.begin() as conn:
        pipeline = conn.execute(text("SELECT PIPELINE_ID FROM CFG_PIPELINES")).scalar_one()
        conn.execute(text("UPDATE CFG_PIPELINES SET OVERLAP_POLICY='SKIP'"))
        active = transitions.create_run(conn, pipeline, ctx.actor)
    invoke(ctx, ActionName.CONTROL, replace(request, then="catch-up"))
    with ctx.engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT PIPELINE_RUN_ID AS id,STATUS AS status,TRIGGER_KIND AS kind "
                "FROM AUD_PIPELINES_RUN_LOG"
            )
        ).all()
    assert next(row.status for row in rows if row.id == active) == RunStatus.IN_PROGRESS
    ticks = [row for row in rows if row.kind == "SCHEDULE"]
    assert len(ticks) == 2 and all(row.status == RunStatus.SKIPPED for row in ticks)


def test_resume_catchup_uses_the_persisted_project_timezone(scheduled_pause):
    ctx, request = scheduled_pause
    with ctx.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_PROJECTS SET TIMEZONE='America/New_York'"))
    invoke(ctx, ActionName.CONTROL, replace(request, then="catch-up"))
    with ctx.engine.connect() as conn:
        keys = (
            conn.execute(text("SELECT RUN_KEY AS key FROM AUD_PIPELINES_RUN_LOG")).scalars().all()
        )
    assert len(keys) == 2
    assert all(datetime.fromisoformat(key.removeprefix("schedule:")).hour == 5 for key in keys)


def test_resume_then_new_pause_does_not_revive_the_old_admitted_outcome(paused_task):
    from etl_craft.engine import standing_pauses, transitions

    ctx, request = paused_task
    invoke(ctx, ActionName.CONTROL, request)
    with ctx.engine.begin() as conn:
        pipeline = conn.execute(text("SELECT PIPELINE_ID FROM CFG_PIPELINES")).scalar_one()
        task = conn.execute(text("SELECT TASK_ID FROM CFG_TASKS")).scalar_one()
        run = transitions.create_run(conn, pipeline, ctx.actor)
    invoke(ctx, ActionName.CONTROL, replace(request, action=ControlAction.RESUME, outcome=None))
    invoke(ctx, ActionName.CONTROL, replace(request, outcome=RunStatus.FAILED))
    with ctx.engine.begin() as conn:
        pause, held = standing_pauses.at_task(conn, pipeline, run, task, datetime.now(UTC))
        assert held and pause.outcome == RunStatus.FAILED


def test_several_atomic_replacements_keep_the_original_admitted_outcome(paused_task):
    from etl_craft.engine import standing_pauses, transitions

    ctx, request = paused_task
    invoke(ctx, ActionName.CONTROL, request)
    with ctx.engine.begin() as conn:
        pipeline = conn.execute(text("SELECT PIPELINE_ID FROM CFG_PIPELINES")).scalar_one()
        task = conn.execute(text("SELECT TASK_ID FROM CFG_TASKS")).scalar_one()
        run = transitions.create_run(conn, pipeline, ctx.actor)
    for outcome in (RunStatus.FAILED, RunStatus.SKIPPED):
        invoke(ctx, ActionName.CONTROL, replace(request, replace=True, outcome=outcome))
    with ctx.engine.begin() as conn:
        pause, held = standing_pauses.at_task(conn, pipeline, run, task, datetime.now(UTC))
        assert not held and pause.outcome == RunStatus.SUCCESS
