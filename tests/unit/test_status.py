"""Pure explanations and CLI documents reuse the selected operation snapshot."""

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from etl_craft.cli import build_parser, main
from etl_craft.cli.commands import Command
from etl_craft.core.errors import ExitCode
from etl_craft.engine.repository.pauses import Pause
from etl_craft.services.operations import pipelines, runs, to_json
from etl_craft.services.operations.status import (
    DependencyView,
    GateWaitView,
    TaskSnapshot,
    explain,
    pipeline_status,
)
from unit.test_operations import ctx as ctx
from unit.test_operations import restore_logger as restore_logger
from unit.test_operations import seed

pytestmark = pytest.mark.unit
NOW = datetime(2026, 10, 9, tzinfo=UTC)


@pytest.fixture
def snapshot(ctx):
    seed(ctx, active=True, task=True)
    view = pipeline_status(ctx, "P")
    return TaskSnapshot(
        view.pipeline,
        view.run,
        view.tasks[0].task_id,
        "load",
        None,
        "ALL",
        0,
        (),
        (),
        (),
        False,
        NOW,
    ), view.tasks[0].explanation.task


@pytest.mark.parametrize(
    "state,action",
    [
        ("not run", "An available supervisor can start this ready task."),
        ("waiting on a gate", "Wait for enough upstream dependencies to be satisfied."),
        ("blocked by failure", "Repair or retry the failed upstream to satisfy the dependencies."),
        ("unsatisfiable", "Repair the upstream outcome or dependency before rerunning."),
        ("retry scheduled", "The supervisor can claim the retry at 2026-10-09T00:01:00+00:00."),
        ("paused", "Resume the pipeline before starting this task."),
        ("success", "No work is needed; this task is already settled."),
        ("skipped", "No work is needed; this task is already settled."),
    ],
)
def test_explanation_goldens(snapshot, state, action):
    captured, task = snapshot
    if state == "waiting on a gate":
        captured = replace(
            captured,
            gate_waits=(
                GateWaitView(None, NOW, NOW + timedelta(seconds=1), 1, NOW + timedelta(minutes=5)),
            ),
        )
    elif state == "blocked by failure":
        captured = replace(
            captured,
            required_count=1,
            dependencies=(
                DependencyView("upstream", "SUCCESS", "FAILED", False, "Failed upstream."),
            ),
        )
    elif state == "unsatisfiable":
        captured = replace(captured, unsatisfiable=True)
    elif state == "retry scheduled":
        attempt = replace(task.attempts[-1], status="QUEUED", not_before=NOW + timedelta(minutes=1))
        captured = replace(
            captured, task=replace(task, status="IN-PROGRESS", attempts=(*task.attempts, attempt))
        )
    elif state == "paused":
        captured = replace(
            captured,
            pipeline=replace(captured.pipeline, paused=Pause(NOW, "operator", "maintenance")),
        )
    elif state in {"success", "skipped"}:
        captured = replace(captured, task=replace(task, status=state.upper()))
    done = explain(captured)
    assert (done.state, done.next_action) == (state, action)
    doc = to_json(done)
    assert doc["schema"] == "etl-craft/explanation/1"
    assert doc["run"]["pipeline_run_id"] == captured.run.pipeline_run_id
    assert doc["task_id"] == captured.task_id
    assert doc["required_count"] == captured.required_count
    assert json.loads(json.dumps(doc)) == doc


def test_retry_due_and_settled_state_take_precedence(snapshot):
    captured, task = snapshot
    attempt = replace(task.attempts[-1], status="QUEUED", not_before=NOW)
    captured = replace(captured, task=replace(task, status="IN-PROGRESS", attempts=(attempt,)))
    assert (
        explain(captured).next_action == "The retry is due; an available supervisor can claim it."
    )
    captured = replace(
        captured,
        task=replace(task, status="SUCCESS"),
        pipeline=replace(captured.pipeline, paused=Pause(NOW, "operator", "maintenance")),
    )
    assert explain(captured).state == "success"


def test_optional_command_configuration_is_built_and_runs(capsys):
    command = Command("plain", "No command options.", run=lambda args, out: 0)
    assert build_parser([command]).parse_args(["plain"]).handler is command.run
    assert main(["plain"], commands=[command]) == 0
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize(
    "command,schema",
    [
        ("status", "etl-craft/status/1"),
        ("explain", "etl-craft/explanation/1"),
        ("validate", "etl-craft/validation/1"),
        ("doctor", "etl-craft/doctor/1"),
        ("lineage", "etl-craft/lineage/1"),
    ],
)
def test_inspection_cli_json_schemas(ctx, command, schema, capsys):
    seed(ctx, active=True, task=True)
    options = ["--pipeline_code", "P"] if command in {"status", "explain", "validate"} else []
    if command == "explain":
        options += ["--task_code", "load"]
    code = main(["--config", str(ctx.config.config_path), command, "--format", "json", *options])
    doc = json.loads(capsys.readouterr().out)
    assert doc["schema"] == schema
    assert code in {ExitCode.SUCCESS, ExitCode.FAILURE}
    if command in {"status", "explain"}:
        assert doc["run"]["pipeline_run_id"] == 1
        assert doc["pipeline"]["pipeline_id"] == 1


def test_waiting_and_paused_backfill_exit_codes(ctx, capsys):
    from fixtures.metadata import add_dependency, add_task

    pipeline, _, task, _ = seed(ctx, active=True, task=True)
    with ctx.engine.begin() as conn:
        after = add_task(conn, pipeline, "after", "PYTHON")
        add_dependency(conn, pipeline, after, task)
    args = ["--config", str(ctx.config.config_path), "run", "--pipeline_code", "P"]
    assert main([*args, "--task_code", "after", "--format", "json"]) == ExitCode.WAITING
    assert json.loads(capsys.readouterr().out)["waiting"] is True
    pipelines.set_pause(ctx, "P", "maintenance", verb="pause")
    assert main(args) == ExitCode.SUCCESS
    capsys.readouterr()
    runs.mark_run(ctx, "P", "FAILED", "release the run before backfill")
    assert (
        main([*args, "--backfill", "2026-10-01:2026-10-02", "--reason", "repair"])
        == ExitCode.INCOMPLETE
    )
