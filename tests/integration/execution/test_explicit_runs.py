"""Run identity selection and older orchestrator clears through the real CLI."""

import pytest
import yaml
from sqlalchemy import text

from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.errors import RunStateError
from etl_craft.engine import runlog, transitions
from fixtures.metadata import add_pipeline


@pytest.mark.parametrize("clear_init", [False, True])
@pytest.mark.chaos
def test_clearing_an_older_dag_run_keeps_its_key_and_date(cli_project, clear_init):
    project = cli_project
    raw = yaml.safe_load(project.config.config_path.read_text())
    raw["Orchestration"]["Mode"] = "remote"
    project.config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    commands = []
    for key, day in [("orchestrator:old", "2026-09-01"), ("orchestrator:new", "2026-09-02")]:
        identity = ("--run-key", key, "--run-date", day)
        for step in [("--init-only",), ("--task_code", "load"), ("--finalize-only",)]:
            code, output = project.run("run", "--pipeline_code", "P", *step, *identity)
            assert code == 0, output
        commands.append(identity)
    with project.engine.connect() as conn:
        before = conn.execute(
            text(
                "SELECT PIPELINE_RUN_ID, RUN_KEY, RUN_DATE, STATUS, END_DATE "
                "FROM AUD_PIPELINES_RUN_LOG ORDER BY PIPELINE_RUN_ID"
            )
        ).all()
    # Clearing a task alone or init as well must reopen only their existing identity.
    steps = ([("--init-only",)] if clear_init else []) + [
        ("--task_code", "load"),
        ("--finalize-only",),
    ]
    for step in steps:
        code, output = project.run("run", "--pipeline_code", "P", *step, *commands[0])
        assert code == 0, output
    with project.engine.connect() as conn:
        after = conn.execute(
            text(
                "SELECT PIPELINE_RUN_ID, RUN_KEY, RUN_DATE, STATUS, END_DATE "
                "FROM AUD_PIPELINES_RUN_LOG ORDER BY PIPELINE_RUN_ID"
            )
        ).all()
        attempts = conn.execute(
            text(
                "SELECT PIPELINE_RUN_ID, ATTEMPT_COUNT FROM AUD_TASK_RUN_LOG "
                "ORDER BY PIPELINE_RUN_ID"
            )
        ).all()
    assert len(after) == 2
    assert after[0][:4] == before[0][:4]
    assert after[1] == before[1]
    assert attempts == [(before[0][0], 2), (before[1][0], 1)]
    code, output = project.run("history", "--pipeline_code", "P", "--run-key", "orchestrator:old")
    assert code == 0 and "2026-09-01" in output and "2026-09-02" not in output
    code, output = project.run("steps", "--pipeline_code", "P", "--run-id", str(before[0][0]))
    assert code == 0 and "SUCCESS" in output


def test_mark_refuses_ambiguous_runs_and_lists_their_identity(cli_project):
    project = cli_project
    with project.engine.begin() as conn:
        # Simulate a catalog lacking the one-active-run constraint; selection must still refuse.
        conn.execute(text("DROP INDEX ux_pipeline_run_one_active"))
        first = transitions.create_run(conn, project.pipeline_id, SYSTEM_ACTOR, run_key="first")
        second = transitions.create_run(conn, project.pipeline_id, SYSTEM_ACTOR, run_key="second")
    code, output = project.run(
        "mark", "--pipeline_code", "P", "--status", "FAILED", "--reason", "repair"
    )
    assert code == 9, output
    for run_id, key in [(first, "first"), (second, "second")]:
        assert f"id={run_id} key={key} kind=MANUAL" in output
    assert "run_date=" in output and "status=IN-PROGRESS" in output
    with project.engine.connect() as conn:
        assert [r.status for r in runlog.run_candidates(conn, project.pipeline_id)] == [
            "IN-PROGRESS",
            "IN-PROGRESS",
        ]


def test_an_id_from_another_pipeline_is_refused(cli_project):
    project = cli_project
    with project.engine.begin() as conn:
        other = add_pipeline(conn, "OTHER")
        foreign = transitions.create_run(conn, other, SYSTEM_ACTOR, run_key="foreign")
    for command, arguments in [
        ("run", ["--task_code", "load"]),
        ("run", ["--init-only"]),
        ("mark", ["--status", "FAILED", "--reason", "repair"]),
        ("cancel", ["--reason", "repair"]),
        ("history", []),
        ("steps", []),
    ]:
        code, output = project.run(
            command, "--pipeline_code", "P", "--run-id", str(foreign), *arguments
        )
        assert code == 9, output
        assert "matched 0 runs" in output
    with project.engine.connect() as conn:
        assert runlog.run_candidates(conn, project.pipeline_id) == []
        assert runlog.select_run(conn, other).status == "IN-PROGRESS"


def test_no_selector_never_reopens_an_ended_run(engine_db):
    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        run_id = transitions.create_run(conn, pipeline, SYSTEM_ACTOR, run_key="ended")
        transitions.finish_run(conn, run_id, "SUCCESS", SYSTEM_ACTOR)
        for resolver in [
            transitions.resolve_run_for_task,
            transitions.resolve_run_for_orchestrator,
        ]:
            with pytest.raises(RunStateError, match="matched 0 runs"):
                resolver(conn, pipeline)
        assert (
            runlog.select_run(conn, pipeline, runlog.RunSelector(run_key="ended")).status
            == "SUCCESS"
        )


@pytest.mark.unit
@pytest.mark.parametrize(
    "identity", [{"run_id": 0}, {"run_key": " "}, {"run_id": 1, "run_key": "x"}]
)
def test_invalid_run_selectors_are_refused(identity):
    with pytest.raises(RunStateError):
        runlog.RunSelector(**identity)


def test_a_mismatched_logical_date_refuses_before_reopening(cli_project):
    project = cli_project
    raw = yaml.safe_load(project.config.config_path.read_text())
    raw["Orchestration"]["Mode"] = "remote"
    project.config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    for step in [("--init-only",), ("--task_code", "load"), ("--finalize-only",)]:
        code, output = project.run(
            "run", "--pipeline_code", "P", *step, "--run-key", "dated", "--run-date", "2026-09-01"
        )
        assert code == 0, output
    for step in [("--init-only",), ("--task_code", "load"), ("--finalize-only",)]:
        code, output = project.run(
            "run", "--pipeline_code", "P", *step, "--run-key", "dated", "--run-date", "2026-09-02"
        )
        assert code != 0 and "date cannot change" in output
    with project.engine.connect() as conn:
        selected = runlog.select_run(conn, project.pipeline_id, runlog.RunSelector(run_key="dated"))
        assert selected.status == "SUCCESS" and str(selected.run_date) == "2026-09-01"
        assert conn.execute(text("SELECT ATTEMPT_COUNT FROM AUD_TASK_RUN_LOG")).scalar_one() == 1
