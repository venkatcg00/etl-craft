"""``mark`` and ``cancel``: an operator's control over local runs, recorded, on both Engine DBs.

Tasks run in real task processes whose handlers are fakes (``fixtures.task_child``).
"""

import logging
import threading
import time
from dataclasses import replace
from pathlib import Path

import pytest
import yaml
from sqlalchemy import text

from etl_craft.cli import main as cli_main
from etl_craft.config import load_config
from etl_craft.core.enums import Mode, RunStatus
from etl_craft.core.errors import ExitCode, RunRefusedError, RunStateError, UsageError
from etl_craft.execution.gates import Clock
from etl_craft.execution.interventions import (
    cancel_run,
    mark_run,
    mark_task,
    record_stand_in_run,
)
from etl_craft.execution.pipeline import RunHooks, run_pipeline
from etl_craft.execution.runner import ChildOptions
from fixtures.metadata import add_dependency, add_pipeline, add_pipeline_dependency, add_task

TESTS_DIR = Path(__file__).parents[2]
CHILD = ChildOptions(module="fixtures.task_child", kill_grace_seconds=2, cancel_poll_seconds=0.2)
NO_WAIT = Clock(sleep=lambda seconds: None)
WHO = "tester@host"


@pytest.fixture(autouse=True)
def child_can_import_fixtures(monkeypatch):
    monkeypatch.setenv("PYTHONPATH", str(TESTS_DIR))


@pytest.fixture(autouse=True)
def restore_logger():
    logger = logging.getLogger("etl_craft")
    handlers, level = list(logger.handlers), logger.level
    yield
    logger.handlers[:] = handlers
    logger.setLevel(level)


@pytest.fixture
def config(engine_db, tmp_path):
    """A craft-connector.yml naming the test Engine DB, loaded."""
    profile = engine_db.config.engine.active
    schema = "public" if profile.jdbc_url.startswith("jdbc:postgresql") else "main"
    block = {"jdbc_url": profile.jdbc_url, "schema": schema}
    if profile.auth_mode != "none":
        block |= {
            "user": profile.user,
            "auth_mode": profile.auth_mode,
            "secret": profile.secret_var,
        }
    raw = {
        "Secrets": {"Source_type": "environment"},
        "Orchestration": {"Mode": "local", "Task_timeout_seconds": 60, "Max_parallel_tasks": 2},
        "Engine": {"dev": block},
    }
    path = (engine_db.config.config_path or tmp_path / "craft-connector.yml").parent / (
        "craft-connector.yml"
    )
    path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    return load_config(path)


@pytest.fixture
def pipeline(engine_db):
    """Pipeline P: extract, then transform; broken fails, and after_broken waits on it;
    alert runs only if extract fails."""
    engine = engine_db.engine
    ids = {}
    with engine.begin() as conn:
        ids["P"] = add_pipeline(conn, "P")
        for code, behaviour in (
            ("extract", "succeed"),
            ("transform", "succeed"),
            ("broken", "fail"),
            ("after_broken", "succeed"),
            ("alert", "succeed"),
        ):
            ids[code] = add_task(conn, ids["P"], code, BEHAVIOUR=behaviour)
        add_dependency(conn, ids["P"], ids["transform"], ids["extract"])
        add_dependency(conn, ids["P"], ids["after_broken"], ids["broken"])
        add_dependency(conn, ids["P"], ids["alert"], ids["extract"], "FAILURE")
    return engine, ids


def statuses(engine, pipeline_run_id):
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT t.TASK_CODE AS code, r.STATUS AS status, r.TARGET_COUNT AS target_count "
                "FROM AUD_TASK_RUN_LOG r JOIN CFG_TASKS t ON t.TASK_ID = r.TASK_ID "
                "WHERE r.PIPELINE_RUN_ID = :id"
            ),
            {"id": pipeline_run_id},
        )
        return {row.code: (row.status, row.target_count) for row in rows}


def run_status(engine, pipeline_run_id):
    with engine.connect() as conn:
        return conn.execute(
            text("SELECT STATUS FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID = :id"),
            {"id": pipeline_run_id},
        ).scalar_one()


def interventions(engine):
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT i.PIPELINE_RUN_ID AS run_id, t.TASK_CODE AS task, i.ACTION AS action, "
                "i.FROM_STATUS AS from_status, i.TO_STATUS AS to_status, "
                "i.TARGET_COUNT AS target_count, i.PREVIOUS_MESSAGE AS previous, "
                "i.REASON AS reason, i.REQUESTED_BY AS who "
                "FROM AUD_RUN_INTERVENTIONS i LEFT JOIN CFG_TASKS t ON t.TASK_ID = i.TASK_ID "
                "ORDER BY i.INTERVENTION_ID"
            )
        )
        return [tuple(row) for row in rows]


def test_a_failed_task_marked_success_lets_its_dependents_run(config, pipeline):
    engine, _ = pipeline
    failed = run_pipeline(engine, config, "P", child=CHILD)
    run_id = failed.pipeline_run_id
    assert failed.status == RunStatus.FAILED
    assert statuses(engine, run_id)["after_broken"] == ("SKIPPED", None)

    marked = mark_task(engine, config, "P", "broken", "SUCCESS", "loaded by hand", requested_by=WHO)
    assert marked.message == (
        f"P.broken: marked SUCCESS under pipeline_run_id={run_id} (was FAILED); the run was "
        "FAILED and is IN-PROGRESS again; reset to run again: after_broken, alert. Run "
        "`etl-craft run --pipeline_code P` to resume it"
    )
    assert run_status(engine, run_id) == "IN-PROGRESS"
    assert interventions(engine) == [
        (
            run_id,
            "broken",
            "MARK",
            "FAILED",
            "SUCCESS",
            None,
            "the source file is missing",
            "loaded by hand",
            WHO,
        ),
        (run_id, None, "REOPEN", "FAILED", "IN-PROGRESS", None, None, "loaded by hand", WHO),
        (
            run_id,
            "after_broken",
            "RESET",
            "SKIPPED",
            None,
            None,
            f"its dependencies can never be met under pipeline_run_id={run_id}",
            "loaded by hand",
            WHO,
        ),
        (
            run_id,
            "alert",
            "RESET",
            "SKIPPED",
            None,
            None,
            f"its dependencies can never be met under pipeline_run_id={run_id}",
            "loaded by hand",
            WHO,
        ),
    ]

    resumed = run_pipeline(engine, config, "P", child=CHILD)
    assert (resumed.status, resumed.pipeline_run_id) == (RunStatus.SUCCESS, run_id)
    assert statuses(engine, run_id) == {
        "extract": ("SUCCESS", 9),
        "transform": ("SUCCESS", 9),
        "broken": ("SUCCESS", None),
        "after_broken": ("SUCCESS", 9),
        "alert": ("SKIPPED", None),
    }
    with engine.connect() as conn:
        message = conn.execute(
            text(
                "SELECT r.ERROR_MESSAGE FROM AUD_TASK_RUN_LOG r JOIN CFG_TASKS t "
                "ON t.TASK_ID = r.TASK_ID WHERE t.TASK_CODE = 'broken'"
            )
        ).scalar_one()
    assert message == f"marked SUCCESS by {WHO}: loaded by hand"


def test_a_marked_success_satisfies_has_data_only_with_a_row_count(config, pipeline):
    engine, ids = pipeline
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_TASK_DEPENDENCY SET DEPENDENCY_TYPE = 'HAS_DATA' WHERE TASK_ID = :t"),
            {"t": ids["after_broken"]},
        )
    run_id = run_pipeline(engine, config, "P", child=CHILD).pipeline_run_id
    mark_task(engine, config, "P", "broken", "SUCCESS", "no row count", requested_by=WHO)
    assert run_pipeline(engine, config, "P", child=CHILD).status == RunStatus.SUCCESS
    assert statuses(engine, run_id)["after_broken"] == ("SKIPPED", None)

    # Stated with a row count, the marked SUCCESS satisfies it.
    mark_task(engine, config, "P", "broken", "SUCCESS", "12 rows", rows=12, requested_by=WHO)
    assert run_pipeline(engine, config, "P", child=CHILD).status == RunStatus.SUCCESS
    assert statuses(engine, run_id)["broken"] == ("SUCCESS", 12)
    assert statuses(engine, run_id)["after_broken"] == ("SUCCESS", 9)


def test_a_run_marked_success_satisfies_a_downstream_gate(config, pipeline):
    engine, ids = pipeline
    with engine.begin() as conn:
        down = add_pipeline(conn, "DOWN")
        add_task(conn, down, "load", BEHAVIOUR="succeed")
        add_pipeline_dependency(conn, down, ids["P"])
    run_id = run_pipeline(engine, config, "P", child=CHILD).pipeline_run_id
    blocked = run_pipeline(engine, config, "DOWN", child=CHILD, clock=NO_WAIT)
    assert blocked.status == RunStatus.SKIPPED

    marked = mark_run(engine, config, "P", "SUCCESS", "broken is not needed", requested_by=WHO)
    assert marked.message == f"P: pipeline_run_id={run_id} marked SUCCESS (was FAILED)"
    assert run_status(engine, run_id) == "SUCCESS"
    # Its tasks keep their statuses.
    assert statuses(engine, run_id)["broken"] == ("FAILED", None)
    assert run_pipeline(engine, config, "DOWN", child=CHILD, clock=NO_WAIT).status == "SUCCESS"


def test_a_stand_in_run_passes_gates_where_the_upstream_cannot_run(config, pipeline):
    engine, _ = pipeline
    with engine.begin() as conn:
        up = add_pipeline(conn, "UP")
        publish = add_task(conn, up, "publish")
        down = add_pipeline(conn, "DOWN")
        load = add_task(conn, down, "load", BEHAVIOUR="succeed")
        add_pipeline_dependency(conn, down, up)
        add_dependency(conn, down, load, publish, "HAS_DATA", upstream_pipeline=up)
    assert run_pipeline(engine, config, "DOWN", child=CHILD, clock=NO_WAIT).status == "SKIPPED"

    with pytest.raises(UsageError, match="name the task with --task_code"):
        record_stand_in_run(engine, config, "UP", "SUCCESS", "dev", rows=3, requested_by=WHO)
    stand_in = record_stand_in_run(
        engine,
        config,
        "UP",
        "SUCCESS",
        "UP only runs in production",
        task_code="publish",
        rows=3,
        requested_by=WHO,
    )
    assert stand_in.message == (
        f"UP.publish: stand-in run pipeline_run_id={stand_in.pipeline_run_id} recorded SUCCESS "
        "with 3 row(s)"
    )
    assert run_status(engine, stand_in.pipeline_run_id) == "SUCCESS"
    assert run_pipeline(engine, config, "DOWN", child=CHILD, clock=NO_WAIT).status == "SUCCESS"
    assert interventions(engine)[-1] == (
        stand_in.pipeline_run_id,
        "publish",
        "NEW_RUN",
        None,
        "SUCCESS",
        3,
        None,
        "UP only runs in production",
        WHO,
    )


def test_cancel_stops_a_running_run(config, pipeline):
    engine, ids = pipeline
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE = 'sleep' WHERE TASK_ID = :t"),
            {"t": ids["extract"]},
        )
        conn.execute(
            text("UPDATE CFG_TASKS SET ACTIVE_FLAG = 'N' WHERE TASK_ID IN (:b, :a)"),
            {"b": ids["broken"], "a": ids["after_broken"]},
        )
    finished = []
    outcome = {}

    def run():
        outcome["run"] = run_pipeline(
            engine, config, "P", child=CHILD, hooks=RunHooks(on_finalized=finished.append)
        )

    thread = threading.Thread(target=run)
    started = time.monotonic()
    thread.start()
    run_id = None
    while time.monotonic() - started < 30:
        with engine.connect() as conn:
            run_id = conn.execute(
                text(
                    "SELECT r.PIPELINE_RUN_ID FROM AUD_TASK_RUN_LOG r JOIN CFG_TASKS t "
                    "ON t.TASK_ID = r.TASK_ID WHERE t.TASK_CODE = 'extract' "
                    "AND r.STATUS = 'IN-PROGRESS'"
                )
            ).scalar_one_or_none()
        if run_id is not None:
            break
        time.sleep(0.1)
    assert run_id is not None
    with pytest.raises(RunRefusedError, match=r"P.extract is running under pipeline_run_id="):
        mark_task(engine, config, "P", "extract", "SUCCESS", "too early", requested_by=WHO)

    cancelled = cancel_run(engine, config, "P", "wrong data", requested_by=WHO)
    assert cancelled.message == (
        f"P: pipeline_run_id={run_id} CANCELLED; stopping 1 running task(s): extract (the "
        "process running each stops it within a few seconds)"
    )
    thread.join(timeout=30)
    assert not thread.is_alive()
    assert time.monotonic() - started < 30
    assert outcome["run"].status == RunStatus.CANCELLED
    assert outcome["run"].message == f"P: pipeline_run_id={run_id} CANCELLED; stopped: extract"
    assert finished == [outcome["run"]]
    assert run_status(engine, run_id) == "CANCELLED"
    # Only what had started is recorded: nothing started after the cancel.
    assert statuses(engine, run_id)["extract"] == ("CANCELLED", None)
    assert "transform" not in statuses(engine, run_id)
    assert [row[:5] for row in interventions(engine)] == [
        (run_id, "extract", "CANCEL", "IN-PROGRESS", "CANCELLED"),
        (run_id, None, "CANCEL", "IN-PROGRESS", "CANCELLED"),
    ]
    # The next run is a new one.
    with pytest.raises(RunStateError, match="P has no run in progress to cancel"):
        cancel_run(engine, config, "P", "again", requested_by=WHO)


def test_what_mark_and_cancel_refuse(config, pipeline):
    engine, _ = pipeline
    with pytest.raises(RunStateError, match="P has no run to mark; `etl-craft mark --new-run`"):
        mark_run(engine, config, "P", "SUCCESS", "why", requested_by=WHO)
    run_id = run_pipeline(engine, config, "P", child=CHILD).pipeline_run_id
    with pytest.raises(UsageError, match="needs a --reason"):
        mark_run(engine, config, "P", "SUCCESS", "  ", requested_by=WHO)
    with pytest.raises(UsageError, match="cannot mark 'CANCELLED'"):
        mark_run(engine, config, "P", "CANCELLED", "why", requested_by=WHO)
    with pytest.raises(UsageError, match="--rows goes with SUCCESS"):
        mark_task(engine, config, "P", "broken", "FAILED", "why", rows=1, requested_by=WHO)
    with pytest.raises(UsageError, match="cannot be negative"):
        mark_task(engine, config, "P", "broken", "SUCCESS", "why", rows=-1, requested_by=WHO)
    with pytest.raises(UsageError, match=f"already FAILED under pipeline_run_id={run_id}"):
        mark_task(engine, config, "P", "broken", "FAILED", "why", requested_by=WHO)
    with pytest.raises(UsageError, match="is already FAILED; nothing to mark"):
        mark_run(engine, config, "P", "FAILED", "why", requested_by=WHO)

    remote = replace(config, mode=Mode.REMOTE)
    for refused in (
        lambda: mark_task(engine, remote, "P", "broken", "SUCCESS", "why"),
        lambda: mark_run(engine, remote, "P", "SUCCESS", "why"),
        lambda: record_stand_in_run(engine, remote, "P", "SUCCESS", "why"),
        lambda: cancel_run(engine, remote, "P", "why"),
    ):
        with pytest.raises(RunRefusedError, match="the orchestrator is the only source of truth"):
            refused()

    mark_task(engine, config, "P", "broken", "SUCCESS", "reopen it", requested_by=WHO)
    with pytest.raises(RunStateError, match=r"has a run in progress \(pipeline_run_id="):
        record_stand_in_run(engine, config, "P", "SUCCESS", "why", requested_by=WHO)
    assert interventions(engine)[0][:3] == (run_id, "broken", "MARK")


def test_the_commands(config, pipeline, capsys, monkeypatch):
    engine, _ = pipeline
    monkeypatch.chdir(config.project_dir)
    run_id = run_pipeline(engine, config, "P", child=CHILD).pipeline_run_id
    code = cli_main(
        ["mark", "--pipeline_code", "P", "--status", "SUCCESS", "--reason", "fine as it is"]
    )
    assert code == ExitCode.SUCCESS
    assert capsys.readouterr().out == f"P: pipeline_run_id={run_id} marked SUCCESS (was FAILED)\n"

    assert cli_main(["history", "--pipeline_code", "P"]) == ExitCode.SUCCESS
    history = capsys.readouterr().out
    assert "Interventions:" in history
    assert "(the run)" in history and "fine as it is" in history

    assert cli_main(["cancel", "--pipeline_code", "P", "--reason", "x"]) == ExitCode.RUN_STATE
    assert "P has no run in progress to cancel" in capsys.readouterr().err
    # A reason is required.
    with pytest.raises(SystemExit) as usage:
        cli_main(["mark", "--pipeline_code", "P", "--status", "SUCCESS", "--new-run"])
    assert usage.value.code == ExitCode.USAGE
