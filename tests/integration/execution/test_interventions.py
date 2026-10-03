"""An operator's control over local runs, recorded, on both Engine DBs: ``mark``, ``cancel``,
``Dependency_gates``, ``run --ignore-dependencies`` and ``run --rerun``.

Tasks run in real task processes whose handlers are fakes (``fixtures.task_child``).
"""

import logging
import threading
import time
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
import yaml
from sqlalchemy import text

from etl_craft.cli import main as cli_main
from etl_craft.config import load_config
from etl_craft.core.enums import Mode, RunStatus
from etl_craft.core.errors import ExitCode, RunRefusedError, RunStateError, UsageError
from etl_craft.engine import runlog
from etl_craft.execution.gates import Clock
from etl_craft.execution.interventions import (
    cancel_run,
    mark_run,
    mark_task,
    pause_pipeline,
    record_stand_in_run,
    resume_pipeline,
    skip_run,
)
from etl_craft.execution.pipeline import (
    RunHooks,
    backfill,
    force_task,
    init_pipeline_run,
    rerun_task,
    run_pipeline,
)
from etl_craft.execution.runner import ChildOptions, Override, attempt_log_path, run_task
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


def attempts(engine, pipeline_run_id):
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT t.TASK_CODE AS code, r.ATTEMPT_COUNT AS attempts FROM AUD_TASK_RUN_LOG r "
                "JOIN CFG_TASKS t ON t.TASK_ID = r.TASK_ID WHERE r.PIPELINE_RUN_ID = :id"
            ),
            {"id": pipeline_run_id},
        )
        return {row.code: row.attempts for row in rows}


@pytest.fixture
def downstream(engine_db):
    """UP, never run; DOWN depends on it, and DOWN.load on UP.publish."""
    engine = engine_db.engine
    with engine.begin() as conn:
        up = add_pipeline(conn, "UP")
        publish = add_task(conn, up, "publish")
        down = add_pipeline(conn, "DOWN")
        load = add_task(conn, down, "load", BEHAVIOUR="succeed")
        add_pipeline_dependency(conn, down, up)
        add_dependency(conn, down, load, publish, upstream_pipeline=up)
    return engine


@pytest.mark.parametrize("policy", ["warn", "off"])
def test_relaxed_gates_let_a_run_through_and_record_it(config, downstream, policy):
    engine = downstream
    relaxed = replace(config, dependency_gates=policy)
    outcome = run_pipeline(engine, relaxed, "DOWN", child=CHILD, clock=NO_WAIT)
    assert outcome.status == RunStatus.SUCCESS
    assert outcome.message.endswith("; dependency gates bypassed for DOWN, load (see history)")
    changes = interventions(engine)
    assert [(task, action) for _, task, action, *_ in changes] == [
        (None, "GATE_BYPASS"),
        ("load", "GATE_BYPASS"),
    ]
    reason = changes[0][7]
    if policy == "warn":
        assert reason == (
            "Dependency_gates is warn: upstream pipeline UP (SUCCESS) has no finished run"
        )
    else:
        assert reason == "Dependency_gates is off: upstream pipeline UP (SUCCESS) not checked"
    assert changes[1][7].endswith(
        "upstream task UP.publish (SUCCESS) "
        + ("has no finished run" if policy == "warn" else "not checked")
    )
    # Nothing satisfied the dependencies, so nothing was consumed.
    with engine.connect() as conn:
        assert (
            conn.execute(text("SELECT COUNT(*) FROM AUD_DEPENDENCY_CONSUMPTION")).scalar_one() == 0
        )
    # Enforced, the same gate skips the next run.
    assert run_pipeline(engine, config, "DOWN", child=CHILD, clock=NO_WAIT).status == "SKIPPED"


def test_a_task_runs_without_its_dependencies_when_told_to(config, pipeline):
    engine, ids = pipeline
    with engine.begin() as conn:
        run_id = runlog.find_or_create_active_run(conn, ids["P"])
    # transform waits for extract, which has not run.
    waiting = run_task(engine, config, "P", "transform", child=CHILD)
    assert waiting.status == RunStatus.SKIPPED and "nothing recorded" in waiting.message
    forced = run_task(
        engine, config, "P", "transform", child=CHILD, override=Override("extract is late")
    )
    assert forced.status == RunStatus.SUCCESS
    assert interventions(engine)[-1][:5] == (
        run_id,
        "transform",
        "IGNORE_DEPENDENCIES",
        None,
        "SUCCESS",
    )
    again = run_task(engine, config, "P", "transform", child=CHILD, override=Override("again"))
    assert again.message.endswith(
        f"already SUCCESS under pipeline_run_id={run_id}; pass --rerun to run it again"
    )
    with pytest.raises(UsageError, match="--ignore-dependencies needs a --reason"):
        run_task(engine, config, "P", "transform", child=CHILD, override=Override(""))
    with pytest.raises(UsageError, match="different overrides"):
        run_task(engine, config, "P", "transform", force=True, override=Override("x"))
    with pytest.raises(RunRefusedError, match="--ignore-dependencies is only available in local"):
        run_task(engine, replace(config, mode=Mode.REMOTE), "P", "x", override=Override("x"))


def test_a_rerun_runs_a_task_and_what_follows_it_again(config, pipeline):
    engine, ids = pipeline
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_TASKS SET ACTIVE_FLAG = 'N' WHERE TASK_ID IN (:b, :a)"),
            {"b": ids["broken"], "a": ids["after_broken"]},
        )
    first = run_pipeline(engine, config, "P", child=CHILD)
    run_id = first.pipeline_run_id
    assert first.status == RunStatus.SUCCESS

    alone = rerun_task(engine, config, "P", "extract", "source fixed", child=CHILD)
    assert alone.status == RunStatus.SUCCESS and alone.pipeline_run_id == run_id
    assert alone.message.startswith(f"P: pipeline_run_id={run_id} SUCCESS")
    assert attempts(engine, run_id) == {"extract": 2, "transform": 1, "alert": 1}

    both = rerun_task(
        engine, config, "P", "extract", "source fixed again", with_downstream=True, child=CHILD
    )
    assert both.status == RunStatus.SUCCESS
    # alert waits for extract to fail, so it is left as it was.
    assert "not run again, their dependencies are not met: alert" in both.message
    assert attempts(engine, run_id) == {"extract": 3, "transform": 2, "alert": 1}
    assert run_status(engine, run_id) == "SUCCESS"
    assert [(task, action, frm, to) for _, task, action, frm, to, *_ in interventions(engine)] == [
        (None, "REOPEN", "SUCCESS", "IN-PROGRESS"),
        ("extract", "RERUN", "SUCCESS", "SUCCESS"),
        (None, "REOPEN", "SUCCESS", "IN-PROGRESS"),
        ("extract", "RERUN", "SUCCESS", "SUCCESS"),
        ("transform", "RERUN", "SUCCESS", "SUCCESS"),
    ]
    with pytest.raises(RunRefusedError, match="--rerun is only available in local mode"):
        rerun_task(engine, replace(config, mode=Mode.REMOTE), "P", "extract", "x")


def test_the_run_options(config, pipeline, capsys, monkeypatch):
    monkeypatch.chdir(config.project_dir)
    for argv, message in (
        (["--rerun"], "apply to one task: pass --task_code"),
        (["--task_code", "extract", "--rerun", "--ignore-dependencies"], "already runs the task"),
        (["--task_code", "extract", "--with-downstream"], "--with-downstream goes with --rerun"),
        (["--task_code", "extract", "--reason", "x"], "--reason goes with"),
        (["--task_code", "extract", "--rerun"], "--rerun needs a --reason"),
        (
            ["--task_code", "extract", "--rerun", "--force", "--reason", "x"],
            "choose one",
        ),
    ):
        assert cli_main(["run", "--pipeline_code", "P", *argv]) == ExitCode.USAGE
        assert message in capsys.readouterr().err


def run_count(engine):
    with engine.connect() as conn:
        return conn.execute(text("SELECT COUNT(*) FROM AUD_PIPELINES_RUN_LOG")).scalar_one()


def test_a_paused_pipeline_starts_nothing_until_resumed(config, pipeline):
    engine, _ = pipeline
    message = pause_pipeline(engine, config, "P", "source is down", requested_by=WHO)
    assert message == "P: paused; `etl-craft run` starts nothing of it until resumed"
    with pytest.raises(RunStateError, match=r"P is already paused since .* by tester@host"):
        pause_pipeline(engine, config, "P", "again", requested_by=WHO)

    held = run_pipeline(engine, config, "P", child=CHILD)
    assert (held.status, held.pipeline_run_id) == (RunStatus.SKIPPED, None)
    assert "P: paused since" in held.message and "source is down; nothing started" in held.message
    assert init_pipeline_run(engine, config, "P").pipeline_run_id is None
    task = run_task(engine, config, "P", "extract", child=CHILD)
    assert task.status == RunStatus.SKIPPED and "nothing started or recorded" in task.message
    rerun = run_task(engine, config, "P", "extract", child=CHILD, override=Override("x"))
    assert "nothing started or recorded" in rerun.message
    assert run_count(engine) == 0

    assert resume_pipeline(engine, config, "P", "source is back", requested_by=WHO) == (
        "P: resumed"
    )
    with pytest.raises(RunStateError, match="P is not paused"):
        resume_pipeline(engine, config, "P", "again", requested_by=WHO)
    assert run_pipeline(engine, config, "P", child=CHILD).pipeline_run_id is not None
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT PAUSED_BY AS by, REASON AS why, RESUMED_BY AS resumed_by, "
                "RESUME_REASON AS resume_why, RESUMED_AT AS resumed_at FROM AUD_PIPELINE_PAUSES"
            )
        ).one()
    assert (row.by, row.why, row.resumed_by, row.resume_why) == (
        WHO,
        "source is down",
        WHO,
        "source is back",
    )
    assert row.resumed_at is not None
    remote = replace(config, mode=Mode.REMOTE)
    with pytest.raises(RunRefusedError, match="pausing the DAG"):
        pause_pipeline(engine, remote, "P", "x")


def test_a_run_paused_while_it_runs_stops_starting_tasks_and_goes_on_once_resumed(config, pipeline):
    engine, ids = pipeline
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE = 'brief' WHERE TASK_ID = :t"),
            {"t": ids["extract"]},
        )
        conn.execute(
            text("UPDATE CFG_TASKS SET ACTIVE_FLAG = 'N' WHERE TASK_ID IN (:b, :a)"),
            {"b": ids["broken"], "a": ids["after_broken"]},
        )
    outcome = {}
    thread = threading.Thread(
        target=lambda: outcome.setdefault("run", run_pipeline(engine, config, "P", child=CHILD))
    )
    thread.start()
    started = time.monotonic()
    while time.monotonic() - started < 30:
        with engine.connect() as conn:
            running = conn.execute(
                text("SELECT COUNT(*) FROM AUD_TASK_RUN_LOG WHERE STATUS = 'IN-PROGRESS'")
            ).scalar_one()
        if running:
            break
        time.sleep(0.05)
    message = pause_pipeline(engine, config, "P", "hold on", requested_by=WHO)
    assert "starts no more tasks and stays IN-PROGRESS" in message
    thread.join(timeout=60)
    held = outcome["run"]
    assert held.status == RunStatus.IN_PROGRESS
    assert "stays IN-PROGRESS, paused since" in held.message
    # extract finished; transform, which waits on it, did not start.
    assert statuses(engine, held.pipeline_run_id) == {"extract": ("SUCCESS", 1)}
    assert run_status(engine, held.pipeline_run_id) == "IN-PROGRESS"

    assert "goes on with" in resume_pipeline(engine, config, "P", "go", requested_by=WHO)
    done = run_pipeline(engine, config, "P", child=CHILD)
    assert (done.status, done.pipeline_run_id) == (RunStatus.SUCCESS, held.pipeline_run_id)
    assert statuses(engine, held.pipeline_run_id)["transform"] == ("SUCCESS", 9)


def test_a_run_skipped_on_purpose_is_recorded_and_seen_downstream(config, pipeline):
    engine, ids = pipeline
    with engine.begin() as conn:
        down = add_pipeline(conn, "DOWN")
        add_task(conn, down, "load", BEHAVIOUR="succeed")
        add_pipeline_dependency(conn, down, ids["P"])
    skipped = skip_run(engine, config, "P", "public holiday", requested_by=WHO)
    assert skipped.message == (
        f"P: pipeline_run_id={skipped.pipeline_run_id} SKIPPED on purpose: public holiday"
    )
    assert run_status(engine, skipped.pipeline_run_id) == "SKIPPED"
    assert interventions(engine)[-1][:5] == (
        skipped.pipeline_run_id,
        None,
        "NEW_RUN",
        None,
        "SKIPPED",
    )
    downstream = run_pipeline(engine, config, "DOWN", child=CHILD, clock=NO_WAIT)
    assert downstream.status == RunStatus.SKIPPED
    assert "upstream pipeline P (SUCCESS)" in downstream.message


def test_the_pause_and_skip_commands(config, pipeline, capsys, monkeypatch):
    monkeypatch.chdir(config.project_dir)
    assert cli_main(["pause", "--pipeline_code", "P", "--reason", "hold"]) == ExitCode.SUCCESS
    assert capsys.readouterr().out.startswith("P: paused;")
    assert cli_main(["run", "--pipeline_code", "P"]) == ExitCode.SUCCESS
    assert "nothing started" in capsys.readouterr().out
    assert cli_main(["resume", "--pipeline_code", "P", "--reason", "go"]) == ExitCode.SUCCESS
    assert capsys.readouterr().out == "P: resumed\n"
    code = cli_main(["run", "--pipeline_code", "P", "--skip", "--reason", "holiday"])
    assert code == ExitCode.SUCCESS
    assert "SKIPPED on purpose: holiday" in capsys.readouterr().out
    assert cli_main(["run", "--pipeline_code", "P", "--skip", "--task_code", "x"]) == (
        ExitCode.USAGE
    )


def runs(engine):
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT p.PIPELINE_CODE AS code, r.STATUS AS status, r.RUN_DATE AS run_date, "
                "r.BACKFILL AS backfill FROM AUD_PIPELINES_RUN_LOG r JOIN CFG_PIPELINES p "
                "ON p.PIPELINE_ID = r.PIPELINE_ID ORDER BY r.PIPELINE_RUN_ID"
            )
        )
        return [(r.code, r.status, str(r.run_date)[:10], r.backfill) for r in rows]


def without_broken(engine, ids):
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_TASKS SET ACTIVE_FLAG = 'N' WHERE TASK_ID IN (:b, :a)"),
            {"b": ids["broken"], "a": ids["after_broken"]},
        )


def test_a_backfill_runs_once_per_date_as_of_that_date(config, pipeline, downstream):
    engine, ids = pipeline
    without_broken(engine, ids)
    today = runlog.today().isoformat()
    # DOWN's upstream UP never ran: a backfill does not check it, nor consume anything.
    done = backfill(
        engine, config, "DOWN", date(2026, 9, 1), date(2026, 9, 3), "reload Sept", child=CHILD
    )
    assert done.status == RunStatus.SUCCESS and done.stopped is None
    assert done.message.startswith(
        "DOWN: backfill of 3 date(s) from 2026-09-01 to 2026-09-03 done: "
    )
    assert runs(engine) == [
        ("DOWN", "SUCCESS", "2026-09-01", "Y"),
        ("DOWN", "SUCCESS", "2026-09-02", "Y"),
        ("DOWN", "SUCCESS", "2026-09-03", "Y"),
    ]
    first = done.runs[0].pipeline_run_id
    log = attempt_log_path(config, "DOWN", first, "load", 1).read_text("utf-8")
    assert "as of 2026-09-01 (backfill)" in log
    reasons = [row[7] for row in interventions(engine) if row[2] == "GATE_BYPASS"]
    assert reasons[0] == (
        "backfill for 2026-09-01: reload Sept; dependencies on other pipelines are not checked, "
        "and nothing is consumed"
    )
    with engine.connect() as conn:
        assert (
            conn.execute(text("SELECT COUNT(*) FROM AUD_DEPENDENCY_CONSUMPTION")).scalar_one() == 0
        )

    # A run of its own gets today, or the date it is given.
    run_pipeline(engine, config, "P", child=CHILD)
    run_pipeline(engine, config, "P", child=CHILD, run_date=date(2026, 8, 31))
    assert runs(engine)[-2:] == [("P", "SUCCESS", today, "N"), ("P", "SUCCESS", "2026-08-31", "N")]


def test_a_plain_run_does_not_take_over_a_backfills_run(config, pipeline):
    engine, ids = pipeline
    with engine.begin() as conn:
        backfill_run = runlog.find_or_create_active_run(
            conn, ids["P"], run_date=date(2025, 1, 1), backfill=True
        )
    with pytest.raises(RunStateError, match="is running backfill run pipeline_run_id="):
        run_pipeline(engine, config, "P", child=CHILD)
    assert run_status(engine, backfill_run) == "IN-PROGRESS"
    assert statuses(engine, backfill_run) == {}


def test_a_backfill_does_not_take_over_a_scheduled_run(config, pipeline):
    engine, ids = pipeline
    with engine.begin() as conn:
        scheduled = runlog.find_or_create_active_run(conn, ids["P"], run_date=date(2026, 9, 2))
    # The run started by itself between two backfill dates is found before the next date.
    with pytest.raises(RunStateError, match="finish or cancel it before a backfill"):
        backfill(engine, config, "P", date(2026, 9, 2), date(2026, 9, 2), "x", child=CHILD)
    assert run_status(engine, scheduled) == "IN-PROGRESS"


def test_a_backfill_stops_when_another_run_starts_between_dates(config, pipeline):
    engine, ids = pipeline
    without_broken(engine, ids)

    def a_scheduled_run_starts(outcome):
        with engine.begin() as conn:
            runlog.find_or_create_active_run(conn, ids["P"])

    done = backfill(
        engine,
        config,
        "P",
        date(2026, 9, 1),
        date(2026, 9, 3),
        "reload",
        child=CHILD,
        hooks=RunHooks(on_finalized=a_scheduled_run_starts),
    )
    assert len(done.runs) == 1 and done.stopped is not None
    assert "backfill stopped at 2026-09-02 after 1 of 3 run(s)" in done.message
    assert "--backfill 2026-09-02:2026-09-03 --reason ..." in done.message


def test_a_backfill_never_meets_a_failure_dependency_on_another_pipeline(config, engine_db):
    engine = engine_db.engine
    with engine.begin() as conn:
        up = add_pipeline(conn, "UP")
        publish = add_task(conn, up, "publish")
        down = add_pipeline(conn, "DOWN")
        load = add_task(conn, down, "load", BEHAVIOUR="succeed")
        alert = add_task(conn, down, "alert", BEHAVIOUR="succeed")
        add_dependency(conn, down, load, publish, upstream_pipeline=up)
        add_dependency(conn, down, alert, publish, "FAILURE", upstream_pipeline=up)
    done = backfill(engine, config, "DOWN", date(2026, 9, 1), date(2026, 9, 2), "x", child=CHILD)
    assert done.status == RunStatus.SUCCESS
    for outcome in done.runs:
        found = statuses(engine, outcome.pipeline_run_id)
        assert found["load"][0] == "SUCCESS"
        assert found["alert"][0] == "SKIPPED"
    with engine.connect() as conn:
        reason = conn.execute(
            text("SELECT ERROR_MESSAGE FROM AUD_TASK_RUN_LOG WHERE TASK_ID = :t"), {"t": alert}
        ).first()[0]
    assert "(FAILURE) is not evaluated in a backfill" in reason


def test_a_backfill_stops_at_the_first_run_that_fails(config, pipeline):
    engine, _ = pipeline
    done = backfill(engine, config, "P", date(2026, 9, 1), date(2026, 9, 5), "x", child=CHILD)
    assert done.status == RunStatus.FAILED
    assert len(done.runs) == 1
    assert done.message.startswith("P: backfill stopped at 2026-09-01 after 1 of 5 run(s): ")
    assert done.message.endswith(
        "`etl-craft run --pipeline_code P --backfill 2026-09-01:2026-09-05` takes up from there "
        "once it is fixed"
    )


def test_what_a_backfill_refuses(config, pipeline):
    engine, ids = pipeline
    with pytest.raises(UsageError, match="2026-09-05 is after 2026-09-01"):
        backfill(engine, config, "P", date(2026, 9, 5), date(2026, 9, 1), "x")
    with pytest.raises(UsageError, match="is 367 dates; one backfill runs at most 366"):
        backfill(engine, config, "P", date(2025, 1, 1), date(2026, 1, 2), "x")
    with pytest.raises(UsageError, match="--backfill needs a --reason"):
        backfill(engine, config, "P", date(2026, 9, 1), date(2026, 9, 1), "")
    with pytest.raises(RunRefusedError, match="--backfill is only available in local mode"):
        backfill(
            engine, replace(config, mode=Mode.REMOTE), "P", date(2026, 9, 1), date(2026, 9, 1), "x"
        )
    with engine.begin() as conn:
        run_id = runlog.find_or_create_active_run(conn, ids["P"], run_date=date(2026, 9, 9))
    with pytest.raises(RunStateError, match=rf"run in progress \(pipeline_run_id={run_id}\)"):
        backfill(engine, config, "P", date(2026, 9, 1), date(2026, 9, 1), "x")
    with pytest.raises(RunStateError, match="as of 2026-09-09, not 2026-09-01"):
        run_pipeline(engine, config, "P", child=CHILD, run_date=date(2026, 9, 1))
    pause_pipeline(engine, config, "P", "hold", requested_by=WHO)
    with engine.begin() as conn:
        runlog.finalize_pipeline_run(conn, run_id, "FAILED")
    held = backfill(engine, config, "P", date(2026, 9, 1), date(2026, 9, 3), "x", child=CHILD)
    assert held.stopped is not None and held.message.startswith(
        "P: backfill stopped at 2026-09-01: "
    )


def test_the_run_date_and_backfill_options(config, engine_db, capsys, monkeypatch):
    # A pipeline with no tasks: the options are what is tested here.
    with engine_db.engine.begin() as conn:
        add_pipeline(conn, "P")
    monkeypatch.chdir(config.project_dir)
    both_days = ["--backfill", "2026-09-01:2026-09-02"]
    assert cli_main(["run", "--pipeline_code", "P", *both_days, "--reason", "r"]) == 0
    assert "backfill of 2 date(s)" in capsys.readouterr().out
    assert cli_main(["run", "--pipeline_code", "P", "--run-date", "2026-08-30"]) == 0
    capsys.readouterr()
    assert cli_main(["history", "--pipeline_code", "P"]) == ExitCode.SUCCESS
    history = capsys.readouterr().out.splitlines()
    assert history[0].startswith("PIPELINE_RUN_ID\tSTATUS\tRUN_DATE\t")
    assert history[1].split("\t")[2] == "2026-08-30"
    assert history[2].split("\t")[2] == "2026-09-02 (backfill)"
    for argv, message in (
        (["--backfill", "2026-09-01"], "is not FROM:TO"),
        (["--run-date", "01/09/2026"], "is not a date: write YYYY-MM-DD"),
    ):
        with pytest.raises(SystemExit):
            cli_main(["run", "--pipeline_code", "P", *argv])
        assert message in capsys.readouterr().err
    code = cli_main(["run", "--pipeline_code", "P", *both_days, "--task_code", "x"])
    assert code == ExitCode.USAGE


def test_a_task_left_running_by_a_dead_process_is_released_with_stale(config, pipeline):
    engine, ids = pipeline
    with engine.begin() as conn:
        run = runlog.find_or_create_active_run(conn, ids["P"])
        runlog.find_or_create_task_run(conn, ids["extract"], run)
    held = run_pipeline(engine, config, "P", child=CHILD)
    assert held.status == RunStatus.IN_PROGRESS
    assert "--stale" in held.message

    with pytest.raises(RunRefusedError, match="pass --stale"):
        mark_task(engine, config, "P", "extract", "FAILED", "process died", requested_by=WHO)
    marked = mark_task(
        engine, config, "P", "extract", "FAILED", "process died", stale=True, requested_by=WHO
    )
    assert "marked FAILED" in marked.message
    assert statuses(engine, run)["extract"][0] == "FAILED"

    done = run_pipeline(engine, config, "P", child=CHILD)
    assert (done.status, done.pipeline_run_id) == (RunStatus.FAILED, run)
    # Resuming retries the task marked FAILED; it succeeds this time, and the run ends.
    assert statuses(engine, run)["extract"][0] == "SUCCESS"
    assert statuses(engine, run)["transform"][0] == "SUCCESS"


def behave(engine, task_id, behaviour):
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE = :b "
                "WHERE TASK_ID = :t AND PARAMETER_NAME = 'BEHAVIOUR'"
            ),
            {"b": behaviour, "t": task_id},
        )


def test_a_rerun_of_a_failed_task_leaves_what_its_failure_skipped_to_run(config, engine_db):
    engine = engine_db.engine
    with engine.begin() as conn:
        q = add_pipeline(conn, "Q")
        a = add_task(conn, q, "a", BEHAVIOUR="fail")
        c = add_task(conn, q, "c", BEHAVIOUR="succeed")
        add_dependency(conn, q, c, a)
    failed = run_pipeline(engine, config, "Q", child=CHILD)
    assert failed.status == RunStatus.FAILED
    assert statuses(engine, failed.pipeline_run_id)["c"][0] == "SKIPPED"

    behave(engine, a, "succeed")
    fixed = rerun_task(engine, config, "Q", "a", "source fixed", child=CHILD)
    assert (fixed.status, fixed.pipeline_run_id) == (RunStatus.IN_PROGRESS, failed.pipeline_run_id)
    assert "1 task(s) still need to run: c" in fixed.message
    assert run_status(engine, failed.pipeline_run_id) == "IN-PROGRESS"

    done = run_pipeline(engine, config, "Q", child=CHILD)
    assert (done.status, done.pipeline_run_id) == (RunStatus.SUCCESS, failed.pipeline_run_id)
    assert statuses(engine, done.pipeline_run_id)["c"][0] == "SUCCESS"


def test_mark_keeps_a_skipped_task_another_pipeline_consumed(config, engine_db):
    engine = engine_db.engine
    with engine.begin() as conn:
        p = add_pipeline(conn, "P")
        ok = add_task(conn, p, "ok", BEHAVIOUR="succeed")
        on_failure = add_task(conn, p, "on_failure", BEHAVIOUR="succeed")
        add_dependency(conn, p, on_failure, ok, "FAILURE")
        q = add_pipeline(conn, "Q")
        down = add_task(conn, q, "down", BEHAVIOUR="succeed")
        add_dependency(conn, q, down, on_failure, "ALWAYS", upstream_pipeline=p)
    p_run = run_pipeline(engine, config, "P", child=CHILD)
    assert statuses(engine, p_run.pipeline_run_id)["on_failure"][0] == "SKIPPED"
    q_run = run_pipeline(engine, config, "Q", child=CHILD)
    assert q_run.status == RunStatus.SUCCESS  # it consumed P.on_failure's SKIPPED row

    marked = mark_task(engine, config, "P", "ok", "FAILED", "wrong data", requested_by=WHO)
    assert f"kept SKIPPED: on_failure (consumed by Q.down run {q_run.pipeline_run_id})" in (
        marked.message
    )
    assert statuses(engine, p_run.pipeline_run_id)["on_failure"][0] == "SKIPPED"


def test_a_rerun_of_a_task_still_running_leaves_its_ended_run_alone(config, pipeline):
    engine, ids = pipeline
    failed = run_pipeline(engine, config, "P", child=CHILD)
    assert failed.status == RunStatus.FAILED
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE AUD_TASK_RUN_LOG SET STATUS = 'IN-PROGRESS' "
                "WHERE TASK_ID = :t AND PIPELINE_RUN_ID = :r"
            ),
            {"t": ids["broken"], "r": failed.pipeline_run_id},
        )

    with pytest.raises(RunStateError, match="broken: already IN-PROGRESS"):
        rerun_task(engine, config, "P", "broken", "source fixed", child=CHILD)
    assert run_status(engine, failed.pipeline_run_id) == "FAILED"
    assert interventions(engine) == []


def test_a_skipped_run_is_ended_for_a_single_task(config, pipeline):
    engine, _ = pipeline
    skipped = skip_run(engine, config, "P", "public holiday", requested_by=WHO)
    with pytest.raises(RunStateError, match="is already SKIPPED"):
        run_task(engine, config, "P", "extract", override=Override("x"), child=CHILD)
    with pytest.raises(RunStateError, match="is already SKIPPED"):
        run_task(engine, config, "P", "extract", child=CHILD)
    assert run_status(engine, skipped.pipeline_run_id) == "SKIPPED"


def test_a_forced_task_reopens_an_ended_run_and_ends_it_again(config, pipeline):
    engine, ids = pipeline
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_TASKS SET ACTIVE_FLAG = 'N' WHERE TASK_ID IN (:b, :a)"),
            {"b": ids["broken"], "a": ids["after_broken"]},
        )
    first = run_pipeline(engine, config, "P", child=CHILD)
    assert first.status == RunStatus.SUCCESS
    behave(engine, ids["transform"], "fail")

    forced = force_task(engine, config, "P", "transform", child=CHILD)
    assert (forced.status, forced.pipeline_run_id) == (RunStatus.FAILED, first.pipeline_run_id)
    assert "1 task(s) did not succeed: transform (FAILED)" in forced.message
    assert run_status(engine, first.pipeline_run_id) == "FAILED"
    assert [(action, frm, to) for _, _, action, frm, to, *_ in interventions(engine)] == [
        ("REOPEN", "SUCCESS", "IN-PROGRESS")
    ]
    assert "--force: transform runs again" in interventions(engine)[0][7]


def test_a_forced_task_in_a_skipped_run_leaves_the_rest_to_run(config, pipeline):
    engine, _ = pipeline
    skipped = skip_run(engine, config, "P", "public holiday", requested_by=WHO)
    forced = force_task(engine, config, "P", "extract", child=CHILD)
    assert (forced.status, forced.pipeline_run_id) == (
        RunStatus.IN_PROGRESS,
        skipped.pipeline_run_id,
    )
    assert "still need to run: after_broken, alert, broken, transform" in forced.message


def test_a_forced_task_under_an_active_run_leaves_it_open(config, pipeline):
    engine, _ = pipeline
    with engine.begin() as conn:
        run_id = runlog.find_or_create_active_run(conn, pipeline[1]["P"])
    forced = force_task(engine, config, "P", "extract", child=CHILD)
    assert (forced.status, forced.pipeline_run_id) == (RunStatus.SUCCESS, run_id)
    assert run_status(engine, run_id) == "IN-PROGRESS"
    assert interventions(engine) == []


def test_a_run_whose_tasks_were_all_skipped_ends_skipped(config, pipeline):
    engine, ids = pipeline
    with engine.begin() as conn:
        q = add_pipeline(conn, "Q")
        down = add_task(conn, q, "down", BEHAVIOUR="succeed")
        add_dependency(conn, q, down, ids["broken"], upstream_pipeline=ids["P"])
        r = add_pipeline(conn, "R")
        add_task(conn, r, "load", BEHAVIOUR="succeed")
        add_pipeline_dependency(conn, r, q)
    run_pipeline(engine, config, "P", child=CHILD)
    skipped = run_pipeline(engine, config, "Q", child=CHILD, clock=NO_WAIT)
    assert statuses(engine, skipped.pipeline_run_id)["down"][0] == "SKIPPED"
    assert skipped.status == RunStatus.SKIPPED
    assert run_status(engine, skipped.pipeline_run_id) == "SKIPPED"
    after = run_pipeline(engine, config, "R", child=CHILD, clock=NO_WAIT)
    assert after.status == RunStatus.SKIPPED
    assert "upstream pipeline Q (SUCCESS)" in after.message


def test_a_met_sla_stays_met_when_the_run_is_ended_again_later(config, engine_db):
    engine = engine_db.engine
    with engine.begin() as conn:
        pipeline_id = add_pipeline(conn, "OK", sla_in_hours=1)
        add_task(conn, pipeline_id, "quick", BEHAVIOUR="succeed")
    lapses = []
    first = run_pipeline(engine, config, "OK", child=CHILD, hooks=RunHooks(lapses.append))
    assert first.sla.status == "MET"
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE AUD_PIPELINES_RUN_LOG SET START_DATE = :start WHERE PIPELINE_RUN_ID = :id"
            ),
            {"start": datetime.now(UTC) - timedelta(days=2), "id": first.pipeline_run_id},
        )
    again = rerun_task(
        engine, config, "OK", "quick", "source fixed", child=CHILD, hooks=RunHooks(lapses.append)
    )
    assert (again.status, again.pipeline_run_id) == (RunStatus.SUCCESS, first.pipeline_run_id)
    assert again.sla.status == "MET" and "BREACHED" not in again.message
    with engine.connect() as conn:
        assert runlog.fetch_run_sla(conn, first.pipeline_run_id).sla_status == "MET"
    assert lapses == []


def test_a_run_cancelled_while_it_is_ended_is_left_cancelled(config, pipeline, monkeypatch):
    engine, ids = pipeline
    with engine.begin() as conn:
        down = add_pipeline(conn, "DOWN")
        add_task(conn, down, "load", BEHAVIOUR="succeed")
        add_pipeline_dependency(conn, down, ids["P"])
        conn.execute(
            text("UPDATE CFG_TASKS SET ACTIVE_FLAG = 'N' WHERE TASK_ID IN (:b, :a)"),
            {"b": ids["broken"], "a": ids["after_broken"]},
        )
    assert run_pipeline(engine, config, "P", child=CHILD).status == RunStatus.SUCCESS
    real = runlog.fetch_run_sla

    def cancelled_first(conn, pipeline_run_id):
        conn.execute(
            text(
                "UPDATE AUD_PIPELINES_RUN_LOG SET STATUS = 'CANCELLED' WHERE PIPELINE_RUN_ID = :id"
            ),
            {"id": pipeline_run_id},
        )
        return real(conn, pipeline_run_id)

    monkeypatch.setattr(runlog, "fetch_run_sla", cancelled_first)
    finished = []
    outcome = run_pipeline(
        engine, config, "DOWN", child=CHILD, hooks=RunHooks(on_finalized=finished.append)
    )
    assert outcome.status == RunStatus.CANCELLED
    assert "it was ended elsewhere" in outcome.message
    assert finished == []
    with engine.connect() as conn:
        consumed = conn.execute(text("SELECT COUNT(*) FROM AUD_DEPENDENCY_CONSUMPTION"))
        assert consumed.scalar_one() == 0
