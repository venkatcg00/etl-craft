"""``run --pipeline_code``: whole pipelines in waves, init and finalize, gates, SLA and the CLI.

Tasks run in real task processes whose handlers are fakes (``fixtures.task_child``), against a
real Engine DB on both dialects.
"""

import logging
from dataclasses import replace
from pathlib import Path

import pytest
import yaml
from sqlalchemy import text

from etl_craft.cli import main as cli_main
from etl_craft.config import EmailConfig, EmailProfile, load_config
from etl_craft.core.enums import Mode, RunStatus
from etl_craft.core.errors import (
    ConnectionTestError,
    ExitCode,
    RemoteUnsupportedError,
    RunRefusedError,
    RunStateError,
)
from etl_craft.execution.gates import Clock
from etl_craft.execution.pipeline import (
    RunHooks,
    finalize_active_run,
    init_pipeline_run,
    run_pipeline,
)
from etl_craft.execution.runner import ChildOptions, attempt_log_path, run_task
from fixtures.metadata import (
    add_dependency,
    add_pipeline,
    add_pipeline_dependency,
    add_task,
    start_run,
    task_run,
    upstream_run,
)

TESTS_DIR = Path(__file__).parents[2]
CHILD = ChildOptions(module="fixtures.task_child", kill_grace_seconds=2)
NO_WAIT = Clock(sleep=lambda seconds: None)


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
                "SELECT t.TASK_CODE AS code, r.STATUS AS status, r.ATTEMPT_COUNT AS attempts, "
                "r.TARGET_COUNT AS target_count "
                "FROM AUD_TASK_RUN_LOG r JOIN CFG_TASKS t ON t.TASK_ID = r.TASK_ID "
                "WHERE r.PIPELINE_RUN_ID = :id"
            ),
            {"id": pipeline_run_id},
        )
        return {row.code: (row.status, row.attempts, row.target_count) for row in rows}


def run_row(engine, pipeline_run_id):
    with engine.connect() as conn:
        return conn.execute(
            text(
                "SELECT STATUS AS status, END_DATE AS end_date, SLA_STATUS AS sla_status "
                "FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID = :id"
            ),
            {"id": pipeline_run_id},
        ).one()


def test_a_pipeline_runs_in_waves_and_fails_when_a_task_fails(config, pipeline, caplog):
    engine, _ = pipeline
    caplog.set_level(logging.INFO, "etl_craft.execution.pipeline")
    outcome = run_pipeline(engine, config, "P", child=CHILD, clock=NO_WAIT)
    run_id = outcome.pipeline_run_id
    assert outcome.status == RunStatus.FAILED
    assert outcome.message == (
        f"P: pipeline_run_id={run_id} FAILED — 1 task(s) did not succeed: broken (FAILED); "
        "skipped because of the failure: after_broken"
    )
    assert statuses(engine, run_id) == {
        "extract": ("SUCCESS", 1, 9),
        "transform": ("SUCCESS", 1, 9),
        "broken": ("FAILED", 1, None),
        # No retry comes for broken in this run, so what waits on it is skipped, not left.
        "after_broken": ("SKIPPED", 1, None),
        "alert": ("SKIPPED", 1, None),
    }
    assert run_row(engine, run_id).status == "FAILED"
    waves = [r.getMessage() for r in caplog.records if ": wave " in r.getMessage()]
    assert waves == ["P: wave 1: extract, broken", "P: wave 2: transform"]


def test_an_interrupted_run_is_resumed_without_repeating_finished_tasks(config, pipeline):
    engine, ids = pipeline
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_TASKS SET ACTIVE_FLAG = 'N' WHERE TASK_ID IN (:b, :a)"),
            {"b": ids["broken"], "a": ids["after_broken"]},
        )
        run_id = start_run(conn, ids["P"])
        task_run(conn, ids["extract"], run_id, "SUCCESS", 42)
    finished = []
    outcome = run_pipeline(
        engine, config, "P", child=CHILD, hooks=RunHooks(on_finalized=finished.append)
    )
    assert (outcome.status, outcome.pipeline_run_id) == (RunStatus.SUCCESS, run_id)
    # extract kept its row from before; the rest ran.
    assert statuses(engine, run_id) == {
        "extract": ("SUCCESS", 1, 42),
        "transform": ("SUCCESS", 1, 9),
        "alert": ("SKIPPED", 1, None),
    }
    assert run_row(engine, run_id).end_date is not None
    assert finished == [outcome]


def test_force_runs_every_task_again(config, pipeline):
    engine, ids = pipeline
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_TASKS SET ACTIVE_FLAG = 'N' WHERE TASK_ID <> :e"),
            {"e": ids["extract"]},
        )
        run_id = start_run(conn, ids["P"])
        task_run(conn, ids["extract"], run_id, "SUCCESS", 42)
    outcome = run_pipeline(engine, config, "P", child=CHILD, force=True)
    assert outcome.status == RunStatus.SUCCESS
    assert statuses(engine, run_id) == {"extract": ("SUCCESS", 2, 9)}


def test_an_unsatisfied_pipeline_dependency_skips_the_run(config, pipeline):
    engine, ids = pipeline
    with engine.begin() as conn:
        upstream = add_pipeline(conn, "UP")
        edge = add_pipeline_dependency(conn, ids["P"], upstream)
    finished = []
    outcome = run_pipeline(
        engine,
        config,
        "P",
        child=CHILD,
        clock=NO_WAIT,
        hooks=RunHooks(on_finalized=finished.append),
    )
    assert outcome.status == RunStatus.SKIPPED
    assert outcome.message.endswith("SKIPPED — upstream pipeline UP (SUCCESS) has no finished run")
    assert run_row(engine, outcome.pipeline_run_id).status == "SKIPPED"
    assert statuses(engine, outcome.pipeline_run_id) == {}
    assert finished == [outcome]

    # Once UP has succeeded, the next run starts, and on success consumes UP's run.
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_TASKS SET ACTIVE_FLAG = 'N' WHERE TASK_ID = :b"), {"b": ids["broken"]}
        )
        conn.execute(
            text("UPDATE CFG_TASKS SET ACTIVE_FLAG = 'N' WHERE TASK_ID = :a"),
            {"a": ids["after_broken"]},
        )
        up_run, _ = upstream_run(conn, upstream, {})
    assert run_pipeline(engine, config, "P", child=CHILD, clock=NO_WAIT).status == "SUCCESS"
    with engine.connect() as conn:
        consumed = conn.execute(
            text(
                "SELECT CONSUMED_PIPELINE_RUN_ID FROM AUD_DEPENDENCY_CONSUMPTION "
                "WHERE PIPELINE_DEPENDENCY_ID = :id"
            ),
            {"id": edge},
        ).scalar_one()
    assert consumed == up_run


def test_remote_mode_refuses_a_whole_pipeline_run(config, pipeline):
    engine, _ = pipeline
    with pytest.raises(RunRefusedError, match="only local mode does"):
        run_pipeline(engine, replace(config, mode=Mode.REMOTE), "P")


def test_an_orchestrator_starts_runs_and_finalizes(config, pipeline):
    engine, ids = pipeline
    remote = replace(config, mode=Mode.REMOTE)
    with pytest.raises(RunStateError, match="P has no active run to finalize"):
        finalize_active_run(engine, remote, "P")
    with pytest.raises(RunStateError, match="--init-only`, starts it"):
        run_task(engine, remote, "P", "extract", child=CHILD)
    # The orchestrator holds the pipeline's dependencies: an unsatisfied one does not stop init.
    with engine.begin() as conn:
        add_pipeline_dependency(conn, ids["P"], add_pipeline(conn, "UP"))

    started = init_pipeline_run(engine, remote, "P")
    assert started.status == RunStatus.IN_PROGRESS
    assert started.message == f"P: pipeline_run_id={started.pipeline_run_id} IN-PROGRESS"
    # A second init resumes the same run.
    assert init_pipeline_run(engine, remote, "P").pipeline_run_id == started.pipeline_run_id
    # A task runs when the orchestrator says, whatever its dependencies: transform before
    # extract, and after_broken after broken failed.
    for code in ("transform", "extract", "broken", "after_broken"):
        run_task(engine, remote, "P", code, child=CHILD)

    ended = finalize_active_run(engine, remote, "P")
    assert ended.status == RunStatus.FAILED
    assert ended.message == (
        f"P: pipeline_run_id={started.pipeline_run_id} FAILED — 1 task(s) did not succeed: "
        "broken (FAILED); not run by the orchestrator: alert"
    )
    run_id = started.pipeline_run_id
    assert statuses(engine, run_id) == {
        "extract": ("SUCCESS", 1, 9),
        "transform": ("SUCCESS", 1, 9),
        "broken": ("FAILED", 1, None),
        "after_broken": ("SUCCESS", 1, 9),
        "alert": ("SKIPPED", 1, None),
    }
    assert run_row(engine, run_id).status == "FAILED"
    with engine.connect() as conn:
        message = conn.execute(
            text("SELECT ERROR_MESSAGE FROM AUD_TASK_RUN_LOG WHERE TASK_ID = :t"),
            {"t": ids["alert"]},
        ).scalar_one()
    assert message == "not run by the orchestrator"

    # A task cleared after the run ended runs again: the run reopens until finalized again.
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE = 'succeed' WHERE TASK_ID = :t"),
            {"t": ids["broken"]},
        )
    rerun = run_task(engine, remote, "P", "broken", child=CHILD)
    assert rerun.status == RunStatus.SUCCESS
    assert run_row(engine, run_id).status == "IN-PROGRESS"
    assert run_row(engine, run_id).end_date is None
    # So does one that already succeeded: a new attempt on the same row.
    assert run_task(engine, remote, "P", "extract", child=CHILD).status == RunStatus.SUCCESS
    assert statuses(engine, run_id)["extract"] == ("SUCCESS", 2, 9)
    log = attempt_log_path(remote, "P", run_id, "extract", 2).read_text("utf-8")
    assert "fake handler doing succeed again" in log
    again = finalize_active_run(engine, remote, "P")
    assert (again.status, again.pipeline_run_id) == (RunStatus.SUCCESS, run_id)


def test_a_run_the_orchestrator_ran_no_task_of_is_skipped(config, pipeline):
    engine, _ = pipeline
    remote = replace(config, mode=Mode.REMOTE)
    run_id = init_pipeline_run(engine, remote, "P").pipeline_run_id
    ended = finalize_active_run(engine, remote, "P")
    assert ended.status == RunStatus.SKIPPED
    assert ended.message == (
        f"P: pipeline_run_id={run_id} SKIPPED; not run by the orchestrator: after_broken, "
        "alert, broken, extract, transform"
    )
    assert run_row(engine, run_id).status == "SKIPPED"


def test_finalize_fails_a_task_whose_orchestrated_process_was_lost(config, pipeline):
    engine, ids = pipeline
    remote = replace(config, mode=Mode.REMOTE)
    run_id = init_pipeline_run(engine, remote, "P").pipeline_run_id
    with engine.begin() as conn:
        task_run(conn, ids["extract"], run_id, status="IN-PROGRESS")
    ended = finalize_active_run(engine, remote, "P")
    assert ended.status == RunStatus.FAILED
    assert "extract (FAILED)" in ended.message
    with engine.connect() as conn:
        message = conn.execute(
            text("SELECT ERROR_MESSAGE FROM AUD_TASK_RUN_LOG WHERE TASK_ID = :t"),
            {"t": ids["extract"]},
        ).scalar_one()
    assert message.startswith("still IN-PROGRESS when the orchestrator finalized the run")


def test_remote_mode_refuses_rules_the_orchestrator_does_not_support(config, pipeline):
    engine, ids = pipeline
    remote = replace(config, mode=Mode.REMOTE)
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE CFG_TASKS SET RUN_CONDITION = 'N', RUN_CONDITION_COUNT = 1 "
                "WHERE TASK_ID = :t"
            ),
            {"t": ids["transform"]},
        )
    with pytest.raises(RemoteUnsupportedError) as refused:
        init_pipeline_run(engine, remote, "P")
    assert str(refused.value).startswith(
        "P has 1 rule(s) the remote orchestrator does not support, so they cannot be applied "
        "in remote mode: P.transform: RUN_CONDITION = 'N' (RUN_CONDITION_COUNT = 1); the remote "
        "orchestrator does not support this."
    )
    assert "or run it in local mode (Orchestration.Mode: local)" in str(refused.value)
    with engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_PIPELINES_RUN_LOG")).scalar_one() == 0
    # Local mode applies the rule itself.
    assert init_pipeline_run(engine, config, "P").status == RunStatus.IN_PROGRESS


def test_the_sla_is_marked_breached_while_the_run_is_still_going(config, engine_db):
    engine = engine_db.engine
    with engine.begin() as conn:
        pipeline_id = add_pipeline(conn, "LATE", sla_in_hours=0.0001)
        add_task(conn, pipeline_id, "slow", BEHAVIOUR="sleep", TASK_TIMEOUT_SECONDS=2)
    lapses = []
    outcome = run_pipeline(engine, config, "LATE", child=CHILD, hooks=RunHooks(lapses.append))
    assert outcome.status == RunStatus.FAILED
    assert run_row(engine, outcome.pipeline_run_id).sla_status == "BREACHED"
    assert "SLA of 0.0001 h BREACHED" in outcome.message
    # Once per run: the watcher saw it first, so finalize does not report it again.
    assert [lapse.pipeline_code for lapse in lapses] == ["LATE"]


def test_finalize_reports_a_lapse_nobody_saw_while_running(config, engine_db):
    engine = engine_db.engine
    with engine.begin() as conn:
        pipeline_id = add_pipeline(conn, "LATE", sla_in_hours=0.0001)
        add_task(conn, pipeline_id, "quick")
        run_id = start_run(conn, pipeline_id)
        conn.execute(
            text(
                "UPDATE AUD_PIPELINES_RUN_LOG SET START_DATE = START_DATE - INTERVAL '1' HOUR "
                "WHERE PIPELINE_RUN_ID = :id"
            )
            if engine.dialect.name == "postgresql"
            else text(
                "UPDATE AUD_PIPELINES_RUN_LOG SET START_DATE = '2000-01-01 00:00:00.000000+00:00' "
                "WHERE PIPELINE_RUN_ID = :id"
            ),
            {"id": run_id},
        )
        task_run(
            conn,
            conn.execute(
                text("SELECT TASK_ID FROM CFG_TASKS WHERE TASK_CODE = 'quick'")
            ).scalar_one(),
            run_id,
        )
    lapses = []

    def broken_hook(outcome):
        raise RuntimeError("cloning is down")

    outcome = finalize_active_run(
        engine, config, "LATE", hooks=RunHooks(lapses.append, broken_hook)
    )
    assert outcome.status == RunStatus.SUCCESS
    assert [lapse.pipeline_run_id for lapse in lapses] == [run_id]


def test_a_failed_connection_test_starts_no_run(config, pipeline):
    engine, ids = pipeline
    with engine.begin() as conn:
        add_task(conn, ids["P"], "notify", handler="EMAIL_ALERT")
    relay = EmailProfile("EMAIL", "down", host="127.0.0.1", port=1, from_address="etl@example.com")
    broken = replace(config, email=EmailConfig("down", {"down": relay}))
    with pytest.raises(ConnectionTestError, match=r"P: a connection test failed.*email relay: 127"):
        run_pipeline(engine, broken, "P", child=CHILD)
    with engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_PIPELINES_RUN_LOG")).scalar_one() == 0


def test_the_command_line(config, pipeline, capsys, monkeypatch):
    engine, _ = pipeline
    monkeypatch.chdir(config.config_path.parent)
    assert cli_main(["run", "--pipeline_code", "P", "--init-only"]) == ExitCode.SUCCESS
    assert capsys.readouterr().out.endswith("IN-PROGRESS\n")
    # The real task process has no handlers installed, so every task that runs fails.
    assert cli_main(["run", "--pipeline_code", "P"]) == ExitCode.FAILURE
    # alert runs because extract failed; transform and after_broken never can.
    out = capsys.readouterr().out
    assert "FAILED — 3 task(s) did not succeed" in out
    assert "skipped because of the failure: transform, after_broken" in out
    with engine.begin() as conn:
        start_run(conn, conn.execute(text("SELECT PIPELINE_ID FROM CFG_PIPELINES")).scalar_one())
    assert cli_main(["run", "--pipeline_code", "P", "--finalize-only"]) == ExitCode.FAILURE
    assert cli_main(["run", "--pipeline_code", "P", "--init-only", "--force"]) == ExitCode.USAGE
    with pytest.raises(SystemExit) as usage:
        cli_main(["run", "--pipeline_code", "P", "--init-only", "--task_code", "extract"])
    assert usage.value.code == ExitCode.USAGE
