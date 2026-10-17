"""Durable event waits, evidence and atomic file consumption on both Engine DBs."""

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from etl_craft.core.errors import InjectedFaultError
from etl_craft.engine.repository.events import consumed_files, run_event_files
from etl_craft.execution.events import check_event
from etl_craft.execution.gates import Clock
from etl_craft.execution.pipeline import run_pipeline
from etl_craft.execution.runner import run_task
from etl_craft.services.validate import validate
from fixtures.metadata import add_dependency, add_pipeline, add_task, finish_run, start_run


@pytest.fixture
def world(engine_db, tmp_path):
    config = replace(engine_db.config, config_path=tmp_path / "craft-connector.yml")
    return engine_db.engine, config


def setup(world, **params):
    engine, config = world
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(
            conn, pipeline, "wait", "EVENT_WAIT", **{"EVENT_POLL_SECONDS": "1", **params}
        )
        run = start_run(conn, pipeline)
    return engine, config, pipeline, task, run


def test_appearing_and_growing_files_resume_the_original_wait(world):
    engine, config, _, task, run = setup(
        world, EVENT_FILE="inbox/*.csv", EVENT_SETTLE_SECONDS="3", EVENT_TIMEOUT_MINUTES="1"
    )
    now = datetime.now(UTC)
    first = check_event(engine, config, run, task, now)
    assert first.next_check_at == now + timedelta(seconds=1)
    path = config.project_dir / "inbox" / "data.csv"
    path.parent.mkdir()
    path.write_text("first")
    assert check_event(engine, config, run, task, now + timedelta(seconds=1)).status is None
    path.write_text("first second")
    assert check_event(engine, config, run, task, now + timedelta(seconds=3)).status is None
    assert check_event(engine, config, run, task, now + timedelta(seconds=5)).status is None
    # A new call has no in-memory state from the original scheduler.
    done = check_event(engine, config, run, task, now + timedelta(seconds=6))
    assert done.status == "SUCCESS"
    with engine.connect() as conn:
        assert run_event_files(conn, run) == (str(path.resolve()),)
        log = conn.execute(
            text("SELECT TASK_LOG AS task_log FROM AUD_TASK_RUN_LOG WHERE TASK_RUN_ID=:id"),
            {"id": done.task_run_id},
        ).scalar_one()
        assert json.loads(log.removeprefix("EVENT_EVIDENCE = ")) == {
            "files": [
                {"path": str(path.resolve()), "size": 12, "mtime_ns": path.stat().st_mtime_ns}
            ]
        }
        wait = conn.execute(
            text(
                "SELECT FIRST_CHECK_AT AS first_check_at, WAIT_UNTIL AS wait_until "
                "FROM AUD_GATE_WAITS WHERE WAIT_KIND='EVENT'"
            )
        ).one()
        assert str(wait.first_check_at).startswith(now.strftime("%Y-%m-%d %H:%M:%S"))
        assert (
            conn.execute(
                text(
                    "SELECT count(*) FROM AUD_TASK_ATTEMPTS "
                    "WHERE STATUS IN ('QUEUED','CLAIMED','RUNNING')"
                )
            ).scalar_one()
            == 0
        )


def test_consumed_path_does_not_match_in_later_run_even_if_rewritten(world):
    engine, config, pipeline, task, run = setup(world, EVENT_FILE="*.csv", EVENT_SETTLE_SECONDS="0")
    path = config.project_dir / "data.csv"
    path.write_text("one")
    now = datetime.now(UTC)
    assert check_event(engine, config, run, task, now).status == "SUCCESS"
    with engine.begin() as conn:
        finish_run(conn, run)
        second = start_run(conn, pipeline)
    path.write_text("two")
    assert check_event(engine, config, second, task, now).next_check_at is not None


@pytest.mark.parametrize("policy,status", [("FAIL", "FAILED"), ("SKIP", "SKIPPED")])
def test_timeout_keeps_budget_across_restart_and_records_outcome(world, policy, status):
    engine, config, _, task, run = setup(
        world, EVENT_FILE="absent", EVENT_TIMEOUT_MINUTES="0.05", EVENT_ON_TIMEOUT=policy
    )
    now = datetime.now(UTC)
    assert check_event(engine, config, run, task, now).next_check_at
    assert check_event(engine, config, run, task, now + timedelta(seconds=1)).next_check_at
    result = check_event(engine, config, run, task, now + timedelta(seconds=3))
    assert result.status == status
    assert "timed out after 0.05 minutes" in result.message
    with engine.connect() as conn:
        assert consumed_files(conn, task) == set()


def test_evidence_and_consumption_rollback_with_outcome(world, monkeypatch):
    engine, config, _, task, run = setup(world, EVENT_FILE="*.csv", EVENT_SETTLE_SECONDS="0")
    (config.project_dir / "data.csv").write_text("data")
    monkeypatch.setenv("ETL_CRAFT_FAULT", "event.after_evidence")
    with pytest.raises(InjectedFaultError):
        check_event(engine, config, run, task, datetime.now(UTC))
    with engine.connect() as conn:
        assert consumed_files(conn, task) == set()
        assert run_event_files(conn, run) == ()
        assert conn.execute(text("SELECT count(*) FROM AUD_TASK_ATTEMPTS")).scalar_one() == 0
    monkeypatch.delenv("ETL_CRAFT_FAULT")
    assert check_event(engine, config, run, task, datetime.now(UTC)).status == "SUCCESS"


@pytest.mark.parametrize("table", ["AUD_TASK_EVENTS", "AUD_EVENT_FILES"])
def test_evidence_and_consumption_are_immutable(world, table):
    engine, config, _, task, run = setup(world, EVENT_FILE="*.csv", EVENT_SETTLE_SECONDS="0")
    (config.project_dir / "data.csv").write_text("data")
    check_event(engine, config, run, task, datetime.now(UTC))
    with pytest.raises(DBAPIError), engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {table}"))


def test_task_command_returns_waiting_without_attempt(world):
    engine, config, _, _, run = setup(world, EVENT_FILE="absent")
    outcome = run_task(engine, config, "P", "wait")
    assert outcome.waiting and outcome.status == "IN-PROGRESS"
    with engine.connect() as conn:
        assert run_event_files(conn, run) == ()
        assert conn.execute(text("SELECT count(*) FROM AUD_TASK_ATTEMPTS")).scalar_one() == 0


def test_validation_uses_the_runtime_condition_checks(world):
    engine, config, _, _, _ = setup(world, EVENT_FILE="*.csv", EVENT_SQL="DELETE FROM source")
    report = validate(engine, config, "P")
    assert report.failed
    assert any("exactly one" in finding.message for finding in report.findings)


def test_pipeline_releases_downstream_scripts_and_sql(world):
    engine, config, pipeline, task, _ = setup(world, EVENT_FILE="*.csv", EVENT_SETTLE_SECONDS="0")
    folder = config.project_dir / "ingestion_scripts"
    folder.mkdir()
    (folder / "consume.py").write_text(
        "from pathlib import Path\n"
        "from etl_craft.scripting import ScriptResult\n"
        "def run(task):\n"
        "    assert task.input_params['files'] == list(task.event_files)\n"
        "    value = Path(task.event_files[0]).read_text()\n"
        "    Path(task.config.project_dir / 'received').write_text(value)\n"
        "    return ScriptResult(row_count=1)\n"
    )
    import yaml

    from etl_craft.config import load_config

    raw = {
        "Secrets": {"Source_type": "environment"},
        "Orchestration": {"Mode": "local"},
        "Engine": {
            "dev": {
                "jdbc_url": config.engine.jdbc_url,
                "schema": config.engine.schema or "main",
                "user": config.engine.user,
                "auth_mode": config.engine.auth_mode,
            }
        },
    }
    if config.engine.secret_var:
        raw["Engine"]["dev"]["secret"] = config.engine.secret_var
    config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    config = load_config(config.config_path)
    with engine.begin() as conn:
        downstream = add_task(
            conn,
            pipeline,
            "consume",
            "PYTHON",
            SCRIPT_NAME="consume.py",
            INPUT_PARAMS='{"files":"$$event_files"}',
        )
        add_dependency(conn, pipeline, downstream, task)
    arrived = False

    def arrive(seconds):
        nonlocal arrived
        if not arrived:
            (config.project_dir / "data.csv").write_text("trigger")
            arrived = True

    outcome = run_pipeline(engine, config, "P", clock=Clock(sleep=arrive))
    assert outcome.status == "SUCCESS"
    assert (config.project_dir / "received").read_text() == "trigger"


def test_scheduler_checks_events_with_no_available_worker_and_resumes(world):
    from etl_craft.core.graph import build_graph
    from etl_craft.engine.repository.dependencies import fetch_pipeline_graph
    from etl_craft.execution.attempt_types import ChildOptions
    from etl_craft.execution.pools import Capacity
    from etl_craft.execution.scheduler import Scheduler

    engine, config, pipeline, task, run = setup(world, EVENT_FILE="*.csv", EVENT_SETTLE_SECONDS="0")
    now = datetime.now(UTC)
    with engine.connect() as conn:
        data = fetch_pipeline_graph(conn, pipeline)
    graph = build_graph(data.tasks, data.same_pipeline_edges)

    class FullPool:
        def capacity(self):
            return Capacity({"ingestion": 1, "warehouse": 1}, {"ingestion": 0, "warehouse": 0})

        def submit(self, spec):
            pytest.fail("an event wait must never submit to a worker")

    def scheduler():
        return Scheduler(
            engine,
            config,
            "P",
            pipeline,
            run,
            {task: "wait"},
            graph,
            force=False,
            clock=Clock(now=lambda: now),
            child=ChildOptions(),
            pool=FullPool(),
            paused=lambda: False,
            settle=lambda failures: [],
        )

    initial = scheduler()
    try:
        assert not initial.step()
    finally:
        initial.close()
    with engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM AUD_TASK_ATTEMPTS")).scalar_one() == 0
    (config.project_dir / "ready.csv").write_text("ready")
    now += timedelta(seconds=1)
    restarted = scheduler()
    assert not restarted.step()
    assert restarted.step()
    restarted.close()
    with engine.connect() as conn:
        assert run_event_files(conn, run) == (str(config.project_dir / "ready.csv"),)


@pytest.mark.parametrize("policy,expected", [("FAIL", "FAILED"), ("SKIP", "SKIPPED")])
def test_timeout_dependents_follow_normal_dependency_types(world, policy, expected):
    from etl_craft.engine import runlog
    from etl_craft.execution.pools import Capacity

    engine, config, pipeline, task, run = setup(
        world, EVENT_FILE="absent", EVENT_TIMEOUT_MINUTES="0", EVENT_ON_TIMEOUT=policy
    )
    with engine.begin() as conn:
        success = add_task(
            conn,
            pipeline,
            "success",
            "EVENT_WAIT",
            EVENT_FILE="absent",
            EVENT_TIMEOUT_MINUTES="0",
            EVENT_ON_TIMEOUT="SKIP",
        )
        failure = add_task(
            conn,
            pipeline,
            "failure",
            "EVENT_WAIT",
            EVENT_FILE="absent",
            EVENT_TIMEOUT_MINUTES="0",
            EVENT_ON_TIMEOUT="SKIP",
        )
        always = add_task(
            conn,
            pipeline,
            "always",
            "EVENT_WAIT",
            EVENT_FILE="absent",
            EVENT_TIMEOUT_MINUTES="0",
            EVENT_ON_TIMEOUT="SKIP",
        )
        add_dependency(conn, pipeline, success, task, "SUCCESS")
        add_dependency(conn, pipeline, failure, task, "FAILURE")
        add_dependency(conn, pipeline, always, task, "ALWAYS")

    class Pool:
        def capacity(self):
            return Capacity({"ingestion": 1, "warehouse": 1}, {"ingestion": 0, "warehouse": 0})

        def submit(self, spec):
            pytest.fail("event waits never use a worker")

    outcome = run_pipeline(engine, config, "P", pool=Pool())
    assert outcome.status == expected
    with engine.connect() as conn:
        state = runlog.fetch_run_state(conn, run, (task, success, failure, always))
        assert state[task].status == expected
        assert state[success].status == "SKIPPED"
        assert state[always].status == "SKIPPED"
        messages = dict(
            conn.execute(
                text(
                    "SELECT TASK_ID AS task_id, ERROR_MESSAGE AS error_message "
                    "FROM AUD_TASK_RUN_LOG WHERE PIPELINE_RUN_ID=:run"
                ),
                {"run": run},
            ).all()
        )
        assert "timed out" in messages[always]
        assert "timed out" not in messages[success]
        if policy == "FAIL":
            assert state[failure].status == "SKIPPED"
            assert "timed out" in messages[failure]
        else:
            assert "timed out" not in messages[failure]


def test_concurrent_checks_commit_one_success_and_one_consumption(world):
    from concurrent.futures import ThreadPoolExecutor
    from contextvars import copy_context

    engine, config, _, task, run = setup(world, EVENT_FILE="*.csv", EVENT_SETTLE_SECONDS="0")
    (config.project_dir / "ready.csv").write_text("ready")
    now = datetime.now(UTC)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(copy_context().run, check_event, engine, config, run, task, now)
            for _ in range(2)
        ]
        results = [future.result() for future in futures]
    assert all(result.status == "SUCCESS" or result.next_check_at for result in results)
    with engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM AUD_TASK_EVENTS")).scalar_one() == 1
        assert conn.execute(text("SELECT count(*) FROM AUD_EVENT_FILES")).scalar_one() == 1
        assert conn.execute(text("SELECT count(*) FROM AUD_TASK_ATTEMPTS")).scalar_one() == 1


def test_cancellation_during_probe_cannot_commit_evidence_or_consumption(world, monkeypatch):
    from etl_craft.core.actor import current_actor
    from etl_craft.core.errors import StaleTransitionError
    from etl_craft.engine import transitions
    from etl_craft.execution import events

    engine, config, _, task, run = setup(world, EVENT_FILE="*.csv", EVENT_SETTLE_SECONDS="0")
    path = config.project_dir / "ready.csv"
    path.write_text("ready")
    original = events.probe

    def cancel_after_probe(*args):
        result = original(*args)
        with engine.begin() as conn:
            transitions.finish_run(conn, run, "CANCELLED", current_actor())
        return result

    monkeypatch.setattr(events, "probe", cancel_after_probe)
    with pytest.raises(StaleTransitionError, match="IN-PROGRESS"):
        check_event(engine, config, run, task, datetime.now(UTC))
    with engine.connect() as conn:
        assert consumed_files(conn, task) == set()
        assert run_event_files(conn, run) == ()
        assert conn.execute(text("SELECT count(*) FROM AUD_TASK_ATTEMPTS")).scalar_one() == 0


def test_absolute_glob_deduplicates_symbolic_link_aliases(world):
    engine, config = world
    path = config.project_dir / "ready.csv"
    path.write_text("ready")
    (config.project_dir / "alias.csv").symlink_to(path)
    engine, config, _, task, run = setup(
        world, EVENT_FILE=str(config.project_dir / "*.csv"), EVENT_SETTLE_SECONDS="0"
    )
    assert check_event(engine, config, run, task, datetime.now(UTC)).status == "SUCCESS"
    with engine.connect() as conn:
        assert run_event_files(conn, run) == (str(path),)
        assert consumed_files(conn, task) == {str(path)}


def test_validation_refuses_has_data_on_an_event_wait(world):
    engine, config, pipeline, task, _ = setup(world, EVENT_FILE="*.csv")
    with engine.begin() as conn:
        downstream = add_task(conn, pipeline, "later", "EVENT_WAIT", EVENT_FILE="later.csv")
        add_dependency(conn, pipeline, downstream, task, "HAS_DATA")
    report = validate(engine, config, "P")
    assert report.failed
    assert any(
        "EVENT_WAIT task and reports no target rows" in finding.message
        for finding in report.findings
    )


def test_sql_probe_and_evidence_use_either_engine_db(world):
    from etl_craft.config import ConnectionProfile

    engine, config, _, task, run = setup(world, EVENT_SQL="SELECT 1 AS ready")
    config = replace(
        config,
        warehouse=ConnectionProfile(
            "WAREHOUSE", "test", "jdbc:duckdb:warehouse.duckdb", "", "none"
        ),
    )
    result = check_event(engine, config, run, task, datetime.now(UTC))
    assert result.status == "SUCCESS"
    with engine.connect() as conn:
        evidence = conn.execute(
            text("SELECT EVIDENCE FROM AUD_TASK_EVENTS WHERE TASK_RUN_ID=:id"),
            {"id": result.task_run_id},
        ).scalar_one()
    assert json.loads(evidence) == {"row": {"ready": 1}}


def test_indefinite_wait_survives_restart_without_resetting_to_finite_timeout(world):
    engine, config, _, task, run = setup(
        world,
        EVENT_FILE="inbox/*.csv",
        EVENT_TIMEOUT_MINUTES="NONE",
        EVENT_POLL_SECONDS="60",
        EVENT_SETTLE_SECONDS="0",
    )
    now = datetime.now(UTC)
    assert check_event(engine, config, run, task, now).next_check_at == now + timedelta(seconds=60)
    with engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT WAIT_UNTIL FROM AUD_GATE_WAITS WHERE WAIT_KIND='EVENT'")
            ).scalar_one()
            is None
        )
    years_later = now + timedelta(days=3650)
    assert check_event(engine, config, run, task, years_later).status is None
    path = config.project_dir / "inbox" / "arrived.csv"
    path.parent.mkdir()
    path.write_text("ready")
    # Notification and restart checks may precede the durable fallback scan.
    assert check_event(engine, config, run, task, years_later, force_check=True).status == "SUCCESS"


def test_scheduler_caches_idle_wait_and_notification_bypasses_fallback(world, monkeypatch):
    from etl_craft.core.graph import build_graph
    from etl_craft.engine.repository.dependencies import fetch_pipeline_graph
    from etl_craft.execution.attempt_types import ChildOptions
    from etl_craft.execution.scheduler import Scheduler
    from etl_craft.handlers.event_wait import probe

    engine, config, pipeline, task, run = setup(
        world,
        EVENT_FILE="*.csv",
        EVENT_SETTLE_SECONDS="0",
        EVENT_POLL_SECONDS="60",
        EVENT_TIMEOUT_MINUTES="NONE",
    )
    with engine.connect() as conn:
        data = fetch_pipeline_graph(conn, pipeline)
    probes = []

    def counted(*args):
        probes.append(1)
        return probe(*args)

    monkeypatch.setattr("etl_craft.execution.events.probe", counted)
    scheduler = Scheduler(
        engine,
        config,
        "P",
        pipeline,
        run,
        {task: "wait"},
        build_graph(data.tasks, data.same_pipeline_edges),
        force=False,
        clock=Clock(),
        child=ChildOptions(),
        pool=None,
        paused=lambda: False,
        settle=lambda failures: [],
    )
    try:
        assert not scheduler.step()
        for _ in range(3):
            assert not scheduler.step()
        assert len(probes) == 1
        (config.project_dir / "arrived.csv").write_text("ready")
        assert scheduler.file_events.changes[task].wait(5)
        assert not scheduler.step()
        assert scheduler.step()
        assert len(probes) == 2
        with engine.connect() as conn:
            assert run_event_files(conn, run) == (str(config.project_dir / "arrived.csv"),)
    finally:
        scheduler.close()
