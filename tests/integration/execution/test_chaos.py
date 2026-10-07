"""Real process admission and recovery across independent command invocations."""

import time
from datetime import UTC, datetime, timedelta

import pytest
import yaml
from sqlalchemy import text

from etl_craft.core.errors import ExitCode
from etl_craft.warehouse.connection import open_warehouse

pytestmark = pytest.mark.chaos


def install_writer(project, *, hold=False):
    """Write once per task run and optionally wait for an explicit test release."""
    waiting = (
        f"    Path({str(project.config.project_dir / 'ready')!r}).touch()\n"
        f"    while not Path({str(project.config.project_dir / 'release')!r}).exists():\n"
        "        time.sleep(0.02)\n"
        if hold
        else ""
    )
    (project.config.ingestion_scripts_dir / "load.py").write_text(
        "from pathlib import Path\nimport time\n"
        "from sqlalchemy import text\nfrom etl_craft.scripting import ScriptResult\n"
        "def run(task):\n"
        + waiting
        + "    with task.warehouse() as engine, engine.begin() as conn:\n"
        "        conn.execute(text('CREATE TABLE IF NOT EXISTS proof "
        "(pipeline_id BIGINT, pipeline_run_id BIGINT, task_run_id BIGINT)'))\n"
        "        conn.execute(text('INSERT INTO proof SELECT :pipeline_id, :pipeline_run_id, "
        ":task_run_id WHERE NOT EXISTS (SELECT 1 FROM proof WHERE task_run_id=:task_run_id)'), "
        "{'pipeline_id':task.pipeline_id,'pipeline_run_id':task.pipeline_run_id,"
        "'task_run_id':task.task_run_id})\n"
        "    return ScriptResult(1)\n"
    )


def assert_complete(project):
    """Require one successful execution and matching warehouse provenance."""
    with project.engine.connect() as conn:
        run = conn.execute(
            text(
                "SELECT PIPELINE_RUN_ID AS pipeline_run_id, STATUS AS status "
                "FROM AUD_PIPELINES_RUN_LOG"
            )
        ).one()
        task = conn.execute(
            text("SELECT TASK_RUN_ID AS task_run_id, STATUS AS status FROM AUD_TASK_RUN_LOG")
        ).one()
        attempts = (
            conn.execute(text("SELECT STATUS FROM AUD_TASK_ATTEMPTS ORDER BY ATTEMPT_NUMBER"))
            .scalars()
            .all()
        )
    assert run.status == task.status == "SUCCESS"
    assert attempts[-1] == "SUCCESS"
    assert not set(attempts) & {"QUEUED", "CLAIMED", "RUNNING"}
    with open_warehouse(project.config, project.engine) as warehouse, warehouse.connect() as conn:
        assert conn.execute(text("SELECT * FROM proof")).all() == [
            (project.pipeline_id, run.pipeline_run_id, task.task_run_id)
        ]


@pytest.mark.parametrize(
    "point",
    [
        "pipeline.after_insert",
        "runner.after_timeout",
        "runner.after_bind",
        "supervisor.after_mkdir",
        "supervisor.after_popen",
    ],
)
def test_parent_hard_exit_at_startup_reconciles_before_retry(cli_project, point):
    project = cli_project
    install_writer(project)
    code, output = project.run("run", "--pipeline_code", "P", fault=point + ":kill")
    assert code == 137, output
    # Advance abandoned leases rather than waiting a minute for each fault case.
    with project.engine.begin() as conn:
        past = datetime.now(UTC) - timedelta(seconds=1)
        conn.execute(text("UPDATE AUD_TASK_ATTEMPTS SET LEASE_EXPIRES_AT=:past"), {"past": past})
        conn.execute(
            text("UPDATE AUD_PIPELINES_RUN_LOG SET LEASE_EXPIRES_AT=:past"), {"past": past}
        )
    code, output = project.run("run", "--pipeline_code", "P")
    assert code == 0, output
    assert_complete(project)


@pytest.mark.parametrize("remote", [False, True])
def test_duplicate_task_delivery_executes_one_attempt(cli_project, remote):
    project = cli_project
    install_writer(project, hold=True)
    if remote:
        raw = yaml.safe_load(project.config.config_path.read_text())
        raw["Orchestration"]["Mode"] = "remote"
        project.config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    identity = ("--run-key", "delivery")
    assert project.run("run", "--pipeline_code", "P", "--init-only", *identity)[0] == 0
    arguments = ("run", "--pipeline_code", "P", "--task_code", "load", *identity)
    first = project.start(*arguments)
    try:
        project.wait_for("SELECT STATUS FROM AUD_TASK_ATTEMPTS", expected="RUNNING", timeout=30)
        second = project.start(*arguments)
        assert second.wait() == (ExitCode.STALE_TRANSITION if remote else ExitCode.SUCCESS), (
            second.output
        )
        assert "already IN-PROGRESS" in second.output
        with project.engine.connect() as conn:
            assert conn.execute(text("SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS")).scalar_one() == 1
        (project.config.project_dir / "release").touch()
        assert first.wait() == 0, first.output
        code, output = project.run("run", "--pipeline_code", "P", "--finalize-only", *identity)
        assert code == 0, output
        assert_complete(project)
    finally:
        (project.config.project_dir / "release").touch()
        first.close()


def test_late_remote_finalize_leaves_the_newer_run_untouched(cli_project):
    project = cli_project
    raw = yaml.safe_load(project.config.config_path.read_text())
    raw["Orchestration"]["Mode"] = "remote"
    project.config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    for step in (("--init-only",), ("--task_code", "load"), ("--finalize-only",)):
        code, output = project.run("run", "--pipeline_code", "P", "--run-key", "old", *step)
        assert code == 0, output
    assert project.run("run", "--pipeline_code", "P", "--run-key", "new", "--init-only")[0] == 0
    with project.engine.connect() as conn:
        before = conn.execute(
            text("SELECT * FROM AUD_PIPELINES_RUN_LOG ORDER BY PIPELINE_RUN_ID")
        ).all()
    code, output = project.run("run", "--pipeline_code", "P", "--run-key", "old", "--finalize-only")
    assert code == 9 and "expected IN-PROGRESS to finalize" in output
    with project.engine.connect() as conn:
        assert (
            conn.execute(text("SELECT * FROM AUD_PIPELINES_RUN_LOG ORDER BY PIPELINE_RUN_ID")).all()
            == before
        )
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS")).scalar_one() == 1


def test_three_pipeline_callers_waiting_on_a_repair_admit_one_supervisor(cli_project):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    from etl_craft.config import load_config
    from etl_craft.core.actor import current_actor
    from etl_craft.core.errors import RunStateError
    from etl_craft.engine import transitions as tr
    from etl_craft.engine.runlog import RunSelector
    from etl_craft.execution.gates import Clock
    from etl_craft.execution.pipeline import run_pipeline
    from fixtures.metadata import add_pipeline, add_pipeline_dependency, upstream_run

    project = cli_project
    install_writer(project, hold=True)
    with project.engine.begin() as conn:
        upstream = add_pipeline(conn, "UP")
        run, _ = upstream_run(conn, upstream, {})
        tr.reopen_run(conn, run, current_actor(), reason="repair while consumers wait")
        add_pipeline_dependency(conn, project.pipeline_id, upstream)
        downstream_run = tr.create_run(conn, project.pipeline_id, current_actor(), status="QUEUED")
    waiting = Barrier(4, timeout=30)

    def wait(seconds):
        if not (project.config.project_dir / "repaired").exists():
            waiting.wait()
            while not (project.config.project_dir / "repaired").exists():
                time.sleep(0.01)
        else:
            time.sleep(min(seconds, 0.01))

    def execute():
        try:
            return run_pipeline(
                project.engine,
                load_config(project.config.config_path),
                "P",
                clock=Clock(sleep=wait),
                selector=RunSelector(run_id=downstream_run),
            )
        except RunStateError as error:
            return error

    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = [pool.submit(execute) for _ in range(3)]
        try:
            waiting.wait()
            with project.engine.begin() as conn:
                tr.finish_run(conn, run, "SUCCESS", current_actor())
            (project.config.project_dir / "repaired").touch()
            project.wait_for("SELECT STATUS FROM AUD_TASK_ATTEMPTS", expected="RUNNING", timeout=30)
            deadline = time.monotonic() + 30
            while sum(f.done() for f in futures) < 2 and time.monotonic() < deadline:
                time.sleep(0.02)
            assert sum(f.done() for f in futures) == 2
            with project.engine.connect() as conn:
                assert (
                    conn.execute(
                        text("SELECT COUNT(*) FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_ID=:id"),
                        {"id": project.pipeline_id},
                    ).scalar_one()
                    == 1
                )
                assert (
                    conn.execute(text("SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS")).scalar_one() == 1
                )
            (project.config.project_dir / "release").touch()
            outcomes = [f.result(timeout=30) for f in futures]
            assert sum(isinstance(result, RunStateError) for result in outcomes) == 2
            assert [r.status for r in outcomes if not isinstance(r, RunStateError)] == ["SUCCESS"]
            with project.engine.connect() as conn:
                assert conn.execute(
                    text("SELECT CONSUMED_REVISION FROM AUD_DEPENDENCY_CONSUMPTION")
                ).scalars().all() == [2]
                assert conn.execute(
                    text("SELECT STATUS FROM AUD_PIPELINES_RUN_LOG ORDER BY PIPELINE_RUN_ID")
                ).scalars().all() == ["SUCCESS", "SUCCESS"]
            with (
                open_warehouse(project.config, project.engine) as warehouse,
                warehouse.connect() as conn,
            ):
                assert conn.execute(text("SELECT COUNT(*) FROM proof")).scalar_one() == 1
        finally:
            (project.config.project_dir / "repaired").touch()
            (project.config.project_dir / "release").touch()


@pytest.mark.parametrize(
    "empty_engine_db",
    [pytest.param("postgresql", marks=pytest.mark.engine_postgres)],
    indirect=True,
)
def test_postgres_outage_fences_the_supervisor_and_recovers(cli_project, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor

    import psycopg

    from etl_craft.config import load_config
    from etl_craft.execution import leases
    from etl_craft.execution.pipeline import run_pipeline
    from fixtures.services import POSTGRES_PASSWORD, POSTGRES_USER, require

    project = cli_project
    install_writer(project, hold=True)
    monkeypatch.setattr(leases, "HEARTBEAT_SECONDS", 0.1)
    service = require("postgres")
    database = project.engine.url.database

    def execute():
        try:
            return run_pipeline(project.engine, load_config(project.config.config_path), "P")
        except Exception as error:
            return error

    with (
        psycopg.connect(
            host=service.host,
            port=service.port,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            dbname="postgres",
            autocommit=True,
        ) as admin,
        ThreadPoolExecutor(max_workers=1) as pool,
    ):
        future = pool.submit(execute)
        try:
            project.wait_for("SELECT STATUS FROM AUD_TASK_ATTEMPTS", expected="RUNNING", timeout=30)
            admin.execute(
                psycopg.sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS false").format(
                    psycopg.sql.Identifier(database)
                )
            )
            terminated = admin.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=%s",
                (database,),
            ).fetchall()
            assert terminated and all(row[0] for row in terminated)
            time.sleep(5)
        finally:
            admin.execute(
                psycopg.sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS true").format(
                    psycopg.sql.Identifier(database)
                )
            )
            (project.config.project_dir / "release").touch()
        outcome = future.result(timeout=30)
        assert isinstance(outcome, Exception), outcome
    project.engine.dispose()
    with project.engine.begin() as conn:
        past = datetime.now(UTC) - timedelta(seconds=1)
        conn.execute(text("UPDATE AUD_TASK_ATTEMPTS SET LEASE_EXPIRES_AT=:past"), {"past": past})
        conn.execute(
            text("UPDATE AUD_PIPELINES_RUN_LOG SET LEASE_EXPIRES_AT=:past"), {"past": past}
        )
    code, output = project.run("run", "--pipeline_code", "P")
    assert code == 0, output
    assert_complete(project)
