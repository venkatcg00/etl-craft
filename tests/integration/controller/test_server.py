"""Leadership, CLI admission and bounded recovery of the real local server."""

import signal
import sys
import threading
import time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
import yaml
from sqlalchemy import text

from etl_craft.controller.leadership import leadership
from etl_craft.controller.supervisor import WorkingSet
from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.enums import ApiRole
from etl_craft.core.errors import RunRefusedError
from etl_craft.engine.repository import controllers
from etl_craft.services.operations import OperationContext
from fixtures.engine_db import postgres_admin
from fixtures.metadata import add_pipeline, add_task, start_run


def test_only_one_leader_and_session_release_not_history_controls_restart(engine_db):
    engine = engine_db.engine
    with leadership(engine):
        first = controllers.start(engine)
        with (
            pytest.raises(RunRefusedError, match=f"controller {first} on .* is active"),
            leadership(engine),
        ):
            pytest.fail("second leader entered")
    # An unclosed history row cannot prevent the replacement from owning the session lock.
    with leadership(engine):
        second = controllers.start(engine)
        assert second > first
        controllers.heartbeat(engine, second, stopped=True)
    with engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT STOPPED_AT FROM AUD_CONTROLLERS WHERE CONTROLLER_ID=:id"),
                {"id": first},
            ).scalar_one()
            is None
        )


def test_working_set_retains_only_active_graphs_and_refreshes_metadata(engine_db):
    ctx = OperationContext(engine_db.engine, engine_db.config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
    with ctx.engine.begin() as conn:
        first = add_pipeline(conn, "P")
        second = add_pipeline(conn, "HISTORY")
        run = start_run(conn, first)
        old = start_run(conn, second)
        conn.execute(
            text(
                "UPDATE AUD_PIPELINES_RUN_LOG SET STATUS='SUCCESS', END_DATE=:now "
                "WHERE PIPELINE_RUN_ID=:id"
            ),
            {"now": datetime.now(UTC), "id": old},
        )
    working = WorkingSet()
    assert [r.pipeline_run_id for r in working.refresh(ctx)] == [run]
    assert set(working.graphs) == {first}
    version = working.graphs[first][0]
    with ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_PIPELINES SET PIPELINE_NAME='updated' WHERE PIPELINE_ID=:id"),
            {"id": first},
        )
    working.refresh(ctx)
    assert working.graphs[first][0] != version
    with ctx.engine.begin() as conn:
        task = add_task(conn, first, "temporary", "PYTHON")
    working.refresh(ctx)
    version = working.graphs[first][0]
    with ctx.engine.begin() as conn:
        conn.execute(text("DELETE FROM CFG_TASKS WHERE TASK_ID=:id"), {"id": task})
    working.refresh(ctx)
    assert working.graphs[first][0] != version
    assert working.graphs[first][1].tasks == []
    with ctx.engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE AUD_PIPELINES_RUN_LOG SET STATUS='SUCCESS', END_DATE=:now "
                "WHERE PIPELINE_RUN_ID=:id"
            ),
            {"now": datetime.now(UTC), "id": run},
        )
    assert working.refresh(ctx) == []
    assert working.graphs == {}


def test_server_picks_up_cli_run_and_second_server_is_refused(cli_project):
    p = cli_project
    server = p.start("controller")
    p.wait_for("SELECT COUNT(*) FROM AUD_CONTROLLERS", expected=1)
    code, output = p.run("controller")
    assert code != 0 and "controller 1 on" in output and "is active" in output
    code, output = p.run("run", "--pipeline-code", "P", "--init-only", "--run-key", "server:one")
    assert code == 0, output
    p.wait_for(
        "SELECT STATUS FROM AUD_PIPELINES_RUN_LOG WHERE RUN_KEY='server:one'", expected="SUCCESS"
    )
    server.signal(signal.SIGTERM)
    assert server.wait() == 0, server.output
    p.wait_for("SELECT COUNT(*) FROM AUD_CONTROLLERS WHERE STOPPED_AT IS NOT NULL", expected=1)
    with p.engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT COUNT(*) FROM AUD_ACTIONS WHERE COMMAND='controller'")
            ).scalar_one()
            == 2
        )
        assert (
            conn.execute(
                text("SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS WHERE STATUS='SUCCESS'")
            ).scalar_one()
            == 1
        )


def blocking_script(project):
    raw = yaml.safe_load(project.config.config_path.read_text())
    raw["Orchestration"]["Shutdown_grace_seconds"] = 0
    project.config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    (project.config.ingestion_scripts_dir / "load.py").write_text(
        "from pathlib import Path\nimport time\nfrom etl_craft.scripting import ScriptResult\n"
        "def run(task):\n    marker=Path('server-first-attempt')\n"
        "    if not marker.exists():\n        marker.write_text('started')\n"
        "        time.sleep(120)\n"
        "    return ScriptResult(1)\n"
    )


def wait_for_script(project):
    deadline = time.monotonic() + 10
    marker = project.config.project_dir / "server-first-attempt"
    while not marker.exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert marker.exists(), "child never reached its scripted blocking point"


def test_shutdown_stops_tasks_and_leaves_exact_run_resumable(cli_project):
    p = cli_project
    blocking_script(p)
    code, output = p.run("run", "--pipeline-code", "P", "--init-only", "--run-key", "shutdown")
    assert code == 0, output
    server = p.start("controller")
    p.wait_for("SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS WHERE STATUS='RUNNING'", expected=1)
    p.wait_for("SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS WHERE PID IS NOT NULL", expected=1)
    wait_for_script(p)
    server.signal(signal.SIGTERM)
    assert server.wait() == 0, server.output
    p.wait_for("SELECT STATUS FROM AUD_PIPELINES_RUN_LOG", expected="IN-PROGRESS")
    p.wait_for("SELECT OWNER_ID FROM AUD_PIPELINES_RUN_LOG", expected=None)
    replacement = p.start("controller")
    p.wait_for("SELECT STATUS FROM AUD_PIPELINES_RUN_LOG", expected="SUCCESS")
    replacement.signal(signal.SIGTERM)
    assert replacement.wait() == 0, replacement.output
    with p.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_PIPELINES_RUN_LOG")).scalar_one() == 1
        assert conn.execute(text("SELECT ATTEMPT_COUNT FROM AUD_TASK_RUN_LOG")).scalar_one() == 2


@pytest.mark.chaos
@pytest.mark.skipif(sys.platform != "linux", reason="process birth and descendant checks use /proc")
def test_killed_server_reconciles_before_retrying_exact_task_run(cli_project):
    p = cli_project
    blocking_script(p)
    assert p.run("run", "--pipeline-code", "P", "--init-only", "--run-key", "recover")[0] == 0
    server = p.start("controller")
    p.wait_for("SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS WHERE STATUS='RUNNING'", expected=1)
    wait_for_script(p)
    server.descendants()
    server.signal(signal.SIGKILL)
    assert server.wait() == -signal.SIGKILL
    with p.engine.begin() as conn:
        expired = datetime.now(UTC) - timedelta(seconds=1)
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET LEASE_EXPIRES_AT=:expired WHERE STATUS='RUNNING'"),
            {"expired": expired},
        )
        conn.execute(
            text("UPDATE AUD_PIPELINES_RUN_LOG SET LEASE_EXPIRES_AT=:expired"), {"expired": expired}
        )
    replacement = p.start("controller")
    p.wait_for("SELECT STATUS FROM AUD_PIPELINES_RUN_LOG", expected="SUCCESS", timeout=20)
    replacement.signal(signal.SIGTERM)
    assert replacement.wait() == 0, replacement.output
    with p.engine.connect() as conn:
        attempts = conn.execute(
            text(
                "SELECT STATUS AS status, TASK_RUN_ID AS task_run_id "
                "FROM AUD_TASK_ATTEMPTS ORDER BY ATTEMPT_NUMBER"
            )
        ).all()
        assert [a.status for a in attempts] == ["LOST", "SUCCESS"]
        assert attempts[0].task_run_id == attempts[1].task_run_id
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_PIPELINES_RUN_LOG")).scalar_one() == 1


@pytest.mark.engine_postgres
def test_execution_notifications_are_visible_only_after_commit(postgres_database):
    from fixtures.engine_db import apply_schema

    engine = postgres_database.engine
    apply_schema(engine)
    with leadership(engine) as leader:
        driver = leader.connection.connection.driver_connection
        with engine.begin() as writer:
            pipeline = add_pipeline(writer, "NOTIFY")
            start_run(writer, pipeline)
            assert list(driver.notifies(timeout=0.02)) == []
        notifications = list(driver.notifies(timeout=0.2, stop_after=1))
        assert len(notifications) == 1
        assert notifications[0].channel == "etl_craft_events"
        assert notifications[0].payload == "public"


def test_shutdown_interrupts_gate_wait_without_starting_a_task(cli_project):
    from fixtures.metadata import add_dependency, task_run

    p = cli_project
    raw = yaml.safe_load(p.config.config_path.read_text())
    raw["Orchestration"]["Shutdown_grace_seconds"] = 0
    p.config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    with p.engine.begin() as conn:
        upstream = add_pipeline(conn, "UPSTREAM")
        task = add_task(conn, upstream, "waiting", "PYTHON")
        upstream_run = start_run(conn, upstream)
        task_run(conn, task, upstream_run, status="IN-PROGRESS")
        add_dependency(conn, p.pipeline_id, p.task_id, task, upstream_pipeline=upstream)
    assert p.run("pause", "--pipeline-code", "UPSTREAM", "--reason", "maintenance")[0] == 0
    assert p.run("run", "--pipeline-code", "P", "--init-only", "--run-key", "gate")[0] == 0
    server = p.start("controller")
    p.wait_for("SELECT COUNT(*) FROM AUD_GATE_WAITS WHERE NEXT_CHECK_AT IS NOT NULL", expected=1)
    started = time.monotonic()
    server.signal(signal.SIGTERM)
    assert server.wait(timeout=5) == 0, server.output
    assert time.monotonic() - started < 5
    with p.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS")).scalar_one() == 0
        assert (
            conn.execute(
                text("SELECT STATUS FROM AUD_PIPELINES_RUN_LOG WHERE RUN_KEY='gate'")
            ).scalar_one()
            == "IN-PROGRESS"
        )


def test_operator_cancellation_takes_precedence_over_shutdown_resume(cli_project):
    from fixtures.metadata import add_dependency

    p = cli_project
    blocking_script(p)
    with p.engine.begin() as conn:
        after = add_task(conn, p.pipeline_id, "after", "PYTHON", SCRIPT_NAME="load.py")
        add_dependency(conn, p.pipeline_id, after, p.task_id)
    assert p.run("run", "--pipeline-code", "P", "--init-only", "--run-key", "cancel")[0] == 0
    server = p.start("controller")
    wait_for_script(p)
    code, output = p.run("abort", "--pipeline-code", "P", "--run-key", "cancel", "--reason", "stop")
    assert code == 0, output
    p.wait_for("SELECT OWNER_ID FROM AUD_PIPELINES_RUN_LOG", expected=None)
    with p.engine.connect() as conn:
        assert (
            conn.execute(text("SELECT STATUS FROM AUD_PIPELINES_RUN_LOG")).scalar_one()
            == "CANCELLED"
        )
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_TASK_RUN_LOG")).scalar_one() == 1
    server.signal(signal.SIGTERM)
    assert server.wait() == 0, server.output
    assert "P: pipeline_run_id=1 CANCELLED" in server.output


POSTGRES_ONLY = pytest.mark.parametrize(
    "empty_engine_db",
    [pytest.param("postgresql", marks=pytest.mark.engine_postgres)],
    indirect=True,
)


@contextmanager
def engine_db_cut_off(engine):
    """Refuse new connections to this test's database and end its sessions, as a restart does."""
    name = psycopg.sql.Identifier(engine.url.database)
    with postgres_admin() as admin:
        admin.execute(psycopg.sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS false").format(name))
        admin.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()",
            (engine.url.database,),
        )
        try:
            yield
        finally:
            admin.execute(psycopg.sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS true").format(name))


def wait_for_output(command, text_, timeout=15):
    deadline = time.monotonic() + timeout
    while text_ not in command.output:
        assert time.monotonic() < deadline, f"{text_!r} never appeared:\n{command.output}"
        time.sleep(0.05)


def released_script(project):
    (project.config.ingestion_scripts_dir / "load.py").write_text(
        "from pathlib import Path\nimport time\nfrom etl_craft.scripting import ScriptResult\n"
        "def run(task):\n    Path('started').write_text('started')\n"
        "    while not Path('release').exists():\n        time.sleep(0.05)\n"
        "    return ScriptResult(1)\n"
    )


@POSTGRES_ONLY
def test_server_and_its_attempt_ride_out_an_engine_db_outage(cli_project):
    p = cli_project
    released_script(p)
    assert p.run("run", "--pipeline-code", "P", "--init-only", "--run-key", "outage")[0] == 0
    server = p.start("controller")
    started = p.config.project_dir / "started"
    deadline = time.monotonic() + 15
    while not started.exists():
        assert time.monotonic() < deadline, server.output
        time.sleep(0.02)
    with engine_db_cut_off(p.engine):
        wait_for_output(server, "the Engine DB is unavailable")
        (p.config.project_dir / "release").write_text("go")
    p.wait_for("SELECT STATUS FROM AUD_PIPELINES_RUN_LOG", expected="SUCCESS", timeout=30)
    assert server.process.poll() is None, server.output
    wait_for_output(server, "the Engine DB answers again")
    with p.engine.connect() as conn:
        statuses = conn.execute(text("SELECT STATUS FROM AUD_TASK_ATTEMPTS")).scalars().all()
    assert statuses == ["SUCCESS"]
    server.signal(signal.SIGTERM)
    assert server.wait() == 0, server.output


@POSTGRES_ONLY
def test_an_outage_longer_than_its_budget_stops_the_server_naming_the_setting(cli_project):
    p = cli_project
    raw = yaml.safe_load(p.config.config_path.read_text())
    raw["Orchestration"]["Engine_db_outage_seconds"] = 2
    p.config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    server = p.start("controller")
    p.wait_for("SELECT COUNT(*) FROM AUD_CONTROLLERS", expected=1)
    with engine_db_cut_off(p.engine):
        code = server.wait(timeout=60)
    assert code != 0
    assert "Orchestration.Engine_db_outage_seconds allows 2s" in server.output


@POSTGRES_ONLY
def test_a_leader_whose_connection_dropped_takes_its_lock_again(engine_db):
    engine = engine_db.engine
    with leadership(engine) as leader:
        before = leader.connection.connection.driver_connection.info.backend_pid
        with postgres_admin() as admin:
            admin.execute("SELECT pg_terminate_backend(%s)", (before,))
        leader.wait(threading.Event())
        assert leader.connection.connection.driver_connection.info.backend_pid != before
        with (
            pytest.raises(RunRefusedError, match="stop it before starting a controller"),
            leadership(engine),
        ):
            pytest.fail("second leader entered")


@POSTGRES_ONLY
def test_a_leader_whose_lock_was_taken_meanwhile_stops(engine_db):
    engine = engine_db.engine
    with leadership(engine) as leader:
        pid = leader.connection.connection.driver_connection.info.backend_pid
        with postgres_admin() as admin:
            admin.execute("SELECT pg_terminate_backend(%s)", (pid,))
        with (
            leadership(engine),
            pytest.raises(RunRefusedError, match="took over while this controller's Engine DB"),
        ):
            leader.wait(threading.Event())
