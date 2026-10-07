"""Leadership, CLI admission and bounded recovery of the real local server."""

import signal
import sys
import time
from datetime import UTC, datetime, timedelta

import pytest
import yaml
from sqlalchemy import text

from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.errors import RunRefusedError
from etl_craft.engine.repository import overseers
from etl_craft.overseer.leadership import leadership
from etl_craft.overseer.server import WorkingSet
from etl_craft.services.operations import OperationContext
from fixtures.metadata import add_pipeline, add_task, start_run


def test_only_one_leader_and_session_release_not_history_controls_restart(engine_db):
    engine = engine_db.engine
    with leadership(engine):
        first = overseers.start(engine)
        with (
            pytest.raises(RunRefusedError, match=f"overseer {first} on .* is active"),
            leadership(engine),
        ):
            pytest.fail("second leader entered")
    # An unclosed history row cannot prevent the replacement from owning the session lock.
    with leadership(engine):
        second = overseers.start(engine)
        assert second > first
        overseers.heartbeat(engine, second, stopped=True)
    with engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT STOPPED_AT FROM AUD_OVERSEERS WHERE OVERSEER_ID=:id"), {"id": first}
            ).scalar_one()
            is None
        )


def test_working_set_retains_only_active_graphs_and_refreshes_metadata(engine_db):
    ctx = OperationContext(engine_db.engine, engine_db.config, SYSTEM_ACTOR)
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
    server = p.start("server")
    p.wait_for("SELECT COUNT(*) FROM AUD_OVERSEERS", expected=1)
    code, output = p.run("server")
    assert code != 0 and "overseer 1 on" in output and "is active" in output
    code, output = p.run("run", "--pipeline_code", "P", "--init-only", "--run-key", "server:one")
    assert code == 0, output
    p.wait_for(
        "SELECT STATUS FROM AUD_PIPELINES_RUN_LOG WHERE RUN_KEY='server:one'", expected="SUCCESS"
    )
    server.signal(signal.SIGTERM)
    assert server.wait() == 0, server.output
    p.wait_for("SELECT COUNT(*) FROM AUD_OVERSEERS WHERE STOPPED_AT IS NOT NULL", expected=1)
    with p.engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT COUNT(*) FROM AUD_ACTIONS WHERE COMMAND='server'")
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
    code, output = p.run("run", "--pipeline_code", "P", "--init-only", "--run-key", "shutdown")
    assert code == 0, output
    server = p.start("server")
    p.wait_for("SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS WHERE STATUS='RUNNING'", expected=1)
    p.wait_for("SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS WHERE PID IS NOT NULL", expected=1)
    wait_for_script(p)
    server.signal(signal.SIGTERM)
    assert server.wait() == 0, server.output
    p.wait_for("SELECT STATUS FROM AUD_PIPELINES_RUN_LOG", expected="IN-PROGRESS")
    p.wait_for("SELECT OWNER_ID FROM AUD_PIPELINES_RUN_LOG", expected=None)
    replacement = p.start("server")
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
    assert p.run("run", "--pipeline_code", "P", "--init-only", "--run-key", "recover")[0] == 0
    server = p.start("server")
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
    replacement = p.start("server")
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
    assert p.run("pause", "--pipeline_code", "UPSTREAM", "--reason", "maintenance")[0] == 0
    assert p.run("run", "--pipeline_code", "P", "--init-only", "--run-key", "gate")[0] == 0
    server = p.start("server")
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
    assert p.run("run", "--pipeline_code", "P", "--init-only", "--run-key", "cancel")[0] == 0
    server = p.start("server")
    wait_for_script(p)
    code, output = p.run(
        "cancel", "--pipeline_code", "P", "--run-key", "cancel", "--reason", "stop"
    )
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
