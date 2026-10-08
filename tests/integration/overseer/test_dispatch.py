"""Ready-task progress, worker-free waits and durable admission budgets."""

import signal
import threading
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from etl_craft.core.graph import build_graph
from etl_craft.engine.repository.dependencies import fetch_pipeline_graph
from etl_craft.engine.repository.tasks import fetch_task_codes
from etl_craft.execution.gates import Clock, check_gate
from etl_craft.execution.runner import ChildOptions
from etl_craft.execution.scheduler import Scheduler
from fixtures.metadata import (
    add_dependency,
    add_pipeline,
    add_pipeline_dependency,
    add_task,
    start_run,
    task_run,
)


@pytest.mark.parametrize("server_mode", [False, True])
def test_fast_branch_starts_its_downstream_before_the_slow_branch_finishes(
    cli_project, server_mode
):
    p = cli_project
    scripts = p.config.ingestion_scripts_dir
    (scripts / "slow.py").write_text(
        "from pathlib import Path\nimport time\nfrom etl_craft.scripting import ScriptResult\n"
        "def run(task):\n    Path('slow-start').touch()\n"
        "    deadline=time.monotonic()+8\n"
        "    while time.monotonic()<deadline:\n"
        "        if Path('fast-downstream').exists():\n            return ScriptResult(1)\n"
        "        time.sleep(.05)\n"
        "    raise RuntimeError('ready downstream was held behind the slow task')\n"
    )
    (scripts / "load.py").write_text(
        "from pathlib import Path\nimport time\nfrom etl_craft.scripting import ScriptResult\n"
        "def run(task):\n    deadline=time.monotonic()+5\n"
        "    while not Path('slow-start').exists() and time.monotonic()<deadline:\n"
        "        time.sleep(.05)\n    return ScriptResult(1)\n"
    )
    (scripts / "after.py").write_text(
        "from pathlib import Path\nfrom etl_craft.scripting import ScriptResult\n"
        "def run(task):\n    Path('fast-downstream').touch()\n    return ScriptResult(1)\n"
    )
    with p.engine.begin() as conn:
        add_task(conn, p.pipeline_id, "slow", "PYTHON", SCRIPT_NAME="slow.py")
        following = add_task(conn, p.pipeline_id, "after_fast", "PYTHON", SCRIPT_NAME="after.py")
        add_dependency(conn, p.pipeline_id, following, p.task_id)
    if server_mode:
        assert p.run("run", "--pipeline_code", "P", "--init-only")[0] == 0
        server = p.start("server")
        p.wait_for("SELECT STATUS FROM AUD_PIPELINES_RUN_LOG", expected="SUCCESS", timeout=20)
        server.signal(signal.SIGTERM)
        assert server.wait() == 0, server.output
    else:
        code, output = p.run("run", "--pipeline_code", "P")
        assert code == 0, output
    with p.engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS WHERE STATUS='SUCCESS'")
            ).scalar_one()
            == 3
        )


def test_one_hundred_waiting_tasks_create_no_worker_jobs_and_resume_the_same_budget(engine_db):
    db = engine_db
    with db.engine.begin() as conn:
        upstream = add_pipeline(conn, "UP")
        up_task = add_task(conn, upstream, "produce")
        up_run = start_run(conn, upstream)
        task_run(conn, up_task, up_run, status="IN-PROGRESS")
        pipeline = add_pipeline(conn, "DOWN")
        run_id = start_run(conn, pipeline)
        for number in range(100):
            task = add_task(conn, pipeline, f"wait_{number}")
            add_dependency(conn, pipeline, task, up_task, upstream_pipeline=upstream)
        data = fetch_pipeline_graph(conn, pipeline)
        codes = fetch_task_codes(conn, pipeline)
    now = datetime.now(UTC)
    clock = Clock(now=lambda: now, sleep=lambda _: pytest.fail("gate slept"))
    config = replace(db.config, limits=replace(db.config.limits, max_parallel_tasks=1))
    workers = [t for t in threading.enumerate() if t.name.startswith("etl-craft-task")]
    scheduler = Scheduler(
        db.engine,
        config,
        "DOWN",
        pipeline,
        run_id,
        codes,
        build_graph(data.tasks, data.same_pipeline_edges),
        force=False,
        clock=clock,
        child=ChildOptions(),
        pool=None,
        paused=lambda: False,
        settle=lambda _: [],
    )
    try:
        assert not scheduler.step()
        assert scheduler.jobs == {}
        assert scheduler.attempted == set()
        assert [t for t in threading.enumerate() if t.name.startswith("etl-craft-task")] == workers
    finally:
        scheduler.close()
    with db.engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT TASK_ID AS task_id, FIRST_CHECK_AT AS first_check_at, "
                "NEXT_CHECK_AT AS next_check_at, WAIT_UNTIL AS wait_until, LOOKS AS looks "
                "FROM AUD_GATE_WAITS ORDER BY TASK_ID"
            )
        ).all()
        assert len(rows) == 100
        assert all(r.looks == 0 for r in rows)
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS")).scalar_one() == 0
    resumed = check_gate(
        db.engine,
        run_id,
        pipeline,
        task_id=rows[0].task_id,
        needed=1,
        clock=Clock(now=lambda: now + timedelta(seconds=1)),
        wait_seconds=9999,
    )
    assert resumed.state == "WAIT"
    with db.engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT FIRST_CHECK_AT AS first_check_at, WAIT_UNTIL AS wait_until, LOOKS AS looks "
                "FROM AUD_GATE_WAITS WHERE TASK_ID=:id"
            ),
            {"id": rows[0].task_id},
        ).one()
    assert (row.first_check_at, row.wait_until, row.looks) == (
        rows[0].first_check_at,
        rows[0].wait_until,
        0,
    )


def test_pipeline_gate_wait_is_persisted_before_admission_and_resumes_after_restart(cli_project):
    p = cli_project
    with p.engine.begin() as conn:
        upstream = add_pipeline(conn, "UP")
        start_run(conn, upstream)
        add_pipeline_dependency(conn, p.pipeline_id, upstream)
        # Keep the upstream running without the local server trying to finalize it.
        conn.execute(
            text("UPDATE CFG_PIPELINES SET ACTIVE_FLAG='N' WHERE PIPELINE_ID=:id"), {"id": upstream}
        )
        queued = start_run(conn, p.pipeline_id)
        conn.execute(
            text("UPDATE AUD_PIPELINES_RUN_LOG SET STATUS='QUEUED' WHERE PIPELINE_RUN_ID=:id"),
            {"id": queued},
        )
    server = p.start("server")
    p.wait_for("SELECT COUNT(*) FROM AUD_GATE_WAITS WHERE TASK_ID IS NULL", expected=1)
    server.signal(signal.SIGTERM)
    assert server.wait() == 0, server.output
    with p.engine.connect() as conn:
        before = conn.execute(
            text("SELECT FIRST_CHECK_AT, WAIT_UNTIL, LOOKS FROM AUD_GATE_WAITS")
        ).one()
    server = p.start("server")
    p.wait_for("SELECT COUNT(*) FROM AUD_OVERSEERS WHERE STOPPED_AT IS NULL", expected=1)
    server.signal(signal.SIGTERM)
    assert server.wait() == 0, server.output
    with p.engine.connect() as conn:
        assert (
            conn.execute(text("SELECT FIRST_CHECK_AT, WAIT_UNTIL, LOOKS FROM AUD_GATE_WAITS")).one()
            == before
        )
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS")).scalar_one() == 0


@pytest.mark.parametrize("wait_seconds,expected_looks", [(3600, 30), (20, 1)])
def test_persisted_gate_looks_follow_the_average_and_stop_at_the_shared_budget(
    engine_db, wait_seconds, expected_looks
):
    from etl_craft.core.time import as_utc
    from etl_craft.engine.repository.trackers import fetch_latest_pipeline_run

    db = engine_db
    with db.engine.begin() as conn:
        upstream = add_pipeline(conn, "UP")
        start_run(conn, upstream)
        down = add_pipeline(conn, "DOWN")
        run_id = start_run(conn, down)
        add_pipeline_dependency(conn, down, upstream)
        latest = fetch_latest_pipeline_run(conn, upstream)
    start = as_utc(latest.start_date)
    now = start + timedelta(seconds=10)
    looks = []
    for _ in range(32):
        result = check_gate(
            db.engine,
            run_id,
            down,
            clock=Clock(now=lambda instant=now: instant),
            wait_seconds=wait_seconds,
        )
        if result.state != "WAIT":
            break
        looks.append(result.next_check_at)
        now = result.next_check_at
    assert result.state == "UNSATISFIED"
    if wait_seconds == 3600:
        assert looks[:2] == [start + timedelta(seconds=210), start + timedelta(seconds=240)]
    with db.engine.connect() as conn:
        row = conn.execute(
            text("SELECT LOOKS AS looks, NEXT_CHECK_AT AS next_check_at FROM AUD_GATE_WAITS")
        ).one()
    assert row.looks == expected_looks
    assert row.next_check_at is None


def test_cancelled_pipeline_admission_stops_waiting_without_starting_a_worker(cli_project):
    p = cli_project
    with p.engine.begin() as conn:
        upstream = add_pipeline(conn, "UP")
        start_run(conn, upstream)
        add_pipeline_dependency(conn, p.pipeline_id, upstream)
        conn.execute(
            text("UPDATE CFG_PIPELINES SET ACTIVE_FLAG='N' WHERE PIPELINE_ID=:id"), {"id": upstream}
        )
        run_id = start_run(conn, p.pipeline_id)
        conn.execute(
            text("UPDATE AUD_PIPELINES_RUN_LOG SET STATUS='QUEUED' WHERE PIPELINE_RUN_ID=:id"),
            {"id": run_id},
        )
    server = p.start("server")
    p.wait_for("SELECT COUNT(*) FROM AUD_GATE_WAITS WHERE TASK_ID IS NULL", expected=1)
    code, output = p.run(
        "cancel",
        "--pipeline_code",
        "P",
        "--run-id",
        str(run_id),
        "--reason",
        "cancel pending admission",
    )
    assert code == 0, output
    server.signal(signal.SIGTERM)
    assert server.wait(timeout=5) == 0, server.output
    with p.engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT STATUS FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID=:id"),
                {"id": run_id},
            ).scalar_one()
            == "CANCELLED"
        )
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS")).scalar_one() == 0


def test_later_queued_or_skipped_ticks_do_not_hide_the_upstreams_active_run(engine_db):
    from etl_craft.core.actor import SYSTEM_ACTOR
    from etl_craft.engine import transitions

    db = engine_db
    with db.engine.begin() as conn:
        up = add_pipeline(conn, "UP")
        start_run(conn, up)
        for status in ("QUEUED", "SKIPPED"):
            transitions.create_run(conn, up, SYSTEM_ACTOR, trigger_kind="SCHEDULE", status=status)
        down = add_pipeline(conn, "DOWN")
        run_id = start_run(conn, down)
        add_pipeline_dependency(conn, down, up)
    result = check_gate(db.engine, run_id, down)
    assert result.state == "WAIT"


def test_shutdown_cancels_all_runs_before_joining_the_shared_pool(cli_project):
    import time

    import yaml

    p = cli_project
    raw = yaml.safe_load(p.config.config_path.read_text())
    raw["Orchestration"].update(Max_parallel_tasks=1, Shutdown_grace_seconds=0)
    p.config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    (p.config.ingestion_scripts_dir / "block.py").write_text(
        "from pathlib import Path\nimport time\n"
        "def run(task):\n    Path('other-run-started').touch()\n    time.sleep(120)\n"
    )
    with p.engine.begin() as conn:
        later = add_task(conn, p.pipeline_id, "after_load", "PYTHON", SCRIPT_NAME="load.py")
        add_dependency(conn, p.pipeline_id, later, p.task_id)
        other = add_pipeline(conn, "OTHER")
        add_task(conn, other, "block", "PYTHON", SCRIPT_NAME="block.py")
    assert p.run("run", "--pipeline_code", "P", "--init-only")[0] == 0
    assert p.run("run", "--pipeline_code", "OTHER", "--init-only")[0] == 0
    server = p.start("server")
    deadline = time.monotonic() + 15
    while not (p.config.project_dir / "other-run-started").exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    assert (p.config.project_dir / "other-run-started").exists(), server.output
    # Let the completed first branch queue its downstream behind the other run's child.
    time.sleep(1.2)
    server.signal(signal.SIGTERM)
    assert server.wait(timeout=5) == 0, server.output
