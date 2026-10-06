"""Live ownership, expired supervisors and fenced recovery on both Engine DBs."""

import os
import signal
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from etl_craft.core.actor import current_actor
from etl_craft.core.errors import RunStateError, StaleTransitionError
from etl_craft.engine import runlog
from etl_craft.engine import transitions as tr
from etl_craft.execution import leases
from etl_craft.execution.reconcile import reconcile, stop_process
from fixtures.metadata import add_pipeline, add_task


def scene(engine):
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "T")
        run = tr.create_run(conn, pipeline, current_actor())
        summary = tr.create_task_run(conn, task, run, current_actor())
        attempt = tr.queue_attempt(conn, summary, current_actor())
        owner = leases.owner_id()
        tr.claim_attempt(
            conn,
            attempt,
            current_actor(),
            owner=owner,
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
        )
        tr.start_attempt(
            conn,
            attempt,
            current_actor(),
            owner=owner,
            host=socket.gethostname(),
            pid=99999999,
            process_start="dead",
        )
    return pipeline, run, summary, attempt, owner


def expire(engine, attempt, *, seconds=1, host=None):
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE AUD_TASK_ATTEMPTS SET LEASE_EXPIRES_AT=:expiry, HOST=:host "
                "WHERE ATTEMPT_ID=:attempt"
            ),
            {
                "expiry": datetime.now(UTC) - timedelta(seconds=seconds),
                "host": host or socket.gethostname(),
                "attempt": attempt,
            },
        )


@pytest.mark.chaos
def test_reconciliation_fences_dead_owner_and_retry(engine_db):
    engine = engine_db.engine
    pipeline, _, summary, attempt, owner = scene(engine)
    assert reconcile(engine).lost == []
    expire(engine, attempt)
    report = reconcile(engine, pipeline_id=pipeline)
    assert report.lost == [attempt]
    assert reconcile(engine).lost == []
    with engine.begin() as conn:
        result = runlog.fetch_task_run_result(conn, summary)
        assert result.status == "FAILED"
        assert "attempt 1 was lost" in result.error_message
        with pytest.raises(StaleTransitionError):
            tr.set_task_log(conn, summary, attempt, owner, "lost output")
        retry = tr.queue_attempt(conn, summary, current_actor())
        tr.claim_attempt(
            conn,
            retry,
            current_actor(),
            owner="new",
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
        )
        with pytest.raises(StaleTransitionError):
            tr.finish_attempt(
                conn,
                attempt,
                "SUCCESS",
                current_actor(),
                owner=owner,
                target_count=99,
                task_log="zombie",
            )
        result = runlog.fetch_task_run_result(conn, summary)
        assert result.status == "IN-PROGRESS"
        assert conn.execute(text("SELECT TARGET_COUNT FROM AUD_TASK_RUN_LOG")).scalar_one() is None
        assert result.attempt_count == 2
        assert conn.execute(text("SELECT TASK_LOG FROM AUD_TASK_RUN_LOG")).scalar_one() != "zombie"


def test_foreign_owner_gets_two_lease_periods_of_grace(engine_db):
    engine = engine_db.engine
    _, _, _, attempt, _ = scene(engine)
    expire(engine, attempt, host="foreign")
    assert reconcile(engine).lost == []
    expire(engine, attempt, seconds=121, host="foreign")
    assert reconcile(engine).lost == [attempt]


@pytest.mark.chaos
def test_expired_owner_cannot_finish_or_renew_before_reconciliation(engine_db):
    engine = engine_db.engine
    _, _, summary, attempt, owner = scene(engine)
    expire(engine, attempt)
    with engine.begin() as conn:
        for operation in ("finish", "renew", "timeout", "log"):
            with pytest.raises(StaleTransitionError):
                if operation == "finish":
                    tr.finish_attempt(conn, attempt, "SUCCESS", current_actor(), owner=owner)
                elif operation == "timeout":
                    tr.time_out_attempt(
                        conn, attempt, current_actor(), owner=owner, error_message="expired owner"
                    )
                elif operation == "log":
                    tr.set_task_log(conn, summary, attempt, owner, "expired output")
                else:
                    tr.renew_lease(
                        conn,
                        attempt,
                        current_actor(),
                        owner=owner,
                        lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
                    )


def test_run_supervisor_refuses_second_owner_and_releases_on_exit(engine_db):
    engine = engine_db.engine
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        run = tr.create_run(conn, pipeline, current_actor())
    with leases.supervise_run(engine, run):
        assert leases.run_owner(run) is not None
        with (
            pytest.raises(RunStateError, match=r"is supervised by.*lease until"),
            leases.supervise_run(engine, run),
        ):
            pytest.fail("second supervisor admitted")
    assert leases.run_owner(run) is None
    with engine.connect() as conn:
        assert conn.execute(text("SELECT OWNER_ID FROM AUD_PIPELINES_RUN_LOG")).scalar_one() is None


def test_expired_run_is_released_only_after_live_attempts_end(engine_db):
    engine = engine_db.engine
    _, run, _, attempt, _ = scene(engine)
    with engine.begin() as conn:
        tr.start_run(
            conn,
            run,
            current_actor(),
            owner="old",
            lease_expires_at=datetime.now(UTC) - timedelta(seconds=1),
        )
    assert reconcile(engine).released == []
    expire(engine, attempt)
    assert reconcile(engine).released == [run]
    with leases.supervise_run(engine, run):
        assert leases.run_owner(run) != "old"


def test_attempt_heartbeat_renews_and_cancels_on_fence(engine_db, monkeypatch):
    engine = engine_db.engine
    _, _, _, attempt, owner = scene(engine)
    monkeypatch.setattr(leases, "HEARTBEAT_SECONDS", 0.02)
    cancel = threading.Event()
    with engine.connect() as conn:
        original = conn.execute(text("SELECT HEARTBEAT_AT FROM AUD_TASK_ATTEMPTS")).scalar_one()
    with (
        pytest.raises(StaleTransitionError, match="lost its heartbeat"),
        leases.heartbeat(engine, "attempt", attempt, owner, cancel),
    ):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            with engine.connect() as conn:
                heartbeat = conn.execute(
                    text("SELECT HEARTBEAT_AT FROM AUD_TASK_ATTEMPTS")
                ).scalar_one()
            if heartbeat != original:
                break
            time.sleep(0.02)
        assert heartbeat != original
        expire(engine, attempt)
        assert cancel.wait(5)


@pytest.mark.unit
def test_reused_pid_is_never_signalled(monkeypatch):
    monkeypatch.setattr("etl_craft.execution.reconcile.process_start", lambda pid: "new-birth")
    monkeypatch.setattr(os, "kill", lambda *args: pytest.fail("reused pid signalled"))
    stop_process(os.getpid(), "old-birth", grace_seconds=0)


@pytest.mark.chaos
def test_parent_sigkill_reconciles_and_retries_real_child(cli_project):
    if sys.platform != "linux":
        pytest.skip("process-tree recovery requires Linux /proc")
    project = cli_project
    script = project.config.ingestion_scripts_dir / "load.py"
    script.write_text(
        "from pathlib import Path\nimport time\nfrom etl_craft.scripting import ScriptResult\n"
        "def run(task):\n    first = not Path('ready').exists()\n    Path('ready').touch()\n"
        "    while first:\n        time.sleep(0.05)\n"
        "    return ScriptResult(7)\n"
    )
    parent = project.start("run", "--pipeline_code", "P")
    project.wait_for("SELECT STATUS FROM AUD_TASK_ATTEMPTS", expected="RUNNING")
    with project.engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT ATTEMPT_ID, PID, PROCESS_START, OWNER_ID, TASK_RUN_ID "
                "FROM AUD_TASK_ATTEMPTS"
            )
        ).one()
    deadline = time.monotonic() + 10
    while not (project.config.project_dir / "ready").exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert (project.config.project_dir / "ready").exists()
    parent.descendants()
    parent.signal(signal.SIGKILL)
    assert parent.wait() == -signal.SIGKILL
    assert leases.process_start(row[1]) == row[2]
    expire(project.engine, row[0])
    with project.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_PIPELINES_RUN_LOG SET LEASE_EXPIRES_AT=:expiry"),
            {"expiry": datetime.now(UTC) - timedelta(seconds=1)},
        )
    code, output = project.run("run", "--pipeline_code", "P")
    assert code == 0, output
    assert leases.process_start(row[1]) != row[2]
    with project.engine.connect() as conn:
        assert conn.execute(
            text("SELECT STATUS FROM AUD_TASK_ATTEMPTS ORDER BY ATTEMPT_NUMBER")
        ).scalars().all() == ["LOST", "SUCCESS"]
        assert conn.execute(text("SELECT TARGET_COUNT FROM AUD_TASK_RUN_LOG")).scalar_one() == 7
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_PIPELINES_RUN_LOG")).scalar_one() == 1
    zombie = subprocess.run(
        [
            sys.executable,
            "-m",
            "etl_craft.execution.child",
            "--config",
            str(project.config.config_path),
            "--task-run-id",
            str(row[4]),
            "--attempt-id",
            str(row[0]),
            "--owner-id",
            row[3],
        ],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert zombie.returncode == 20, zombie.stderr
    with project.engine.connect() as conn:
        assert conn.execute(text("SELECT TARGET_COUNT FROM AUD_TASK_RUN_LOG")).scalar_one() == 7


@pytest.mark.chaos
def test_child_sigkill_is_failed_by_live_parent(cli_project):
    if sys.platform != "linux":
        pytest.skip("process-tree recovery requires Linux /proc")
    project = cli_project
    (project.config.ingestion_scripts_dir / "load.py").write_text(
        "import time\ndef run(task):\n    time.sleep(60)\n"
    )
    parent = project.start("run", "--pipeline_code", "P")
    project.wait_for("SELECT STATUS FROM AUD_TASK_ATTEMPTS", expected="RUNNING")
    with project.engine.connect() as conn:
        pid = conn.execute(text("SELECT PID FROM AUD_TASK_ATTEMPTS")).scalar_one()
    code, output = project.run("run", "--pipeline_code", "P")
    assert code == 9, output
    assert "is supervised by" in output
    code, output = project.run(
        "mark",
        "--pipeline_code",
        "P",
        "--task_code",
        "load",
        "--status",
        "FAILED",
        "--stale",
        "--reason",
        "test live lease",
    )
    assert code == 10, output
    code, output = project.run("reconcile", "--pipeline_code", "P")
    assert code == 0 and "0 attempt(s) LOST" in output, output
    parent.descendants()
    os.kill(pid, signal.SIGKILL)
    assert parent.wait() == 1, parent.output
    with project.engine.connect() as conn:
        assert conn.execute(text("SELECT STATUS FROM AUD_TASK_ATTEMPTS")).scalar_one() == "FAILED"
        assert (
            conn.execute(text("SELECT STATUS FROM AUD_PIPELINES_RUN_LOG")).scalar_one() == "FAILED"
        )


@pytest.mark.chaos
def test_concurrent_reconciliation_waits_for_verified_child_to_stop(engine_db, monkeypatch):
    engine = engine_db.engine
    _, run, _, attempt, _ = scene(engine)
    expire(engine, attempt)
    with engine.begin() as conn:
        tr.start_run(
            conn,
            run,
            current_actor(),
            owner="old",
            lease_expires_at=datetime.now(UTC) - timedelta(seconds=1),
        )
    stopping, stopped = threading.Event(), threading.Event()

    def stop(pid, birth):
        stopping.set()
        assert stopped.wait(5)

    monkeypatch.setattr("etl_craft.execution.reconcile.stop_process", stop)
    with ThreadPoolExecutor(2) as pool:
        first = pool.submit(reconcile, engine)
        try:
            assert stopping.wait(5)
            second = pool.submit(reconcile, engine)
            time.sleep(0.1)
            assert not second.done()
        finally:
            stopped.set()
        reports = [first.result(timeout=10), second.result(timeout=10)]
    assert sum(len(report.lost) for report in reports) == 1
    assert sum(len(report.released) for report in reports) == 1


def test_run_heartbeat_renews_its_exact_owner(engine_db, monkeypatch):
    engine = engine_db.engine
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        run = tr.create_run(conn, pipeline, current_actor())
    monkeypatch.setattr(leases, "HEARTBEAT_SECONDS", 0.02)
    with leases.supervise_run(engine, run):
        with engine.connect() as conn:
            before = conn.execute(
                text("SELECT LEASE_EXPIRES_AT FROM AUD_PIPELINES_RUN_LOG")
            ).scalar_one()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            with engine.connect() as conn:
                after, owner = conn.execute(
                    text("SELECT LEASE_EXPIRES_AT, OWNER_ID FROM AUD_PIPELINES_RUN_LOG")
                ).one()
            if leases.as_utc(after) > leases.as_utc(before):
                break
            time.sleep(0.02)
        assert leases.as_utc(after) > leases.as_utc(before)
        assert owner == leases.run_owner(run)
        assert not leases.run_cancel().is_set()


@pytest.mark.unit
def test_owner_identifies_the_process_and_each_supervisor():
    first, second = leases.owner_id(), leases.owner_id()
    assert first != second
    host, pid, birth, suffix = first.split(":")
    assert host == socket.gethostname()
    assert int(pid) == os.getpid()
    assert birth == leases.process_start(os.getpid())
    assert len(suffix) == 8
    assert int(suffix, 16) >= 0
