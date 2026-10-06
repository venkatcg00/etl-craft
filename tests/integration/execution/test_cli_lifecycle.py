"""Lifecycle boundaries through the real CLI, on both Engine DB dialects."""

import signal
import sys
import time
from pathlib import Path

import pytest
from sqlalchemy import text


@pytest.mark.parametrize(
    "point", ["runner.after_bind", "supervisor.after_mkdir", "supervisor.after_popen"]
)
@pytest.mark.chaos
def test_a_fault_after_binding_records_failure_and_allows_retry(cli_project, point):
    project = cli_project
    assert project.run("run", "--pipeline_code", "P", "--init-only")[0] == 0
    code, output = project.run("run", "--pipeline_code", "P", "--task_code", "load", fault=point)
    assert code != 0, output
    project.wait_for("SELECT STATUS FROM AUD_TASK_RUN_LOG", expected="FAILED")
    with project.engine.connect() as conn:
        error = conn.execute(text("SELECT ERROR_MESSAGE FROM AUD_TASK_RUN_LOG")).scalar_one()
        assert point in error
    code, output = project.run("run", "--pipeline_code", "P", "--task_code", "load")
    assert code == 0, output
    project.wait_for("SELECT STATUS FROM AUD_TASK_RUN_LOG", expected="SUCCESS")
    with project.engine.connect() as conn:
        assert conn.execute(text("SELECT ATTEMPT_COUNT FROM AUD_TASK_RUN_LOG")).scalar_one() == 2


def test_a_timeout_parse_fault_leaves_no_bound_attempt(cli_project):
    project = cli_project
    assert project.run("run", "--pipeline_code", "P", "--init-only")[0] == 0
    code, output = project.run(
        "run", "--pipeline_code", "P", "--task_code", "load", fault="runner.after_timeout"
    )
    assert code != 0, output
    with project.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_TASK_RUN_LOG")).scalar_one() == 0


@pytest.mark.parametrize("mode", ["", ":kill"])
@pytest.mark.chaos
def test_a_child_failure_after_recording_success_keeps_the_recorded_outcome(cli_project, mode):
    project = cli_project
    code, output = project.run("run", "--pipeline_code", "P", fault="child.after_outcome" + mode)
    assert code == 0, output
    project.wait_for("SELECT STATUS FROM AUD_TASK_RUN_LOG", expected="SUCCESS")
    project.wait_for("SELECT STATUS FROM AUD_PIPELINES_RUN_LOG", expected="SUCCESS")


def test_a_hard_exit_after_binding_can_be_cancelled_without_starting_a_child(cli_project):
    project = cli_project
    assert project.run("run", "--pipeline_code", "P", "--init-only")[0] == 0
    code, _ = project.run(
        "run", "--pipeline_code", "P", "--task_code", "load", fault="runner.after_bind:kill"
    )
    assert code == 137
    project.wait_for("SELECT STATUS FROM AUD_TASK_RUN_LOG", expected="IN-PROGRESS")
    code, output = project.run("cancel", "--pipeline_code", "P", "--reason", "lost CLI")
    assert code == 0, output
    project.wait_for("SELECT STATUS FROM AUD_TASK_RUN_LOG", expected="CANCELLED")


@pytest.mark.skipif(sys.platform != "linux", reason="process-tree inspection requires Linux")
@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGHUP])
def test_signal_stops_the_real_cli_child_and_grandchild(cli_project, sig):
    project = cli_project
    script = project.config.ingestion_scripts_dir / "load.py"
    script.write_text(
        "import subprocess, sys, time\ndef run(task):\n"
        "    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])\n"
        "    print('grandchild ready', flush=True)\n    time.sleep(60)\n"
    )
    assert project.run("run", "--pipeline_code", "P", "--init-only")[0] == 0
    parent = project.start("run", "--pipeline_code", "P", "--task_code", "load")
    try:
        deadline = time.monotonic() + 10
        children = []
        while time.monotonic() < deadline:
            children = parent.descendants()
            if len(children) >= 2:
                break
            time.sleep(0.02)
        assert len(children) >= 2, parent.output
        parent.signal(sig)
        assert parent.wait() != 0, parent.output
        project.wait_for("SELECT STATUS FROM AUD_TASK_RUN_LOG", expected="FAILED")

        def alive(pid):
            try:
                fields = (
                    Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()
                )
                return fields[0] != "Z"
            except FileNotFoundError:
                return False

        deadline = time.monotonic() + 5
        while any(alive(pid) for pid in children) and time.monotonic() < deadline:
            time.sleep(0.02)
        assert not any(alive(pid) for pid in children), parent.output
    finally:
        parent.close()
