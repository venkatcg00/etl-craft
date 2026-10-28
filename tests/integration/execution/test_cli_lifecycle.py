"""Lifecycle boundaries through the real CLI, on both Engine DB dialects."""

import json
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
    assert project.run("run", "--pipeline-code", "P", "--init-only")[0] == 0
    code, output = project.run("run", "--pipeline-code", "P", "--task-code", "load", fault=point)
    assert code != 0, output
    project.wait_for("SELECT STATUS FROM AUD_TASK_RUN_LOG", expected="FAILED")
    with project.engine.connect() as conn:
        error = conn.execute(text("SELECT ERROR_MESSAGE FROM AUD_TASK_RUN_LOG")).scalar_one()
        assert point in error
    code, output = project.run("run", "--pipeline-code", "P", "--task-code", "load")
    assert code == 0, output
    project.wait_for("SELECT STATUS FROM AUD_TASK_RUN_LOG", expected="SUCCESS")
    with project.engine.connect() as conn:
        assert conn.execute(text("SELECT ATTEMPT_COUNT FROM AUD_TASK_RUN_LOG")).scalar_one() == 2


def test_a_timeout_parse_fault_leaves_no_bound_attempt(cli_project):
    project = cli_project
    assert project.run("run", "--pipeline-code", "P", "--init-only")[0] == 0
    code, output = project.run(
        "run", "--pipeline-code", "P", "--task-code", "load", fault="runner.after_timeout"
    )
    assert code != 0, output
    with project.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_TASK_RUN_LOG")).scalar_one() == 0


@pytest.mark.parametrize("mode", ["", ":kill"])
@pytest.mark.chaos
def test_a_child_failure_after_recording_success_keeps_the_recorded_outcome(cli_project, mode):
    project = cli_project
    code, output = project.run("run", "--pipeline-code", "P", fault="child.after_outcome" + mode)
    assert code == 0, output
    project.wait_for("SELECT STATUS FROM AUD_TASK_RUN_LOG", expected="SUCCESS")
    project.wait_for("SELECT STATUS FROM AUD_PIPELINES_RUN_LOG", expected="SUCCESS")


def test_a_hard_exit_after_binding_does_not_fabricate_cancellation_acknowledgement(cli_project):
    project = cli_project
    assert project.run("run", "--pipeline-code", "P", "--init-only")[0] == 0
    code, _ = project.run(
        "run", "--pipeline-code", "P", "--task-code", "load", fault="runner.after_bind:kill"
    )
    assert code == 137
    project.wait_for("SELECT STATUS FROM AUD_TASK_RUN_LOG", expected="IN-PROGRESS")
    code, output = project.run(
        "abort", "--pipeline-code", "P", "--reason", "lost CLI", "--format", "json"
    )
    assert code == 0, output
    request = json.loads(output)
    assert request["status"] == "REQUESTED" and request["completed_at"] is None
    assert request["run_status"] == "IN-PROGRESS"
    assert len(request["attempts"]) == 1
    attempt = request["attempts"][0]
    assert attempt["status"] == "CLAIMED" and attempt["pid"] is None
    assert attempt["process_ended_at"] is None and attempt["queries_ended_at"] is None
    project.wait_for("SELECT STATUS FROM AUD_TASK_RUN_LOG", expected="IN-PROGRESS")


@pytest.mark.skipif(sys.platform != "linux", reason="process-tree inspection requires Linux")
@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGHUP])
def test_signal_stops_the_real_cli_child_and_grandchild(cli_project, sig):
    project = cli_project
    script = project.config.python_scripts_dir / "load.py"
    script.write_text(
        "import subprocess, sys, time\ndef run(task):\n"
        "    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])\n"
        "    print('grandchild ready', flush=True)\n    time.sleep(60)\n"
    )
    code, output = project.run("pipelines", "refresh")
    assert code == 0, output
    assert project.run("run", "--pipeline-code", "P", "--init-only")[0] == 0
    parent = project.start("run", "--pipeline-code", "P", "--task-code", "load")
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


def test_local_attempt_keeps_full_file_and_configured_tail(cli_project):
    import yaml

    from etl_craft.config import load_config
    from etl_craft.core.actor import SYSTEM_ACTOR
    from etl_craft.core.enums import ActionName, ApiRole
    from etl_craft.services.operations import OperationContext
    from etl_craft.services.operations.actions import invoke
    from etl_craft.services.operations.requests import LogInput

    p = cli_project
    raw = yaml.safe_load(p.config.config_path.read_text())
    raw["Logs"] = {"Tail_bytes": 11}
    p.config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    p.config = load_config(p.config.config_path)
    (p.config.python_scripts_dir / "load.py").write_text(
        "from etl_craft.scripting import ScriptResult\n"
        "def run(task):\n"
        "    print('full-log-start-' + 'x' * 24000 + '-tail-end')\n"
        "    return ScriptResult(7)\n"
    )
    code, output = p.run("pipelines", "refresh")
    assert code == 0, output
    code, output = p.run("run", "--pipeline-code", "P")
    assert code == 0, output
    with p.engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT a.ATTEMPT_ID AS attempt_id, a.LOG_PATH AS log_path, "
                "t.TASK_LOG AS task_log FROM AUD_TASK_ATTEMPTS a "
                "JOIN AUD_TASK_RUN_LOG t ON t.TASK_RUN_ID=a.TASK_RUN_ID"
            )
        ).one()
    assert len(row.task_log.split("\n\n")[-1].encode()) <= 11
    assert "TARGET_COUNT = 7" in row.task_log
    ctx = OperationContext(p.engine, p.config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
    full = b"".join(invoke(ctx, ActionName.LOGS, LogInput(attempt_id=row.attempt_id)))
    assert b"full-log-start-" in full
    assert b"x" * 24000 in full and b"-tail-end" in full
