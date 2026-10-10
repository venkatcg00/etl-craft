"""The local pool keeps the pool rules: one process per attempt, UNKNOWN handles, slots per kind."""

import time

import yaml
from sqlalchemy import text

from etl_craft.execution.pools import ExecutionHandle, HandleState
from etl_craft.execution.pools.local import LocalPool
from etl_craft.execution.runner import admit_attempt
from fixtures.metadata import add_task


def hold(project, name="load.py"):
    """A script that says it started, then waits for the test to release it."""
    folder = project.config.project_dir
    (project.config.ingestion_scripts_dir / name).write_text(
        "import time\nfrom pathlib import Path\nfrom etl_craft.scripting import ScriptResult\n"
        "def run(task):\n"
        f"    Path({str(folder)!r}, 'ready-' + task.task_code).touch()\n"
        f"    while not Path({str(folder / 'release')!r}).exists():\n"
        "        time.sleep(0.02)\n"
        "    return ScriptResult(1)\n"
    )


def wait_until(predicate, timeout=30):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.02)


def admitted(project):
    assert project.run("run", "--pipeline_code", "P", "--init-only")[0] == 0
    with project.engine.connect() as conn:
        run_id = conn.execute(text("SELECT PIPELINE_RUN_ID FROM AUD_PIPELINES_RUN_LOG")).scalar()
    return admit_attempt(
        project.engine, project.config, project.task_id, "load", "P", run_id, False
    )


def test_an_attempt_submitted_twice_runs_once_and_unknown_handles_say_so(cli_project):
    project = cli_project
    hold(project)
    spec = admitted(project)
    pool = LocalPool(project.engine, project.config)
    try:
        handle = pool.submit(spec)
        assert pool.submit(spec) == handle
        wait_until((project.config.project_dir / "ready-load").exists)
        assert pool.status(handle).state is HandleState.RUNNING
        assert pool.reconcile()[0].handle == handle
        capacity = pool.capacity()
        assert capacity.free["ingestion"] == capacity.slots["ingestion"] - 1
        (project.config.project_dir / "release").touch()
        wait_until(lambda: pool.status(handle).state is not HandleState.RUNNING)
    finally:
        (project.config.project_dir / "release").touch()
        pool.close()
    assert pool.status(handle).state is HandleState.UNKNOWN  # reported once, then forgotten
    assert pool.status(ExecutionHandle("local", spec.attempt_id + 1000)).state == "UNKNOWN"
    assert pool.status(ExecutionHandle("elsewhere", spec.attempt_id)).state == "UNKNOWN"
    with project.engine.connect() as conn:
        attempts = conn.execute(text("SELECT STATUS FROM AUD_TASK_ATTEMPTS")).scalars().all()
    assert attempts == ["SUCCESS"]


def test_cancel_stops_the_task_process_and_records_the_attempt(cli_project):
    project = cli_project
    hold(project)
    spec = admitted(project)
    pool = LocalPool(project.engine, project.config)
    try:
        handle = pool.submit(spec)
        wait_until((project.config.project_dir / "ready-load").exists)
        pool.cancel(handle, 1)
        wait_until(lambda: pool.status(handle).state is not HandleState.RUNNING)
    finally:
        (project.config.project_dir / "release").touch()
        pool.close()
    with project.engine.connect() as conn:
        row = conn.execute(
            text("SELECT STATUS AS status, ERROR_MESSAGE AS error FROM AUD_TASK_RUN_LOG")
        ).one()
    assert row.status == "FAILED"
    assert "interrupted" in row.error or "SIGTERM" in row.error, row.error


def test_ingestion_slots_keep_python_tasks_from_overlapping(cli_project):
    project = cli_project
    raw = yaml.safe_load(project.config.config_path.read_text())
    raw["Orchestration"]["Local_ingestion_slots"] = 1
    project.config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    script = (
        "import time\nfrom etl_craft.scripting import ScriptResult\n"
        "def run(task):\n    time.sleep(0.5)\n    return ScriptResult(1)\n"
    )
    (project.config.ingestion_scripts_dir / "load.py").write_text(script)
    (project.config.ingestion_scripts_dir / "second.py").write_text(script)
    with project.engine.begin() as conn:
        add_task(conn, project.pipeline_id, "second", "PYTHON", SCRIPT_NAME="second.py")
    code, output = project.run("run", "--pipeline_code", "P")
    assert code == 0, output
    with project.engine.connect() as conn:
        spans = conn.execute(
            text(
                "SELECT STARTED_AT AS started, ENDED_AT AS ended FROM AUD_TASK_ATTEMPTS "
                "ORDER BY STARTED_AT"
            )
        ).all()
    assert len(spans) == 2
    first, second = spans
    assert first.ended <= second.started, spans
