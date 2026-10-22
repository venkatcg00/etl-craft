"""Real agents execute project files through HTTP without Engine DB credentials."""

import signal
import socket
import threading
import time

import httpx
import pytest
import yaml
from sqlalchemy import text

from etl_craft.config import load_config
from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.tokens import TokenCreate

pytestmark = pytest.mark.parametrize(
    "empty_engine_db",
    [pytest.param("postgresql", marks=pytest.mark.engine_postgres)],
    indirect=True,
)


@pytest.fixture
def cluster(cli_project, monkeypatch, tmp_path):
    p = cli_project
    raw = yaml.safe_load(p.config.config_path.read_text())
    raw = {
        "Secrets": raw["Secrets"],
        "Orchestration": raw["Orchestration"],
        "Deployment": {"Profile": "CONTROLLER_AGENTS"},
        "Engine": raw["Engine"],
        "Warehouse": raw["Warehouse"],
    }
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    raw["Orchestration"]["Api_address"] = f"127.0.0.1:{port}"
    p.config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    p.config = load_config(p.config.config_path)
    ctx = OperationContext(p.engine, p.config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
    token = invoke(ctx, ActionName.TOKEN_CREATE, TokenCreate(name="host-a", role=ApiRole.AGENT))
    ui = p.start("ui")
    url = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        try:
            if httpx.get(url + "/health", timeout=0.5).status_code == 200:
                break
        except httpx.TransportError:
            pass
        assert ui.process.poll() is None, ui.output
        threading.Event().wait(0.02)
    else:
        pytest.fail(ui.output)
    raw["Orchestration"]["Api_address"] = "127.0.0.1:0"
    p.config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    agent_dir = tmp_path / "agent-project"
    agent_dir.mkdir()
    raw.pop("Engine")
    (agent_dir / "craft-connector.yml").write_text(yaml.safe_dump(raw, sort_keys=False))
    (agent_dir / "python_scripts").mkdir()
    (agent_dir / "python_scripts" / "load.py").write_text(
        "from etl_craft.scripting import ScriptResult\n"
        "def run(task):\n"
        '    print("project copy ran")\n'
        "    return ScriptResult(7)\n"
    )
    yield p, url, token, agent_dir


def test_agent_runs_from_its_own_project_without_engine_credentials(cluster, monkeypatch):
    p, url, token, agent_dir = cluster
    controller = p.start("controller", "--no-ui")
    code, output = p.run("run", "--pipeline-code", "P", "--init-only", "--run-key", "agent-test")
    assert code == 0, output
    p.wait_for("SELECT STATUS FROM AUD_TASK_ATTEMPTS", expected="QUEUED")
    monkeypatch.setenv("ETL_CRAFT_TOKEN", token.token)
    agent = p.start("agent", "--api", url, "--project", str(agent_dir), "--drain-seconds", "0")
    try:
        p.wait_for("SELECT STATUS FROM AUD_TASK_ATTEMPTS", expected="SUCCESS", timeout=20)
        with p.engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT OWNER_ID AS owner_id, TARGET_COUNT AS target_count "
                    "FROM AUD_TASK_ATTEMPTS"
                )
            ).one()
            logs = conn.execute(text("SELECT CONTENT AS content FROM AUD_ATTEMPT_LOG_CHUNKS")).all()
        assert row.owner_id == str(token.metadata.token_id)
        assert row.target_count == 7
        assert "project copy ran" in "".join(r.content for r in logs)
        agent.signal(signal.SIGTERM)
        assert agent.wait(timeout=40) == 0, agent.output
        p.wait_for("SELECT STATUS FROM AUD_AGENTS", expected="GONE")
    except AssertionError as error:
        raise AssertionError(
            f"{error}\nagent:\n{agent.output}\ncontroller:\n{controller.output}"
        ) from error


@pytest.mark.chaos
@pytest.mark.parametrize("failure", ["agent-killed", "api-cut-off"])
def test_a_lost_agent_stops_its_writer_before_a_second_holder(cluster, monkeypatch, failure):
    from datetime import UTC, datetime, timedelta

    from etl_craft.core.actor import acting_as
    from etl_craft.execution.leases import process_start
    from etl_craft.execution.reconcile import reconcile
    from etl_craft.handlers.resources import LocalLocker
    from etl_craft.warehouse.connection import open_warehouse

    p, url, token, agent_dir = cluster
    (agent_dir / "python_scripts" / "load.py").write_text(
        "import threading\n"
        "from sqlalchemy import text\n"
        "def run(task):\n"
        "    with task.warehouse() as engine:\n"
        "        with engine.begin() as conn:\n"
        '            conn.execute(text("CREATE TABLE evidence (id INTEGER)"))\n'
        '        (task.config.project_dir / "writing").touch()\n'
        "        counter = 0\n"
        "        while True:\n"
        "            with engine.begin() as conn:\n"
        '                conn.execute(text("INSERT INTO evidence VALUES (:id)"), {"id": counter})\n'
        "            counter += 1\n"
        "            threading.Event().wait(.02)\n"
    )
    p.start("controller", "--no-ui")
    code, output = p.run("run", "--pipeline-code", "P", "--init-only", "--run-key", failure)
    assert code == 0, output
    monkeypatch.setenv("ETL_CRAFT_TOKEN", token.token)
    agent = p.start("agent", "--api", url, "--project", str(agent_dir), "--drain-seconds", "0")
    deadline = time.monotonic() + 20
    while not (agent_dir / "writing").exists():
        assert time.monotonic() < deadline, agent.output
        assert agent.process.poll() is None, agent.output
        threading.Event().wait(0.02)
    with p.engine.connect() as conn:
        attempt = conn.execute(
            text(
                "SELECT ATTEMPT_ID AS attempt_id, PID AS pid, PROCESS_START AS birth, "
                "LEASE_EXPIRES_AT AS lease FROM AUD_TASK_ATTEMPTS"
            )
        ).one()
    if failure == "agent-killed":
        agent.signal(signal.SIGKILL)
        agent.wait()
    else:
        ui = p.processes[1]
        ui.signal(signal.SIGKILL)
        ui.wait()
    deadline = time.monotonic() + (5 if failure == "agent-killed" else 55)
    while process_start(attempt.pid) == attempt.birth:
        assert time.monotonic() < deadline, agent.output
        threading.Event().wait(0.02)
    # The old writer must end while its reservation still excludes a successor.
    assert datetime.now(UTC) < attempt.lease
    with p.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET LEASE_EXPIRES_AT=:past WHERE ATTEMPT_ID=:id"),
            {"past": datetime.now(UTC) - timedelta(seconds=1), "id": attempt.attempt_id},
        )
    with acting_as(SYSTEM_ACTOR):
        reconcile(p.engine)
        config = load_config(agent_dir / "craft-connector.yml", agent=True)
        with (
            open_warehouse(config, LocalLocker(p.engine, p.config)) as warehouse,
            warehouse.begin() as conn,
        ):
            before = conn.execute(text("SELECT COUNT(*) AS n FROM evidence")).scalar_one()
            conn.execute(text("INSERT INTO evidence VALUES (-1)"))
            assert (
                conn.execute(text("SELECT COUNT(*) AS n FROM evidence")).scalar_one() == before + 1
            )
    with p.engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT STATUS AS status FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"),
                {"id": attempt.attempt_id},
            ).scalar_one()
            == "LOST"
        )
