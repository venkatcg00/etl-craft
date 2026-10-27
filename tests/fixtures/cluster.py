"""Real API-only agents and immutable publications shared by cluster tests."""

import base64
import socket
import threading
import time

import httpx2 as httpx
import pytest
import yaml

from etl_craft.config import load_config
from etl_craft.config.loader import SECTIONS
from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.tokens import TokenCreate


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
        "Artifacts": {"Store": (tmp_path / "artifacts").as_uri()},
    }
    raw["Warehouse"]["dev"]["Jdbc_url"] = f"jdbc:duckdb:{tmp_path / 'warehouse.duckdb'}"
    p.config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    p.config = load_config(p.config.config_path)
    ctx = OperationContext(p.engine, p.config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
    token = invoke(
        ctx, ActionName.TOKEN_CREATE, TokenCreate(name="host-a", role=ApiRole.AGENT, pool="default")
    )
    _, url = start_ui(p)
    raw["Orchestration"]["Api_address"] = "127.0.0.1:0"
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


def publish_project(project, agent_dir):
    """Publish execution bytes, then leave an agent with only its bootstrap config."""
    from etl_craft.engine.project_files import collect
    from etl_craft.services.operations.bundles import PublishBundle

    uploaded = collect(agent_dir)
    raw = yaml.safe_load((agent_dir / "craft-connector.yml").read_text())
    raw["Engine"] = yaml.safe_load(project.config.config_path.read_text())["Engine"]
    uploaded["craft-connector.yml"] = base64.b64encode(
        yaml.safe_dump({key: raw[key] for key in SECTIONS if key in raw}, sort_keys=False).encode()
    ).decode()
    ctx = OperationContext(project.engine, project.config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
    invoke(ctx, ActionName.BUNDLE_PUBLISH, PublishBundle(files=uploaded))


def start_ui(project, *, fault=None):
    """Start one independent API copy, restoring the bootstrap address after it is ready."""
    raw = yaml.safe_load(project.config.config_path.read_text())
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    raw["Orchestration"]["Api_address"] = f"127.0.0.1:{port}"
    project.config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    process = project.start("ui", fault=fault)
    url = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        try:
            if httpx.get(url + "/health", timeout=0.5).status_code == 200:
                break
        except httpx.TransportError:
            pass
        assert process.process.poll() is None, process.output
        threading.Event().wait(0.02)
    else:
        pytest.fail(process.output)
    raw["Orchestration"]["Api_address"] = "127.0.0.1:0"
    project.config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    return process, url
