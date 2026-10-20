"""Split API copies share durable state and leave supervision to the controller."""

import json
import os
import signal
import socket
import threading
import time

import httpx
import pytest
import yaml
from sqlalchemy import text

from etl_craft import __version__
from etl_craft.core.actor import Actor, ActorKind
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.services.operations import OperationContext
from fixtures.actions import act


def start_listener(project, command, *args):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    raw = yaml.safe_load(project.config.config_path.read_text())
    raw["Orchestration"]["Api_address"] = f"127.0.0.1:{port}"
    project.config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    process = project.start(command, *args)
    url = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            response = httpx.get(url + "/health", timeout=0.5)
            if response.status_code == 200:
                assert response.json()["status"] == "ok"
                return process, url
        except httpx.TransportError:
            pass
        assert process.process.poll() is None, process.output
        threading.Event().wait(0.02)
    pytest.fail(f"{command} did not listen: {process.output}")


@pytest.mark.parametrize(
    "empty_engine_db",
    [pytest.param("postgresql", marks=pytest.mark.engine_postgres)],
    indirect=True,
)
def test_two_ui_processes_share_sessions_runs_refresh_and_revocation(cli_project):
    p = cli_project
    ctx = OperationContext(
        p.engine, p.config, Actor("test-admin", ActorKind.SERVICE), role=ApiRole.ADMIN
    )
    act(ctx, ActionName.ACCOUNT_ADD, name="alice", role=ApiRole.OPERATOR, password="local-password")
    first, one = start_listener(p, "ui")
    second, two = start_listener(p, "ui")
    login = httpx.post(
        one + "/api/v1/login/password", json={"name": "alice", "password": "local-password"}
    )
    assert login.status_code == 200, login.text
    session = login.json()
    headers = {"Authorization": "Bearer " + session["access_token"]}
    who = httpx.get(two + "/api/v1/whoami", headers=headers)
    assert who.status_code == 200 and who.json()["name"] == "alice"
    admission = httpx.post(
        one + "/api/v1/runs",
        headers=headers,
        json={"pipeline_code": "P", "init_only": True, "run_key": "split-api"},
    )
    assert admission.status_code == 200, admission.text
    run_id = admission.json()["run"]["pipeline_run_id"]
    detail = httpx.get(two + f"/api/v1/runs/{run_id}", headers=headers)
    assert detail.status_code == 200 and detail.json()["pipeline_run_id"] == run_id
    renewed = httpx.post(one + "/api/v1/login/refresh", json={"token": session["refresh_token"]})
    assert renewed.status_code == 200, renewed.text
    renewed_headers = {"Authorization": "Bearer " + renewed.json()["access_token"]}
    assert httpx.get(two + "/api/v1/pipelines", headers=renewed_headers).status_code == 200
    first.signal(signal.SIGTERM)
    assert first.wait() == 0, first.output
    assert httpx.get(two + "/api/v1/whoami", headers=renewed_headers).status_code == 200
    logout = httpx.post(two + "/api/v1/login/revoke", json={"token": session["refresh_token"]})
    assert logout.status_code in (200, 204), logout.text
    for bearer in (headers, renewed_headers):
        assert httpx.get(two + "/api/v1/whoami", headers=bearer).status_code == 401
    second.signal(signal.SIGTERM)
    assert second.wait() == 0, second.output
    with p.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) AS n FROM AUD_CONTROLLERS")).scalar_one() == 0
        assert conn.execute(text("SELECT COUNT(*) AS n FROM AUD_TASK_ATTEMPTS")).scalar_one() == 0
        assert (
            conn.execute(text("SELECT COUNT(*) AS n FROM AUD_PIPELINES_RUN_LOG")).scalar_one() == 1
        )


def test_controller_no_ui_exposes_only_health_and_still_supervises(cli_project, monkeypatch):
    p = cli_project
    (p.config.project_dir / "sitecustomize.py").write_text(
        "import sys\n"
        "class BlockWeb:\n"
        "    def find_spec(self, fullname, path=None, target=None):\n"
        "        if fullname.split('.')[0] in ('fastapi', 'uvicorn', 'pydantic', 'jinja2'):\n"
        "            raise AssertionError('headless imported ' + fullname)\n"
        "sys.meta_path.insert(0, BlockWeb())\n"
    )
    monkeypatch.setenv("PYTHONPATH", str(p.config.project_dir), prepend=os.pathsep)
    controller, url = start_listener(p, "controller", "--no-ui")
    for path in ("/", "/api/v1/docs", "/api/v1/health", "/api/v1/login/methods"):
        assert httpx.get(url + path).status_code == 404
    assert httpx.post(url + "/health").status_code != 200
    code, output = p.run("run", "--pipeline-code", "P", "--init-only", "--run-key", "headless")
    assert code == 0, output
    p.wait_for(
        "SELECT STATUS FROM AUD_PIPELINES_RUN_LOG WHERE RUN_KEY='headless'", expected="SUCCESS"
    )
    controller.signal(signal.SIGTERM)
    assert controller.wait() == 0, controller.output


@pytest.mark.parametrize(
    "empty_engine_db", [pytest.param("sqlite", marks=pytest.mark.engine_sqlite)], indirect=True
)
def test_ui_refuses_sqlite_before_listening_or_creating_controller_state(cli_project):
    p = cli_project
    code, output = p.run("ui")
    assert code == 3, output
    for part in ("Engine.Jdbc_url", "sqlite", "requires PostgreSQL", "etl-craft controller"):
        assert part in output
    with p.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) AS n FROM AUD_CONTROLLERS")).scalar_one() == 0
        assert (
            conn.execute(text("SELECT COUNT(*) AS n FROM AUD_PIPELINES_RUN_LOG")).scalar_one() == 0
        )


@pytest.mark.parametrize("mismatch", ["missing", "future", "edited"])
def test_controller_refuses_mismatched_package_schema_before_supervising(cli_project, mismatch):
    p = cli_project
    with p.engine.begin() as conn:
        if mismatch == "future":
            conn.execute(
                text(
                    "INSERT INTO SCHEMA_MIGRATIONS (SOURCE,VERSION,CHECKSUM) "
                    "VALUES ('ENGINE','9999_future.sql',:checksum)"
                ),
                {"checksum": "0" * 64},
            )
        elif mismatch == "edited":
            conn.execute(
                text("UPDATE SCHEMA_MIGRATIONS SET CHECKSUM=:checksum WHERE SOURCE='ENGINE'"),
                {"checksum": "0" * 64},
            )
        else:
            conn.execute(
                text(
                    "DELETE FROM SCHEMA_MIGRATIONS WHERE SOURCE='ENGINE' AND VERSION="
                    "(SELECT MAX(VERSION) FROM SCHEMA_MIGRATIONS WHERE SOURCE='ENGINE')"
                )
            )
    code, output = p.run("controller", "--no-ui")
    assert code == 13, output
    for part in ("Engine DB", "package version", __version__, "etl-craft migrate"):
        assert part in output
    with p.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) AS n FROM AUD_CONTROLLERS")).scalar_one() == 0
    if p.config.engine.auth_mode != "NONE":
        code, output = p.run("ui")
        assert code == 13, output
        assert "package version" in output and __version__ in output


def test_ui_missing_extra_is_refused_and_plain_controller_keeps_health_only(
    cli_project, monkeypatch, capsys, cli_logging
):
    from etl_craft.cli import main
    from etl_craft.cli.commands import controller
    from etl_craft.services.operations import processes

    p = cli_project
    monkeypatch.setenv("ETL_CRAFT_CONFIG", str(p.config.config_path))
    monkeypatch.setattr(processes, "find_spec", lambda name: None)
    assert main(["ui"]) == 3
    message = capsys.readouterr().err
    for part in ("ui dependencies", "missing", "expected the ui extra", "etl-craft[ui]"):
        assert part in message
    called = []
    monkeypatch.setattr(controller, "find_spec", lambda name: None)
    monkeypatch.setattr(controller, "serve_health", lambda ctx, stop: called.append(ctx.actor.name))
    assert main(["controller"]) == 0
    assert called == ["test-admin"]
    with p.engine.connect() as conn:
        rows = conn.execute(text("SELECT COMMAND AS command FROM AUD_ACTIONS")).all()
        assert any(row.command == "ui" for row in rows)
        assert conn.execute(text("SELECT COUNT(*) AS n FROM AUD_CONTROLLERS")).scalar_one() == 0


@pytest.mark.parametrize(
    "empty_engine_db",
    [pytest.param("postgresql", marks=pytest.mark.engine_postgres)],
    indirect=True,
)
def test_ui_readiness_recovers_after_engine_db_disconnect_and_schema_repair(cli_project):
    from fixtures.engine_db import engine_db_cut_off

    p = cli_project
    process, url = start_listener(p, "ui")
    with engine_db_cut_off(p.engine):
        response = httpx.get(url + "/health", timeout=5)
        assert response.status_code == 503, response.text
        assert "Engine DB" in response.text and "not ready" in response.text
    assert httpx.get(url + "/health", timeout=5).status_code == 200
    with p.engine.begin() as conn:
        original = conn.execute(
            text(
                "SELECT CHECKSUM AS checksum FROM SCHEMA_MIGRATIONS "
                "WHERE SOURCE='ENGINE' ORDER BY VERSION LIMIT 1"
            )
        ).scalar_one()
        conn.execute(
            text(
                "UPDATE SCHEMA_MIGRATIONS SET CHECKSUM=:bad "
                "WHERE SOURCE='ENGINE' AND CHECKSUM=:original"
            ),
            {"bad": "0" * 64, "original": original},
        )
    response = httpx.get(url + "/health")
    assert response.status_code == 503 and "package version" in response.text
    with p.engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE SCHEMA_MIGRATIONS SET CHECKSUM=:original "
                "WHERE SOURCE='ENGINE' AND CHECKSUM=:bad"
            ),
            {"bad": "0" * 64, "original": original},
        )
    assert httpx.get(url + "/health").status_code == 200
    assert process.process.poll() is None


@pytest.mark.parametrize(
    "empty_engine_db",
    [pytest.param("postgresql", marks=pytest.mark.engine_postgres)],
    indirect=True,
)
@pytest.mark.parametrize("trusted", [False, True])
def test_ui_trusts_only_configured_proxy_hops_and_records_client_origin(cli_project, trusted):
    p = cli_project
    raw = yaml.safe_load(p.config.config_path.read_text())
    raw["Docs_site"] = {"Trusted_proxies": ["127.0.0.0/8" if trusted else "192.0.2.0/24"]}
    p.config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    _, url = start_listener(p, "ui")
    import os

    headers = {
        "Authorization": "Bearer " + os.environ["ETL_CRAFT_TOKEN"],
        "X-Forwarded-For": "203.0.113.8, 127.0.0.2",
        "X-Forwarded-Proto": "https",
    }
    response = httpx.post(
        url + "/api/v1/controls",
        headers=headers,
        json={"pipeline_code": "P", "action": "PAUSE", "reason": "proxy check"},
    )
    assert response.status_code == 200, response.text
    with p.engine.connect() as conn:
        arguments = conn.execute(
            text("SELECT ARGUMENTS AS arguments FROM AUD_ACTIONS WHERE COMMAND='control'")
        ).scalar_one()
    if isinstance(arguments, str):
        arguments = json.loads(arguments)
    origin = arguments["_http"]
    assert origin["client_address"] == ("203.0.113.8" if trusted else "127.0.0.1")
    assert origin["scheme"] == ("https" if trusted else "http")
    if trusted:
        response = httpx.post(
            url + "/api/v1/controls",
            headers={**headers, "X-Forwarded-Proto": "javascript"},
            json={"pipeline_code": "P", "action": "RESUME", "reason": "bad proto"},
        )
        assert response.status_code == 400 and "X-Forwarded-Proto" in response.text
        with p.engine.connect() as conn:
            assert (
                conn.execute(
                    text("SELECT COUNT(*) AS n FROM AUD_ACTIONS WHERE COMMAND='control'")
                ).scalar_one()
                == 1
            )


def install_draining_routes(project):
    """Give the real UI process deterministic in-flight work and an unending response."""
    (project.config.project_dir / "sitecustomize.py").write_text("""
import asyncio
from pathlib import Path
from fastapi.responses import StreamingResponse
from etl_craft.api import serve
factory = serve.create_app

def app_with_work(*args, **kwargs):
    app = factory(*args, **kwargs)
    @app.get('/hold')
    async def hold():
        Path('holding').write_text('yes')
        while not Path('release').exists():
            await asyncio.sleep(0.01)
        return {'finished': True}
    @app.get('/poll')
    async def poll():
        Path('polling').write_text('yes')
        await asyncio.to_thread(app.state.gateway.stop.wait)
        return []
    @app.get('/blocked')
    async def blocked():
        import threading
        Path('blocked').write_text('yes')
        await asyncio.to_thread(threading.Event().wait)
    @app.get('/stream')
    async def stream():
        async def chunks():
            yield b'started\\n'
            await asyncio.Event().wait()
        return StreamingResponse(chunks())
    return app
serve.create_app = app_with_work
""")


@pytest.mark.parametrize(
    "empty_engine_db",
    [pytest.param("postgresql", marks=pytest.mark.engine_postgres)],
    indirect=True,
)
def test_ui_sigterm_withdraws_readiness_drains_work_and_bounds_open_stream(
    cli_project, monkeypatch
):
    from concurrent.futures import ThreadPoolExecutor

    p = cli_project
    raw = yaml.safe_load(p.config.config_path.read_text())
    raw["Orchestration"]["Shutdown_grace_seconds"] = 3
    p.config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    install_draining_routes(p)
    monkeypatch.setenv("PYTHONPATH", str(p.config.project_dir), prepend=os.pathsep)
    process, url = start_listener(p, "ui")
    with (
        ThreadPoolExecutor(2) as requests,
        httpx.stream("GET", url + "/stream", timeout=8) as stream,
    ):
        lines = stream.iter_lines()
        assert next(lines) == "started"
        held = requests.submit(httpx.get, url + "/hold", timeout=8)
        poll = requests.submit(httpx.get, url + "/poll", timeout=8)
        deadline = time.monotonic() + 5
        while not all((p.config.project_dir / name).exists() for name in ("holding", "polling")):
            assert time.monotonic() < deadline, process.output
            threading.Event().wait(0.01)
        stopped_at = time.monotonic()
        process.signal(signal.SIGTERM)
        deadline = stopped_at + 2
        while True:
            response = httpx.get(url + "/health")
            if response.status_code == 503:
                break
            assert time.monotonic() < deadline
            threading.Event().wait(0.01)
        assert "shutting down" in response.text
        assert httpx.get(url + "/api/v1/pipelines").status_code == 503
        (p.config.project_dir / "release").write_text("yes")
        assert held.result().json() == {"finished": True}
        assert poll.result().json() == []
        with pytest.raises(httpx.RemoteProtocolError):
            next(lines)
        assert process.wait(timeout=5) == 0, process.output
        assert time.monotonic() - stopped_at < 4
    with p.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) AS n FROM AUD_CONTROLLERS")).scalar_one() == 0


@pytest.mark.parametrize(
    "empty_engine_db",
    [pytest.param("postgresql", marks=pytest.mark.engine_postgres)],
    indirect=True,
)
def test_ui_shutdown_deadline_closes_blocked_executor_work(cli_project, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor

    p = cli_project
    raw = yaml.safe_load(p.config.config_path.read_text())
    raw["Orchestration"]["Shutdown_grace_seconds"] = 1
    p.config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    install_draining_routes(p)
    monkeypatch.setenv("PYTHONPATH", str(p.config.project_dir), prepend=os.pathsep)
    process, url = start_listener(p, "ui")
    with ThreadPoolExecutor(1) as requests:
        blocked = requests.submit(httpx.get, url + "/blocked", timeout=5)
        deadline = time.monotonic() + 5
        while not (p.config.project_dir / "blocked").exists():
            assert time.monotonic() < deadline, process.output
            threading.Event().wait(0.01)
        stopped_at = time.monotonic()
        process.signal(signal.SIGTERM)
        assert process.wait(timeout=3) == 0, process.output
        assert time.monotonic() - stopped_at < 2
        with pytest.raises(httpx.RemoteProtocolError):
            blocked.result()
