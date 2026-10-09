"""Real token authorization and shared operation documents on both Engine DBs."""

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from etl_craft.api.app import create_app
from etl_craft.core.actor import Actor, ActorKind, current_actor
from etl_craft.core.errors import UsageError
from etl_craft.core.text import sha256_hex
from etl_craft.engine import transitions
from etl_craft.services.operations import (
    OperationContext,
    api_reads,
    inspect,
    to_json,
    tokens,
)
from etl_craft.services.operations.status import explain_task
from fixtures.metadata import add_pipeline, add_task, start_run


@pytest.fixture
def api(engine_db, tmp_path):
    ctx = OperationContext(
        engine_db.engine,
        replace(engine_db.config, log_dir=tmp_path / "logs"),
        Actor("owner", ActorKind.HUMAN),
    )
    with ctx.engine.begin() as conn:
        engine_db.dialect.refresh_metadata_triggers(conn)
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "T")
        run = start_run(conn, pipeline)
        summary = transitions.find_or_create_task_run(conn, task, run).task_run_id
        attempt = transitions.queue_attempt(conn, summary, current_actor())
    creds = {role: tokens.create_token(ctx, role, role) for role in tokens.ROLES}
    client = TestClient(create_app(ctx))
    return ctx, client, creds, run, attempt


def headers(credential):
    return {"Authorization": "Bearer " + credential.token}


@pytest.mark.parametrize("role", tokens.ROLES)
def test_read_routes_require_token_and_share_cli_documents(api, role):
    ctx, client, creds, run, attempt = api
    h = headers(creds[role])
    docs = {
        "/api/v1/health": {"schema": "etl-craft/health/1", "status": "ok"},
        "/api/v1/pipelines": to_json(inspect.list_pipelines(ctx)),
        "/api/v1/pipelines/P": to_json(api_reads.get_pipeline(ctx, "P")),
        "/api/v1/pipelines/P/runs": to_json(api_reads.get_runs(ctx, "P")),
        f"/api/v1/runs/{run}": to_json(api_reads.get_run_status(ctx, run).run),
        f"/api/v1/runs/{run}/tasks": to_json(inspect.pipeline_steps(ctx, "P")),
        f"/api/v1/runs/{run}/tasks/T/explain": to_json(explain_task(ctx, "P", "T")),
        f"/api/v1/attempts/{attempt}": to_json(api_reads.get_attempt(ctx, attempt)),
    }
    for path, expected in docs.items():
        assert client.get(path).status_code == 401
        response = client.get(path, headers=h)
        assert response.status_code == 200, response.text
        assert response.json() == expected
    assert (
        client.get("/api/v1/health", headers={"Authorization": "Bearer unknown"}).status_code == 401
    )
    assert client.get("/api/v1/runs/999999", headers=h).status_code == 404
    assert client.get("/api/v1/attempts/999999", headers=h).status_code == 404
    assert client.get("/api/v1/pipelines/missing", headers=h).status_code == 404
    assert client.get("/api/v1/pipelines/P/runs?limit=101", headers=h).status_code == 422
    assert (
        client.get("/api/v1/openapi.json").json()["components"]["securitySchemes"]["HTTPBearer"][
            "scheme"
        ]
        == "bearer"
    )


@pytest.mark.parametrize("role", tokens.ROLES)
def test_every_write_route_enforces_role_and_passes_actor_to_service(api, role, monkeypatch):
    ctx, client, creds, run, _attempt = api
    calls = []

    def done(caller, *args, **kwargs):
        calls.append((caller.actor.name, args, kwargs))
        return inspect.list_pipelines(ctx)

    cases = [
        ("/api/v1/pipelines/P/runs", "etl_craft.services.operations.runs.trigger_run", {}),
        (
            "/api/v1/pipelines/P/backfills",
            "etl_craft.services.operations.backfills.run_backfill",
            {"first": "2026-01-01", "last": "2026-01-02", "reason": "test"},
        ),
        (
            "/api/v1/pipelines/P/pause",
            "etl_craft.services.operations.pipelines.set_pause",
            {"reason": "test"},
        ),
        (
            "/api/v1/pipelines/P/resume",
            "etl_craft.services.operations.pipelines.set_pause",
            {"reason": "test"},
        ),
        (
            f"/api/v1/runs/{run}/cancel",
            "etl_craft.services.operations.runs.cancel_run",
            {"reason": "test"},
        ),
        (
            f"/api/v1/runs/{run}/tasks/T/mark",
            "etl_craft.services.operations.tasks.mark_task",
            {"status": "SUCCESS", "reason": "test"},
        ),
        (
            f"/api/v1/runs/{run}/tasks/T/rerun",
            "etl_craft.services.operations.tasks.rerun_task",
            {"reason": "test"},
        ),
    ]
    for path, operation, body in cases:
        monkeypatch.setattr(operation, done)
        assert client.post(path, json=body).status_code == 401
        response = client.post(path, json=body, headers=headers(creds[role]))
        assert response.status_code == (403 if role == "viewer" else 200), response.text
        if role != "viewer":
            assert calls[-1][0] == role
            assert calls[-1][1][0] == "P"
            if "/runs/" in path:
                assert calls[-1][2]["selector"].run_id == run
    if role == "viewer":
        assert calls == []
    response = client.post(
        "/api/v1/pipelines/P/pause", json={"reason": " "}, headers=headers(creds["operator"])
    )
    assert response.status_code == 422


@pytest.mark.parametrize("role", tokens.ROLES)
def test_token_admin_routes_and_immediate_revocation(api, role):
    _ctx, client, creds, _run, _attempt = api
    h = headers(creds[role])
    assert client.get("/api/v1/tokens", headers=h).status_code == (200 if role == "admin" else 403)
    response = client.post(
        "/api/v1/tokens", headers=h, json={"name": "new", "role": "viewer", "expires": "90d"}
    )
    assert response.status_code == (200 if role == "admin" else 403)
    response = client.post(f"/api/v1/tokens/{creds['viewer'].metadata.token_id}/revoke", headers=h)
    assert response.status_code == (200 if role == "admin" else 403)
    if role == "admin":
        assert client.get("/api/v1/health", headers=headers(creds["viewer"])).status_code == 401
        assert "token_sha256" not in response.text.lower()
    assert client.get("/api/v1/tokens").status_code == 401
    assert client.post("/api/v1/tokens", json={"name": "x", "role": "viewer"}).status_code == 401
    assert client.post("/api/v1/tokens/1/revoke").status_code == 401


def test_tokens_store_hash_only_expiry_validation_and_audit(api):
    ctx, client, creds, _run, _attempt = api
    value = tokens.create_token(ctx, "expires", "operator", "1d")
    with ctx.engine.begin() as conn:
        digest = conn.execute(
            text("SELECT TOKEN_SHA256 AS digest FROM CFG_API_TOKENS WHERE TOKEN_ID=:id"),
            {"id": value.metadata.token_id},
        ).scalar_one()
        assert digest == sha256_hex(value.token.encode())
        conn.execute(
            text("UPDATE CFG_API_TOKENS SET EXPIRES_AT=:now WHERE TOKEN_ID=:id"),
            {"now": datetime.now(UTC) - timedelta(seconds=1), "id": value.metadata.token_id},
        )
        requests = conn.execute(
            text("SELECT COMMAND AS command, ARGUMENTS AS arguments FROM AUD_ACTIONS")
        ).all()
        assert len(requests) == 4
        assert all(row.command == "token create" for row in requests)
        audit = str(requests)
        assert value.token not in audit
    assert client.get("/api/v1/health", headers=headers(value)).status_code == 401
    assert tokens.authenticate(ctx, "") is None
    assert tokens.authenticate(ctx, "x" * 257) is None
    for expires in ["0d", "-1d", "1h", "36501d", "9" * 5000 + "d"]:
        with pytest.raises(UsageError):
            tokens.create_token(ctx, "bad", "viewer", expires)
    with pytest.raises(UsageError):
        tokens.create_token(ctx, "bad", "root")
    with pytest.raises(UsageError):
        tokens.revoke_token(ctx, 999999)
    response = client.post(
        "/api/v1/pipelines/P/pause",
        headers=headers(creds["operator"]),
        json={"reason": "maintenance"},
    )
    assert response.status_code == 200, response.text
    assert response.json()["pipeline"]["paused"]["paused_by"] == "operator"
    assert (
        client.post(
            "/api/v1/pipelines/P/pause",
            headers=headers(creds["operator"]),
            json={"reason": "again"},
        ).status_code
        == 409
    )
    with ctx.engine.connect() as conn:
        assert (
            conn.execute(text("SELECT PAUSED_BY AS actor FROM AUD_PIPELINE_PAUSES")).scalar_one()
            == "operator"
        )


def test_log_streaming_offsets_and_path_boundaries(api, tmp_path):
    ctx, client, creds, run, attempt = api
    path = ctx.config.log_dir / "P" / f"run-{run}" / "T.attempt-1.log"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"first\nsecond\n")
    url = f"/api/v1/attempts/{attempt}/log"
    for role in tokens.ROLES:
        assert client.get(url, headers=headers(creds[role])).status_code == 404
    with ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET LOG_PATH=:path WHERE ATTEMPT_ID=:id"),
            {"path": str(path), "id": attempt},
        )
    for role in tokens.ROLES:
        assert client.get(url, headers=headers(creds[role])).content == b"first\nsecond\n"
    assert client.get(url).status_code == 401
    assert client.get(url + "?offset=6", headers=headers(creds["viewer"])).content == b"second\n"
    assert client.get(url + "?offset=-1", headers=headers(creds["viewer"])).status_code == 422
    outside = tmp_path / "secret"
    outside.write_text("secret")
    path.unlink()
    path.symlink_to(outside)
    assert client.get(url, headers=headers(creds["viewer"])).status_code == 404


def test_run_history_cursor_is_bounded_and_exact(api):
    ctx, client, creds, run, _attempt = api
    with ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_PIPELINES_RUN_LOG SET STATUS='SUCCESS' WHERE PIPELINE_RUN_ID=:id"),
            {"id": run},
        )
        pipeline = conn.execute(
            text("SELECT PIPELINE_ID AS id FROM CFG_PIPELINES WHERE PIPELINE_CODE='P'")
        ).scalar_one()
        second = start_run(conn, pipeline)
    h = headers(creds["viewer"])
    first = client.get("/api/v1/pipelines/P/runs?limit=1", headers=h).json()
    assert first["runs"][0]["pipeline_run_id"] == second
    assert first["before"] == second
    last = client.get(f"/api/v1/pipelines/P/runs?limit=1&before={second}", headers=h).json()
    assert last["runs"][0]["pipeline_run_id"] == run
    assert last["before"] is None


def test_token_metadata_does_not_capture_hash_or_accept_project_scope(api):
    ctx, client, creds, _run, _attempt = api
    with ctx.engine.begin() as conn:
        assert (
            conn.execute(
                text("SELECT COUNT(*) FROM AUD_METADATA_CHANGES WHERE TABLE_NAME='CFG_API_TOKENS'")
            ).scalar_one()
            == 0
        )
        conn.execute(
            text("UPDATE CFG_API_TOKENS SET PROJECT_ID=1 WHERE TOKEN_ID=:id"),
            {"id": creds["viewer"].metadata.token_id},
        )
    assert client.get("/api/v1/health", headers=headers(creds["viewer"])).status_code == 401


def test_engine_unavailable_response_does_not_echo_storage_parameters(api, monkeypatch):
    from sqlalchemy.exc import SQLAlchemyError

    _ctx, client, _creds, _run, _attempt = api

    def broken(*args):
        raise SQLAlchemyError("secret credential")

    monkeypatch.setattr(tokens, "authenticate", broken)
    response = client.get("/api/v1/health", headers={"Authorization": "Bearer secret"})
    assert response.status_code == 503
    assert "secret" not in response.text


def test_live_http_trigger_is_executed_by_overseer_and_shutdown_is_clean(cli_project):
    import signal
    import socket
    import time
    from urllib.error import URLError
    from urllib.request import Request, urlopen

    import yaml

    p = cli_project
    with socket.socket() as bound:
        bound.bind(("127.0.0.1", 0))
        port = bound.getsockname()[1]
    path = p.config.config_path
    config = yaml.safe_load(path.read_text())
    config["Orchestration"]["Api_address"] = f"127.0.0.1:{port}"
    path.write_text(yaml.safe_dump(config, sort_keys=False))
    code, output = p.run(
        "token", "create", "--name", "web", "--role", "operator", "--format", "json"
    )
    assert code == 0, output
    credential = json.loads(output)["token"]
    server = p.start("server")
    p.wait_for("SELECT COUNT(*) FROM AUD_OVERSEERS", expected=1)
    base = f"http://127.0.0.1:{port}/api/v1"
    deadline = time.monotonic() + 10
    while True:
        try:
            with urlopen(
                Request(base + "/health", headers={"Authorization": "Bearer " + credential}),
                timeout=2,
            ) as response:
                assert response.status == 200
            break
        except URLError:
            assert time.monotonic() < deadline, server.output
            time.sleep(0.02)
    with urlopen(
        Request(
            base + "/pipelines/P/runs",
            data=b'{"reason":"HTTP refresh"}',
            headers={"Authorization": "Bearer " + credential, "Content-Type": "application/json"},
        ),
        timeout=5,
    ) as response:
        result = json.loads(response.read())
    run = result["run"]["pipeline_run_id"]
    assert result["run"]["started_by"] == "web"
    p.wait_for(
        f"SELECT STATUS FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID={run}", expected="SUCCESS"
    )
    server.signal(signal.SIGTERM)
    assert server.wait() == 0, server.output
    p.wait_for("SELECT COUNT(*) FROM AUD_OVERSEERS WHERE STOPPED_AT IS NOT NULL", expected=1)
    with p.engine.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT ACTOR AS actor FROM AUD_ACTIONS WHERE COMMAND='run' "
                    "ORDER BY ACTION_ID DESC LIMIT 1"
                )
            ).scalar_one()
            == "web"
        )
        arguments = conn.execute(
            text(
                "SELECT ARGUMENTS AS args FROM AUD_ACTIONS WHERE COMMAND='run' "
                "ORDER BY ACTION_ID DESC LIMIT 1"
            )
        ).scalar_one()
        arguments = json.loads(arguments) if isinstance(arguments, str) else arguments
        assert arguments["reason"] == "HTTP refresh"


def test_tokens_work_after_released_schema_upgrade(empty_engine_db, tmp_path, monkeypatch):
    from etl_craft.engine.migrations import apply_pending_migrations
    from fixtures.released_schema import install

    db = empty_engine_db
    install(db, "0.2.0")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ETL_CRAFT_MIGRATIONS_DIR", raising=False)
    applied = apply_pending_migrations(db.engine)
    assert "0018_api_tokens.sql" in applied
    ctx = OperationContext(db.engine, db.config, current_actor())
    created = tokens.create_token(ctx, "upgraded", "viewer", "1d")
    assert tokens.authenticate(ctx, created.token).name == "upgraded"
    assert apply_pending_migrations(db.engine) == []
