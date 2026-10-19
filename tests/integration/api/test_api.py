"""Real token authorization and shared operation documents on both Engine DBs."""

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import event, text
from sqlalchemy import inspect as sql_inspect

from etl_craft.api.app import create_app
from etl_craft.core.actor import Actor, ActorKind, current_actor
from etl_craft.core.enums import ApiRole
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
from etl_craft.services.operations.models import HistoryQuery
from etl_craft.services.operations.status import explain_task
from fixtures.metadata import add_pipeline, add_task, start_run


@pytest.fixture
def api(engine_db, tmp_path):
    ctx = OperationContext(
        engine_db.engine,
        replace(engine_db.config, log_dir=tmp_path / "logs"),
        Actor("owner", ActorKind.USER),
        role=ApiRole.ADMIN,
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
        "/api/v1/pipelines/P/runs": to_json(
            inspect.run_history(ctx, "P", HistoryQuery(all_runs=True))
        ),
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
def test_catalog_gaps_require_viewer_and_do_not_publish_lineage(api, role):
    ctx, client, creds, _run, _attempt = api
    assert client.get("/api/v1/catalog/gaps").status_code == 401
    with ctx.engine.begin() as conn:
        pipeline = conn.execute(
            text("SELECT PIPELINE_ID AS pipeline_id FROM CFG_PIPELINES WHERE PIPELINE_CODE='P'")
        ).scalar_one()
        add_task(
            conn,
            pipeline,
            "lineage",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT="api.output",
            SOURCE_SQL="SELECT 1 AS id",
        )
    with ctx.engine.connect() as conn:
        before = conn.execute(
            text("SELECT COUNT(*) AS count FROM AUD_LINEAGE_VERSIONS")
        ).scalar_one()
    statements = []

    def capture(_conn, _cursor, statement, *_args):
        statements.append(statement)

    event.listen(ctx.engine, "before_cursor_execute", capture)
    response = client.get("/api/v1/catalog/gaps", headers=headers(creds[role]))
    assert response.status_code == 200
    body = response.json()
    assert body["schema"] == "etl-craft/catalog-gaps/1"
    assert body["compared_at"] is None and body["contract_version"]
    assert any(r["table"] == "api.output" for r in body["records"])
    compared = client.get("/api/v1/catalog/gaps?compare=true", headers=headers(creds[role]))
    assert compared.status_code == 200 and compared.json()["compared_at"]
    event.remove(ctx.engine, "before_cursor_execute", capture)
    assert not any("AUD_LINEAGE_VERSION" in sql.upper() for sql in statements)
    with ctx.engine.connect() as conn:
        after = conn.execute(
            text("SELECT COUNT(*) AS count FROM AUD_LINEAGE_VERSIONS")
        ).scalar_one()
    assert after == before


def test_empty_catalog_does_not_claim_a_warehouse_check(api):
    _ctx, client, creds, _run, _attempt = api
    response = client.get("/api/v1/catalog/gaps?compare=true", headers=headers(creds["viewer"]))
    assert response.status_code == 200
    assert response.json()["records"] == []
    assert response.json()["compared_at"] is None


@pytest.mark.parametrize("role", tokens.ROLES)
def test_every_write_route_enforces_role_and_passes_actor_to_service(api, role, monkeypatch):
    ctx, client, creds, run, _attempt = api
    calls = []

    def done(caller, *args, **kwargs):
        calls.append((caller.actor.name, args, kwargs))
        return inspect.list_pipelines(ctx)

    cases = [
        (
            "/api/v1/pipelines/P/tasks/T/run",
            "etl_craft.services.operations.runs.execute_run",
            {"run_id": run, "standalone": True},
        ),
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
            f"/api/v1/runs/{run}/tasks/T/mark",
            "etl_craft.services.operations.tasks.mark_task",
            {"status": "SUCCESS", "reason": "test"},
        ),
        (
            f"/api/v1/runs/{run}/tasks/T/mark",
            "etl_craft.services.operations.tasks.mark_task",
            {"watermark": "0", "reason": "reload"},
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
            if operation.endswith("execute_run"):
                request = calls[-1][1][0]
                assert request.pipeline_code == "P" and request.standalone
                assert request.selector.run_id == run
            else:
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
        assert len(requests) == len(creds) + 1
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
    assert first["entries"][0]["pipeline_run_id"] == second
    assert first["before"] == second
    last = client.get(f"/api/v1/pipelines/P/runs?limit=1&before={second}", headers=h).json()
    assert last["entries"][0]["pipeline_run_id"] == run
    assert last["before"] is None


def test_token_metadata_does_not_capture_hash_or_accept_project_scope(api):
    ctx, client, creds, _run, _attempt = api
    with ctx.engine.begin() as conn:
        changes = (
            conn.execute(
                text(
                    "SELECT AFTER_JSON FROM AUD_METADATA_CHANGES WHERE TABLE_NAME='CFG_API_TOKENS'"
                )
            )
            .scalars()
            .all()
        )
        assert changes
        for change in changes:
            assert "token_sha256" not in str(change).lower()
            for credential in creds.values():
                assert credential.token not in str(change)
        assert "TOKEN_SHA256" not in {
            column["name"].upper()
            for column in sql_inspect(conn).get_columns(
                "aud_cfg_api_tokens_history"
                if conn.dialect.name == "postgresql"
                else "AUD_CFG_API_TOKENS_HISTORY"
            )
        }
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


def test_live_http_trigger_is_executed_by_controller_and_shutdown_is_clean(cli_project):
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
    server = p.start("controller")
    p.wait_for("SELECT COUNT(*) FROM AUD_CONTROLLERS", expected=1)
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
    p.wait_for("SELECT COUNT(*) FROM AUD_CONTROLLERS WHERE STOPPED_AT IS NOT NULL", expected=1)
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
    ctx = OperationContext(db.engine, db.config, current_actor(), role=ApiRole.ADMIN)
    created = tokens.create_token(ctx, "upgraded", "viewer", "1d")
    assert tokens.authenticate(ctx, created.token).name == "upgraded"
    assert apply_pending_migrations(db.engine) == []


def test_watermark_reset_validates_and_audits_the_api_actor(api):
    from etl_craft.config import ConnectionProfile
    from etl_craft.engine.repository.offsets import fetch_task_offset

    ctx, client, creds, run, attempt = api
    config = replace(
        ctx.config,
        warehouse=ConnectionProfile(
            "WAREHOUSE", "local", "jdbc:duckdb:warehouse.duckdb", "", "none"
        ),
    )
    client = TestClient(create_app(replace(ctx, config=config)))
    with ctx.engine.begin() as conn:
        task = conn.execute(text("SELECT TASK_ID FROM CFG_TASKS WHERE TASK_CODE='T'")).scalar_one()
        from fixtures.metadata import set_task_target

        set_task_target(conn, task, "s.events")
        for name, value in {
            "SQL_ACTION": "APPEND_TABLE",
            "SOURCE_SQL": "SELECT id FROM raw.events WHERE id > $$watermark",
            "WATERMARK_COLUMN": "id",
            "WATERMARK_TYPE": "NUMBER",
            "WATERMARK_START": "0",
        }.items():
            conn.execute(
                text(
                    "INSERT INTO CFG_TASK_PARAMETERS(TASK_ID, PARAMETER_NAME, PARAMETER_VALUE) "
                    "VALUES (:task, :name, :value)"
                ),
                {"task": task, "name": name, "value": value},
            )
    path = f"/api/v1/runs/{run}/tasks/T/mark"
    h = headers(creds["operator"])
    for body in (
        {"reason": "reset"},
        {"status": "SUCCESS", "watermark": "1", "reason": "reset"},
        {"watermark": "1", "rows": 1, "reason": "reset"},
    ):
        assert client.post(path, json=body, headers=h).status_code == 400
    assert (
        client.post(path, json={"watermark": "1", "reason": "reset"}, headers=h).status_code == 409
    )
    with ctx.engine.begin() as conn:
        transitions.cancel_attempt(conn, attempt, current_actor(), error_message="cancelled")
    response = client.post(path, json={"watermark": "1", "reason": "reset"}, headers=h)
    assert response.status_code == 200, response.text
    with ctx.engine.connect() as conn:
        assert fetch_task_offset(conn, task).offset_value == "1"
        intervention = conn.execute(
            text("SELECT REQUESTED_BY, REASON FROM AUD_RUN_INTERVENTIONS WHERE TASK_ID=:task"),
            {"task": task},
        ).one()
        assert tuple(intervention) == ("operator", "reset")


def test_server_refuses_an_api_address_in_use_before_taking_leadership(cli_project):
    import socket

    import yaml

    p = cli_project
    with socket.socket() as taken:
        taken.bind(("127.0.0.1", 0))
        taken.listen()
        port = taken.getsockname()[1]
        path = p.config.config_path
        config = yaml.safe_load(path.read_text())
        config["Orchestration"]["Api_address"] = f"127.0.0.1:{port}"
        path.write_text(yaml.safe_dump(config, sort_keys=False))
        code, output = p.run("controller")
    assert code == 10, output
    assert f"Orchestration.Api_address 127.0.0.1:{port} cannot be used" in output
    assert "set Api_address to a free port" in output
    with p.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_CONTROLLERS")).scalar_one() == 0


def test_task_run_refuses_unknown_control_fields(api):
    _ctx, client, creds, _run, _attempt = api
    response = client.post(
        "/api/v1/pipelines/P/tasks/T/run",
        json={"standalone": True, "force": True},
        headers=headers(creds["operator"]),
    )
    assert response.status_code == 422
    assert "force" in response.text and "extra_forbidden" in response.text


def test_scoped_control_routes_share_durable_identity_and_role_policy(api):
    _, client, creds, run, attempt = api
    path = "/api/v1/pipelines/P/controls"
    body = {
        "action": "HOLD",
        "task_code": "T",
        "run_id": run,
        "reason": "maintenance",
        "request_key": "hold:T",
    }
    assert client.post(path, json=body, headers=headers(creds["viewer"])).status_code == 403
    first = client.post(path, json=body, headers=headers(creds["operator"]))
    assert first.status_code == 200
    document = first.json()
    assert document["schema"] == "etl-craft/control/1"
    assert document["action"] == "HOLD" and document["status"] == "COMPLETED"
    assert document["requested_by"] == "operator" and document["task_code"] == "T"
    assert document["run_status"] == "IN-PROGRESS" and document["attempt_ids"] == []
    repeated = client.post(path, json=body, headers=headers(creds["operator"]))
    assert repeated.json()["control_id"] == document["control_id"]
    read = client.get(
        f"/api/v1/control-requests/{document['control_id']}", headers=headers(creds["viewer"])
    )
    assert read.status_code == 200 and read.json() == document
    assert (
        client.post(
            path, json={**body, "unexpected": True}, headers=headers(creds["operator"])
        ).status_code
        == 422
    )
    stop = client.post(
        path,
        json={**body, "action": "STOP", "request_key": "stop:T"},
        headers=headers(creds["operator"]),
    )
    assert stop.status_code == 200 and stop.json()["status"] == "COMPLETED"
    assert stop.json()["run_status"] == "IN-PROGRESS"
    assert stop.json()["attempt_ids"] == [attempt]


def test_recovery_capabilities_and_bulk_routes_share_exact_requests(api):
    _, client, creds, run, _ = api
    viewer, operator = headers(creds["viewer"]), headers(creds["operator"])
    caps = client.get(f"/api/v1/pipelines/P/capabilities?run_id={run}", headers=viewer)
    assert caps.status_code == 200 and caps.json()["schema"] == "etl-craft/capabilities/1"
    assert all(not a["available"] for a in caps.json()["actions"])
    body = {
        "requests": [
            {
                "pipeline_code": "P",
                "run_id": run,
                "task_code": "T",
                "action": "HOLD",
                "reason": "selected task",
                "request_key": "bulk:T",
            },
            {
                "pipeline_code": "P",
                "run_id": run,
                "action": "RESTART",
                "reason": "restart",
                "request_key": "bulk:restart",
            },
        ]
    }
    assert client.post("/api/v1/controls/bulk", json=body, headers=viewer).status_code == 403
    result = client.post("/api/v1/controls/bulk", json=body, headers=operator)
    assert result.status_code == 200, result.text
    outcomes = result.json()["outcomes"]
    assert outcomes[0]["control"]["task_code"] == "T"
    assert outcomes[1]["control"] is None and outcomes[1]["error"]
    repeated = client.post("/api/v1/controls/bulk", json=body, headers=operator)
    assert (
        repeated.json()["outcomes"][0]["control"]["control_id"]
        == outcomes[0]["control"]["control_id"]
    )
    rejected = client.post(
        "/api/v1/controls/bulk",
        json={"requests": [{**body["requests"][0], "unexpected": True}]},
        headers=operator,
    )
    assert rejected.status_code == 422


def test_attempt_endpoint_retains_failure_evidence_and_retry_policy(api):
    from etl_craft.core.enums import FailureClass
    from etl_craft.core.failures import Failure

    ctx, client, creds, _run, attempt = api
    with ctx.engine.begin() as conn:
        transitions.claim_attempt(
            conn,
            attempt,
            current_actor(),
            owner="test",
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
        )
        transitions.record_attempt_failure(
            conn, attempt, Failure(FailureClass.PERMANENT, "ProgrammingError", "42P01")
        )
        transitions.finish_attempt(conn, attempt, "FAILED", current_actor(), owner="test")
    response = client.get(f"/api/v1/attempts/{attempt}", headers=headers(creds[ApiRole.VIEWER]))
    assert response.status_code == 200
    body = response.json()
    assert body["failure_class"] == "PERMANENT"
    assert body["failure_source"] == "ProgrammingError"
    assert body["failure_code"] == "42P01"
    assert body["retry_on"] == "TRANSIENT"
    assert not body["retryable"]
    assert "RETRY_ON=TRANSIENT" in body["retry_reason"]
