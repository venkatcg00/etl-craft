"""Real token authorization and shared operation documents on both Engine DBs."""

import json
from dataclasses import fields, replace
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import event, text
from sqlalchemy import inspect as sql_inspect

from etl_craft.api.app import create_app
from etl_craft.core.actor import Actor, ActorKind, current_actor
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.core.errors import UsageError
from etl_craft.core.text import sha256_hex
from etl_craft.engine import transitions
from etl_craft.services.operations import OperationContext, to_json, tokens
from etl_craft.services.operations.actions import ACTIONS, action_path, invoke
from etl_craft.services.operations.requests import (
    AttemptId,
    HistoryInput,
    PipelineInput,
    RunId,
    RunSelection,
    TagFilter,
    TaskSelection,
)
from fixtures.metadata import add_pipeline, add_task, start_run

ROLES = tuple(role for role in ApiRole if role != ApiRole.AGENT)


def create_token(ctx, name, role, expires=None):
    request = tokens.TokenCreate(name=name, role=role, expires=expires or "365d")
    return invoke(ctx, ActionName.TOKEN_CREATE, request)


@pytest.fixture
def api(engine_db, tmp_path, request):
    ctx = OperationContext(
        engine_db.engine,
        replace(engine_db.config, log_dir=tmp_path / "logs"),
        Actor("owner", ActorKind.USER),
        role=ApiRole.ADMIN,
    )
    with ctx.engine.begin() as conn:
        engine_db.dialect.refresh_metadata_triggers(conn)
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "T", **getattr(request, "param", {}))
        run = start_run(conn, pipeline)
        summary = transitions.find_or_create_task_run(conn, task, run).task_run_id
        attempt = transitions.queue_attempt(conn, summary, current_actor())
    creds = {role: create_token(ctx, role, role) for role in ROLES}
    client = TestClient(create_app(ctx))
    return ctx, client, creds, run, attempt


def headers(credential):
    return {"Authorization": "Bearer " + credential.token}


@pytest.mark.parametrize("role", ROLES)
def test_read_routes_require_token_and_share_cli_documents(api, role):
    ctx, client, creds, run, attempt = api
    h = headers(creds[role])
    selected = f"pipeline_code=P&run_id={run}"
    docs = {
        "/api/v1/health": {"schema": "etl-craft/health/1", "status": "ok"},
        "/api/v1/projects/default/pipelines": to_json(
            invoke(ctx, ActionName.PIPELINES, TagFilter())
        ),
        "/api/v1/projects/default/pipelines/P": to_json(
            invoke(ctx, ActionName.PIPELINE, PipelineInput(pipeline_code="P"))
        ),
        "/api/v1/projects/default/history?pipeline_code=P&all=true": to_json(
            invoke(ctx, ActionName.HISTORY, HistoryInput(pipeline_code="P", all=True))
        ),
        f"/api/v1/projects/default/runs/{run}": to_json(
            invoke(ctx, ActionName.RUN_DETAIL, RunId(run_id=run))
        ),
        f"/api/v1/projects/default/steps?{selected}": to_json(
            invoke(ctx, ActionName.STEPS, RunSelection(pipeline_code="P", run_id=run))
        ),
        f"/api/v1/projects/default/explanations?{selected}&task_code=T": to_json(
            invoke(
                ctx,
                ActionName.EXPLAIN,
                TaskSelection(pipeline_code="P", task_code="T", run_id=run),
            )
        ),
        f"/api/v1/projects/default/attempts/{attempt}": to_json(
            invoke(ctx, ActionName.ATTEMPT, AttemptId(attempt_id=attempt))
        ),
    }
    for path, expected in docs.items():
        assert client.get(path).status_code == 401
        response = client.get(path, headers=h)
        assert response.status_code == 200, response.text
        assert response.json() == expected
    assert (
        client.get("/api/v1/health", headers={"Authorization": "Bearer unknown"}).status_code == 401
    )

    assert client.get("/api/v1/projects/default/runs/999999", headers=h).status_code == 404
    assert client.get("/api/v1/projects/default/attempts/999999", headers=h).status_code == 404
    assert client.get("/api/v1/projects/default/pipelines/missing", headers=h).status_code == 404
    refused = client.get("/api/v1/projects/default/history?pipeline_code=P&limit=101", headers=h)
    assert refused.status_code == 400 and "`limit`" in refused.json()["message"]
    unknown = client.get("/api/v1/projects/default/history?pipeline_code=P&unknown=1", headers=h)
    assert unknown.status_code == 400 and "`unknown`" in unknown.json()["message"]
    assert (
        client.get("/api/v1/openapi.json").json()["components"]["securitySchemes"]["HTTPBearer"][
            "scheme"
        ]
        == "bearer"
    )


def audit_counts(engine):
    with engine.connect() as conn:
        names = [n for n in sql_inspect(conn).get_table_names() if n.upper().startswith("AUD_")]
        return {n: conn.execute(text(f'SELECT COUNT(*) FROM "{n}"')).scalar_one() for n in names}


def test_read_routes_record_only_named_admin_access_or_explicit_refusals(api):
    ctx, client, creds, run, attempt = api
    given = {"pipeline_code": "P", "task_code": "T", "run_id": run, "attempt_id": attempt}
    given |= {
        "control_id": 1,
        "target": "staging.t",
        "project": "default",
        "reason": "Review",
        "upload_id": 1,
        "version": "test",
    }
    audited = {ActionName.PROJECT_LIST, ActionName.POOL_LIST, ActionName.ACCESS_LIST}
    reads = [action for action in ACTIONS.values() if action.method == "GET"]
    # Hourly token-use history is covered separately; these reads start inside that hour.
    with ctx.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_API_TOKENS SET LAST_USED_AT=CURRENT_TIMESTAMP"))
    before = audit_counts(ctx.engine)
    answered = {}
    for action in reads:
        names = {f.name for f in fields(action.request)}
        values = {k: v for k, v in given.items() if k in names}
        path = action_path(action).format(**{"project": "default", **values})
        query = {k: v for k, v in values.items() if "{" + k + "}" not in action_path(action)}
        response = client.get(path, params=query, headers=headers(creds[ApiRole.ADMIN]))
        answered[action.name] = response.status_code
        expected = dict(before)
        if action.name in audited or action.name in {
            ActionName.CONTROL_REQUEST,
            ActionName.AGENT_UPLOAD_CONTENT,
        }:
            table = next(name for name in before if name.upper() == "AUD_ACTIONS")
            expected[table] += 1
            with ctx.engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT COMMAND AS command,ACTOR AS actor FROM AUD_ACTIONS "
                        "ORDER BY ACTION_ID DESC LIMIT 1"
                    )
                ).one()
                assert row.command == action.name and row.actor == ApiRole.ADMIN
                if action.name == ActionName.CONTROL_REQUEST:
                    assert response.status_code == 404
                    assert response.json()["error"] == "ResourceNotFoundError"
        assert audit_counts(ctx.engine) == expected, action.name
        before = expected
    # Most reads answer; the rest refuse this fixture's data without additional writes.
    assert sum(code == 200 for code in answered.values()) > len(reads) // 2, answered
    assert all(code < 500 for code in answered.values()), answered


@pytest.mark.parametrize("role", ROLES)
def test_catalog_gaps_require_viewer_and_do_not_publish_lineage(api, role):
    ctx, client, creds, _run, _attempt = api
    assert client.get("/api/v1/projects/default/catalog/gaps").status_code == 401
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
            SELECT_QUERY="SELECT 1 AS id",
        )
    with ctx.engine.connect() as conn:
        before = conn.execute(
            text("SELECT COUNT(*) AS count FROM AUD_LINEAGE_VERSIONS")
        ).scalar_one()
    statements = []

    def capture(_conn, _cursor, statement, *_args):
        statements.append(statement)

    event.listen(ctx.engine, "before_cursor_execute", capture)
    response = client.get("/api/v1/projects/default/catalog/gaps", headers=headers(creds[role]))
    assert response.status_code == 200
    body = response.json()
    assert body["schema"] == "etl-craft/catalog-gaps/1"
    assert body["compared_at"] is None and body["contract_version"]
    assert any(r["table"] == "api.output" for r in body["records"])
    compared = client.get(
        "/api/v1/projects/default/catalog/gaps?compare=true", headers=headers(creds[role])
    )
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
    response = client.get(
        "/api/v1/projects/default/catalog/gaps?compare=true", headers=headers(creds["viewer"])
    )
    assert response.status_code == 200
    assert response.json()["records"] == []
    assert response.json()["compared_at"] is None


@pytest.mark.parametrize("role", ROLES)
def test_every_write_route_enforces_role_and_passes_actor_to_service(api, role, monkeypatch):
    ctx, client, creds, run, _attempt = api
    original_control = ACTIONS[ActionName.CONTROL]
    calls = []

    def done(caller, request):
        calls.append((caller.actor.name, request))
        return invoke(ctx, ActionName.PIPELINES, TagFilter())

    cases = [
        (
            ActionName.RUN,
            {"pipeline_code": "P", "task_code": "T", "run_id": run, "standalone": True},
        ),
        (ActionName.RUN, {"pipeline_code": "P"}),
        (
            ActionName.RUN,
            {
                "pipeline_code": "P",
                "backfill_from": "2026-01-01",
                "backfill_to": "2026-01-02",
                "reason": "test",
            },
        ),
        (ActionName.CONTROL, {"pipeline_code": "P", "action": "PAUSE", "reason": "test"}),
        (ActionName.CONTROL, {"pipeline_code": "P", "action": "RESUME", "reason": "test"}),
        (
            ActionName.MARK,
            {
                "pipeline_code": "P",
                "run_id": run,
                "task_code": "T",
                "status": "SUCCESS",
                "reason": "t",
            },
        ),
        (
            ActionName.MARK,
            {
                "pipeline_code": "P",
                "run_id": run,
                "task_code": "T",
                "watermark": "0",
                "reason": "r",
            },
        ),
        (
            ActionName.RUN,
            {"pipeline_code": "P", "run_id": run, "task_code": "T", "rerun": True, "reason": "t"},
        ),
    ]
    for name, body in cases:
        action = ACTIONS[name]
        monkeypatch.setitem(ACTIONS, name, replace(action, call=done))
        assert (
            client.post(action_path(action).format(project="default"), json=body).status_code == 401
        )
        response = client.post(
            action_path(action).format(project="default"), json=body, headers=headers(creds[role])
        )
        assert response.status_code == (403 if role == "viewer" else 200), response.text
        if role != "viewer":
            caller, request = calls[-1]
            assert caller == role
            assert request.pipeline_code == "P"
            assert request.run_id == body.get("run_id")
    if role == "viewer":
        assert calls == []
    monkeypatch.setitem(ACTIONS, ActionName.CONTROL, original_control)
    response = client.post(
        "/api/v1/projects/default/controls",
        json={"pipeline_code": "P", "action": "PAUSE", "reason": " "},
        headers=headers(creds["operator"]),
    )
    assert response.status_code == 400 and "--reason" in response.json()["message"]


@pytest.mark.parametrize("role", ROLES)
def test_token_admin_routes_and_immediate_revocation(api, role):
    _ctx, client, creds, _run, _attempt = api
    h = headers(creds[role])
    assert client.get("/api/v1/tokens", headers=h).status_code == (200 if role == "admin" else 403)
    response = client.post(
        "/api/v1/tokens", headers=h, json={"name": "new", "role": "viewer", "expires": "90d"}
    )
    assert response.status_code == (200 if role == "admin" else 403)
    response = client.post(
        "/api/v1/tokens/revoke", headers=h, json={"token_id": creds["viewer"].metadata.token_id}
    )
    assert response.status_code == (200 if role == "admin" else 403)
    if role == "admin":
        assert client.get("/api/v1/health", headers=headers(creds["viewer"])).status_code == 401
        assert "token_sha256" not in response.text.lower()
    assert client.get("/api/v1/tokens").status_code == 401
    assert client.post("/api/v1/tokens", json={"name": "x", "role": "viewer"}).status_code == 401
    assert client.post("/api/v1/tokens/revoke", json={"token_id": 1}).status_code == 401


def test_tokens_store_hash_only_expiry_validation_and_audit(api):
    ctx, client, creds, _run, _attempt = api
    value = create_token(ctx, "expires", "operator", "1d")
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
            create_token(ctx, "bad", "viewer", expires)
    root = client.post(
        "/api/v1/tokens", headers=headers(creds["admin"]), json={"name": "bad", "role": "root"}
    )
    assert root.status_code == 400 and "`role`" in root.json()["message"]
    with pytest.raises(UsageError):
        invoke(ctx, ActionName.TOKEN_REVOKE, tokens.TokenRevoke(token_id=999999))
    pause = {"pipeline_code": "P", "action": "PAUSE", "reason": "maintenance"}
    response = client.post(
        "/api/v1/projects/default/controls", headers=headers(creds["operator"]), json=pause
    )
    assert response.status_code == 200, response.text
    assert response.json()["pipeline"]["paused"]["paused_by"] == "operator"
    assert (
        client.post(
            "/api/v1/projects/default/controls",
            headers=headers(creds["operator"]),
            json={**pause, "reason": "again"},
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
    url = f"/api/v1/projects/default/attempts/{attempt}/log"
    for role in ROLES:
        assert client.get(url, headers=headers(creds[role])).status_code == 404
    with ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET LOG_PATH=:path WHERE ATTEMPT_ID=:id"),
            {"path": str(path), "id": attempt},
        )
    for role in ROLES:
        assert client.get(url, headers=headers(creds[role])).content == b"first\nsecond\n"
    assert client.get(url).status_code == 401
    assert client.get(url + "?offset=6", headers=headers(creds["viewer"])).content == b"second\n"
    assert client.get(url + "?offset=-1", headers=headers(creds["viewer"])).status_code == 400
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
    first = client.get(
        "/api/v1/projects/default/history?pipeline_code=P&all=true&limit=1", headers=h
    ).json()
    assert first["entries"][0]["pipeline_run_id"] == second
    assert first["before"] == second
    last = client.get(
        f"/api/v1/projects/default/history?pipeline_code=P&all=true&limit=1&before={second}",
        headers=h,
    ).json()
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

    monkeypatch.setattr("etl_craft.api.routes.authenticate", broken)
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
            base + "/projects/default/runs",
            data=b'{"pipeline_code":"P","reason":"HTTP refresh"}',
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
    created = create_token(ctx, "upgraded", "viewer", "1d")
    assert tokens.authenticate(ctx, created.token).name == "upgraded"
    assert apply_pending_migrations(db.engine) == []


@pytest.mark.parametrize(
    "api",
    [
        {
            "TARGET_OBJECT": "s.events",
            "SQL_ACTION": "APPEND_TABLE",
            "SELECT_QUERY": "SELECT id FROM raw.events WHERE id > $$watermark",
            "WATERMARK_COLUMN": "id",
            "WATERMARK_TYPE": "NUMBER",
            "WATERMARK_START": "0",
        }
    ],
    indirect=True,
)
def test_watermark_reset_validates_and_audits_the_api_actor(api):
    from etl_craft.config import ConnectionProfile
    from etl_craft.engine.repository.offsets import fetch_task_offset

    ctx, client, creds, run, attempt = api
    config = replace(
        ctx.config,
        warehouse=ConnectionProfile(
            "WAREHOUSE", "local", "jdbc:duckdb:warehouse.duckdb", "", "NONE"
        ),
    )
    client = TestClient(create_app(replace(ctx, config=config)))
    with ctx.engine.begin() as conn:
        task = conn.execute(text("SELECT TASK_ID FROM CFG_TASKS WHERE TASK_CODE='T'")).scalar_one()
    path = "/api/v1/projects/default/marks"
    h = headers(creds["operator"])
    selected = {"pipeline_code": "P", "task_code": "T", "run_id": run}
    for body in (
        {"reason": "reset"},
        {"status": "SUCCESS", "watermark": "1", "reason": "reset"},
        {"watermark": "1", "rows": 1, "reason": "reset"},
    ):
        assert client.post(path, json={**selected, **body}, headers=h).status_code == 400
    reset = {**selected, "watermark": "1", "reason": "reset"}
    assert client.post(path, json=reset, headers=h).status_code == 409
    with ctx.engine.begin() as conn:
        transitions.cancel_attempt(conn, attempt, current_actor(), error_message="cancelled")
    response = client.post(path, json=reset, headers=h)
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


def test_run_refuses_unknown_fields_and_host_only_options(api):
    _ctx, client, creds, _run, _attempt = api
    h = headers(creds["operator"])
    task = {"pipeline_code": "P", "task_code": "T", "standalone": True}
    unknown = client.post(
        "/api/v1/projects/default/runs", json={**task, "unexpected": True}, headers=h
    )
    assert unknown.status_code == 400 and "`unexpected`" in unknown.json()["message"]
    forced = client.post(
        "/api/v1/projects/default/runs", json={"pipeline_code": "P", "force": True}, headers=h
    )
    assert forced.status_code == 400
    assert "`force` works only on the controller's host" in forced.json()["message"]


def test_scoped_control_routes_share_durable_identity_and_role_policy(api):
    _, client, creds, run, attempt = api
    path = "/api/v1/projects/default/controls"
    body = {
        "pipeline_code": "P",
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
        f"/api/v1/projects/default/control-requests/{document['control_id']}",
        headers=headers(creds["viewer"]),
    )
    assert read.status_code == 200 and read.json() == document
    assert (
        client.post(
            path, json={**body, "unexpected": True}, headers=headers(creds["operator"])
        ).status_code
        == 400
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
    caps = client.get(
        f"/api/v1/projects/default/capabilities?pipeline_code=P&run_id={run}", headers=viewer
    )
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
    assert (
        client.post("/api/v1/projects/default/controls/bulk", json=body, headers=viewer).status_code
        == 403
    )
    result = client.post("/api/v1/projects/default/controls/bulk", json=body, headers=operator)
    assert result.status_code == 200, result.text
    outcomes = result.json()["outcomes"]
    assert outcomes[0]["control"]["task_code"] == "T"
    assert outcomes[1]["control"] is None and outcomes[1]["error"]
    repeated = client.post("/api/v1/projects/default/controls/bulk", json=body, headers=operator)
    assert (
        repeated.json()["outcomes"][0]["control"]["control_id"]
        == outcomes[0]["control"]["control_id"]
    )
    rejected = client.post(
        "/api/v1/projects/default/controls/bulk",
        json={"requests": [{**body["requests"][0], "unexpected": True}]},
        headers=operator,
    )
    assert rejected.status_code == 400


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
    response = client.get(
        f"/api/v1/projects/default/attempts/{attempt}", headers=headers(creds[ApiRole.VIEWER])
    )
    assert response.status_code == 200
    body = response.json()
    assert body["failure_class"] == "PERMANENT"
    assert body["failure_source"] == "ProgrammingError"
    assert body["failure_code"] == "42P01"
    assert body["retry_on"] == "TRANSIENT"
    assert not body["retryable"]
    assert "RETRY_ON=TRANSIENT" in body["retry_reason"]


def test_an_exact_run_id_cannot_cross_the_route_project_even_for_admin(api):
    ctx, client, creds, _run, _attempt = api
    with ctx.engine.begin() as conn:
        other = conn.execute(
            text(
                "INSERT INTO CFG_PROJECTS (PROJECT_CODE,PROJECT_NAME,DEFAULT_POOL) "
                "VALUES ('other','Other','default') RETURNING PROJECT_ID AS project_id"
            )
        ).scalar_one()
        pipeline = conn.execute(
            text(
                "INSERT INTO CFG_PIPELINES (PROJECT_ID,PIPELINE_CODE,PIPELINE_NAME,REFRESH_TYPE) "
                "VALUES (:id,'P','Other P','FULL') RETURNING PIPELINE_ID AS pipeline_id"
            ),
            {"id": other},
        ).scalar_one()
        foreign_run = start_run(conn, pipeline)
    credential = headers(creds[ApiRole.ADMIN])
    assert (
        client.get(f"/api/v1/projects/default/runs/{foreign_run}", headers=credential).status_code
        == 404
    )
    assert (
        client.get(f"/api/v1/projects/other/runs/{foreign_run}", headers=credential).status_code
        == 200
    )
    response = client.get("/api/v1/projects/other/pipelines", headers=credential)
    assert [row["pipeline_id"] for row in response.json()["pipelines"]] == [pipeline]
    from etl_craft.services.operations.requests import NoInput

    invoke(ctx, ActionName.DOCS_VERSION, NoInput())
    invoke(replace(ctx, project="other"), ActionName.DOCS_VERSION, NoInput())
    audit = client.get("/api/v1/projects/other/audit", headers=credential).json()
    with ctx.engine.connect() as conn:
        foreign_actions = set(
            conn.execute(
                text("SELECT ACTION_ID AS action_id FROM AUD_ACTIONS WHERE PROJECT_ID=1")
            ).scalars()
        )
    assert foreign_actions and not foreign_actions.intersection(
        row["action_id"] for row in audit["actions"]
    )
    owned_pipeline_changes = [
        row for row in audit["changes"] if row["table_name"] == "CFG_PIPELINES"
    ]
    assert owned_pipeline_changes
    assert all(row["after_json"]["project_id"] == other for row in owned_pipeline_changes)


def test_foreign_project_prefix_cannot_masquerade_as_a_local_pipeline_grant(api):
    from etl_craft.services.operations.status import DependencyView
    from etl_craft.services.operations.visibility import visible_result

    ctx, _client, _creds, _run, _attempt = api
    with ctx.engine.begin() as conn:
        add_pipeline(conn, "foreign")
    value = DependencyView(
        "foreign.secret",
        "SUCCESS",
        "SUCCESS",
        True,
        "private status",
        selected_pipeline_run_id=777,
        pipeline_dependency_id=1,
    )
    redacted = visible_result(replace(ctx, role=ApiRole.VIEWER, project_id=1), value)
    assert redacted.selected_pipeline_run_id is None
    assert redacted.status is None and "private" not in redacted.reason
