"""Agent credentials and exact owned attempts are the only entrance to the agent protocol."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from etl_craft import __version__
from etl_craft.api.app import create_app
from etl_craft.config import ConnectionProfile
from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.enums import ActionName, ApiRole, DeploymentProfile
from etl_craft.core.ownership import attempt_holder
from etl_craft.core.text import sha256_hex
from etl_craft.engine import transitions
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.serialization import encode
from etl_craft.services.operations.tokens import TokenCreate
from fixtures.agents import resources
from fixtures.metadata import add_pipeline, add_task

pytestmark = pytest.mark.parametrize(
    "empty_engine_db",
    [pytest.param("postgresql", marks=pytest.mark.engine_postgres)],
    indirect=True,
)


@pytest.fixture
def agent_scene(engine_db, tmp_path):
    warehouse = ConnectionProfile(
        "WAREHOUSE", "test", f"jdbc:duckdb:{tmp_path / 'w.duckdb'}", "", "NONE", schema="main"
    )
    config = replace(
        engine_db.config, deployment=DeploymentProfile.CONTROLLER_AGENTS, warehouse=warehouse
    )
    ctx = OperationContext(engine_db.engine, config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
    agent = invoke(ctx, ActionName.TOKEN_CREATE, TokenCreate(name="agent-a", role=ApiRole.AGENT))
    human = invoke(ctx, ActionName.TOKEN_CREATE, TokenCreate(name="person", role=ApiRole.ADMIN))
    with TestClient(create_app(ctx)) as client:
        headers = {"Authorization": f"Bearer {agent.token}"}
        registered = client.post(
            "/api/v1/agent/register",
            headers=headers,
            json={"version": __version__, "pool": "default", "resources": encode(resources())},
        )
        assert registered.status_code == 200, registered.text
        yield (
            engine_db,
            client,
            headers,
            {"Authorization": f"Bearer {human.token}"},
            agent.metadata.token_id,
            config,
        )


def queue(db):
    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "T")
        run = transitions.create_run(conn, pipeline, SYSTEM_ACTOR)
        summary = transitions.find_or_create_task_run(conn, task, run).task_run_id
        return transitions.queue_attempt(conn, summary, SYSTEM_ACTOR)


def claim(scene):
    db, client, headers, _, _, _ = scene
    attempt = queue(db)
    answer = client.post(
        "/api/v1/agent/claim",
        headers=headers,
        json={"version": __version__, "free_kinds": ["warehouse"]},
    )
    assert answer.status_code == 200, answer.text
    assert answer.json()["data"]["task"]["attempt_id"] == attempt
    return attempt


def test_agent_and_account_credentials_cannot_cross_protocols(agent_scene):
    _, client, agent, person, _, _ = agent_scene
    response = client.get("/api/v1/pipelines", headers=agent)
    assert response.status_code == 403
    response = client.post(
        "/api/v1/agent/deregister", headers=person, json={"version": __version__}
    )
    assert response.status_code == 403


def test_every_agent_request_refuses_a_different_package_version(agent_scene):
    _, client, headers, _, _, _ = agent_scene
    response = client.post(
        "/api/v1/agent/deregister", headers=headers, json={"version": "wrong-version"}
    )
    assert response.status_code == 409
    assert "wrong-version" in response.text and __version__ in response.text


def test_a_queued_attempt_is_claimed_by_its_token_identity(agent_scene):
    db, _, _, _, owner, _ = agent_scene
    attempt = claim(agent_scene)
    with db.engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT OWNER_ID AS owner_id, STATUS AS status "
                "FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"
            ),
            {"id": attempt},
        ).one()
    assert row.owner_id == str(owner)
    assert row.status == "CLAIMED"


def test_log_chunks_are_ordered_shared_and_idempotent(agent_scene):
    db, client, headers, _, _, _ = agent_scene
    attempt = claim(agent_scene)
    chunk = {"version": __version__, "attempt_id": attempt, "sequence": 0, "content": "hello\n"}
    for _ in range(2):
        response = client.post("/api/v1/agent/log", headers=headers, json=chunk)
        assert response.status_code == 200, response.text
    with db.engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT CONTENT AS content, BYTE_LENGTH AS byte_length "
                "FROM AUD_ATTEMPT_LOG_CHUNKS WHERE ATTEMPT_ID=:id"
            ),
            {"id": attempt},
        ).all()
    assert len(rows) == 1 and rows[0].content == "hello\n" and rows[0].byte_length == 6
    for changed in ({"content": "other"}, {"sequence": 2}):
        response = client.post("/api/v1/agent/log", headers=headers, json=chunk | changed)
        assert response.status_code == 400
        assert str(attempt) in response.text and "sequence" in response.text


def test_an_expired_owner_cannot_append_a_chunk(agent_scene):
    db, client, headers, _, _, _ = agent_scene
    attempt = claim(agent_scene)
    with db.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET LEASE_EXPIRES_AT=:past WHERE ATTEMPT_ID=:id"),
            {"id": attempt, "past": datetime.now(UTC) - timedelta(seconds=1)},
        )
    response = client.post(
        "/api/v1/agent/log",
        headers=headers,
        json={"version": __version__, "attempt_id": attempt, "sequence": 0, "content": "late"},
    )
    assert response.status_code == 409
    with db.engine.connect() as conn:
        assert (
            conn.execute(text("SELECT COUNT(*) AS count FROM AUD_ATTEMPT_LOG_CHUNKS")).scalar_one()
            == 0
        )


def post(scene, route, **fields):
    _, client, headers, _, _, _ = scene
    return client.post(
        "/api/v1/agent/" + route, headers=headers, json={"version": __version__, **fields}
    )


def test_an_identical_ending_commits_once_even_when_delivered_concurrently(agent_scene):
    from fixtures.races import two_at_once

    db, _, _, _, _, _ = agent_scene
    attempt = claim(agent_scene)
    response = post(agent_scene, "report", attempt_id=attempt, operation="START", pid=123)
    assert response.status_code == 200, response.text
    ending = {"attempt_id": attempt, "operation": "END", "status": "SUCCESS", "exit_code": 0}
    responses = two_at_once(
        lambda: post(agent_scene, "report", **ending),
        lambda: post(agent_scene, "report", **ending),
        at="etl_craft.services.operations.agent_reports._end",
    )
    assert [r.status_code for r in responses] == [200, 200], [r.text for r in responses]
    response = post(agent_scene, "report", **(ending | {"status": "FAILED"}))
    assert response.status_code == 409
    with db.engine.connect() as conn:
        row = conn.execute(
            text("SELECT STATUS AS status FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"),
            {"id": attempt},
        ).one()
        receipts = conn.execute(
            text(
                "SELECT COUNT(*) AS n FROM AUD_ACTIONS "
                "WHERE COMMAND='agent report' AND OUTCOME='SUCCESS'"
            )
        ).scalar_one()
    assert row.status == "SUCCESS"
    assert receipts == 1


def test_duplicate_upload_after_an_api_copy_restarts_is_still_idempotent(agent_scene):

    db, _, headers, _, _, _ = agent_scene
    attempt = claim(agent_scene)
    chunk = {"attempt_id": attempt, "sequence": 0, "content": "shared text\n"}
    assert post(agent_scene, "log", **chunk).status_code == 200
    ctx = OperationContext(
        db.engine,
        replace(db.config, deployment=DeploymentProfile.CONTROLLER_AGENTS),
        SYSTEM_ACTOR,
        role=ApiRole.ADMIN,
    )
    with TestClient(create_app(ctx)) as other:
        reply = other.post(
            "/api/v1/agent/log", headers=headers, json={"version": __version__, **chunk}
        )
        assert reply.status_code == 200, reply.text
    with db.engine.connect() as conn:
        rows = conn.execute(text("SELECT CONTENT AS content FROM AUD_ATTEMPT_LOG_CHUNKS")).all()
    assert [r.content for r in rows] == ["shared text\n"]


def test_expired_lease_cannot_be_renewed_or_report_success(agent_scene):
    attempt = claim(agent_scene)
    db, _, _, _, _, _ = agent_scene
    with db.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET LEASE_EXPIRES_AT=:past WHERE ATTEMPT_ID=:id"),
            {"past": datetime.now(UTC) - timedelta(seconds=1), "id": attempt},
        )
    response = post(agent_scene, "renew", attempts=[attempt], resources=encode(resources()))
    assert response.status_code == 200, response.text
    assert response.json()["data"]["renewed"] == []
    response = post(agent_scene, "report", attempt_id=attempt, operation="END", status="SUCCESS")
    assert response.status_code == 409


def test_expired_agent_cannot_refresh_capacity_without_registering_again(agent_scene):
    db, _, _, _, agent_id, _ = agent_scene
    with db.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_AGENTS SET HEARTBEAT_AT=:past WHERE AGENT_ID=:id"),
            {"past": datetime.now(UTC) - timedelta(seconds=31), "id": agent_id},
        )
    response = post(agent_scene, "renew", attempts=[], resources=encode(resources()))
    assert response.status_code == 409, response.text
    assert "expired" in response.text


def second_agent(scene, *, measured=None):
    from etl_craft.services.operations.identity import authenticated_context
    from etl_craft.services.operations.tokens import authenticate

    db, client, _, _, _, _ = scene
    ctx = OperationContext(
        db.engine,
        replace(db.config, deployment=DeploymentProfile.CONTROLLER_AGENTS),
        SYSTEM_ACTOR,
        role=ApiRole.ADMIN,
    )
    token = invoke(ctx, ActionName.TOKEN_CREATE, TokenCreate(name="agent-b", role=ApiRole.AGENT))
    headers = {"Authorization": f"Bearer {token.token}"}
    answer = client.post(
        "/api/v1/agent/register",
        headers=headers,
        json={
            "version": __version__,
            "pool": "default",
            "resources": encode(measured or resources()),
        },
    )
    assert answer.status_code == 200, answer.text
    return authenticated_context(ctx, authenticate(ctx, token.token)), headers


def test_commands_are_durable_acknowledged_and_refused_for_gone_agents(agent_scene):
    _, client, _, human, agent_id, _ = agent_scene
    for operation in ("DRAIN", "RESUME", "STOP"):
        response = client.post(
            "/api/v1/agents/commands",
            headers=human,
            json={"agent_id": agent_id, "command": operation},
        )
        assert response.status_code == 200, response.text
        command_id = response.json()["data"]["command_id"]
        reply = post(agent_scene, "renew", attempts=[], resources=encode(resources()))
        commands = reply.json()["data"]["commands"]
        assert any(c["command_id"] == command_id and c["command"] == operation for c in commands)
        reply = post(
            agent_scene,
            "renew",
            attempts=[],
            resources=encode(resources()),
            acknowledged_commands=[command_id],
        )
        assert reply.status_code == 200, reply.text
        assert not reply.json()["data"]["commands"]
    assert post(agent_scene, "deregister").status_code == 200
    response = client.post(
        "/api/v1/agents/commands", headers=human, json={"agent_id": agent_id, "command": "RESUME"}
    )
    assert response.status_code == 409 and str(agent_id) in response.text


def test_two_agents_racing_on_an_overlapping_scope_reserve_it_only_once(agent_scene):
    from etl_craft.services.operations import agent_claims
    from etl_craft.services.operations.agent_models import AgentClaim
    from etl_craft.services.operations.identity import authenticated_context
    from etl_craft.services.operations.tokens import authenticate
    from fixtures.races import two_at_once

    db, client, first_headers, _, _, _ = agent_scene
    measured = resources(cpus=0.3)
    reply = post(agent_scene, "renew", attempts=[], resources=encode(measured))
    assert reply.status_code == 200, reply.text
    second, _ = second_agent(agent_scene, measured=measured)
    raw = first_headers["Authorization"].removeprefix("Bearer ")
    first = authenticated_context(second, authenticate(second, raw))
    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "RACE")
        run = transitions.create_run(conn, pipeline, SYSTEM_ACTOR)
        attempts = []
        for code in ("A", "B"):
            task = add_task(conn, pipeline, code)
            summary = transitions.find_or_create_task_run(conn, task, run).task_run_id
            attempts.append(transitions.queue_attempt(conn, summary, SYSTEM_ACTOR))
    request = AgentClaim(version=__version__, free_kinds=("warehouse",))
    results = two_at_once(
        lambda: agent_claims.claim_once(first, request, first.credential_token_id),
        lambda: agent_claims.claim_once(second, request, second.credential_token_id),
        at="etl_craft.services.operations.agent_claims.claim_once",
    )
    assert sum(r is not None for r in results) == 1
    with db.engine.connect() as conn:
        claimed = (
            conn.execute(
                text(
                    "SELECT ATTEMPT_ID AS attempt_id FROM AUD_TASK_ATTEMPTS WHERE STATUS='CLAIMED'"
                )
            )
            .scalars()
            .all()
        )
    assert len(claimed) == 1 and claimed[0] in attempts
    answer = client.get("/api/v1/agents", headers=agent_scene[3])
    assert answer.status_code == 200, answer.text
    assert all(a["free"]["warehouse"] == 0 for a in answer.json()["agents"])


def test_a_stale_lock_passes_to_an_agent_only_after_it_confirms_the_holder(agent_scene):
    from etl_craft.config.targets import active_catalog
    from etl_craft.engine import locks, resource_locks

    db, client, headers, _, _, config = agent_scene
    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "LOCKED")
        task = add_task(conn, pipeline, "write", TARGET_OBJECT="s.fenced")
        run = transitions.create_run(conn, pipeline, SYSTEM_ACTOR)
        summary = transitions.find_or_create_task_run(conn, task, run).task_run_id
        attempt = transitions.queue_attempt(conn, summary, SYSTEM_ACTOR)
    claimed = client.post(
        "/api/v1/agent/claim",
        headers=headers,
        json={"version": __version__, "free_kinds": ["warehouse"]},
    )
    assert claimed.json()["data"]["task"]["attempt_id"] == attempt
    key = locks.target("s.fenced", active_catalog(config)).key
    old = resource_locks.Holder("action:old", "old", SYSTEM_ACTOR)
    with db.engine.begin() as conn:
        assert resource_locks.acquire(conn, key, "target", old).budget is not None
        conn.execute(
            text("UPDATE AUD_RESOURCE_LOCKS SET LEASE_EXPIRES_AT=:past"),
            {"past": datetime.now(UTC) - timedelta(seconds=1)},
        )
    lock = {
        "version": __version__,
        "attempt_id": attempt,
        "operation": "LOCK",
        "lock_key": key,
        "lock_wait_seconds": 5,
    }
    first = client.post("/api/v1/agent/report", headers=headers, json=lock)
    assert first.status_code == 200, first.text
    assert first.json()["data"]["acquired"] is False
    assert first.json()["data"]["stale_holder"] == "action:old"
    second = client.post(
        "/api/v1/agent/report", headers=headers, json=lock | {"confirmed_holder": "action:old"}
    )
    assert second.status_code == 200, second.text
    assert second.json()["data"]["acquired"] is True
    with db.engine.connect() as conn:
        holder = conn.execute(text("SELECT HOLDER_ID AS holder_id FROM AUD_RESOURCE_LOCKS"))
        assert holder.scalar_one() == attempt_holder(
            attempt, sha256_hex(headers["Authorization"].removeprefix("Bearer ").encode())
        )


@pytest.mark.parametrize("change", ["revoked", "expired", "account-disabled"])
def test_a_waiting_claim_rechecks_its_credential_before_admitting_work(agent_scene, change):
    from etl_craft.engine import resource_locks
    from etl_craft.services.operations import agent_claims
    from fixtures.races import paused_after

    db, client, headers, _, agent_id, _ = agent_scene

    def waiting_claim():
        return client.post(
            "/api/v1/agent/claim",
            headers=headers,
            json={"version": __version__, "free_kinds": ["warehouse"]},
        )

    def no_work_yet(*args, **kwargs):
        return True

    with paused_after(waiting_claim, owner=agent_claims, name="claim_once", when=no_work_yet):
        with db.engine.begin() as conn:
            if change == "account-disabled":
                conn.execute(
                    text(
                        "UPDATE CFG_ACCOUNTS SET ACTIVE_FLAG='N' WHERE ACCOUNT_ID="
                        "(SELECT ACCOUNT_ID FROM CFG_API_TOKENS WHERE TOKEN_ID=:id)"
                    ),
                    {"id": agent_id},
                )
            else:
                column = "REVOKED_AT" if change == "revoked" else "EXPIRES_AT"
                conn.execute(
                    text(f"UPDATE CFG_API_TOKENS SET {column}=:past WHERE TOKEN_ID=:id"),
                    {"past": datetime.now(UTC) - timedelta(seconds=1), "id": agent_id},
                )
        queued = queue(db)
        with db.engine.begin() as conn:
            resource_locks.notify(conn)
    with db.engine.connect() as conn:
        status = conn.execute(
            text("SELECT STATUS AS status FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"),
            {"id": queued},
        ).scalar_one()
    assert status == "QUEUED"


def test_claim_clears_the_previous_queued_reason(agent_scene):
    db, client, headers, _, _, _ = agent_scene
    attempt = queue(db)
    with db.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET ERROR_MESSAGE=:reason WHERE ATTEMPT_ID=:id"),
            {"reason": "QUEUED: no eligible agent", "id": attempt},
        )
    answer = client.post(
        "/api/v1/agent/claim",
        headers=headers,
        json={"version": __version__, "free_kinds": ["warehouse"]},
    )
    assert answer.status_code == 200, answer.text
    assert answer.json()["data"]["task"]["attempt_id"] == attempt
    with db.engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT STATUS AS status, ERROR_MESSAGE AS error_message "
                "FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"
            ),
            {"id": attempt},
        ).one()
    assert row.status == "CLAIMED"
    assert row.error_message is None


@pytest.mark.parametrize("confirmed", [False, True])
def test_cancellation_needs_warehouse_confirmation_from_the_pinned_owner(agent_scene, confirmed):
    from etl_craft.core.enums import AttemptStatus, ControlAction, ControlStatus
    from etl_craft.services.operations.requests import ControlRequest

    db, client, headers, _, _, config = agent_scene
    attempt = claim(agent_scene)
    ctx = OperationContext(db.engine, config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
    pending = invoke(
        ctx,
        ActionName.CONTROL,
        ControlRequest(pipeline_code="P", action=ControlAction.ABORT, reason="cancel owned work"),
    )
    response = client.post(
        "/api/v1/agent/report",
        headers=headers,
        json={
            "version": __version__,
            "attempt_id": attempt,
            "operation": "CANCEL",
            "queries_ended": confirmed,
            "error_message": "warehouse confirmation still pending",
        },
    )
    assert response.status_code == 200, response.text
    with db.engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT STATUS AS status, CANCEL_REQUESTED_AT AS requested, "
                "CANCEL_ACKNOWLEDGED_AT AS acknowledged FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"
            ),
            {"id": attempt},
        ).one()
        from etl_craft.engine.controls import get

        view = get(conn, pending.control_id)
    assert row.status == AttemptStatus.CLAIMED
    assert row.requested is not None
    assert (row.acknowledged is not None) == confirmed
    assert view.status == ControlStatus.ACKNOWLEDGED
    assert (view.attempts[0].queries_ended_at is not None) == confirmed
    if not confirmed:
        assert "warehouse confirmation still pending" in view.message
    if confirmed:
        ended = client.post(
            "/api/v1/agent/report",
            headers=headers,
            json={
                "version": __version__,
                "attempt_id": attempt,
                "operation": "END",
                "status": "CANCELLED",
                "queries_ended": True,
            },
        )
        assert ended.status_code == 200, ended.text
        from etl_craft.execution.control_flow import advance

        advance(db.engine, pending.pipeline_run_id, observed=True)
        with db.engine.connect() as conn:
            assert get(conn, pending.control_id).status == ControlStatus.COMPLETED


@pytest.mark.parametrize("invalid", ["expired", "unrequested"])
def test_cancellation_acknowledgement_requires_a_live_pinned_attempt(agent_scene, invalid):
    from etl_craft.core.enums import ControlAction
    from etl_craft.services.operations.requests import ControlRequest

    db, client, headers, _, _, config = agent_scene
    attempt = claim(agent_scene)
    if invalid == "expired":
        ctx = OperationContext(db.engine, config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
        invoke(
            ctx,
            ActionName.CONTROL,
            ControlRequest(
                pipeline_code="P", action=ControlAction.ABORT, reason="cancel owned work"
            ),
        )
        with db.engine.begin() as conn:
            conn.execute(
                text("UPDATE AUD_TASK_ATTEMPTS SET LEASE_EXPIRES_AT=:past WHERE ATTEMPT_ID=:id"),
                {"past": datetime.now(UTC) - timedelta(seconds=1), "id": attempt},
            )
    response = client.post(
        "/api/v1/agent/report",
        headers=headers,
        json={
            "version": __version__,
            "attempt_id": attempt,
            "operation": "CANCEL",
            "queries_ended": True,
        },
    )
    assert response.status_code == (409 if invalid == "expired" else 403), response.text
    with db.engine.connect() as conn:
        acknowledged = conn.execute(
            text(
                "SELECT CANCEL_ACKNOWLEDGED_AT AS acknowledged FROM AUD_TASK_ATTEMPTS "
                "WHERE ATTEMPT_ID=:id"
            ),
            {"id": attempt},
        ).scalar_one()
        observations = conn.execute(
            text(
                "SELECT COUNT(*) AS observations FROM AUD_ATTEMPT_ACKNOWLEDGEMENTS "
                "WHERE ATTEMPT_ID=:id"
            ),
            {"id": attempt},
        ).scalar_one()
    assert acknowledged is None and observations == 0
