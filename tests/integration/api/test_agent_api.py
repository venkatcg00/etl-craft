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
    from fixtures.round_trips import count_round_trips

    db, _, _, _, owner, _ = agent_scene
    attempt = queue(db)
    with count_round_trips(db.engine) as counter:
        reply = claim_available(agent_scene)
    assert reply.data["task"]["attempt_id"] == attempt
    assert counter.counts["engine_statements"] == 13, counter.queries
    assert counter.counts["engine_transactions"] == 2
    assert counter.counts["engine_connections"] == 0
    with db.engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT OWNER_ID AS owner_id, STATUS AS status, RESERVED_CPU AS cpu, "
                "RESERVED_MEMORY_MB AS memory FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"
            ),
            {"id": attempt},
        ).one()
    assert row.owner_id == str(owner)
    assert row.status == "CLAIMED"
    assert row.cpu == 0.25 and row.memory == 128


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


def second_agent(scene, *, measured=None, pool="default"):
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
            "pool": pool,
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


def queue_pipeline_work(db, code, *, count=1, priority=0, limit=8, **parameters):
    """Queue independent tasks with explicit order and admission metadata."""
    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, code)
        conn.execute(
            text(
                "UPDATE CFG_PIPELINES SET PRIORITY=:priority, MAX_PARALLEL_TASKS=:limit "
                "WHERE PIPELINE_ID=:id"
            ),
            {"priority": priority, "limit": limit, "id": pipeline},
        )
        run = transitions.create_run(conn, pipeline, SYSTEM_ACTOR)
        attempts = []
        for index in range(count):
            task = add_task(conn, pipeline, f"T{index}", **parameters)
            summary = transitions.find_or_create_task_run(conn, task, run).task_run_id
            attempts.append(transitions.queue_attempt(conn, summary, SYSTEM_ACTOR))
    return attempts


def claim_available(scene, *, config=None):
    """Exercise the nonblocking claim service used by every long poll."""
    from etl_craft.core.enums import SlotKind
    from etl_craft.services.operations.agent_claims import claim_once
    from etl_craft.services.operations.agent_models import AgentClaim

    db, _, _, _, owner, current = scene
    ctx = OperationContext(
        db.engine, config or current, SYSTEM_ACTOR, role=ApiRole.AGENT, credential_token_id=owner
    )
    return claim_once(ctx, AgentClaim(version=__version__, free_kinds=(SlotKind.WAREHOUSE,)), owner)


def test_queue_priority_and_oldest_tie_break(agent_scene):
    db = agent_scene[0]
    low = queue_pipeline_work(db, "LOW", priority=-1)[0]
    first = queue_pipeline_work(db, "FIRST", priority=5)[0]
    second = queue_pipeline_work(db, "SECOND", priority=5)[0]
    with db.engine.begin() as conn:
        for identity, queued_at in (
            (low, datetime(2025, 1, 1, tzinfo=UTC)),
            (first, datetime(2026, 1, 2, tzinfo=UTC)),
            (second, datetime(2026, 1, 1, tzinfo=UTC)),
        ):
            conn.execute(
                text("UPDATE AUD_TASK_ATTEMPTS SET QUEUED_AT=:queued WHERE ATTEMPT_ID=:id"),
                {"id": identity, "queued": queued_at},
            )
    assert claim_available(agent_scene).data["task"]["attempt_id"] == second
    assert claim_available(agent_scene).data["task"]["attempt_id"] == first
    with db.engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT STATUS AS status FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"),
                {"id": low},
            )
            .one()
            .status
            == "QUEUED"
        )


def test_pipeline_limit_skips_blocked_work_and_preserves_its_reason(agent_scene):
    db = agent_scene[0]
    first, blocked = queue_pipeline_work(db, "LIMITED", count=2, priority=10, limit=1)
    other = queue_pipeline_work(db, "OTHER")[0]
    assert claim_available(agent_scene).data["task"]["attempt_id"] == first
    assert claim_available(agent_scene).data["task"]["attempt_id"] == other
    assert claim_available(agent_scene) is None
    with db.engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT STATUS AS status, ERROR_MESSAGE AS reason "
                "FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"
            ),
            {"id": blocked},
        ).one()
    assert row.status == "QUEUED"
    assert "pipeline LIMITED" in row.reason
    assert "MAX_PARALLEL_TASKS=1" in row.reason


def test_warehouse_limit_is_shared_between_pipelines(agent_scene):
    db = agent_scene[0]
    queue_pipeline_work(db, "ONE")
    second = queue_pipeline_work(db, "TWO")[0]
    config = replace(agent_scene[-1], max_warehouse_queries=1)
    assert claim_available(agent_scene, config=config) is not None
    assert claim_available(agent_scene, config=config) is None
    with db.engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT STATUS AS status, ERROR_MESSAGE AS reason "
                "FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"
            ),
            {"id": second},
        ).one()
    assert row.status == "QUEUED"
    assert "warehouse at MAX_WAREHOUSE_QUERIES=1" in row.reason


def test_resource_request_does_not_sum_the_cluster(agent_scene):
    db = agent_scene[0]
    queued = queue_pipeline_work(db, "TOO_BIG", MEMORY_REQUEST_MB="40960")[0]
    assert claim_available(agent_scene) is None
    with db.engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT STATUS AS status, RESERVED_CPU AS cpu, "
                "RESERVED_MEMORY_MB AS memory FROM AUD_TASK_ATTEMPTS "
                "WHERE ATTEMPT_ID=:id"
            ),
            {"id": queued},
        ).one()
    assert row.status == "QUEUED"
    assert row.cpu == row.memory == 0


@pytest.mark.parametrize("status", ["SUCCESS", "FAILED", "CANCELLED", "TIMED_OUT"])
def test_claimed_resources_are_released_with_the_ending(agent_scene, status):
    from etl_craft.core.enums import AttemptStatus

    db, _, _, _, owner, _ = agent_scene
    queued = queue_pipeline_work(db, "RELEASE", CPU_REQUEST="1", MEMORY_REQUEST_MB="256")[0]
    assert claim_available(agent_scene) is not None
    with db.engine.begin() as conn:
        before = conn.execute(
            text(
                "SELECT RESERVED_CPU AS cpu, RESERVED_MEMORY_MB AS memory "
                "FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"
            ),
            {"id": queued},
        ).one()
        assert before.cpu == 1
        assert before.memory == 256
        transitions.finish_attempt(
            conn, queued, AttemptStatus(status), SYSTEM_ACTOR, owner=str(owner)
        )
        after = conn.execute(
            text(
                "SELECT RESERVED_CPU AS cpu, RESERVED_MEMORY_MB AS memory "
                "FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"
            ),
            {"id": queued},
        ).one()
        assert after.cpu == after.memory == 0


@pytest.mark.parametrize("bound", ["pipeline", "warehouse"])
def test_pipeline_limit_holds_with_a_thousand_ready_attempts(agent_scene, bound):
    db = agent_scene[0]
    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "BURST")
        conn.execute(
            text("UPDATE CFG_PIPELINES SET MAX_PARALLEL_TASKS=:limit WHERE PIPELINE_ID=:id"),
            {"id": pipeline, "limit": 1 if bound == "pipeline" else 1000},
        )
        run = transitions.create_run(conn, pipeline, SYSTEM_ACTOR)
        conn.execute(
            text(
                "INSERT INTO CFG_TASKS (PIPELINE_ID,TASK_CODE,TASK_TYPE,HANDLER) "
                "SELECT :id, 'T'||CAST(n AS varchar), 'ETL', 'SQL' "
                "FROM generate_series(1,1000) AS n"
            ),
            {"id": pipeline},
        )
        conn.execute(
            text(
                "INSERT INTO AUD_TASK_RUN_LOG (PIPELINE_RUN_ID,TASK_ID,STATUS) "
                "SELECT :run,TASK_ID,'IN-PROGRESS' FROM CFG_TASKS WHERE PIPELINE_ID=:id"
            ),
            {"run": run, "id": pipeline},
        )
        conn.execute(
            text(
                "INSERT INTO AUD_TASK_ATTEMPTS "
                "(TASK_RUN_ID,ATTEMPT_NUMBER,STATUS,QUEUED_AT) "
                "SELECT TASK_RUN_ID,1,'QUEUED',clock_timestamp() "
                "FROM AUD_TASK_RUN_LOG WHERE PIPELINE_RUN_ID=:run"
            ),
            {"run": run},
        )
    config = replace(agent_scene[-1], max_warehouse_queries=1 if bound == "warehouse" else None)
    assert claim_available(agent_scene, config=config) is not None
    assert claim_available(agent_scene, config=config) is None
    with db.engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT COUNT(*) FILTER (WHERE STATUS='CLAIMED') AS claimed, "
                "COUNT(*) FILTER (WHERE STATUS='QUEUED') AS queued "
                "FROM AUD_TASK_ATTEMPTS"
            )
        ).one()
    assert row.claimed == 1
    assert row.queued == 999


def test_resource_reason_names_the_request_and_largest_individual_agent(agent_scene):
    db = agent_scene[0]
    queued = queue_pipeline_work(db, "RESOURCE_REASON", MEMORY_REQUEST_MB="40960")[0]
    assert claim_available(agent_scene) is None
    with db.engine.connect() as conn:
        row = conn.execute(
            text("SELECT ERROR_MESSAGE AS reason FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"),
            {"id": queued},
        ).one()
    for part in ("needs 40960 MB", "largest agent in pool default", "MB free of", "Add capacity"):
        assert part in row.reason

    _, client, _, human, _, _ = agent_scene
    status = client.get(
        "/api/v1/status", headers=human, params={"pipeline_code": "RESOURCE_REASON"}
    )
    assert status.status_code == 200, status.text
    assert status.json()["tasks"][0]["error"] == row.reason
    explanation = client.get(
        "/api/v1/explanations",
        headers=human,
        params={"pipeline_code": "RESOURCE_REASON", "task_code": "T0"},
    )
    assert explanation.status_code == 200, explanation.text
    assert explanation.json()["state"] == "queued"
    assert explanation.json()["next_action"] == row.reason


def test_large_python_work_is_claimed_only_by_the_agent_that_fits(agent_scene):
    from etl_craft.core.enums import SlotKind
    from etl_craft.core.resources import GIB
    from etl_craft.services.operations.agent_claims import claim_once
    from etl_craft.services.operations.agent_models import AgentClaim

    db, client, _, _, _, config = agent_scene
    ctx = OperationContext(db.engine, config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
    large = invoke(
        ctx, ActionName.TOKEN_CREATE, TokenCreate(name="large-agent", role=ApiRole.AGENT)
    )
    headers = {"Authorization": f"Bearer {large.token}"}
    large_resources = resources(cpus=8, memory=8 * GIB, node="large-host", host="large")
    large_resources = replace(
        large_resources,
        allocation=replace(large_resources.allocation, host_cpus=8, host_memory=8 * GIB),
    )
    registered = client.post(
        "/api/v1/agent/register",
        headers=headers,
        json={
            "version": __version__,
            "pool": "default",
            "resources": encode(large_resources),
        },
    )
    assert registered.status_code == 200, registered.text
    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "LARGE")
        task = add_task(
            conn, pipeline, "load", handler="PYTHON", CPU_REQUEST="2", MEMORY_REQUEST_MB="3072"
        )
        run = transitions.create_run(conn, pipeline, SYSTEM_ACTOR)
        summary = transitions.find_or_create_task_run(conn, task, run).task_run_id
        attempt = transitions.queue_attempt(conn, summary, SYSTEM_ACTOR)
    request = AgentClaim(version=__version__, free_kinds=(SlotKind.PYTHON,))
    for owner, fits in ((agent_scene[4], False), (large.metadata.token_id, True)):
        agent_ctx = replace(ctx, role=ApiRole.AGENT, credential_token_id=owner)
        reply = claim_once(agent_ctx, request, owner)
        if fits:
            assert reply.data["task"]["attempt_id"] == attempt
        else:
            assert reply is None


@pytest.mark.parametrize("pool", ["missing", "disabled"])
def test_agent_registration_requires_an_active_pool(agent_scene, pool):
    db, client, _, _, _, config = agent_scene
    if pool == "disabled":
        with db.engine.begin() as conn:
            conn.execute(
                text("INSERT INTO CFG_POOLS (POOL_NAME,ACTIVE_FLAG) VALUES ('disabled','N')")
            )
    ctx = OperationContext(db.engine, config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
    token = invoke(ctx, ActionName.TOKEN_CREATE, TokenCreate(name="new-agent", role=ApiRole.AGENT))
    answer = client.post(
        "/api/v1/agent/register",
        headers={"Authorization": f"Bearer {token.token}"},
        json={"version": __version__, "pool": pool, "resources": encode(resources())},
    )
    assert answer.status_code == 404, answer.text
    for part in (pool, "CFG_POOLS", "pools.csv", "config apply"):
        assert part in answer.text


@pytest.mark.parametrize("bound", ["pipeline", "warehouse"])
def test_agents_on_different_hosts_claim_under_one_deployment_limit(agent_scene, bound):
    from etl_craft.core.enums import SlotKind
    from etl_craft.services.operations.agent_claims import claim_once
    from etl_craft.services.operations.agent_models import AgentClaim
    from fixtures.races import two_at_once

    db, _, _, _, owner, config = agent_scene
    second, _ = second_agent(agent_scene, measured=resources(node="other-node", host="other-host"))
    if bound == "warehouse":
        config = replace(config, max_warehouse_queries=1)
    first = OperationContext(
        db.engine, config, SYSTEM_ACTOR, role=ApiRole.AGENT, credential_token_id=owner
    )
    second = replace(second, config=config)
    queue_pipeline_work(db, "BOUNDED", count=2, limit=1 if bound == "pipeline" else 8)
    request = AgentClaim(version=__version__, free_kinds=(SlotKind.WAREHOUSE,))
    results = two_at_once(
        lambda: claim_once(first, request, owner),
        lambda: claim_once(second, request, second.credential_token_id),
        at="etl_craft.services.operations.agent_claims.claim_once",
    )
    assert sum(reply is not None for reply in results) == 1
    with db.engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT COUNT(*) AS claimed, SUM(RESERVED_CPU) AS cpu, "
                "SUM(RESERVED_MEMORY_MB) AS memory "
                "FROM AUD_TASK_ATTEMPTS WHERE STATUS='CLAIMED'"
            )
        ).one()
    assert row.claimed == 1 and row.cpu == 0.25 and row.memory == 128


def test_draining_agent_withdraws_free_resources(agent_scene):
    db, client, _, human, agent_id, _ = agent_scene
    response = client.post(
        "/api/v1/agents/commands", headers=human, json={"agent_id": agent_id, "command": "DRAIN"}
    )
    assert response.status_code == 200, response.text
    command_id = response.json()["data"]["command_id"]
    response = post(
        agent_scene,
        "renew",
        attempts=[],
        status="DRAINING",
        resources=encode(resources()),
        acknowledged_commands=[command_id],
    )
    assert response.status_code == 200, response.text
    queued = queue_pipeline_work(db, "WAIT_FOR_AGENT")[0]
    assert claim_available(agent_scene) is None
    answer = client.get("/api/v1/agents", headers=human)
    assert answer.status_code == 200, answer.text
    agent = next(a for a in answer.json()["agents"] if a["agent_id"] == agent_id)
    assert agent["free_cpu"] == agent["free_memory_mb"] == 0
    from io import StringIO

    from etl_craft.cli.output import Output
    from etl_craft.services.operations.agents import AgentsView
    from etl_craft.services.operations.serialization import from_json

    text_output = StringIO()
    Output(stdout=text_output).agents(from_json(AgentsView, answer.json()))
    assert "MEMORY_RESERVED_MB" in text_output.getvalue()
    assert "POOL\tAGENTS\tCPU" in text_output.getvalue()
    with db.engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT STATUS AS status FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"),
                {"id": queued},
            ).scalar_one()
            == "QUEUED"
        )


def test_missing_default_pool_cannot_bypass_custom_pool_admission(agent_scene):
    from etl_craft.core.enums import SlotKind
    from etl_craft.services.operations.agent_claims import claim_once
    from etl_craft.services.operations.agent_models import AgentClaim

    db = agent_scene[0]
    with db.engine.begin() as conn:
        conn.execute(text("INSERT INTO CFG_POOLS (POOL_NAME) VALUES ('heavy')"))
    ctx, _ = second_agent(agent_scene, pool="heavy")
    attempt = queue_pipeline_work(db, "CUSTOM_POOL", POOL="heavy")[0]
    with db.engine.begin() as conn:
        conn.execute(text("DELETE FROM CFG_POOLS WHERE POOL_NAME='default'"))
    assert (
        claim_once(
            ctx,
            AgentClaim(version=__version__, free_kinds=(SlotKind.WAREHOUSE,)),
            ctx.credential_token_id,
        )
        is None
    )
    with db.engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT STATUS AS status, ERROR_MESSAGE AS reason "
                "FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"
            ),
            {"id": attempt},
        ).one()
    assert row.status == "QUEUED"
    for part in ("CFG_POOLS.default", "missing", "expected", "restore", "config apply"):
        assert part in row.reason


def test_slot_ceiling_reason_names_free_and_advertised_slots(agent_scene):
    from etl_craft.core.resources import Ceilings, size_agent

    measured = resources()
    ceilings = Ceilings(2, 2, 0)
    measured = replace(
        measured, ceilings=ceilings, sizing=size_agent(measured.allocation, ceilings)
    )
    response = post(agent_scene, "renew", attempts=[], resources=encode(measured))
    assert response.status_code == 200, response.text
    db = agent_scene[0]
    attempt = queue_pipeline_work(db, "NO_WAREHOUSE_SLOTS")[0]
    assert claim_available(agent_scene) is None
    with db.engine.connect() as conn:
        reason = conn.execute(
            text("SELECT ERROR_MESSAGE AS reason FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"),
            {"id": attempt},
        ).scalar_one()
    assert "warehouse slots 0 free of 0" in reason
    assert "adjust task requests or agent slots" in reason
