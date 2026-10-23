"""Pool configuration and pipeline limits survive both Engine DB initializations."""

import pytest
from sqlalchemy import text

from etl_craft.config.model import DEFAULT_MAX_PARALLEL_TASKS
from fixtures.metadata import add_pipeline


def test_pipeline_limit_default_matches_the_configuration_constant(engine_db):
    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "PLACEMENT")
        row = conn.execute(
            text(
                "SELECT MAX_PARALLEL_TASKS AS limit_value, PRIORITY AS priority "
                "FROM CFG_PIPELINES WHERE PIPELINE_ID=:id"
            ),
            {"id": pipeline},
        ).one()
        assert row.limit_value == DEFAULT_MAX_PARALLEL_TASKS
        assert row.priority == 0
        pool = conn.execute(
            text("SELECT ACTIVE_FLAG AS active_flag FROM CFG_POOLS WHERE POOL_NAME='default'")
        ).one()
        assert pool.active_flag == "Y"
        history = conn.execute(
            text(
                "SELECT POOL_NAME AS pool_name, CURRENT_FLAG AS current_flag, "
                "OPERATION AS operation "
                "FROM AUD_CFG_POOLS_HISTORY WHERE POOL_NAME='default'"
            )
        ).one()
        assert history.pool_name == "default"
        assert history.current_flag == "Y" and history.operation == "INSERT"


@pytest.mark.parametrize("bound", ["pipeline", "warehouse", "cpu", "memory", "kind"])
def test_two_local_claims_atomically_observe_one_limit(engine_db, bound):
    from datetime import UTC, datetime, timedelta

    from etl_craft.core.actor import SYSTEM_ACTOR
    from etl_craft.engine import agent_transitions, transitions
    from fixtures.metadata import add_task, start_run
    from fixtures.races import two_at_once

    db = engine_db.engine
    with db.begin() as conn:
        pipelines = [add_pipeline(conn, "FIRST")]
        pipelines.append(pipelines[0] if bound == "pipeline" else add_pipeline(conn, "SECOND"))
        if bound == "pipeline":
            conn.execute(text("UPDATE CFG_PIPELINES SET MAX_PARALLEL_TASKS=1"))
        attempts = []
        for index, pipeline in enumerate(pipelines):
            run = start_run(conn, pipeline)
            task = add_task(conn, pipeline, f"T{index}")
            summary = transitions.find_or_create_task_run(conn, task, run).task_run_id
            attempts.append(transitions.queue_attempt(conn, summary, SYSTEM_ACTOR))
    capacity = {
        "host": "shared-host",
        "warehouse_limit": 1 if bound == "warehouse" else None,
        "local_cpu": 0.25 if bound == "cpu" else 100,
        "local_memory_mb": 128 if bound == "memory" else 10000,
        "local_slots": 100,
        "python_slots": 100,
        "warehouse_slots": 1 if bound == "kind" else 100,
        "lease": datetime.now(UTC) + timedelta(seconds=60),
    }

    def claim(index):
        with db.begin() as conn:
            return agent_transitions.claim_with_capacity(
                conn,
                attempts[index],
                SYSTEM_ACTOR,
                capacity | {"owner": f"owner-{index}"},
                wait=True,
            )

    results = two_at_once(
        lambda: claim(0),
        lambda: claim(1),
        at=(db, "begin"),
    )
    assert sorted(results) == [False, True]
    with db.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT STATUS AS status, RESERVED_CPU AS cpu, "
                "RESERVED_MEMORY_MB AS memory FROM AUD_TASK_ATTEMPTS"
            )
        ).all()
    assert sum(row.status == "CLAIMED" for row in rows) == 1
    assert sum(row.cpu for row in rows) == 0.25
    assert sum(row.memory for row in rows) == 128


def test_local_claim_selects_priority_before_oldest(engine_db):
    from datetime import UTC, datetime, timedelta

    from etl_craft.core.actor import SYSTEM_ACTOR
    from etl_craft.engine import agent_transitions, transitions
    from fixtures.metadata import add_task, start_run

    attempts = []
    with engine_db.engine.begin() as conn:
        for index, priority in enumerate((0, 10)):
            pipeline = add_pipeline(conn, f"PRIORITY{index}")
            conn.execute(
                text("UPDATE CFG_PIPELINES SET PRIORITY=:priority WHERE PIPELINE_ID=:id"),
                {"priority": priority, "id": pipeline},
            )
            task = add_task(conn, pipeline, "T")
            summary = transitions.find_or_create_task_run(
                conn, task, start_run(conn, pipeline)
            ).task_run_id
            attempts.append(transitions.queue_attempt(conn, summary, SYSTEM_ACTOR))
        limits = {
            "owner": "local",
            "host": "host",
            "lease": datetime.now(UTC) + timedelta(seconds=60),
            "candidate_ids": tuple(attempts),
        }
        assert not agent_transitions.claim_with_capacity(
            conn, attempts[0], SYSTEM_ACTOR, limits, wait=True
        )
        assert agent_transitions.claim_with_capacity(
            conn, attempts[1], SYSTEM_ACTOR, limits, wait=True
        )


def test_expired_reservation_is_released_on_lost(engine_db):
    from datetime import UTC, datetime, timedelta

    from etl_craft.core.actor import SYSTEM_ACTOR
    from etl_craft.engine import transitions
    from fixtures.metadata import add_task, start_run

    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "LOST_CAPACITY")
        task = add_task(conn, pipeline, "T")
        summary = transitions.find_or_create_task_run(
            conn, task, start_run(conn, pipeline)
        ).task_run_id
        attempt = transitions.queue_attempt(conn, summary, SYSTEM_ACTOR)
        transitions.claim_attempt(
            conn,
            attempt,
            SYSTEM_ACTOR,
            owner="owner",
            host="host",
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
        )
        before = conn.execute(
            text(
                "SELECT RESERVED_CPU AS cpu, RESERVED_MEMORY_MB AS memory "
                "FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"
            ),
            {"id": attempt},
        ).one()
        assert before.cpu == 0.25 and before.memory == 128
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET LEASE_EXPIRES_AT=:past WHERE ATTEMPT_ID=:id"),
            {"id": attempt, "past": datetime.now(UTC) - timedelta(seconds=1)},
        )
        transitions.lose_attempt(conn, attempt, SYSTEM_ACTOR, owner="owner")
        after = conn.execute(
            text(
                "SELECT STATUS AS status, RESERVED_CPU AS cpu, RESERVED_MEMORY_MB AS memory "
                "FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"
            ),
            {"id": attempt},
        ).one()
        assert after.status == "LOST"
        assert after.cpu == after.memory == 0


@pytest.mark.parametrize("name", ["default", 'heavy"python', "123"])
def test_seeded_pool_identity_round_trips_through_audit(engine_db, name):
    from etl_craft.core.actor import SYSTEM_ACTOR
    from etl_craft.core.enums import ActionName, ApiRole
    from etl_craft.services.operations import OperationContext
    from etl_craft.services.operations.actions import invoke
    from etl_craft.services.operations.requests import AuditInput

    ctx = OperationContext(engine_db.engine, engine_db.config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
    if name != "default":
        with engine_db.engine.begin() as conn:
            conn.execute(text("INSERT INTO CFG_POOLS (POOL_NAME) VALUES (:name)"), {"name": name})
    view = invoke(ctx, ActionName.AUDIT, AuditInput())
    pool = next(
        change
        for change in view.changes
        if change.table_name == "CFG_POOLS" and change.after_json["pool_name"] == name
    )
    assert pool.row_key == name
    assert pool.after_json["pool_name"] == name
