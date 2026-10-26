"""Scrapes during uncommitted admission and lease reconciliation see committed state."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.engine import Connection

from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.enums import AttemptStatus
from etl_craft.engine import transitions
from etl_craft.engine.metrics import install
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.metrics import scrape
from fixtures.metadata import add_pipeline, add_task, start_run
from fixtures.metrics import value
from fixtures.races import paused_after


@pytest.mark.parametrize("expiry", [False, True])
def test_scrape_during_admission_or_expiry_reads_only_committed_state(engine_db, expiry):
    engine = engine_db.engine
    ctx = OperationContext(engine, engine_db.config, SYSTEM_ACTOR)
    install(engine).leader = True
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "RACE")
        task = add_task(conn, pipeline, "TASK")
        run = start_run(conn, pipeline)
        summary = transitions.find_or_create_task_run(conn, task, run).task_run_id
        attempt = transitions.queue_attempt(conn, summary, SYSTEM_ACTOR)
        if expiry:
            transitions.claim_attempt(
                conn,
                attempt,
                SYSTEM_ACTOR,
                owner="local",
                lease_expires_at=datetime.now(UTC) - timedelta(seconds=1),
            )

    def change():
        with engine.begin() as conn:
            if expiry:
                transitions.lose_attempt(conn, attempt, SYSTEM_ACTOR, owner="local")
            else:
                transitions.claim_attempt(
                    conn,
                    attempt,
                    SYSTEM_ACTOR,
                    owner="local",
                    lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
                )

    query = "transition_lose_attempt" if expiry else "transition_claim_attempt"

    def at_boundary(conn, clause, *args, **kwargs):
        return clause.get_execution_options().get("etl_craft_query") == query

    with paused_after(change, owner=Connection, name="execute", when=at_boundary):
        body = scrape(ctx).body
        status = AttemptStatus.CLAIMED if expiry else AttemptStatus.QUEUED
        assert value(body, "attempts", project="1", status=status) == 1
        assert value(body, "lost_attempts_total", project="1") == 0
    body = scrape(ctx).body
    if expiry:
        assert value(body, "lost_attempts_total", project="1") == 1
    else:
        assert value(body, "attempts", project="1", status=AttemptStatus.CLAIMED) == 1


def test_scrape_during_run_admission_keeps_old_gauge_until_commit(engine_db):
    from etl_craft.core.enums import RunStatus

    engine = engine_db.engine
    ctx = OperationContext(engine, engine_db.config, SYSTEM_ACTOR)
    registry = install(engine)
    registry.leader = True
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "QUEUED_RUN")
        run = transitions.create_run(conn, pipeline, SYSTEM_ACTOR, status=RunStatus.QUEUED)

    def admit():
        with engine.begin() as conn:
            transitions.admit_run(conn, run, SYSTEM_ACTOR)

    def at_boundary(conn, clause, *args, **kwargs):
        return clause.get_execution_options().get("etl_craft_query") == "transition_admit_run"

    with paused_after(admit, owner=Connection, name="execute", when=at_boundary):
        body = scrape(ctx).body
        assert value(body, "runs", project="1", status=RunStatus.QUEUED) == 1
        assert value(body, "admission_latency_seconds_count", project="1") == 0
    body = scrape(ctx).body
    assert value(body, "runs", project="1", status=RunStatus.IN_PROGRESS) == 1
    assert value(body, "admission_latency_seconds_count", project="1") == 1


def test_enabling_metrics_during_transaction_start_does_not_mutate_listeners(engine_db):
    engine = engine_db.engine

    def transaction():
        with engine.begin() as conn:
            conn.exec_driver_sql("SELECT 1")

    def at_begin(conn, sql, *args, **kwargs):
        return sql == "BEGIN IMMEDIATE" or sql.startswith("SELECT set_config(")

    with paused_after(transaction, owner=Connection, name="exec_driver_sql", when=at_begin):
        registry = install(engine)
    assert registry is install(engine)
