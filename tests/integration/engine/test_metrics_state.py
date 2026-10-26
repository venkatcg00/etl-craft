"""Live gauges and committed process events on both Engine DBs."""

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

from sqlalchemy import event, text
from sqlalchemy.exc import OperationalError

from etl_craft.core.actor import SYSTEM_ACTOR, Actor, ActorKind, acting_as, current_actor
from etl_craft.core.enums import AttemptStatus, RunStatus
from etl_craft.engine import transitions
from etl_craft.engine.metrics import install
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations import metrics as operation
from fixtures.metadata import add_pipeline, add_task, start_run
from fixtures.metrics import parse, value


def context(db):
    ctx = OperationContext(db.engine, db.config, SYSTEM_ACTOR)
    registry = install(db.engine)
    registry.leader = True
    return ctx, registry


def queued(db):
    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "METRICS")
        task = add_task(conn, pipeline, "TASK")
        run = start_run(conn, pipeline)
        summary = transitions.find_or_create_task_run(conn, task, run).task_run_id
        attempt = transitions.queue_attempt(conn, summary, SYSTEM_ACTOR)
    return run, summary, attempt


def test_live_work_and_committed_lost_retry_and_terminal_events(engine_db):
    ctx, registry = context(engine_db)
    run, summary, attempt = queued(engine_db)
    now = datetime.now(UTC)
    with ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET QUEUED_AT=:at"), {"at": now - timedelta(seconds=10)}
        )
    body = operation.scrape(ctx, now=now).body
    parse(body)
    assert value(body, "runs", project="1", status=RunStatus.IN_PROGRESS) == 1
    assert value(body, "queue_depth", project="1", pool="default", slot_kind="warehouse") == 1
    assert (
        value(body, "queue_oldest_age_seconds", project="1", pool="default", slot_kind="warehouse")
        == 10
    )
    with ctx.engine.begin() as conn:
        transitions.claim_attempt(
            conn,
            attempt,
            SYSTEM_ACTOR,
            owner="local-owner",
            lease_expires_at=now + timedelta(seconds=60),
        )
    assert value(operation.scrape(ctx).body, "dispatch_latency_seconds_count", project="1") == 1
    with ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET LEASE_EXPIRES_AT=:at"),
            {"at": now - timedelta(seconds=1)},
        )
        transitions.lose_attempt(conn, attempt, SYSTEM_ACTOR, owner="local-owner")
    body = operation.scrape(ctx).body
    assert value(body, "lost_attempts_total", project="1") == 1
    assert value(body, "lease_expiries_total", project="1") == 1
    with ctx.engine.begin() as conn:
        transitions.queue_attempt(conn, summary, SYSTEM_ACTOR)
    assert value(operation.scrape(ctx).body, "retries_total", project="1") == 1
    with ctx.engine.begin() as conn:
        active = transitions.active_attempt(conn, summary)
        transitions.cancel_attempt(conn, active.attempt_id, SYSTEM_ACTOR, error_message="test")
        transitions.finish_run(conn, run, RunStatus.FAILED, SYSTEM_ACTOR)
    assert (
        value(operation.scrape(ctx).body, "runs_ended_total", project="1", status=RunStatus.FAILED)
        == 1
    )
    registry.leader = False
    body = operation.scrape(ctx).body
    assert value(body, "controller_leader") == 0
    assert all(
        name not in {"etl_craft_runs", "etl_craft_queue_depth"} for name, _ in parse(body)[1]
    )


def test_rollback_and_nested_rollback_do_not_publish_events(engine_db):
    ctx, _ = context(engine_db)
    _, _, attempt = queued(engine_db)
    with ctx.engine.connect() as conn:
        transaction = conn.begin()
        transitions.cancel_attempt(conn, attempt, SYSTEM_ACTOR, error_message="rolled back")
        transaction.rollback()
    assert (
        value(
            operation.scrape(ctx).body,
            "attempts_ended_total",
            project="1",
            status=AttemptStatus.CANCELLED,
        )
        == 0
    )

    with ctx.engine.begin() as conn:
        nested = conn.begin_nested()
        transitions.cancel_attempt(conn, attempt, SYSTEM_ACTOR, error_message="savepoint")
        nested.rollback()
    assert (
        value(
            operation.scrape(ctx).body,
            "attempts_ended_total",
            project="1",
            status=AttemptStatus.CANCELLED,
        )
        == 0
    )


def test_gate_waits_and_heartbeat_age_use_explicit_times(engine_db):
    ctx, registry = context(engine_db)
    run, _, _ = queued(engine_db)
    now = datetime(2026, 10, 26, 10, 0, tzinfo=UTC)
    registry.heartbeat = (now - timedelta(seconds=46)).timestamp()
    with ctx.engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO AUD_GATE_WAITS (PIPELINE_RUN_ID,FIRST_CHECK_AT,NEXT_CHECK_AT,"
                "LOOKS,WAIT_KIND) VALUES (:run,:now,:now,0,'EVENT')"
            ),
            {"run": run, "now": now},
        )
    body = operation.scrape(ctx, now=now).body
    assert value(body, "gate_waits", project="1") == 1
    assert value(body, "controller_heartbeat_age_seconds") == 46
    with ctx.engine.begin() as conn:
        conn.execute(text("UPDATE AUD_GATE_WAITS SET NEXT_CHECK_AT=NULL"))
    assert value(operation.scrape(ctx, now=now).body, "gate_waits", project="1") == 0


def test_fixed_scrape_statement_count_and_no_audit_writes(engine_db):
    ctx, _ = context(engine_db)
    queued(engine_db)
    counts = []

    def record(*args):
        counts.append(1)

    event.listen(ctx.engine, "after_cursor_execute", record)
    try:
        operation.scrape(ctx)
        first = len(counts)
        with ctx.engine.begin() as conn:
            for index in range(25):
                conn.execute(
                    text(
                        "INSERT INTO CFG_PROJECTS (PROJECT_CODE,PROJECT_NAME,DEFAULT_POOL) "
                        "VALUES (:code,:code,'default')"
                    ),
                    {"code": f"PROJECT_{index}"},
                )
                pipeline = add_pipeline(conn, f"HISTORY_{index}")
                run = start_run(conn, pipeline)
                transitions.finish_run(conn, run, RunStatus.SUCCESS, SYSTEM_ACTOR)
            before = conn.execute(text("SELECT COUNT(*) FROM AUD_ACTIONS")).scalar_one()
        counts.clear()
        assert operation.scrape(ctx).status == 200
        assert len(counts) == first
        assert first <= 5
        with ctx.engine.connect() as conn:
            assert conn.execute(text("SELECT COUNT(*) FROM AUD_ACTIONS")).scalar_one() == before
    finally:
        event.remove(ctx.engine, "after_cursor_execute", record)


def test_busy_slot_and_database_down_have_distinct_secret_safe_remedies(engine_db, monkeypatch):
    ctx, registry = context(engine_db)
    with registry.scrape_lock:
        assert operation.scrape(ctx).status == 503

    @contextmanager
    def busy(*args):
        yield False

    with monkeypatch.context() as patch:
        patch.setattr(operation, "connection_slot", busy)
        response = operation.scrape(ctx)
        assert response.status == 503
        for part in ("metrics connection", "busy", "expected a free probe session", "retry"):
            assert part in response.body

    def unavailable(*args):
        raise OperationalError("secret SQL", {}, Exception("password=private"))

    monkeypatch.setattr(operation, "probe_session", unavailable)
    response = operation.scrape(ctx)
    assert response.status == 503
    for part in ("metrics Engine DB", "unavailable", "expected a readable", "check connectivity"):
        assert part in response.body
    assert "private" not in response.body and "secret SQL" not in response.body


def test_statement_labels_are_catalog_names_or_unknown_and_never_sql(engine_db):
    ctx, registry = context(engine_db)
    with ctx.engine.connect() as conn:
        conn.execute(
            text("SELECT 'private-password'").execution_options(etl_craft_query='bad"\nlabel')
        )
    body = registry.render({})
    assert value(body, "engine_db_statement_latency_seconds_count", query="unknown") >= 1
    assert "private-password" not in body and "bad" not in body


def test_scrape_uses_system_actor_and_never_checks_out_a_work_connection(engine_db):
    ctx, _ = context(engine_db)
    seen = []
    baseline = ctx.engine.pool.checkedout()

    def observe(*args):
        seen.append((current_actor(), ctx.engine.pool.checkedout()))

    event.listen(ctx.engine, "after_cursor_execute", observe)
    try:
        with acting_as(Actor("unsigned visitor", ActorKind.USER)):
            assert operation.scrape(ctx).status == 200
        assert seen and all(actor == SYSTEM_ACTOR and count == baseline for actor, count in seen)
    finally:
        event.remove(ctx.engine, "after_cursor_execute", observe)


def test_invalidated_transaction_rolls_back_without_reconnecting(engine_db):
    ctx, _ = context(engine_db)
    with ctx.engine.connect() as conn:
        conn.execute(text("SELECT 1"))
        conn.invalidate()
        conn.rollback()
        assert conn.invalidated and not conn.in_transaction()
    assert operation.scrape(ctx).status == 200
