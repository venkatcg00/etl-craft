"""Every warehouse writer contends for one durable, fenced resource owner."""

import threading
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
from etl_craft.core.errors import LockTimeoutError, StaleTransitionError
from etl_craft.engine import locks, resource_locks, transitions
from etl_craft.engine.resource_locks import Holder, acquire, release, renew
from etl_craft.handlers.resources import LocalLocker
from fixtures.metadata import add_pipeline, add_task
from fixtures.races import two_at_once


def holder(name, attempt_id=None, owner=None):
    return Holder(name, owner or name, SYSTEM_ACTOR, attempt_id)


def take(db, who, key=42, confirmed=None):
    with db.engine.begin() as conn:
        return resource_locks.acquire(conn, key, "test target", who, confirmed)


def expire(db):
    with db.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_RESOURCE_LOCKS SET LEASE_EXPIRES_AT=:past"),
            {"past": datetime.now(UTC) - timedelta(seconds=1)},
        )


def read(db):
    with db.engine.connect() as conn:
        return conn.execute(text("SELECT HOLDER_ID AS holder_id FROM AUD_RESOURCE_LOCKS")).one()


def test_two_holders_cannot_take_the_same_resource(engine_db):
    a, b = holder("a"), holder("b")
    results = [
        attempt.budget is not None
        for attempt in two_at_once(
            lambda: take(engine_db, a),
            lambda: take(engine_db, b),
            at=(engine_db.engine, "begin"),
        )
    ]
    assert sum(results) == 1
    assert read(engine_db).holder_id == ("a" if results[0] else "b")


def test_an_expired_lock_passes_only_once_its_holder_is_confirmed(engine_db):
    a, b = holder("a"), holder("b")
    assert take(engine_db, a).budget is not None
    expire(engine_db)
    first = take(engine_db, b)
    assert (first.budget, first.stale_holder) == (None, "a")
    assert read(engine_db).holder_id == "a"
    assert take(engine_db, b, confirmed="a").budget is not None
    with engine_db.engine.begin() as conn:
        release(conn, 42, a)
        with pytest.raises(StaleTransitionError, match="ownership ended or expired"):
            renew(conn, 42, a)
    assert read(engine_db).holder_id == "b"


def test_action_target_locks_are_shared_and_timeout_names_the_holder(engine_db):
    target = locks.target("catalog.schema.table", "catalog")
    locker = LocalLocker(engine_db.engine, engine_db.config)
    with target.hold(locker):
        started, finished = threading.Event(), threading.Event()
        errors = []

        def contend():
            with acting_as(SYSTEM_ACTOR):
                started.set()
                try:
                    with target.hold(locker, wait_seconds=0.1):
                        pytest.fail("a second holder entered the target")
                except LockTimeoutError as error:
                    errors.append(str(error))
                finally:
                    finished.set()

        thread = threading.Thread(target=contend)
        thread.start()
        assert started.wait(5)
        assert finished.wait(10)
        thread.join(1)
        assert len(errors) == 1
        assert "catalog.schema.table" in errors[0]
        assert "holder=action:" in errors[0]
        assert "held for" in errors[0]
        assert "0.1s" in errors[0]
    with target.hold(locker, wait_seconds=0.1):
        pass


def test_a_confirmation_for_another_holder_does_not_take_the_lock(engine_db):
    a, b, c = holder("a"), holder("b"), holder("c")
    assert take(engine_db, a).budget is not None
    expire(engine_db)
    assert take(engine_db, c, confirmed="a").budget is not None
    expire(engine_db)
    late = take(engine_db, b, confirmed="a")
    assert (late.budget, late.stale_holder) == (None, "c")
    assert read(engine_db).holder_id == "c"


def test_a_lost_attempt_keeps_its_lock_until_a_successor_confirms_it(engine_db):
    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "T")
        run = transitions.create_run(conn, pipeline, SYSTEM_ACTOR)
        summary = transitions.find_or_create_task_run(conn, task, run).task_run_id
        attempt = transitions.queue_attempt(conn, summary, SYSTEM_ACTOR)
        transitions.claim_attempt(
            conn,
            attempt,
            SYSTEM_ACTOR,
            owner="agent",
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
        )
        assert acquire(conn, 42, "target", holder("attempt", attempt, "agent")).budget
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET LEASE_EXPIRES_AT=:past WHERE ATTEMPT_ID=:id"),
            {"past": datetime.now(UTC) - timedelta(seconds=1), "id": attempt},
        )
        transitions.lose_attempt(conn, attempt, SYSTEM_ACTOR, owner="agent")
    assert read(engine_db).holder_id == "attempt"
    assert take(engine_db, holder("action")).stale_holder == "attempt"
    assert take(engine_db, holder("action"), confirmed="attempt").budget is not None


def test_claiming_confirmation_cannot_steal_a_live_lock(engine_db):
    old, incoming = holder("old"), holder("incoming")
    assert take(engine_db, old).budget is not None
    result = take(engine_db, incoming, confirmed=old.holder_id)
    assert result.budget is None
    assert result.stale_holder is None
    assert read(engine_db).holder_id == old.holder_id


@pytest.mark.parametrize("confirmable", [True, False])
def test_failed_local_warehouse_work_confirms_before_unlocking(engine_db, confirmable):
    confirmed = []

    def confirm(holder):
        assert read(engine_db).holder_id == holder
        confirmed.append(holder)
        if not confirmable:
            raise LockTimeoutError("warehouse still running")

    old = resource_locks.Owner(engine_db.engine, engine_db.config, confirm, "old")
    new = resource_locks.Owner(engine_db.engine, engine_db.config, confirmed.append, "new")
    expected = ConnectionError if confirmable else LockTimeoutError
    with (
        pytest.raises(expected),
        resource_locks.hold(old, 42, "s.target", 1),
    ):
        raise ConnectionError("warehouse connection dropped")
    assert confirmed == ["action:old"]
    if not confirmable:
        assert read(engine_db).holder_id == "action:old"
        expire(engine_db)
    with resource_locks.hold(new, 42, "s.target", 1):
        assert confirmed == ["action:old"] * (1 if confirmable else 2)
        assert read(engine_db).holder_id == "action:new"
    with engine_db.engine.connect() as conn:
        remaining = conn.execute(text("SELECT COUNT(*) AS remaining FROM AUD_RESOURCE_LOCKS")).one()
        assert remaining.remaining == 0


def test_grouped_resources_use_one_statement_for_each_lifecycle_step(engine_db):
    from fixtures.round_trips import count_round_trips

    who = holder("group")
    resources = ((42, "target"), (43, "warehouse"))
    with count_round_trips(engine_db.engine) as counter:
        with engine_db.engine.begin() as conn:
            budgets = resource_locks.reserve(conn, resources, who, {})
        with engine_db.engine.begin() as conn:
            assert resource_locks.renew(conn, (42, 43), who) > 0
        with engine_db.engine.begin() as conn:
            resource_locks.release(conn, (42, 43), who)
    assert set(budgets) == {42, 43}
    from etl_craft.dialects.engine import for_engine

    # PostgreSQL also sets the actor per transaction and notifies waiters on release.
    overhead = 4 if for_engine(engine_db.engine).supports_api_replicas else 0
    assert counter.result()["engine_statements"] == 3 + overhead
    assert counter.result()["engine_transactions"] == 3


def test_a_busy_resource_rolls_back_the_other_reservations(engine_db):
    assert take(engine_db, holder("old"), key=43).budget is not None
    owner = resource_locks.Owner(engine_db.engine, engine_db.config, lambda _: None, "new")
    with (
        pytest.raises(LockTimeoutError, match="holder=old"),
        resource_locks.hold(owner, 42, "target", 0.01, ((43, "warehouse"),)),
    ):
        pytest.fail("the whole resource set was not acquired")
    with engine_db.engine.connect() as conn:
        rows = conn.execute(
            text("SELECT LOCK_KEY AS lock_key, HOLDER_ID AS holder_id FROM AUD_RESOURCE_LOCKS")
        ).all()
    assert {row.lock_key: row.holder_id for row in rows} == {43: "old"}


def test_two_holders_cannot_split_a_group_of_resources(engine_db):
    def reserve_all(who):
        with engine_db.engine.begin() as conn:
            budgets = resource_locks.reserve(conn, ((42, "target"), (43, "warehouse")), who, {})
            if len(budgets) != 2:
                conn.rollback()
                return False
        return True

    a, b = holder("a"), holder("b")
    wins = two_at_once(
        lambda: reserve_all(a), lambda: reserve_all(b), at=(engine_db.engine, "begin")
    )
    assert sum(wins) == 1
    with engine_db.engine.connect() as conn:
        rows = conn.execute(
            text("SELECT LOCK_KEY AS lock_key, HOLDER_ID AS holder_id FROM AUD_RESOURCE_LOCKS")
        ).all()
    assert {row.lock_key for row in rows} == {42, 43}
    assert {row.holder_id for row in rows} == {"a" if wins[0] else "b"}
