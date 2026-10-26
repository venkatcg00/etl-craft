"""Flag snapshots for one rule serialize; independent rules retain parallel waves."""

import threading
from dataclasses import replace

import pytest

from etl_craft.engine import locks
from etl_craft.handlers import business_rules
from fixtures.business_rules import add_rule
from fixtures.metadata import add_pipeline, start_run
from fixtures.races import two_at_once
from fixtures.sql_warehouse import warehouses


@pytest.fixture
def rule_world(sql_world, engine_db):
    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "BR_LOCK")
        run = start_run(conn, pipeline)
    w = replace(
        sql_world,
        config=replace(sql_world.config, engine=engine_db.config.engine),
        engine_db=engine_db.engine,
        pipeline_id=pipeline,
        pipeline_run_id=run,
        tasks={},
    )
    w.task("rules")
    return w


@warehouses("postgres")
def test_concurrent_checks_cannot_publish_competing_snapshots(rule_world, monkeypatch):
    w = rule_world
    add_rule(w, "one", "SELECT order_id FROM $$rows_in_scope")
    context = replace(w.task("rules"), handler="BUSINESS_RULES", force=True)
    monkeypatch.setattr(business_rules._RuleRunner, "_scope", lambda self, checked: "1=1")
    monkeypatch.setattr(
        business_rules._RuleRunner, "_keys", lambda self, checked, scope: ({"11"}, {"11"}, 1, 0)
    )
    outcomes = two_at_once(
        lambda: business_rules.run(context, w.engine_db),
        lambda: business_rules.run(context, w.engine_db),
        at=(locks.ResourceLock, "hold"),
    )
    assert sorted(outcome.insert_count for outcome in outcomes) == [0, 1]
    assert sum(outcome.update_count for outcome in outcomes) == 0
    from sqlalchemy import text

    with w.engine_db.connect() as conn:
        rows = (
            conn.execute(
                text(
                    "SELECT BUSINESS_RULE_KEY AS key, ACTIVE_FLAG AS active FROM "
                    "AUD_BUSINESS_RULES_RESULTS"
                )
            )
            .mappings()
            .all()
        )
    assert [dict(row) for row in rows] == [{"key": "11", "active": "Y"}]


@warehouses("postgres")
def test_different_rules_check_in_parallel(rule_world, monkeypatch):
    w = rule_world
    add_rule(w, "one", "SELECT order_id FROM $$rows_in_scope")
    add_rule(w, "two", "SELECT order_id FROM $$rows_in_scope")
    context = replace(w.task("rules"), handler="BUSINESS_RULES")
    barrier = threading.Barrier(2, timeout=10)
    monkeypatch.setattr(business_rules._RuleRunner, "_scope", lambda self, checked: "1=1")

    def keys(self, checked, scope):
        barrier.wait()
        return {str(checked.rule.business_rule_id)}, {str(checked.rule.business_rule_id)}, 1, 0

    monkeypatch.setattr(business_rules._RuleRunner, "_keys", keys)
    result = business_rules.run(context, w.engine_db)
    assert result.insert_count == 2
    assert result.update_count == 0


def test_a_bounded_rule_lock_failure_names_the_rule_and_remedy(engine_db):
    from etl_craft.core.actor import SYSTEM_ACTOR
    from etl_craft.core.errors import LockTimeoutError
    from etl_craft.engine import resource_locks
    from etl_craft.handlers.resources import LocalLocker

    lock = locks.business_rule(17, "customer_total")
    holder = resource_locks.Holder("action:other-check", "other-check", SYSTEM_ACTOR)
    with engine_db.engine.begin() as conn:
        assert resource_locks.acquire(conn, lock.key, lock.name, holder).budget is not None
    try:
        with (
            pytest.raises(LockTimeoutError) as failure,
            lock.hold(LocalLocker(engine_db.engine, engine_db.config), wait_seconds=0.01),
        ):
            pytest.fail("a second holder entered the rule check")
        for part in (
            "customer_total",
            "BUSINESS_RULE_ID=17",
            "lock unavailable",
            "other-check",
            "inspect its attempt before retrying",
        ):
            assert part in str(failure.value)
    finally:
        with engine_db.engine.begin() as conn:
            resource_locks.release(conn, lock.key, holder)
