"""Clearing business-rule flags with bounded statements and one transaction."""

import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import event, text

from etl_craft.engine.repository.business_rules import (
    BusinessRule,
    begin_rule_run,
    clear_rule_keys,
    fetch_active_rule_keys,
    finish_rule_run,
    flag_rule_keys,
)
from fixtures.metadata import add_pipeline, add_task, insert, start_run, task_run

NOW = datetime(2026, 1, 1, tzinfo=UTC)
BEFORE = NOW - timedelta(days=1)


def seed_flags(db, count):
    keys = [f"key-{i}" for i in range(count)]
    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "rules", handler="BUSINESS_RULES")
        task_run_id = task_run(conn, task, start_run(conn, pipeline), "IN-PROGRESS")
        rules = []
        for name in ("primary", "other"):
            rule_id = insert(
                conn,
                "INSERT INTO CFG_BUSINESS_RULES (BUSINESS_RULE_NAME, PIPELINE_ID, TASK_ID, "
                "BUSINESS_RULE_SQL, BUSINESS_RULE_TYPE, BUSINESS_RULE_KEY_COLUMN, TARGET_TABLE, "
                "SEQUENCE_NUMBER) VALUES (:name, :pipeline, :task, 'SELECT 1', 'REJECT', "
                "'ROW_ID', 'sales.orders', 1)",
                "BUSINESS_RULE_ID",
                name=name,
                pipeline=pipeline,
                task=task,
            )
            rule = BusinessRule(rule_id, name, "SELECT 1", "REJECT", "ROW_ID", "sales.orders", 1)
            binding = begin_rule_run(conn, rule_id, task_run_id, BEFORE)
            rules.append((rule, binding.business_rule_run_id))
        rule, run_id = rules[0]
        if keys:
            flag_rule_keys(conn, rule, run_id, keys[:1], BEFORE)
            clear_rule_keys(conn, rule.business_rule_id, keys[:1], BEFORE)
        flag_rule_keys(conn, rule, run_id, [*keys, "keep"], BEFORE)
        other, other_run = rules[1]
        flag_rule_keys(conn, other, other_run, ["key-0"], BEFORE)
    return keys, rules


@contextmanager
def parameter_limit(conn):
    """Use the statement budget on SQLite, restoring it before the connection returns."""
    driver = conn.connection.driver_connection
    if conn.dialect.name != "sqlite":
        yield
        return
    previous = driver.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 1002)
    try:
        yield
    finally:
        driver.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, previous)


def observe_updates(engine, callback):
    def before_execute(conn, cursor, statement, parameters, context, executemany):
        if "UPDATE AUD_BUSINESS_RULES_RESULTS" in statement:
            callback(parameters)

    event.listen(engine, "before_cursor_execute", before_execute)
    return before_execute


def assert_cleared(db, keys, rules):
    rule, run_id = rules[0]
    other, _ = rules[1]
    with db.engine.connect() as conn:
        assert fetch_active_rule_keys(conn, rule.business_rule_id) == {"keep"}
        assert fetch_active_rule_keys(conn, other.business_rule_id) == {"key-0"}
        cleared = conn.execute(
            text(
                "SELECT COUNT(*) FROM AUD_BUSINESS_RULES_RESULTS "
                "WHERE BUSINESS_RULE_ID = :rule AND ACTIVE_FLAG = 'N' AND END_DATE = :now"
            ),
            {"rule": rule.business_rule_id, "now": NOW},
        ).scalar_one()
        assert cleared == len(keys)
        history = conn.execute(
            text(
                "SELECT COUNT(*) FROM AUD_BUSINESS_RULES_RESULTS "
                "WHERE BUSINESS_RULE_ID = :rule AND ACTIVE_FLAG = 'N' AND END_DATE = :before"
            ),
            {"rule": rule.business_rule_id, "before": BEFORE},
        ).scalar_one()
        assert history == bool(keys)
        assert (
            conn.execute(
                text(
                    "SELECT STATUS FROM AUD_BUSINESS_RULES_RUN_LOG "
                    "WHERE BUSINESS_RULE_RUN_ID = :run"
                ),
                {"run": run_id},
            ).scalar_one()
            == "SUCCESS"
        )


def clear_and_record(db, keys, rules):
    sizes = []
    listener = observe_updates(db.engine, lambda parameters: sizes.append(len(parameters)))
    rule, run_id = rules[0]
    try:
        with db.engine.begin() as conn, parameter_limit(conn):
            clear_rule_keys(conn, rule.business_rule_id, keys, NOW)
            finish_rule_run(conn, run_id, "SUCCESS", NOW)
    finally:
        event.remove(db.engine, "before_cursor_execute", listener)
    assert all(size <= 1002 for size in sizes)
    assert bool(sizes) == bool(keys)
    assert_cleared(db, keys, rules)


@pytest.mark.parametrize("count", [0, 1000, 1001])
def test_clearing_flags_at_the_statement_budget(engine_db, count):
    keys, rules = seed_flags(engine_db, count)
    clear_and_record(engine_db, keys, rules)


def test_tens_of_thousands_of_flags_can_be_cleared(engine_db):
    count = 40000 if engine_db.dialect.name == "sqlite" else 70000
    keys, rules = seed_flags(engine_db, count)
    clear_and_record(engine_db, keys, rules)


def test_a_later_batch_failure_rolls_back_flags_and_rule_completion(engine_db):
    keys, rules = seed_flags(engine_db, 2501)
    rule, run_id = rules[0]
    updates = []

    def fail_second_batch(parameters):
        updates.append(len(parameters))
        if len(updates) == 2:
            raise RuntimeError("second flag batch failed")

    listener = observe_updates(engine_db.engine, fail_second_batch)
    try:
        with (
            pytest.raises(RuntimeError, match="second flag batch failed"),
            engine_db.engine.begin() as conn,
        ):
            flag_rule_keys(conn, rule, run_id, ["new"], NOW)
            clear_rule_keys(conn, rule.business_rule_id, keys, NOW)
            finish_rule_run(conn, run_id, "SUCCESS", NOW)
    finally:
        event.remove(engine_db.engine, "before_cursor_execute", listener)
    assert len(updates) == 2
    with engine_db.engine.connect() as conn:
        assert fetch_active_rule_keys(conn, rule.business_rule_id) == set(keys) | {"keep"}
        assert (
            conn.execute(
                text(
                    "SELECT STATUS FROM AUD_BUSINESS_RULES_RUN_LOG "
                    "WHERE BUSINESS_RULE_RUN_ID = :run"
                ),
                {"run": run_id},
            ).scalar_one()
            == "IN-PROGRESS"
        )
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) FROM AUD_BUSINESS_RULES_RESULTS "
                    "WHERE BUSINESS_RULE_KEY = 'new' OR END_DATE = :now"
                ),
                {"now": NOW},
            ).scalar_one()
            == 0
        )
