"""Business rules against real warehouses: flagging, clearing, waves, retries and failures."""

from dataclasses import replace

import pytest
from sqlalchemy import text

from etl_craft.core.errors import HandlerError
from etl_craft.handlers import business_rules
from fixtures.metadata import insert


@pytest.fixture
def world(sql_world):
    """Orders written by this run, customers, and a BUSINESS_RULES task with no rules yet."""
    w = sql_world
    w.execute(
        f"CREATE TABLE {w.name('customers')} AS "
        "SELECT 1 AS id, 'Y' AS active UNION ALL SELECT 2, 'N' UNION ALL SELECT 3, 'N'"
    )
    w.run(
        "orders",
        SQL_ACTION="CREATE_TABLE",
        TARGET_OBJECT="orders",
        SOURCE_SQL="SELECT 10 AS order_id, 1 AS customer_id, 5 AS amount "
        "UNION ALL SELECT 11, 2, -3 UNION ALL SELECT 12, 3, 7",
    )
    w.task("rules")
    return w


def add_rule(w, name, sql, *, sequence=1, key="order_id", rule_type="REJECT"):
    with w.engine_db.begin() as conn:
        return insert(
            conn,
            "INSERT INTO CFG_BUSINESS_RULES (BUSINESS_RULE_NAME, PIPELINE_ID, TASK_ID, "
            "BUSINESS_RULE_SQL, BUSINESS_RULE_TYPE, BUSINESS_RULE_KEY_COLUMN, TARGET_TABLE, "
            "SEQUENCE_NUMBER) VALUES (:name, :p, :t, :rule_sql, :type, :key, :target, :seq)",
            "BUSINESS_RULE_ID",
            name=name,
            p=w.pipeline_id,
            t=w.tasks["rules"],
            rule_sql=sql,
            type=rule_type,
            key=key,
            target=f"{w.schema}.orders",
            seq=sequence,
        )


def run_rules(w, *, force=False, rerun=False):
    context = replace(w.task("rules"), handler="BUSINESS_RULES", force=force, rerun=rerun)
    return business_rules.run(context, w.engine_db)


def flags(w):
    with w.engine_db.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT r.BUSINESS_RULE_NAME AS name, x.BUSINESS_RULE_KEY AS rule_key, "
                "x.ACTIVE_FLAG AS active, x.STATUS AS status FROM AUD_BUSINESS_RULES_RESULTS x "
                "JOIN CFG_BUSINESS_RULES r ON r.BUSINESS_RULE_ID = x.BUSINESS_RULE_ID"
            )
        )
        return sorted(tuple(row) for row in rows)


def run_log(w):
    with w.engine_db.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT r.BUSINESS_RULE_NAME AS name, l.STATUS AS status "
                "FROM AUD_BUSINESS_RULES_RUN_LOG l "
                "JOIN CFG_BUSINESS_RULES r ON r.BUSINESS_RULE_ID = l.BUSINESS_RULE_ID"
            )
        )
        return sorted(tuple(row) for row in rows)


def test_rules_flag_breaking_rows_and_a_rerun_or_a_forced_run_clears_fixed_ones(world):
    w = world
    inactive = f"SELECT 1 FROM {w.schema}.customers c WHERE c.id = t.customer_id AND c.active = 'N'"
    negative = f"SELECT 1 FROM {w.schema}.customers c WHERE c.id = t.customer_id AND t.amount < 0"
    add_rule(w, "inactive_customer", inactive)
    add_rule(w, "negative_amount", negative, sequence=2, rule_type="REPORT")

    first = run_rules(w)
    assert (first.insert_count, first.update_count) == (3, 0)
    assert flags(w) == [
        ("inactive_customer", "11", "Y", "REJECT"),
        ("inactive_customer", "12", "Y", "REJECT"),
        ("negative_amount", "11", "Y", "REPORT"),
    ]
    # A retry under the same task run does not run the rules again.
    assert (run_rules(w).insert_count, run_rules(w).update_count) == (0, 0)

    # Customer 2 becomes active; the task run again after it succeeded checks the run's rows
    # again and clears order 11.
    w.execute(f"UPDATE {w.name('customers')} SET active = 'Y' WHERE id = 2")
    rerun = run_rules(w, rerun=True)
    assert (rerun.insert_count, rerun.update_count) == (0, 1)

    # Customer 3 becomes active; a forced run checks every row and clears order 12.
    w.execute(f"UPDATE {w.name('customers')} SET active = 'Y' WHERE id = 3")
    forced = run_rules(w, force=True)
    assert (forced.insert_count, forced.update_count) == (0, 1)
    assert flags(w) == [
        ("inactive_customer", "11", "N", "REJECT"),
        ("inactive_customer", "12", "N", "REJECT"),
        ("negative_amount", "11", "Y", "REPORT"),
    ]


def test_a_failing_rule_fails_the_task_after_its_wave_finishes(world):
    w = world
    add_rule(w, "good", f"SELECT 1 FROM {w.schema}.customers c WHERE c.id = t.customer_id")
    add_rule(w, "broken", f"SELECT 1 FROM {w.schema}.no_such_table x WHERE x.id = t.order_id")
    add_rule(w, "later", "SELECT 1", sequence=2)
    with pytest.raises(HandlerError) as error:
        run_rules(w)
    message = str(error.value)
    assert message.startswith(
        "1 of 2 business rule(s) failed in wave SEQUENCE_NUMBER=1: broken on "
    )
    assert "find the keys that break the rule failed" in message
    # The good rule finished and recorded its flags; the later wave never started.
    assert run_log(w) == [("broken", "FAILED"), ("good", "SUCCESS")]
    assert len(flags(w)) == 3
    # Once the table exists, running the task again reruns only the failed rule, then the rest.
    w.execute(f"CREATE TABLE {w.name('no_such_table')} AS SELECT 11 AS id")
    retried = run_rules(w)
    assert run_log(w) == [("broken", "SUCCESS"), ("good", "SUCCESS"), ("later", "SUCCESS")]
    assert retried.insert_count == 1 + 3


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"key": "order id"}, "BUSINESS_RULE_KEY_COLUMN='order id' is not a plain column name"),
        ({"sql": "DELETE FROM x"}, "BUSINESS_RULE_SQL must be a read-only SELECT"),
        ({"sql": "SELECT 1; SELECT 2"}, "must be one correlated SELECT; it holds 2 statements"),
    ],
)
def test_a_bad_rule_stops_the_task_before_any_rule_runs(world, changes, message):
    w = world
    add_rule(w, "fine", "SELECT 1")
    add_rule(w, "bad", changes.get("sql", "SELECT 1"), key=changes.get("key", "order_id"))
    with pytest.raises(HandlerError, match=message):
        run_rules(w)
    assert run_log(w) == []


def test_a_task_without_rules_fails(world):
    with pytest.raises(HandlerError, match="no active row in CFG_BUSINESS_RULES"):
        run_rules(world)
