"""Business rules against real warehouses: flagging, clearing, waves, retries and failures."""

from dataclasses import replace

import pytest
from sqlalchemy import text

from etl_craft.core.errors import HandlerError
from etl_craft.handlers import business_rules
from fixtures.business_rules import add_rule, run_rules


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
        SELECT_QUERY="SELECT 10 AS order_id, 1 AS customer_id, 5 AS amount "
        "UNION ALL SELECT 11, 2, -3 UNION ALL SELECT 12, 3, 7",
    )
    w.task("rules")
    return w


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


def test_rules_receive_pipeline_and_task_run_identities(world):
    w = world
    context = replace(w.task("rules"), handler="BUSINESS_RULES")
    add_rule(
        w,
        "identities",
        f"SELECT t.order_id FROM $$rows_in_scope t WHERE :pipeline_id = {context.pipeline_id} "
        f"AND :pipeline_run_id = {context.pipeline_run_id} "
        f"AND :task_run_id = {context.task_run_id}",
    )
    before = w.rows(f"SELECT pipeline_id, pipeline_run_id, task_run_id FROM {w.name('orders')}")
    result = business_rules.run(context, w.engine_db)
    assert result.insert_count == 3
    assert (
        w.rows(f"SELECT pipeline_id, pipeline_run_id, task_run_id FROM {w.name('orders')}")
        == before
    )


def test_rules_flag_breaking_rows_and_a_rerun_or_a_forced_run_clears_fixed_ones(world):
    w = world
    inactive = (
        f"SELECT t.order_id FROM $$rows_in_scope t JOIN {w.schema}.customers c ON "
        f"c.id = t.customer_id AND c.active = 'N'"
    )
    negative = (
        f"SELECT t.order_id FROM $$rows_in_scope t JOIN {w.schema}.customers c ON "
        f"c.id = t.customer_id AND t.amount < 0"
    )
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
    add_rule(
        w,
        "good",
        f"SELECT t.order_id FROM $$rows_in_scope t JOIN {w.schema}.customers c ON "
        f"c.id = t.customer_id",
    )
    add_rule(
        w,
        "broken",
        f"SELECT t.order_id FROM $$rows_in_scope t JOIN {w.schema}.no_such_table x "
        f"ON x.id = t.order_id",
    )
    add_rule(w, "later", "SELECT order_id FROM $$rows_in_scope", sequence=2)
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
        ({"sql": "SELECT 1; SELECT 2"}, "must be one read-only SELECT; it holds 2 statements"),
    ],
)
def test_a_bad_rule_stops_the_task_before_any_rule_runs(world, changes, message):
    w = world
    add_rule(w, "fine", "SELECT order_id FROM $$rows_in_scope")
    add_rule(w, "bad", changes.get("sql", "SELECT 1"), key=changes.get("key", "order_id"))
    with pytest.raises(HandlerError, match=message) as failure:
        run_rules(w)
    assert not failure.value.retryable
    assert run_log(w) == []


def test_a_task_without_rules_fails(world):
    with pytest.raises(HandlerError, match="no active row in CFG_BUSINESS_RULES"):
        run_rules(world)


def test_a_deleted_rows_flag_waits_for_a_forced_check(world):
    w = world
    inactive = (
        f"SELECT t.order_id FROM $$rows_in_scope t JOIN {w.schema}.customers c ON "
        f"c.id = t.customer_id AND c.active = 'N'"
    )
    add_rule(w, "inactive_customer", inactive)
    assert run_rules(w).insert_count == 2
    # A normal check defers flags for deleted keys to an explicit full check.
    w.execute(f"DELETE FROM {w.name('orders')} WHERE order_id = 11")
    gone = run_rules(w, rerun=True)
    assert (gone.insert_count, gone.update_count) == (0, 0)
    assert flags(w) == [
        ("inactive_customer", "11", "Y", "REJECT"),
        ("inactive_customer", "12", "Y", "REJECT"),
    ]
    assert run_rules(w, force=True).update_count == 1
    assert flags(w) == [
        ("inactive_customer", "11", "N", "REJECT"),
        ("inactive_customer", "12", "Y", "REJECT"),
    ]


@pytest.mark.parametrize("action", ["SCD1_MERGE", "SCD2_MERGE"])
def test_only_the_active_version_of_a_versioned_table_is_judged(sql_world, action):
    w = sql_world
    w.execute(
        f"CREATE TABLE {w.name('customers')} AS SELECT 1 AS id, 'Y' AS active UNION ALL "
        "SELECT 2, 'N'"
    )
    w.setup("orders", "SELECT 10 AS order_id, 1 AS customer_id", action)
    params = {
        "SQL_ACTION": action,
        "TARGET_OBJECT": "orders",
        "MERGE_KEY": "order_id",
        "MERGE_COMPARE_COLUMNS": "customer_id",
    }
    w.run("orders", SELECT_QUERY="SELECT 10 AS order_id, 2 AS customer_id", **params)
    w.run("orders", SELECT_QUERY="SELECT 10 AS order_id, 1 AS customer_id", **params)
    w.task("rules")
    add_rule(
        w,
        "inactive_customer",
        f"SELECT t.order_id FROM $$rows_in_scope t JOIN {w.schema}.customers c ON "
        f"c.id = t.customer_id AND c.active = 'N'",
    )
    # The closed version points at inactive customer 2; only the active one is judged.
    forced = run_rules(w, force=True)
    assert forced.insert_count == 0
    assert flags(w) == []


@pytest.mark.parametrize("ending", [";", " -- the last line is a comment", ";\n-- and a comment"])
def test_rule_sql_endings_work_when_flagging_and_clearing(world, ending):
    w = world
    add_rule(
        w, "negative_amount", "SELECT t.order_id FROM $$rows_in_scope t WHERE t.amount < 0" + ending
    )
    first = run_rules(w)
    assert (first.insert_count, first.update_count) == (1, 0)
    assert flags(w) == [("negative_amount", "11", "Y", "REJECT")]
    w.execute(f"UPDATE {w.name('orders')} SET amount = 3 WHERE order_id = 11")
    cleared = run_rules(w, rerun=True)
    assert (cleared.insert_count, cleared.update_count) == (0, 1)
    assert flags(w) == [("negative_amount", "11", "N", "REJECT")]
    assert run_log(w) == [("negative_amount", "SUCCESS")]


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("SELECT order_id FROM $$rows_in_scope WHERE amount < 0", {"11"}),
        ('SELECT order_id AS "ORDER_ID" FROM $$rows_in_scope WHERE amount < 0', {"11"}),
        (
            "WITH etl_craft_rows_in_scope AS (SELECT 999 AS order_id), "
            "etl_craft_0_scope AS (SELECT 999 AS order_id) "
            "SELECT order_id FROM $$rows_in_scope WHERE amount < 0",
            {"11"},
        ),
        (
            "SELECT t.order_id FROM $$rows_in_scope t JOIN "
            "(SELECT customer_id FROM $$rows_in_scope GROUP BY customer_id HAVING COUNT(*) > 1) g "
            "ON g.customer_id = t.customer_id",
            {"10", "11", "12"},
        ),
        (
            "SELECT order_id FROM (SELECT order_id, ROW_NUMBER() OVER (ORDER BY order_id DESC) "
            "AS position FROM $$rows_in_scope) ranked WHERE position = 1",
            {"12"},
        ),
        (
            "WITH negative AS (SELECT * FROM $$rows_in_scope WHERE amount < 0) "
            "SELECT order_id FROM negative",
            {"11"},
        ),
        (
            "SELECT t.order_id FROM $$rows_in_scope t LEFT JOIN {customers} c "
            "ON c.id = t.customer_id AND c.active = 'N' WHERE c.id IS NULL",
            {"10", "11", "12"},
        ),
    ],
)
def test_complete_queries_run_without_a_predicate_wrapper(world, query, expected):
    w = world
    w.execute(f"UPDATE {w.name('orders')} SET customer_id = 1")
    add_rule(w, "native", query.format(customers=w.name("customers")))
    assert run_rules(w).insert_count == len(expected)
    assert {key for _, key, active, _ in flags(w) if active == "Y"} == expected


def test_duplicate_and_outside_keys_report_counts_without_duplicate_flags(world, caplog):
    w = world
    rule = add_rule(
        w,
        "counts",
        "SELECT order_id FROM $$rows_in_scope "
        "UNION ALL SELECT order_id FROM $$rows_in_scope UNION ALL SELECT 999 AS order_id",
    )
    with caplog.at_level("INFO", logger="etl_craft.handlers.business_rules"):
        result = run_rules(w)
    assert result.variables[f"BUSINESS_RULE_{rule}"] == {
        "returned": 7,
        "flagged": 3,
        "cleared": 0,
        "dropped": 1,
    }
    assert "7 returned, 3 flagged, 0 cleared, 1 dropped" in caplog.text
    assert len(flags(w)) == 3


def test_null_keys_fail_before_flag_publication(world, caplog):
    w = world
    add_rule(w, "null_keys", "SELECT CAST(NULL AS INTEGER) AS order_id FROM $$rows_in_scope")
    with pytest.raises(HandlerError) as failure:
        run_rules(w)
    for part in (
        "null_keys",
        "3 NULL key(s)",
        "order_id",
        "expected non-NULL",
        "correct BUSINESS_RULE_SQL",
    ):
        assert part in str(failure.value)
        assert part in caplog.text
    assert flags(w) == []
    assert run_log(w) == [("null_keys", "FAILED")]


def test_corrected_out_of_run_flags_wait_for_force(world):
    w = world
    add_rule(w, "negative", "SELECT order_id FROM $$rows_in_scope WHERE amount < 0")
    assert run_rules(w).insert_count == 1
    w.execute(
        f"UPDATE {w.name('orders')} SET amount = 5, PIPELINE_RUN_ID = 999 WHERE order_id = 11"
    )
    assert run_rules(w, rerun=True).update_count == 0
    assert flags(w) == [("negative", "11", "Y", "REJECT")]
    assert run_rules(w, force=True).update_count == 1
    assert flags(w) == [("negative", "11", "N", "REJECT")]


def test_shared_cloud_rule_contract(sql_world):
    from fixtures.business_rules import check_business_rule_queries

    check_business_rule_queries(sql_world)


def test_check_query_count_is_constant_above_500_and_db_clears_are_batched(world, monkeypatch):
    from sqlalchemy import event

    w = world
    source = (
        "SELECT order_id, -1 AS amount FROM (VALUES "
        + ",".join(f"({number})" for number in range(1001))
        + ") v(order_id)"
    )
    w.run("bulk", SQL_ACTION="CREATE_TABLE", TARGET_OBJECT="bulk_orders", SELECT_QUERY=source)
    add_rule(
        w, "bulk", "SELECT order_id FROM $$rows_in_scope WHERE amount < 0", target="bulk_orders"
    )
    queries = []
    original = business_rules._RuleRunner._query

    def query(self, sql, **options):
        queries.append(sql)
        return original(self, sql, **options)

    monkeypatch.setattr(business_rules._RuleRunner, "_query", query)
    assert run_rules(w).insert_count == 1001
    assert len(queries) == 2
    queries.clear()
    w.execute(f"UPDATE {w.name('bulk_orders')} SET amount = 1")
    batches = []

    def update(conn, cursor, statement, parameters, context, executemany):
        if statement.lstrip().upper().startswith("UPDATE AUD_BUSINESS_RULES_RESULTS"):
            batches.append(len(parameters))

    event.listen(w.engine_db, "before_cursor_execute", update)
    try:
        assert run_rules(w, rerun=True).update_count == 1001
    finally:
        event.remove(w.engine_db, "before_cursor_execute", update)
    assert len(queries) == 2
    assert len(batches) == 2
    assert max(batches) <= 1002


def test_an_upgraded_fragment_keeps_the_same_breaking_row_keys(world):
    from etl_craft.dialects.business_rules import converted_rule
    from etl_craft.warehouse.connection import warehouse_dialect

    w = world
    original = f"SELECT 1 FROM {w.schema}.customers c WHERE c.id=t.customer_id AND c.active='N'"
    query = converted_rule(original, "order_id", warehouse_dialect(w.config).sqlglot_dialect)
    add_rule(w, "converted", query)
    assert run_rules(w).insert_count == 2
    assert flags(w) == [("converted", "11", "Y", "REJECT"), ("converted", "12", "Y", "REJECT")]


def test_a_wildcard_without_the_key_fails_before_publication(world):
    w = world
    add_rule(w, "wildcard", f"SELECT * FROM {w.name('customers')}")
    with pytest.raises(HandlerError) as failure:
        run_rules(w)
    for part in (
        "wildcard",
        "returns",
        "id",
        "active",
        "expected BUSINESS_RULE_KEY_COLUMN='order_id'",
        "select that target row key",
    ):
        assert part in str(failure.value)
    assert flags(w) == []
    assert run_log(w) == [("wildcard", "FAILED")]


@pytest.mark.parametrize("has_key", [True, False])
def test_validate_resolves_wildcard_rule_output_on_the_warehouse(world, has_key):
    from sqlalchemy import event
    from sqlalchemy.engine import Engine

    from etl_craft.core.enums import Handler
    from etl_craft.services.doctor import Status
    from etl_craft.services.validate import validate

    w = world
    query = "SELECT * FROM $$rows_in_scope" if has_key else f"SELECT * FROM {w.name('customers')}"
    add_rule(w, "wildcard", query)
    with w.engine_db.begin() as conn:
        conn.execute(
            text("UPDATE CFG_TASKS SET HANDLER=:handler WHERE TASK_ID=:task"),
            {"handler": Handler.BUSINESS_RULES, "task": w.tasks["rules"]},
        )
    probes = []

    def query(conn, cursor, statement, parameters, context, executemany):
        normalized = statement.replace('"', "").replace("`", "").lower()
        if w.name("orders").lower() in normalized and "where 1=0" in normalized:
            probes.append(statement)

    event.listen(Engine, "before_cursor_execute", query)
    try:
        report = validate(w.engine_db, w.config)
    finally:
        event.remove(Engine, "before_cursor_execute", query)
    assert len(probes) == 1
    failures = [
        finding.message
        for finding in report.findings
        if finding.where == "P.rules" and finding.status is Status.FAIL
    ]
    if has_key:
        assert failures == []
    else:
        assert any(
            "expected BUSINESS_RULE_KEY_COLUMN='order_id'" in message for message in failures
        )
