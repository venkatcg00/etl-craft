"""REJECT_LIMIT on every local warehouse: the good rows load, the rest wait in <target>__rejects."""

from datetime import date

import pytest

from etl_craft.core.errors import HandlerError
from fixtures.sql_rejects import MERGE, MESSY, SHAPE, check_rejects, loaded, set_aside
from fixtures.sql_warehouse import warehouses

ORDERS = {**MERGE, "SQL_ACTION": "SCD1_MERGE", "TARGET_OBJECT": "orders"}


@pytest.mark.parametrize("merge", ["SCD1_MERGE", "SCD2_MERGE"])
def test_a_messy_merge_loads_its_good_rows_and_sets_the_rest_aside(sql_world, merge):
    check_rejects(sql_world, "orders", merge)
    assert "orders__rejects" in sql_world.tables()


def test_over_the_limit_the_most_common_reasons_are_named(sql_world):
    w = sql_world
    w.setup("orders", SHAPE, "SCD1_MERGE")
    with pytest.raises(
        HandlerError,
        match=(
            r"4 of 6 row\(s\) do not fit, more than REJECT_LIMIT=50% allows \(3\), so nothing "
            r"was written\. Most common: id MERGE_KEY returned more than once \(2 rows\); id NULL "
            r"in MERGE_KEY \(1 rows\); order_date does not cast to "
        ),
    ):
        w.run("load", **ORDERS, SOURCE_SQL=MESSY, REJECT_LIMIT="50%")
    assert loaded(w, "orders") == []


def test_with_a_dedupe_order_duplicate_keys_load_their_first_row(sql_world):
    w = sql_world
    w.setup("orders", SHAPE, "SCD1_MERGE")
    params = {**ORDERS, "MERGE_DEDUPE_ORDER": "order_date DESC", "REJECT_LIMIT": "2"}
    result = w.run("load", **params, SOURCE_SQL=MESSY)
    assert (result.source_count, result.insert_count, result.reject_count) == (6, 3, 2)
    assert loaded(w, "orders") == [
        (1, date(2026, 1, 31), "Pune"),
        (3, date(2026, 1, 3), "Goa"),
        (4, date(2026, 1, 4), "Leh"),
    ]
    assert [row[3] for row in set_aside(w, "orders")] == [
        "does not cast to date",
        "null in merge_key",
    ]


def test_without_a_limit_a_row_that_does_not_fit_fails_the_task(sql_world):
    w = sql_world
    w.setup("orders", SHAPE, "SCD1_MERGE")
    with pytest.raises(HandlerError):
        w.run("load", **ORDERS, SOURCE_SQL=MESSY)
    assert loaded(w, "orders") == []
    assert "orders__rejects" not in w.tables()


@pytest.mark.parametrize("action", ["APPEND_TABLE", "OVERWRITE_TABLE"])
def test_appends_and_overwrites_set_aside_values_that_do_not_cast(sql_world, action):
    w = sql_world
    w.setup("orders", SHAPE, action)
    source = (
        "SELECT CAST(1 AS BIGINT) AS id, '2026-01-31' AS order_date, 'Pune' AS city "
        "UNION ALL SELECT 2, 'someday', 'Delhi' UNION ALL SELECT NULL, '2026-01-04', 'Leh'"
    )
    params = {"SQL_ACTION": action, "TARGET_OBJECT": "orders", "SOURCE_SQL": source}
    result = w.run("load", **params, REJECT_LIMIT="1")
    assert (result.source_count, result.insert_count, result.reject_count) == (3, 2, 1)
    assert loaded(w, "orders") == [(1, date(2026, 1, 31), "Pune"), (None, date(2026, 1, 4), "Leh")]
    run = w.task("load").task_run_id
    assert set_aside(w, "orders") == [("2", "someday", "order_date", "does not cast to date", run)]


def test_a_retry_replaces_only_its_own_rejects(sql_world):
    w = sql_world
    w.setup("orders", SHAPE, "APPEND_TABLE")
    params = {"SQL_ACTION": "APPEND_TABLE", "TARGET_OBJECT": "orders", "REJECT_LIMIT": "5"}
    w.run("load", **params, SOURCE_SQL="SELECT 1 AS id, 'never' AS order_date, 'Pune' AS city")
    first = w.task("load").task_run_id
    w.new_run()
    two_bad = (
        "SELECT 2 AS id, 'soon' AS order_date, 'Goa' AS city UNION ALL SELECT 3, 'late', 'Goa'"
    )
    w.run("load", **params, SOURCE_SQL=two_bad)
    retry = w.run(
        "load", **params, SOURCE_SQL="SELECT 3 AS id, 'late' AS order_date, 'Goa' AS city"
    )
    second = w.task("load").task_run_id
    assert second != first and retry.reject_count == 1
    assert set_aside(w, "orders") == [
        ("1", "never", "order_date", "does not cast to date", first),
        ("3", "late", "order_date", "does not cast to date", second),
    ]
    clean = w.run(
        "load", **params, SOURCE_SQL="SELECT 4 AS id, '2026-01-04' AS order_date, 'Leh' AS city"
    )
    assert clean.reject_count == 0
    assert [row[4] for row in set_aside(w, "orders")] == [first]


def test_a_new_column_in_the_select_is_added_to_the_rejects_table(sql_world):
    w = sql_world
    w.setup("orders", SHAPE, "APPEND_TABLE")
    params = {"SQL_ACTION": "APPEND_TABLE", "TARGET_OBJECT": "orders", "REJECT_LIMIT": "1"}
    w.run("load", **params, SOURCE_SQL="SELECT 1 AS id, 'x' AS order_date, 'Pune' AS city")
    w.execute(f"ALTER TABLE {w.name('orders')} ADD COLUMN region VARCHAR")
    w.run(
        "load",
        **params,
        SOURCE_SQL="SELECT 2 AS id, 'y' AS order_date, 'Goa' AS city, 'west' AS region",
    )
    assert w.rows(f"SELECT region FROM {w.name('orders__rejects')}") == [("west",)]


@warehouses("postgres")
def test_postgresql_sets_aside_text_longer_than_the_declared_length(sql_world):
    w = sql_world
    w.setup(
        "orders", "SELECT CAST(1 AS BIGINT) AS id, CAST('x' AS VARCHAR(5)) AS city", "APPEND_TABLE"
    )
    source = "SELECT 1 AS id, 'Pune' AS city UNION ALL SELECT 2, 'Bengaluru'"
    result = w.run(
        "load",
        SQL_ACTION="APPEND_TABLE",
        TARGET_OBJECT="orders",
        SOURCE_SQL=source,
        REJECT_LIMIT="1",
    )
    assert result.reject_count == 1
    assert w.rows(f"SELECT id, city FROM {w.name('orders')}") == [(1, "Pune")]
    assert w.rows(f"SELECT id, city, REJECT_REASON FROM {w.name('orders__rejects')}") == [
        ("2", "Bengaluru", "does not cast to character varying(5)")
    ]
