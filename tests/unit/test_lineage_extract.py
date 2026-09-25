"""Tracing one SELECT's columns to their sources."""

import pytest

from etl_craft.core.errors import MetadataError
from etl_craft.services.lineage import Edge, LineageGraph, extract, table_name

pytestmark = pytest.mark.unit


def edges(sql, target="sales.orders", dialect="postgres", catalog="analytics"):
    return [
        (e.target_column, e.source_object, e.source_column, e.transformation)
        for e in extract(sql, target, dialect=dialect, catalog=catalog)
    ]


def test_copies_expressions_constants_and_ctes():
    sql = """
        WITH o AS (SELECT id, cust_id, amount FROM staging.orders)
        SELECT o.id AS order_id, c.name, o.amount * c.rate AS amount_usd,
               COUNT(*) AS n, 'x' AS source_system
        FROM o JOIN analytics.ref.customers c ON c.id = o.cust_id
        GROUP BY 1, 2, 3
    """
    assert edges(sql) == [
        ("order_id", "staging.orders", "id", "copy"),
        ("name", "ref.customers", "name", "copy"),
        # One edge per source column of an expression.
        ("amount_usd", "ref.customers", "rate", "o.amount * c.rate"),
        ("amount_usd", "staging.orders", "amount", "o.amount * c.rate"),
        ("n", None, None, "COUNT(*)"),
        ("source_system", None, None, "'x'"),
    ]


def test_names_are_lower_case_without_the_active_database():
    assert table_name(["ANALYTICS", "Sales", "Orders"], "analytics") == "sales.orders"
    assert table_name(["other_db", "sales", "orders"], "analytics") == "other_db.sales.orders"
    assert table_name(["", "sales", "orders"], None) == "sales.orders"
    assert {
        e.target_object
        for e in extract("SELECT 1 AS a", "ANALYTICS.Sales.X", dialect=None, catalog="analytics")
    } == {"sales.x"}


@pytest.mark.parametrize(
    ("sql", "message"),
    [
        ("SELECT * FROM staging.orders", "SELECT \\* cannot be traced"),
        ("SELECT FROM WHERE", "could not be parsed"),
        ("UPDATE t SET a = 1", "not a SELECT"),
    ],
)
def test_sql_that_cannot_be_traced_says_why(sql, message):
    with pytest.raises(MetadataError, match=message):
        extract(sql, "s.t", dialect="postgres")


def test_the_graph_walks_across_tasks_both_ways():
    graph = LineageGraph(
        [
            Edge("staging.orders", "amount", "raw.orders", "amt", "copy", "INGEST.stage"),
            Edge("sales.orders", "amount_usd", "staging.orders", "amount", "a * r", "SALES.load"),
            Edge("sales.orders", "amount_usd", "ref.rates", "rate", "a * r", "SALES.load"),
            Edge("mart.daily", "total", "sales.orders", "amount_usd", "SUM(a)", "MART.build"),
        ]
    )
    up = [
        (level, e.source_object, e.source_column)
        for level, e in graph.upstream("mart.daily", "total")
    ]
    assert up == [
        (1, "sales.orders", "amount_usd"),
        (2, "ref.rates", "rate"),
        (2, "staging.orders", "amount"),
        (3, "raw.orders", "amt"),
    ]
    assert [level for level, _ in graph.upstream("mart.daily", "total", depth=1)] == [1]
    down = [(level, e.target_object) for level, e in graph.downstream("raw.orders", "amt")]
    assert down == [(1, "staging.orders"), (2, "sales.orders"), (3, "mart.daily")]
    assert graph.upstream_tables("mart.daily") == [
        (1, "sales.orders", "MART.build"),
        (2, "ref.rates", "SALES.load"),
        (2, "staging.orders", "SALES.load"),
        (3, "raw.orders", "INGEST.stage"),
    ]
    assert graph.columns("sales.orders") == ["amount_usd"]
