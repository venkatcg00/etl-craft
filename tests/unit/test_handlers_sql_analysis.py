"""Reading SQL with sqlglot: one read-only query passes, any write is named, and SQL sqlglot
cannot parse falls back to the word-level check."""

import re
from pathlib import Path

import pytest

from etl_craft.core import text
from etl_craft.handlers.sql.analysis import Reading, read_only_problem, read_query

DEMO_SQL = sorted((Path(__file__).parents[2] / "examples" / "demo" / "sql_files").glob("*.sql"))

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT t.copy, t.grant_date FROM sales.orders t",
        'SELECT "Create" AS created FROM sales.orders',
        "SELECT 'drop table x' AS note -- delete me later\nFROM sales.orders",
        "WITH recent AS (SELECT id FROM sales.orders) SELECT id FROM recent",
        "SELECT 1 AS n UNION ALL SELECT 2",
        "VALUES (1, 'a')",
        "SELECT 1;",
    ],
)
def test_a_read_only_query_passes(sql):
    assert read_query(sql, "postgres").problem is None
    assert read_only_problem(sql, "postgres") is None


def test_a_column_named_like_a_statement_no_longer_reads_as_one():
    sql = "SELECT t.copy FROM sales.orders t"
    assert text.read_only_problem(sql) == "contains ['COPY'] — a read-only SELECT should not"
    assert read_only_problem(sql, "duckdb") is None


@pytest.mark.parametrize(
    ("sql", "problem"),
    [
        ("INSERT INTO sales.orders SELECT 1", "is not a query but INSERT"),
        ("UPDATE sales.orders SET amount = 0", "is not a query but UPDATE"),
        ("DROP TABLE sales.orders", "is not a query but DROP"),
        ("CALL refresh_everything()", "is not a query but CALL"),
        (
            "WITH gone AS (DELETE FROM sales.orders RETURNING *) SELECT * FROM gone",
            "contains DELETE, which a read-only query cannot",
        ),
        ("SELECT 1; SELECT 2", "holds 2 statements"),
    ],
)
def test_a_statement_that_writes_is_named(sql, problem):
    assert read_only_problem(sql, "postgres") == problem


def test_sql_sqlglot_cannot_parse_says_where_and_falls_back_to_the_word_check():
    reading = read_query("SELECT id,\n  amount FROM sales.orders WHERE", "postgres")
    assert reading.problem is None
    assert reading.unparsed is not None and reading.unparsed.startswith("line 2, column ")

    assert read_only_problem("SELECT id FROM sales.orders WHERE", "postgres") is None
    assert read_only_problem("SELEC id FROM sales.orders", "postgres") == (
        "starts with 'SELEC', not SELECT/WITH"
    )


def test_each_warehouse_dialect_reads_its_own_syntax():
    qualify = (
        "SELECT id FROM sales.orders "
        "QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1"
    )
    assert read_query(qualify, "snowflake") == read_query(qualify, "duckdb")
    assert read_only_problem(qualify, "snowflake") is None
    assert (
        read_only_problem("SELECT id FROM sales.orders TABLESAMPLE (10 PERCENT)", "databricks")
        is None
    )


@pytest.mark.parametrize("dialect", ["duckdb", "postgres", "trino", "snowflake", "databricks"])
def test_the_demo_sql_reads_as_read_only_queries_in_every_warehouse_dialect(dialect):
    assert DEMO_SQL
    for path in DEMO_SQL:
        raw = path.read_text("utf-8")

        def uses(token, raw=raw):
            return re.search(rf"\$\${token}\b", raw) is not None

        sql = text.substitute_task_tokens(
            raw,
            pipeline_run_id=7,
            refresh_type="INCREMENTAL",
            pipeline_run_id_substitution=uses("pipeline_run_id"),
            filter_enabled=uses("pipeline_run_id_filter"),
            pipeline_id_substitution=uses("pipeline_id"),
            task_run_id_substitution=uses("task_run_id"),
            run_date_substitution=uses("run_date"),
        )
        assert read_query(sql, dialect) == Reading(), path.name
