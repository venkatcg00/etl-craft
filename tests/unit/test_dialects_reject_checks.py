"""What each warehouse checks before it lets a value into a column of a declared type."""

import pytest

from etl_craft.dialects.warehouse.registry import for_key

pytestmark = pytest.mark.unit


def test_a_cast_is_tried_and_a_declared_length_is_measured():
    duckdb = for_key("duckdb")
    assert duckdb.reject_checks("city", "DATE") == [
        ("TRY_CAST(city AS DATE) IS NULL", "does not cast to DATE")
    ]
    assert duckdb.reject_checks("city", "character varying(12)")[1] == (
        "LENGTH(CAST(city AS VARCHAR)) > 12",
        "is longer than 12 characters",
    )
    assert for_key("databricks").reject_checks("c", "VARCHAR(3)")[1][0] == (
        "LENGTH(CAST(c AS STRING)) > 3"
    )
    snowflake = for_key("snowflake").reject_checks("c", "NUMBER(5,2)")
    assert snowflake == [
        ("TRY_CAST(CAST(c AS VARCHAR) AS NUMBER(5,2)) IS NULL", "does not cast to NUMBER(5,2)")
    ]


def test_postgresql_asks_the_types_input_function():
    assert for_key("postgres").reject_checks("c", "character varying(5)") == [
        (
            "NOT pg_input_is_valid(CAST(c AS TEXT), 'character varying(5)')",
            "does not cast to character varying(5)",
        )
    ]
