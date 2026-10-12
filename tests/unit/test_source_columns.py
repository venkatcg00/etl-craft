"""Source schema comparison and portable metadata ordering."""

from unittest.mock import Mock

import pytest

from etl_craft.engine.repository.source_columns import SourceColumn
from etl_craft.services.source_columns import drift, warehouse_columns

pytestmark = pytest.mark.unit


def test_warehouse_ordinals_are_normalized_to_positive_positions(monkeypatch):
    dialect = Mock()
    dialect.column_metadata_query.return_value = ("SELECT column_name, ordinal_position", {})
    dialect.full_column_types.return_value = {"z": "BIGINT", "a": "VARCHAR"}
    conn = Mock()
    conn.execute.return_value.all.return_value = [("z", 0), ("a", 1)]
    monkeypatch.setattr(
        "etl_craft.services.source_columns.warehouse_dialect", lambda config: dialect
    )
    monkeypatch.setattr(
        "etl_craft.services.source_columns.active_catalog", lambda config: "analytics"
    )
    assert warehouse_columns(conn, Mock(), "raw.orders") == [
        SourceColumn("raw.orders", "z", "BIGINT", 1),
        SourceColumn("raw.orders", "a", "VARCHAR", 2),
    ]


def test_duckdb_ignores_declared_string_widths():
    assert (
        drift(
            [SourceColumn("raw.orders", "name", "VARCHAR(100)", 1)],
            [SourceColumn("raw.orders", "name", "VARCHAR", 1)],
            "duckdb",
        )
        == []
    )


@pytest.mark.parametrize(
    ("dialect", "declared", "deployed", "matches"),
    [
        ("snowflake", "BIGINT", "NUMBER(38,0)", True),
        ("snowflake", "NUMBER", "NUMBER(38,0)", True),
        ("snowflake", "NUMBER(12)", "NUMBER(12,0)", True),
        ("snowflake", "STRING", "VARCHAR(100)", True),
        ("snowflake", "VARCHAR(50)", "VARCHAR(100)", False),
        ("snowflake", "TIMESTAMP", "TIMESTAMP_NTZ(9)", True),
        ("snowflake", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ(9)", False),
        ("databricks", "VARCHAR", "STRING", True),
        ("trino", "STRING", "VARCHAR", True),
        ("postgres", "INTEGER", "BIGINT", False),
        ("postgres", "NUMERIC(12)", "NUMERIC(12,0)", True),
        ("postgres", "DECIMAL(12,2)", "DECIMAL(12,3)", False),
    ],
)
def test_type_aliases_and_explicit_sizes(dialect, declared, deployed, matches):
    found = drift(
        [SourceColumn("raw.orders", "value", declared, 1)],
        [SourceColumn("raw.orders", "value", deployed, 1)],
        dialect,
    )
    assert bool(found) is not matches
