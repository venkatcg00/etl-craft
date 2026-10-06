"""Warehouse definitions retain metadata or refuse atomic CTAS before executing it."""

import pytest

from etl_craft.core.errors import HandlerError
from etl_craft.dialects.warehouse.replacement import replacement_ddl

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "dialect,ddl,retained",
    [
        (
            "trino",
            "CREATE TABLE c.s.t (id integer) COMMENT 'table description' "
            "WITH (format = 'PARQUET', partitioning = ARRAY['id'])",
            ["table description", "partitioning", "PARQUET"],
        ),
        (
            "databricks",
            "CREATE TABLE c.s.t (id INT) USING DELTA DEFAULT COLLATION UTF8_BINARY "
            "PARTITIONED BY (id) "
            "COMMENT 'table description' TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')",
            [
                "table description",
                "DEFAULT COLLATION UTF8_BINARY",
                "PARTITIONED BY",
                "delta.enableChangeDataFeed",
            ],
        ),
        (
            "snowflake",
            "CREATE TABLE c.s.t (id NUMBER) COMMENT = 'table description' "
            "DATA_RETENTION_TIME_IN_DAYS = 3",
            ["table description", "DATA_RETENTION_TIME_IN_DAYS", "COPY GRANTS"],
        ),
    ],
)
def test_atomic_ctas_retains_table_properties(dialect, ddl, retained):
    sql = replacement_ddl(ddl, "c.s.t", "SELECT 1 AS id", dialect)
    assert sql.startswith("CREATE OR REPLACE TABLE c.s.t")
    assert "AS SELECT 1 AS id" in sql
    for property_ in retained:
        assert property_ in sql


@pytest.mark.parametrize("metadata", ["NOT NULL", "DEFAULT 3", "COMMENT 'column description'"])
@pytest.mark.parametrize("dialect", ["trino", "databricks", "snowflake"])
def test_atomic_ctas_refuses_column_metadata(dialect, metadata):
    with pytest.raises(HandlerError, match="cannot preserve column metadata"):
        replacement_ddl(f"CREATE TABLE c.s.t (id INT {metadata})", "c.s.t", "SELECT 1", dialect)


def test_incomplete_definition_is_refused():
    with pytest.raises(HandlerError, match="complete table definition"):
        replacement_ddl("CREATE TABLE c.s.t AS SELECT 1", "c.s.t", "SELECT 2", "trino")
