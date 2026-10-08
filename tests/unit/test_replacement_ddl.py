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


@pytest.mark.parametrize(
    "dialect,identity,properties,clone",
    [
        (
            "databricks",
            "BIGINT GENERATED ALWAYS AS IDENTITY",
            "USING DELTA LOCATION 's3://bucket/table' COMMENT 'kept' "
            "TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')",
            "DEEP CLONE c.s.candidate LOCATION 's3://bucket/table'",
        ),
        (
            "snowflake",
            "BIGINT NOT NULL AUTOINCREMENT START 1 INCREMENT 1 ORDER",
            "COMMENT = 'kept' DATA_RETENTION_TIME_IN_DAYS = 3",
            "CLONE c.s.candidate COPY GRANTS",
        ),
    ],
)
def test_identity_candidates_keep_properties_and_publish_atomically(
    dialect, identity, properties, clone
):
    from etl_craft.dialects.warehouse.replacement import identity_replacement

    create, publish = identity_replacement(
        f"CREATE TABLE c.s.target (id BIGINT, row_id {identity}) {properties}",
        "c.s.target",
        "c.s.candidate",
        f"id BIGINT, row_id {identity}",
        dialect,
    )
    assert create.startswith("CREATE TABLE c.s.candidate")
    assert "kept" in create
    assert "IDENTITY" in create or "AUTOINCREMENT" in create
    assert "LOCATION" not in create
    assert publish == f"CREATE OR REPLACE TABLE c.s.target {clone}"


@pytest.mark.parametrize("dialect", ["databricks", "snowflake"])
def test_identity_replacement_refuses_business_column_constraints(dialect):
    from etl_craft.dialects.warehouse.replacement import identity_replacement

    with pytest.raises(HandlerError, match="column metadata"):
        identity_replacement(
            "CREATE TABLE c.s.target (id BIGINT NOT NULL)",
            "c.s.target",
            "c.s.candidate",
            "id BIGINT, row_id BIGINT",
            dialect,
        )


@pytest.mark.parametrize("collation", ["UTF8_BINARY", "UTF8_LCASE"])
def test_databricks_identity_replacement_keeps_inherited_column_collations(collation):
    from etl_craft.dialects.warehouse.replacement import identity_replacement

    create, publish = identity_replacement(
        f"CREATE TABLE c.s.target (code STRING COLLATE {collation}, "
        "row_id BIGINT GENERATED ALWAYS AS IDENTITY) USING DELTA "
        f"DEFAULT COLLATION {collation}",
        "c.s.target",
        "c.s.candidate",
        "code STRING, row_id BIGINT GENERATED ALWAYS AS IDENTITY",
        "databricks",
    )
    assert f"DEFAULT COLLATION {collation}" in create
    assert "code STRING" in create
    assert "GENERATED ALWAYS AS IDENTITY" in create
    assert publish == "CREATE OR REPLACE TABLE c.s.target DEEP CLONE c.s.candidate"


@pytest.mark.parametrize(
    "definition, columns",
    [
        (
            "CREATE TABLE t (code STRING COLLATE UTF8_LCASE) USING DELTA "
            "DEFAULT COLLATION UTF8_BINARY",
            "code STRING",
        ),
        ("CREATE TABLE t (code STRING COLLATE UTF8_BINARY) USING DELTA", "code STRING"),
        (
            "CREATE TABLE t (code STRING COLLATE UTF8_BINARY NOT NULL) USING DELTA "
            "DEFAULT COLLATION UTF8_BINARY",
            "code STRING",
        ),
        (
            "CREATE TABLE t (code STRING COLLATE UTF8_BINARY) USING DELTA "
            "DEFAULT COLLATION UTF8_BINARY",
            "code STRING COLLATE UTF8_LCASE",
        ),
    ],
)
def test_databricks_replacement_refuses_distinct_collations_and_other_constraints(
    definition, columns
):
    from etl_craft.dialects.warehouse.replacement import identity_replacement

    with pytest.raises(HandlerError, match=r"column metadata|column collations"):
        identity_replacement(definition, "t", "candidate", columns, "databricks")
