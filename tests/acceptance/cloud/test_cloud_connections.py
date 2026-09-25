"""Databricks and Snowflake, connected through craft-connector.yml with separate fields.

Each test creates a table in each table format the warehouse offers, reads it back and drops it.
The credentials come from ``fixtures.cloud``.
"""

import os
import uuid

import pytest
from sqlalchemy import text

from etl_craft.config.targets import active_catalog
from etl_craft.core.enums import TableFormat
from etl_craft.core.text import qualify
from etl_craft.warehouse.connection import build_warehouse_engine, warehouse_dialect
from fixtures.cloud import DATABRICKS_VARS, SNOWFLAKE_VARS, require_variables, write_config


def create_read_drop(config, schema, params=None):
    dialect = warehouse_dialect(config)
    table = qualify(f"{schema}.etl_craft_{uuid.uuid4().hex[:10]}", active_catalog(config))
    engine = build_warehouse_engine(config)
    try:
        with engine.connect() as conn:
            dialect.create_table_as(conn, table, "SELECT 1 AS id, 'a' AS code", params or {})
            try:
                assert conn.execute(text(f"SELECT code FROM {table}")).scalar_one() == "a"
            finally:
                conn.execute(text(f"DROP TABLE {table}"))
            conn.commit()
    finally:
        engine.dispose()
    return dialect.key


@pytest.mark.cloud_databricks
@pytest.mark.parametrize(
    ("table_format", "key"),
    [(TableFormat.NATIVE, "databricks"), (TableFormat.ICEBERG, "databricks_iceberg")],
)
def test_databricks(tmp_path, table_format, key):
    require_variables("DATABRICKS", DATABRICKS_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_DATABRICKS_{name}" for name in DATABRICKS_VARS}
    config = write_config(tmp_path, "Databricks", fields, table_format)
    assert config.warehouse.active.auth_mode == "token"
    schema = os.environ["ETL_CRAFT_TEST_DATABRICKS_SCHEMA"]
    assert create_read_drop(config, schema) == key


@pytest.mark.cloud_snowflake
@pytest.mark.parametrize(
    ("table_format", "key"),
    [(TableFormat.NATIVE, "snowflake"), (TableFormat.ICEBERG, "snowflake_iceberg")],
)
def test_snowflake(tmp_path, table_format, key):
    require_variables("SNOWFLAKE", SNOWFLAKE_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_SNOWFLAKE_{name}" for name in SNOWFLAKE_VARS}
    config = write_config(tmp_path, "Snowflake", fields, table_format)
    assert config.warehouse.active.auth_mode == "token"
    schema = os.environ["ETL_CRAFT_TEST_SNOWFLAKE_SCHEMA"]
    assert create_read_drop(config, schema) == key
