"""Databricks and Snowflake, connected through craft-connector.yml with separate fields.

Each test creates a table in each table format the warehouse offers, reads it back and drops it.
The credentials come from ``ETL_CRAFT_TEST_<VENDOR>_*`` variables; a test skips when they are
not set, and fails instead with ``ETL_CRAFT_REQUIRE_SERVICES=1``. When the Databricks JDBC URL
names no ``httpPath``, ``ETL_CRAFT_TEST_DATABRICKS_HTTP_PATH`` supplies it.
"""

import os
import uuid

import pytest
import yaml
from sqlalchemy import text

from etl_craft.config import load_config
from etl_craft.config.targets import active_catalog
from etl_craft.core.enums import TableFormat
from etl_craft.core.text import qualify
from etl_craft.warehouse.connection import build_warehouse_engine, warehouse_dialect

DATABRICKS_VARS = ("JDBC_URL", "CATALOG", "SCHEMA", "TOKEN")
SNOWFLAKE_VARS = ("USER", "ACCOUNT", "DATABASE", "SCHEMA", "WAREHOUSE", "ROLE", "TOKEN")


def require_variables(vendor, names):
    missing = [f"ETL_CRAFT_TEST_{vendor}_{name}" for name in names]
    missing = [name for name in missing if not os.environ.get(name)]
    if missing:
        message = f"{vendor.title()} credentials are not set: {', '.join(missing)}"
        if os.environ.get("ETL_CRAFT_REQUIRE_SERVICES") == "1":
            pytest.fail(message, pytrace=False)
        pytest.skip(message)


def write_config(tmp_path, name, fields, table_format):
    raw = {
        "Secrets": {"Source_type": "environment"},
        "Orchestration": {"Mode": "local"},
        "Engine": {"dev": {"jdbc_url": "jdbc:sqlite:engine.db"}},
        "Warehouse": {"Name": name, "Table_format": str(table_format), "dev": fields},
    }
    path = tmp_path / "craft-connector.yml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    return load_config(path)


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
    # The SQL warehouse's path may be kept apart from the URL; it is not a secret.
    http_path = os.environ.get("ETL_CRAFT_TEST_DATABRICKS_HTTP_PATH")
    url = os.environ["ETL_CRAFT_TEST_DATABRICKS_JDBC_URL"]
    if http_path and "httppath=" not in url.lower():
        fields["jdbc_url"] = f"{url.rstrip(';')};httpPath={http_path}"
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
