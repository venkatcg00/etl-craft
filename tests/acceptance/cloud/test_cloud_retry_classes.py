"""Real syntax and interrupted-connection retry contracts on both cloud warehouses."""

import uuid

import pytest
from sqlalchemy import text

from etl_craft.config.targets import active_catalog
from etl_craft.core.enums import TableFormat
from etl_craft.warehouse.connection import build_warehouse_engine
from fixtures.cloud import DATABRICKS_VARS, SNOWFLAKE_VARS, require_variables, write_config
from fixtures.engine_db import prepared_sqlite_engine_db
from fixtures.metadata import add_pipeline, start_run
from fixtures.retry_warehouse import check_connection_retry, check_syntax_error
from fixtures.sql_warehouse import SqlWorld


@pytest.fixture(
    params=[
        pytest.param("DATABRICKS", marks=pytest.mark.cloud_databricks),
        pytest.param("SNOWFLAKE", marks=pytest.mark.cloud_snowflake),
    ]
)
def cloud_retry_world(request, tmp_path, sqlite_schema_template, engine_actor):
    vendor = request.param
    variables = DATABRICKS_VARS if vendor == "DATABRICKS" else SNOWFLAKE_VARS
    require_variables(vendor, variables)
    fields = {name.lower(): f"ETL_CRAFT_TEST_{vendor}_{name}" for name in variables}
    config = write_config(tmp_path, vendor.title(), fields, TableFormat.NATIVE)
    engine = prepared_sqlite_engine_db(tmp_path, sqlite_schema_template).engine
    warehouse = build_warehouse_engine(config)
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        run = start_run(conn, pipeline)
    # A test-owned schema keeps the SQL task's fixed target names isolated.
    schema = f"retry_{uuid.uuid4().hex[:10]}"
    catalog = active_catalog(config)
    with warehouse.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {catalog}.{schema}"))
    world = SqlWorld(vendor.lower(), config, engine, warehouse, catalog, schema, pipeline, run)
    try:
        yield world
    finally:
        with warehouse.begin() as conn:
            conn.execute(text(f"DROP SCHEMA {catalog}.{schema} CASCADE"))
        warehouse.dispose()
        engine.dispose()


def test_cloud_syntax_error_is_permanent(cloud_retry_world):
    check_syntax_error(cloud_retry_world)


def test_cloud_connection_is_retried(cloud_retry_world, monkeypatch):
    check_connection_retry(cloud_retry_world, monkeypatch)
