"""Cloning a table into Databricks (Delta, and Delta with UniForm) and Snowflake (native).

A mirror of ``CFG_PIPELINES`` under a name unique to the run is created with one column, then
cloned into: the missing columns are added and the rows copied. It is dropped afterwards.
Snowflake Iceberg mirrors need a Cloning external volume, which the test account does not name.
"""

import os
import uuid
from dataclasses import replace

import pytest
from sqlalchemy import text

from etl_craft.config import CloningConfig
from etl_craft.config.targets import active_catalog
from etl_craft.core.enums import CloningScope, TableFormat
from etl_craft.services import cloning
from etl_craft.warehouse.connection import build_warehouse_engine, warehouse_dialect
from fixtures.cloud import DATABRICKS_VARS, SNOWFLAKE_VARS, require_variables, write_config
from fixtures.engine_db import apply_schema, sqlite_engine_db
from fixtures.metadata import add_pipeline

pytestmark = pytest.mark.timeout(600)


def gains_columns(tmp_path, name, fields, table_format, schema):
    project = tmp_path / "etl-craft"
    project.mkdir()
    config = write_config(project, name, fields, table_format)
    config = replace(config, cloning=CloningConfig(enabled=True, scope=CloningScope.CFG))
    engine_db = sqlite_engine_db(project).engine
    apply_schema(engine_db)
    with engine_db.begin() as conn:
        add_pipeline(conn, "SALES")
    warehouse = build_warehouse_engine(config)
    dialect = warehouse_dialect(config)
    mirror = f"{active_catalog(config)}.{schema}.etl_craft_mirror_{uuid.uuid4().hex[:8]}"
    columns = next(
        cols
        for table, cols in cloning._engine_tables(engine_db, config, ("CFG_",))
        if table == "CFG_PIPELINES"
    )
    try:
        with warehouse.begin() as conn:
            conn.execute(text(f"CREATE TABLE {mirror} (PIPELINE_ID BIGINT)"))
        cloned = cloning._clone_table(
            engine_db, warehouse, dialect, config, "CFG_PIPELINES", mirror, columns
        )
        assert not cloned.created and cloned.rows == 1
        assert "PIPELINE_CODE" in cloned.added and "PIPELINE_ID" not in cloned.added
        with warehouse.connect() as conn:
            assert conn.execute(text(f"SELECT PIPELINE_CODE FROM {mirror}")).all() == [("SALES",)]
        again = cloning._clone_table(
            engine_db, warehouse, dialect, config, "CFG_PIPELINES", mirror, columns
        )
        assert again.added == () and again.rows == 1
    finally:
        with warehouse.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {mirror}"))
        warehouse.dispose()
        engine_db.dispose()


@pytest.mark.cloud_databricks
@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
def test_a_mirror_gains_columns_on_databricks(tmp_path, table_format):
    require_variables("DATABRICKS", DATABRICKS_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_DATABRICKS_{name}" for name in DATABRICKS_VARS}
    schema = os.environ["ETL_CRAFT_TEST_DATABRICKS_SCHEMA"]
    gains_columns(tmp_path, "Databricks", fields, table_format, schema)


@pytest.mark.cloud_snowflake
def test_a_mirror_gains_columns_on_snowflake(tmp_path):
    require_variables("SNOWFLAKE", SNOWFLAKE_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_SNOWFLAKE_{name}" for name in SNOWFLAKE_VARS}
    schema = os.environ["ETL_CRAFT_TEST_SNOWFLAKE_SCHEMA"]
    gains_columns(tmp_path, "Snowflake", fields, TableFormat.NATIVE, schema)
