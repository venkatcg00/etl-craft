"""Archive verification and schema evolution on the live cloud warehouses."""

import os
from uuid import uuid4

import pytest
from sqlalchemy import text

from etl_craft.config.targets import active_catalog
from etl_craft.core.enums import TableFormat
from etl_craft.warehouse.connection import build_warehouse_engine, warehouse_dialect
from fixtures.archive import exercise_archive
from fixtures.cloud import DATABRICKS_VARS, SNOWFLAKE_VARS, require_variables, write_config

pytestmark = pytest.mark.timeout(600)


@pytest.mark.parametrize(
    "vendor,names,table_format",
    [
        pytest.param(
            "DATABRICKS", DATABRICKS_VARS, TableFormat.NATIVE, marks=pytest.mark.cloud_databricks
        ),
        pytest.param(
            "SNOWFLAKE", SNOWFLAKE_VARS, TableFormat.NATIVE, marks=pytest.mark.cloud_snowflake
        ),
        pytest.param(
            "DATABRICKS", DATABRICKS_VARS, TableFormat.ICEBERG, marks=pytest.mark.cloud_databricks
        ),
    ],
)
def test_cloud_archive_preserves_values_and_evolves(tmp_path, vendor, names, table_format):
    require_variables(vendor, names)
    fields = {name.lower(): f"ETL_CRAFT_TEST_{vendor}_{name}" for name in names}
    config = write_config(tmp_path, vendor.title(), fields, table_format)
    schema = os.environ[f"ETL_CRAFT_TEST_{vendor}_SCHEMA"]
    catalog = active_catalog(config)
    table = f"AUD_ARCHIVE_{uuid4().hex[:12]}"
    warehouse = build_warehouse_engine(config)
    try:
        exercise_archive(
            warehouse, warehouse_dialect(config), catalog, schema, config.cloning, table
        )
    finally:
        with warehouse.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {catalog}.{schema}.{table}"))
        warehouse.dispose()
