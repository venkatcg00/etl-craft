"""Project-scoped reject retention on the live warehouse SQL implementations."""

import os
from uuid import uuid4

import pytest
from sqlalchemy import text
from test_cloud_sql_actions import close_world, cloud_world

from etl_craft.core.enums import TableFormat
from fixtures.cloud import DATABRICKS_VARS, SNOWFLAKE_VARS, require_variables
from fixtures.warehouse_retention import check_retention

pytestmark = [pytest.mark.timeout(600), pytest.mark.usefixtures("engine_actor")]


@pytest.mark.parametrize(
    "vendor,names",
    [
        pytest.param("DATABRICKS", DATABRICKS_VARS, marks=pytest.mark.cloud_databricks),
        pytest.param("SNOWFLAKE", SNOWFLAKE_VARS, marks=pytest.mark.cloud_snowflake),
    ],
)
def test_cloud_reject_retention_preserves_recent_and_foreign_rows(tmp_path, vendor, names):
    require_variables(vendor, names)
    fields = {name.lower(): f"ETL_CRAFT_TEST_{vendor}_{name}" for name in names}
    schema = os.environ[f"ETL_CRAFT_TEST_{vendor}_SCHEMA"]
    world = cloud_world(tmp_path, vendor.title(), fields, TableFormat.NATIVE, schema, isolated=True)
    table = "retained_" + uuid4().hex[:12]
    with world.warehouse.connect() as conn:
        conn.execute(text("SELECT 1 AS reachable"))
    try:
        check_retention(world, table)
    finally:
        try:
            with world.warehouse.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {world.name(table + '__rejects')}"))
                conn.execute(text(f"DROP TABLE IF EXISTS {world.name(table)}"))
        finally:
            close_world(world)
