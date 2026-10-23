"""On Snowflake, the next lock holder cancels the expired holder's query before writing."""

import os
import uuid

import pytest

from acceptance.cloud.test_cloud_sql_actions import cloud_world
from etl_craft.core.enums import TableFormat
from etl_craft.handlers.resources import LocalLocker
from fixtures.cloud import SNOWFLAKE_VARS, require_variables
from fixtures.lock_fencing import KEY, OLD, expired_lock, lock_holder, start_old_write

pytestmark = [
    pytest.mark.timeout(600),
    pytest.mark.usefixtures("engine_actor"),
    pytest.mark.xdist_group("warehouse-fencing"),
]


@pytest.mark.cloud_snowflake
def test_snowflake_next_holder_cancels_the_expired_holders_query(tmp_path):
    require_variables("SNOWFLAKE", SNOWFLAKE_VARS)
    fields = {key.capitalize(): f"ETL_CRAFT_TEST_SNOWFLAKE_{key}" for key in SNOWFLAKE_VARS}
    schema = os.environ["ETL_CRAFT_TEST_SNOWFLAKE_SCHEMA"]
    w = cloud_world(tmp_path, "Snowflake", fields, TableFormat.NATIVE, schema)
    table = w.name(f"etl_fenced_{uuid.uuid4().hex[:10]}")
    w.execute(f"CREATE TABLE {table} (writer VARCHAR(10))")
    expired_lock(w)
    # SYSTEM$WAIT keeps the INSERT running well past the takeover unless it is cancelled.
    slow = f"INSERT INTO {table} SELECT 'old' WHERE SYSTEM$WAIT(120) IS NOT NULL"
    engine, thread, errors = start_old_write(w, slow, wait_seconds=90)
    try:
        with LocalLocker(w.engine_db, w.config).hold(KEY, "fenced target", 30):
            assert lock_holder(w) != OLD
            w.execute(f"INSERT INTO {table} VALUES ('new')")
        thread.join(60)
        assert not thread.is_alive()
        assert errors, "the expired holder's query finished instead of being cancelled"
        assert w.rows(f"SELECT writer FROM {table}") == [("new",)]
    finally:
        w.execute(f"DROP TABLE IF EXISTS {table}")
        engine.dispose()
        w.warehouse.dispose()


@pytest.mark.parametrize(
    "vendor,variables,slow",
    [
        pytest.param(
            "Snowflake",
            SNOWFLAKE_VARS,
            "SELECT SYSTEM$WAIT(120) AS cancel_slow",
            marks=pytest.mark.cloud_snowflake,
        ),
        pytest.param(
            "Databricks",
            None,
            "SELECT MAX(sha2(CAST(id AS STRING),256)) AS cancel_slow FROM range(10000000000)",
            marks=pytest.mark.cloud_databricks,
        ),
    ],
)
def test_tagged_cloud_query_cancellation_confirms_the_statement_ended(
    tmp_path, vendor, variables, slow
):
    from etl_craft.engine.resource_locks import holder_tag
    from etl_craft.execution.query_cancellation import confirm
    from etl_craft.warehouse.connection import warehouse_dialect
    from fixtures.cloud import DATABRICKS_VARS

    variables = variables or DATABRICKS_VARS
    prefix = vendor.upper()
    require_variables(prefix, variables)
    fields = {key.capitalize(): f"ETL_CRAFT_TEST_{prefix}_{key}" for key in variables}
    schema = os.environ[f"ETL_CRAFT_TEST_{prefix}_SCHEMA"]
    w = cloud_world(tmp_path, vendor, fields, TableFormat.NATIVE, schema)
    engine, thread, errors = start_old_write(w, slow, wait_seconds=90)
    try:
        assert confirm(w.config, holder_tag(OLD)) is None
        thread.join(5)
        assert not thread.is_alive()
        assert errors, "cancel was acknowledged while the original statement still ran"
        with w.warehouse.begin() as conn:
            assert warehouse_dialect(w.config).running_tagged(conn, holder_tag(OLD)) == 0
    finally:
        engine.dispose()
        w.warehouse.dispose()
