"""Unmanaged autocommit, driver binds and partial failure on both cloud warehouses."""

import os
import uuid
from dataclasses import replace

import pytest

from acceptance.cloud.test_cloud_sql_actions import cloud_world
from etl_craft.core.enums import Handler, TableFormat
from etl_craft.core.errors import HandlerError
from etl_craft.handlers.sql_script import run
from fixtures.cloud import DATABRICKS_VARS, SNOWFLAKE_VARS, require_variables

pytestmark = [pytest.mark.timeout(600), pytest.mark.usefixtures("engine_actor")]


@pytest.mark.parametrize(
    "vendor,variables",
    [
        pytest.param("Databricks", DATABRICKS_VARS, marks=pytest.mark.cloud_databricks),
        pytest.param("Snowflake", SNOWFLAKE_VARS, marks=pytest.mark.cloud_snowflake),
    ],
)
def test_sql_script_cloud_autocommit(tmp_path, vendor, variables, caplog):
    prefix = vendor.upper()
    require_variables(prefix, variables)
    fields = {key.capitalize(): f"ETL_CRAFT_TEST_{prefix}_{key}" for key in variables}
    schema = os.environ[f"ETL_CRAFT_TEST_{prefix}_SCHEMA"]
    w = cloud_world(tmp_path, vendor, fields, TableFormat.NATIVE, schema)
    table = w.name(f"etl_script_{uuid.uuid4().hex[:10]}")
    path = w.project / "sql_scripts" / "test.sql"
    path.write_text(
        f"CREATE TABLE {table} (id BIGINT, label VARCHAR(20)); "
        f"INSERT INTO {table} VALUES (:number, '10%'); SELECT :run_date;",
        "utf-8",
    )
    context = replace(
        w.task("custom", SCRIPT_FILE="sql_scripts/test.sql", INPUT_PARAMS='{"number": 7}'),
        handler=Handler.SQL_SCRIPT,
    )
    created = False
    try:
        with caplog.at_level("INFO", logger="etl_craft.handlers.sql_script"):
            run(context, w.engine_db)
        created = True
        path.write_text(
            f"INSERT INTO {table} VALUES (8, '20%'); SELECT * FROM {table}_absent; "
            f"INSERT INTO {table} VALUES (9, '30%');",
            "utf-8",
        )
        with pytest.raises(HandlerError, match="statement 2 failed"):
            run(context, w.engine_db)
        with w.warehouse.connect() as conn:
            rows = conn.exec_driver_sql(f"SELECT id, label FROM {table} ORDER BY id").mappings()
            assert [{key.lower(): value for key, value in row.items()} for row in rows] == [
                {"id": 7, "label": "10%"},
                {"id": 8, "label": "20%"},
            ]
        assert "statement 1:" in caplog.text and "statement 2:" in caplog.text
        assert "rows affected=" in caplog.text and "duration=" in caplog.text
    finally:
        if created:
            w.execute(f"DROP TABLE IF EXISTS {table}")
        w.warehouse.dispose()
        w.engine_db.dispose()
