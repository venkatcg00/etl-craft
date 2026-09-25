"""Every SQL action on Databricks (Delta, and Delta with UniForm) and Snowflake (native, Iceberg).

One test per warehouse and table format walks the whole action vocabulary against the live
warehouse, in its configured schema, with table names unique to the run; the tables are dropped
afterwards. Credentials come from the same variables as ``test_cloud_connections``.
"""

import os
import uuid
from dataclasses import replace

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from etl_craft.config.targets import active_catalog
from etl_craft.core.enums import TableFormat
from etl_craft.engine import runlog
from etl_craft.handlers import business_rules
from etl_craft.warehouse.connection import build_warehouse_engine
from fixtures.cloud import DATABRICKS_VARS, SNOWFLAKE_VARS, require_variables, write_config
from fixtures.engine_db import apply_schema, sqlite_engine_db
from fixtures.metadata import add_pipeline, insert
from fixtures.sql_warehouse import SqlWorld

# Some forty statements against a remote warehouse, one to three seconds each, cold start aside.
pytestmark = pytest.mark.timeout(1200)


def cloud_world(tmp_path, name, fields, table_format, schema):
    project = tmp_path / "etl-craft"
    (project / "sql_files").mkdir(parents=True)
    config = write_config(project, name, fields, table_format)
    engine_db = sqlite_engine_db(project).engine
    apply_schema(engine_db)
    with engine_db.begin() as conn:
        pipeline_id = add_pipeline(conn, "P")
        run_id = runlog.find_or_create_active_run(conn, pipeline_id)
    warehouse = build_warehouse_engine(config)
    return SqlWorld(
        "cloud",
        config,
        engine_db,
        warehouse,
        active_catalog(config),
        schema,
        pipeline_id,
        run_id,
    )


def walk_every_action(w):
    suffix = uuid.uuid4().hex[:8]
    names = {
        base: f"etl_craft_{base}_{suffix}" for base in ("orders", "customers", "history", "events")
    }
    scd = {"MERGE_KEY": "id", "MERGE_COMPARE_COLUMNS": "name"}
    # Typed like a real source column: Snowflake gives a bare literal its own length, and the
    # target takes the SELECT's types.
    ann = "CAST('Ann' AS VARCHAR(20))"
    try:
        created = w.run(
            "create",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT=names["orders"],
            SOURCE_SQL="SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2, 'b'",
        )
        assert created.insert_count == 2
        assert sorted(w.rows(f"SELECT row_id FROM {w.name(names['orders'])}")) == [(1,), (2,)]

        copy = names["orders"] + "_copy"
        w.setup(copy, f"SELECT id, name FROM {w.name(names['orders'])}", "OVERWRITE_TABLE")
        overwrite = w.run(
            "overwrite",
            SQL_ACTION="OVERWRITE_TABLE",
            TARGET_OBJECT=names["orders"] + "_copy",
            SOURCE_SQL=f"SELECT id, name FROM {w.name(names['orders'])}",
        )
        assert overwrite.insert_count == 2

        shape = f"SELECT 1 AS id, {ann} AS name"
        w.setup(names["customers"], shape, "SCD1_MERGE")
        w.setup(names["history"], shape, "SCD2_MERGE")
        first = w.run(
            "scd1",
            SQL_ACTION="SCD1_MERGE",
            TARGET_OBJECT=names["customers"],
            SOURCE_SQL=f"SELECT 1 AS id, {ann} AS name UNION ALL SELECT 2, 'Bo'",
            **scd,
        )
        assert (first.insert_count, first.update_count) == (2, 0)
        second = w.run(
            "scd1",
            SQL_ACTION="SCD1_MERGE",
            TARGET_OBJECT=names["customers"],
            SOURCE_SQL=f"SELECT 1 AS id, {ann} AS name UNION ALL SELECT 2, 'Bob'",
            **scd,
        )
        assert (second.insert_count, second.update_count) == (0, 1)

        w.run(
            "scd2",
            SQL_ACTION="SCD2_MERGE",
            TARGET_OBJECT=names["history"],
            SOURCE_SQL=f"SELECT 1 AS id, {ann} AS name",
            **scd,
        )
        changed = w.run(
            "scd2",
            SQL_ACTION="SCD2_MERGE",
            TARGET_OBJECT=names["history"],
            SOURCE_SQL="SELECT 1 AS id, 'Anna' AS name",
            **scd,
        )
        assert (changed.update_count, changed.insert_count, changed.target_count) == (1, 1, 2)

        w.setup(names["events"], "SELECT 1 AS id", "APPEND_TABLE")
        appended = w.run(
            "append",
            SQL_ACTION="APPEND_TABLE",
            TARGET_OBJECT=names["events"],
            SOURCE_SQL="SELECT 1 AS id UNION ALL SELECT 2",
        )
        assert appended.insert_count == 2

        # A business rule over the merged customers: flag those that have an order.
        w.task("rules")
        with w.engine_db.begin() as conn:
            insert(
                conn,
                "INSERT INTO CFG_BUSINESS_RULES (BUSINESS_RULE_NAME, PIPELINE_ID, TASK_ID, "
                "BUSINESS_RULE_SQL, BUSINESS_RULE_TYPE, BUSINESS_RULE_KEY_COLUMN, TARGET_TABLE, "
                "SEQUENCE_NUMBER) VALUES ('has_order', :p, :t, :rule_sql, 'REPORT', 'id', "
                ":target, 1)",
                "BUSINESS_RULE_ID",
                p=w.pipeline_id,
                t=w.tasks["rules"],
                rule_sql=f"SELECT 1 FROM {w.name(names['orders'])} o WHERE o.id = t.id",
                target=f"{w.schema}.{names['customers']}",
            )
        rules = business_rules.run(
            replace(w.task("rules"), handler="BUSINESS_RULES", force=True), w.engine_db
        )
        assert rules.insert_count == 2

        soft = w.run(
            "soft",
            SQL_ACTION="DELETE_ROWS",
            TARGET_OBJECT=names["customers"],
            MERGE_KEY="id",
            SOURCE_SQL="SELECT 1 AS id",
        )
        hard = w.run(
            "hard",
            SQL_ACTION="DELETE_ROWS",
            TARGET_OBJECT=names["customers"],
            MERGE_KEY="id",
            HARD_DELETE="true",
            SOURCE_SQL="SELECT 2 AS id",
        )
        assert (soft.delete_count, hard.delete_count) == (1, 1)
        assert w.rows(f"SELECT id, delete_flag FROM {w.name(names['customers'])}") == [(1, "Y")]

        w.finish("create")
        w.run("drop", SQL_ACTION="DROP_TABLE", TARGET_OBJECT=names["orders"])
        with pytest.raises(DBAPIError):
            w.rows(f"SELECT 1 FROM {w.name(names['orders'])}")
    finally:
        for base in (*names.values(), names["orders"] + "_copy"):
            with w.warehouse.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {w.name(base)}"))
        w.warehouse.dispose()
        w.engine_db.dispose()


@pytest.mark.cloud_databricks
@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
def test_every_action_on_databricks(tmp_path, table_format):
    require_variables("DATABRICKS", DATABRICKS_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_DATABRICKS_{name}" for name in DATABRICKS_VARS}
    schema = os.environ["ETL_CRAFT_TEST_DATABRICKS_SCHEMA"]
    walk_every_action(cloud_world(tmp_path, "Databricks", fields, table_format, schema))


@pytest.mark.cloud_snowflake
@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
def test_every_action_on_snowflake(tmp_path, table_format):
    require_variables("SNOWFLAKE", SNOWFLAKE_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_SNOWFLAKE_{name}" for name in SNOWFLAKE_VARS}
    schema = os.environ["ETL_CRAFT_TEST_SNOWFLAKE_SCHEMA"]
    walk_every_action(cloud_world(tmp_path, "Snowflake", fields, table_format, schema))
