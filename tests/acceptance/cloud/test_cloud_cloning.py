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
from etl_craft.core.actor import purpose
from etl_craft.core.enums import CloningScope, TableFormat
from etl_craft.engine import locks
from etl_craft.handlers.resources import LocalLocker
from etl_craft.services import cloning
from etl_craft.warehouse.connection import build_warehouse_engine, warehouse_dialect
from fixtures.cloning import insert_pause
from fixtures.cloud import DATABRICKS_VARS, SNOWFLAKE_VARS, require_variables, write_config
from fixtures.engine_db import apply_schema, sqlite_engine_db
from fixtures.metadata import add_pipeline

pytestmark = [pytest.mark.timeout(600), pytest.mark.usefixtures("engine_actor")]


def clone_one(engine, warehouse, dialect, config, table, mirror, columns):
    with locks.ResourceLock(locks.CLONE.name, locks.CLONE.key).hold(LocalLocker(engine, config)):
        return cloning._clone_table(engine, warehouse, dialect, config, table, mirror, columns)


def gains_columns(tmp_path, name, fields, table_format, schema):
    project = tmp_path / "etl-craft"
    project.mkdir()
    config = write_config(project, name, fields, table_format)
    config = replace(config, cloning=CloningConfig(enabled=True, scope=CloningScope.CFG))
    engine_db = sqlite_engine_db(project).engine
    apply_schema(engine_db)
    with engine_db.begin() as conn:
        add_pipeline(conn, "SALES")
        conn.execute(text("CREATE TABLE CFG_TEAM (PASSWORD_HASH VARCHAR)"))
        conn.execute(text("INSERT INTO CFG_TEAM VALUES ('team-owned')"))
        conn.execute(
            text(
                "INSERT INTO CFG_ACCOUNTS (ACCOUNT_NAME,ACCOUNT_KIND,ROLE,PASSWORD_HASH) "
                "VALUES ('person','USER','viewer','scrypt:private')"
            )
        )
    warehouse = build_warehouse_engine(config)
    dialect = warehouse_dialect(config)
    mirror = f"{active_catalog(config)}.{schema}.etl_craft_mirror_{uuid.uuid4().hex[:8]}"
    accounts_mirror = mirror + "_accounts"
    audit_mirror = mirror + "_audit"
    inventory = dict(cloning._engine_tables(engine_db, config, ("CFG_",)))
    assert "CFG_TEAM" not in inventory
    columns = next(
        cols
        for table, cols in cloning._engine_tables(engine_db, config, ("CFG_",))
        if table == "CFG_PIPELINES"
    )
    try:
        account_copy = clone_one(
            engine_db,
            warehouse,
            dialect,
            config,
            "CFG_ACCOUNTS",
            accounts_mirror,
            inventory["CFG_ACCOUNTS"],
        )
        assert account_copy.rows == 1
        with warehouse.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM {accounts_mirror}"))
            names = result.keys()
            assert "password_hash" not in {k.lower() for k in names}
            assert "scrypt:private" not in str(result.all())
        with warehouse.begin() as conn:
            conn.execute(text(f"CREATE TABLE {mirror} (PIPELINE_ID BIGINT)"))
        cloned = clone_one(engine_db, warehouse, dialect, config, "CFG_PIPELINES", mirror, columns)
        assert not cloned.created and cloned.rows == 1
        assert "PIPELINE_CODE" in cloned.added and "PIPELINE_ID" not in cloned.added
        with warehouse.connect() as conn:
            assert conn.execute(text(f"SELECT PIPELINE_CODE FROM {mirror}")).all() == [("SALES",)]
        again = clone_one(engine_db, warehouse, dialect, config, "CFG_PIPELINES", mirror, columns)
        assert again.added == () and again.rows == 1
        audit_columns = dict(cloning._engine_tables(engine_db, config, ("AUD_",)))[
            "AUD_PIPELINE_PAUSES"
        ]
        with engine_db.begin() as conn:
            pipeline = conn.execute(
                text("SELECT PIPELINE_ID AS pipeline_id FROM CFG_PIPELINES")
            ).scalar_one()
            pause = insert_pause(conn, pipeline, "baseline")
        copied = clone_one(
            engine_db,
            warehouse,
            dialect,
            config,
            "AUD_PIPELINE_PAUSES",
            audit_mirror,
            audit_columns,
        )
        assert copied.rows == 1
        with engine_db.begin() as conn:
            conn.execute(
                text(
                    "UPDATE AUD_PIPELINE_PAUSES SET REASON='incremental' WHERE PIPELINE_PAUSE_ID=:p"
                ),
                {"p": pause},
            )
        assert (
            clone_one(
                engine_db,
                warehouse,
                dialect,
                config,
                "AUD_PIPELINE_PAUSES",
                audit_mirror,
                audit_columns,
            ).rows
            == 1
        )
        assert (
            clone_one(
                engine_db,
                warehouse,
                dialect,
                config,
                "AUD_PIPELINE_PAUSES",
                audit_mirror,
                audit_columns,
            ).rows
            == 0
        )
        with warehouse.connect() as conn:
            assert (
                conn.execute(text(f"SELECT REASON FROM {audit_mirror}")).scalar_one()
                == "incremental"
            )
        token = purpose.set("retention")
        try:
            with engine_db.begin() as conn:
                conn.execute(
                    text("DELETE FROM AUD_PIPELINE_PAUSES WHERE PIPELINE_PAUSE_ID=:p"), {"p": pause}
                )
        finally:
            purpose.reset(token)
        clone_one(
            engine_db,
            warehouse,
            dialect,
            config,
            "AUD_PIPELINE_PAUSES",
            audit_mirror,
            audit_columns,
        )
        with warehouse.connect() as conn:
            assert conn.execute(text(f"SELECT COUNT(*) FROM {audit_mirror}")).scalar_one() == 0
    finally:
        with warehouse.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {mirror}"))
            conn.execute(text(f"DROP TABLE IF EXISTS {accounts_mirror}"))
            conn.execute(text(f"DROP TABLE IF EXISTS {audit_mirror}"))
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
