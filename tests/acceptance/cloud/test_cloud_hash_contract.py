"""Cloud change hashes match the same canonical digest as local warehouses."""

import hashlib
import os

import pytest
from sqlalchemy import text

from etl_craft.config.targets import active_catalog
from etl_craft.core.enums import TableFormat
from etl_craft.warehouse.connection import build_warehouse_engine, warehouse_dialect
from fixtures.cloud import DATABRICKS_VARS, SNOWFLAKE_VARS, require_variables, write_config
from fixtures.hash_contract import golden_sql

pytestmark = pytest.mark.timeout(1200)


@pytest.mark.parametrize(
    "vendor",
    [
        pytest.param("DATABRICKS", marks=pytest.mark.cloud_databricks),
        pytest.param("SNOWFLAKE", marks=pytest.mark.cloud_snowflake),
    ],
)
def test_cloud_hash_matches_the_canonical_golden_digest(tmp_path, vendor):
    names = DATABRICKS_VARS if vendor == "DATABRICKS" else SNOWFLAKE_VARS
    require_variables(vendor, names)
    fields = {name.lower(): f"ETL_CRAFT_TEST_{vendor}_{name}" for name in names}
    config = write_config(tmp_path, vendor.title(), fields, TableFormat.NATIVE)
    active_catalog(config)
    engine = build_warehouse_engine(config)
    try:
        expression = golden_sql(warehouse_dialect(config))
        payload = (
            "NV0:V5:a|b:cV2:é😀V26:2020-01-02T03:04:05.123456"
            "V26:2020-01-02T03:04:05.123456V7:12.3400V5:falseV10:2020-01-02"
        )
        expected = hashlib.md5(payload.encode()).hexdigest()
        with engine.connect() as conn:
            assert conn.execute(text(f"SELECT {expression}")).scalar_one() == expected
            zone = (
                "ALTER SESSION SET TIMEZONE = 'Asia/Kolkata'"
                if vendor == "SNOWFLAKE"
                else "SET TIME ZONE 'Asia/Kolkata'"
            )
            conn.execute(text(zone))
            assert conn.execute(text(f"SELECT {expression}")).scalar_one() == expected
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "vendor",
    [
        pytest.param("DATABRICKS", marks=pytest.mark.cloud_databricks),
        pytest.param("SNOWFLAKE", marks=pytest.mark.cloud_snowflake),
    ],
)
def test_cloud_typed_target_rehash_and_unchanged_merge(tmp_path, vendor):
    import uuid

    from etl_craft.engine import transitions
    from etl_craft.engine.repository.hash_versions import clear_hash_version
    from etl_craft.services.rehash import rehash
    from fixtures.engine_db import apply_schema, sqlite_engine_db
    from fixtures.metadata import add_pipeline
    from fixtures.sql_warehouse import SqlWorld

    names = DATABRICKS_VARS if vendor == "DATABRICKS" else SNOWFLAKE_VARS
    require_variables(vendor, names)
    fields = {name.lower(): f"ETL_CRAFT_TEST_{vendor}_{name}" for name in names}
    project = tmp_path / "etl-craft"
    project.mkdir()
    config = write_config(project, vendor.title(), fields, TableFormat.NATIVE)
    db = sqlite_engine_db(project).engine
    apply_schema(db)
    with db.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        run = transitions.create_active_run(conn, pipeline)
    warehouse = build_warehouse_engine(config)
    w = SqlWorld(
        "cloud",
        config,
        db,
        warehouse,
        active_catalog(config),
        os.environ[f"ETL_CRAFT_TEST_{vendor}_SCHEMA"],
        pipeline,
        run,
    )
    target = f"etl_hash_{uuid.uuid4().hex[:12]}"
    aware = (
        "CAST('2020-01-02T08:34:05.123456+05:30' AS TIMESTAMP)"
        if vendor == "DATABRICKS"
        else "TO_TIMESTAMP_TZ('2020-01-02 08:34:05.123456+05:30')"
    )
    source = f"SELECT 1 AS id, CAST(12.34 AS DECIMAL(12,4)) AS amount, {aware} AS stamp"
    params = {
        "SQL_ACTION": "SCD1_MERGE",
        "TARGET_OBJECT": target,
        "SOURCE_SQL": source,
        "MERGE_KEY": "id",
        "MERGE_COMPARE_COLUMNS": "amount|stamp",
    }
    try:
        w.setup(target, source, "SCD1_MERGE")
        assert w.run("merge", **params).insert_count == 1
        expected = hashlib.md5(b"V7:12.3400V26:2020-01-02T03:04:05.123456").hexdigest()
        assert w.rows(f"SELECT HASH_KEY FROM {w.name(target)}") == [(expected,)]
        w.execute(f"UPDATE {w.name(target)} SET HASH_KEY='legacy'")
        with db.begin() as conn:
            clear_hash_version(conn, w.name(target))
        rehash(db, config, f"{w.schema}.{target}")
        assert w.rows(f"SELECT HASH_KEY FROM {w.name(target)}") == [(expected,)]
        assert w.run("merge", **params).update_count == 0
    finally:
        with warehouse.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {w.name(target)}"))
        warehouse.dispose()
        db.dispose()
