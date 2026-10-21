"""Merge crash recovery keeps warehouse rows and Engine DB identities on both Engine DBs."""

from dataclasses import replace

import pytest

from etl_craft.config import ConnectionProfile
from etl_craft.config.targets import active_catalog
from etl_craft.warehouse.connection import build_warehouse_engine
from fixtures.metadata import add_pipeline, start_run
from fixtures.sql_merges import MERGE_FAULTS, check_killed_merge
from fixtures.sql_warehouse import SqlWorld

pytestmark = pytest.mark.chaos


@pytest.mark.parametrize("kind,point", MERGE_FAULTS)
def test_merge_process_loss_converges_with_each_engine_db(engine_db, tmp_path, kind, point):
    db = engine_db
    config = replace(
        db.config,
        warehouse=ConnectionProfile(
            "WAREHOUSE",
            "test",
            f"jdbc:duckdb:{tmp_path / 'merge.duckdb'}",
            "",
            "NONE",
            schema="main",
        ),
    )
    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        run = start_run(conn, pipeline)
    warehouse = build_warehouse_engine(config)
    try:
        world = SqlWorld(
            "duckdb", config, db.engine, warehouse, active_catalog(config), "main", pipeline, run
        )
        check_killed_merge(world, kind, point)
    finally:
        warehouse.dispose()
