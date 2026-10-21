"""Both SCD actions retain audit stamps and enforce live/deleted state."""

import pytest
from sqlalchemy import text

from etl_craft.core.errors import HandlerError
from etl_craft.services.upgrade_targets import upgrade_targets
from fixtures.metadata import add_pipeline, add_task
from fixtures.scd_lifecycle import check_scd1_upgrade, check_scd_lifecycle, create_legacy_scd1


@pytest.mark.parametrize("kind", ["SCD1_MERGE", "SCD2_MERGE"])
def test_scd_lifecycle(sql_world, kind):
    check_scd_lifecycle(sql_world, "lifecycle", kind)


def test_legacy_scd1_upgrade(sql_world):
    check_scd1_upgrade(sql_world, "legacy")


def test_upgrade_refuses_unknown_delete_state_before_adding_columns(sql_world):
    w = sql_world
    create_legacy_scd1(w, "invalid_state")
    w.execute(f"UPDATE {w.name('invalid_state')} SET DELETE_FLAG = NULL")
    with pytest.raises(HandlerError, match="NULL or invalid DELETE_FLAG"):
        upgrade_targets(w.engine_db, w.config)
    assert "active_flag" not in w.columns("invalid_state")


def test_setup_and_upgrade_refuse_mixed_scd_lifecycles(sql_world):
    w = sql_world
    for kind in ("SCD1_MERGE", "SCD2_MERGE"):
        w.task(
            kind,
            SQL_ACTION=kind,
            TARGET_OBJECT="mixed",
            SELECT_QUERY="SELECT 1 AS id",
            MERGE_KEY="id",
            MERGE_COMPARE_COLUMNS="id",
        )
    with pytest.raises(HandlerError, match="different row lifecycles"):
        w.setup("mixed", "SELECT 1 AS id", "SCD1_MERGE")
    with pytest.raises(HandlerError, match="different row lifecycles"):
        upgrade_targets(w.engine_db, w.config)
    assert "mixed" not in w.tables()


def test_merge_refuses_opposite_writer_in_another_pipeline_with_a_qualified_alias(sql_world):
    w = sql_world
    source = "SELECT 1 AS id, CAST('Ann' AS VARCHAR(20)) AS name"
    w.setup("shared", source, "SCD1_MERGE")
    with w.engine_db.begin() as conn:
        pipeline = add_pipeline(conn, "OTHER")
        other = add_task(
            conn,
            pipeline,
            "other",
            "SQL",
            SQL_ACTION="SCD2_MERGE",
            TARGET_OBJECT=w.name("shared"),
            SELECT_QUERY=source,
            MERGE_KEY="id",
            MERGE_COMPARE_COLUMNS="name",
        )
    params = {
        "SQL_ACTION": "SCD1_MERGE",
        "TARGET_OBJECT": "shared",
        "SELECT_QUERY": source,
        "MERGE_KEY": "id",
        "MERGE_COMPARE_COLUMNS": "name",
    }
    with pytest.raises(HandlerError, match="conflicts with an active SCD2_MERGE writer"):
        w.run("load", **params)
    assert w.rows(f"SELECT COUNT(*) FROM {w.name('shared')}") == [(0,)]
    with w.engine_db.begin() as conn:
        conn.execute(
            text("UPDATE CFG_TASKS SET ACTIVE_FLAG = 'N' WHERE TASK_ID = :id"), {"id": other}
        )
    assert w.run("load", **params).insert_count == 1
