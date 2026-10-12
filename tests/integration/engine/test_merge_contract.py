"""Merge target contracts hold across active pipelines on both Engine DBs."""

from sqlalchemy import text

from etl_craft.engine.repository.tasks import conflicting_merge_action
from fixtures.metadata import add_pipeline, add_task


def test_opposite_merge_writer_matches_aliases_but_not_other_catalogs_or_retired_pipelines(
    engine_db,
):
    with engine_db.engine.begin() as conn:
        first = add_pipeline(conn, "FIRST")
        second = add_pipeline(conn, "SECOND")
        task = add_task(
            conn, first, "load", "SQL", SQL_ACTION="SCD1_MERGE", TARGET_OBJECT="raw.orders"
        )
        add_task(
            conn,
            second,
            "other_catalog",
            "SQL",
            SQL_ACTION="SCD2_MERGE",
            TARGET_OBJECT="elsewhere.raw.orders",
        )
        assert conflicting_merge_action(conn, task, "warehouse.raw.orders", "SCD1_MERGE") is None
        add_task(
            conn,
            second,
            "opposite",
            "SQL",
            SQL_ACTION="scd2_merge",
            TARGET_OBJECT="WAREHOUSE.raw.orders",
        )
        assert (
            conflicting_merge_action(conn, task, "warehouse.raw.orders", "SCD1_MERGE")
            == "SCD2_MERGE"
        )
        conn.execute(
            text("UPDATE CFG_PIPELINES SET ACTIVE_FLAG='N' WHERE PIPELINE_ID=:id"),
            {"id": second},
        )
        assert conflicting_merge_action(conn, task, "warehouse.raw.orders", "SCD1_MERGE") is None
