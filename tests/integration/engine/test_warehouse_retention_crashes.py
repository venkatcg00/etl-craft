"""Warehouse cleanup remains idempotent across process loss without inventing success counts."""

from datetime import UTC, datetime

import pytest
from sqlalchemy import text

from etl_craft.warehouse.connection import build_warehouse_engine

pytestmark = pytest.mark.chaos


@pytest.mark.parametrize("point", ["warehouse_after_write", "warehouse_after_commit"])
def test_killed_reject_cleanup_recovers_and_retains_its_admitted_counts(cli_project, point):
    project = cli_project
    with project.engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO CFG_TABLES (SCHEMA_NAME,TABLE_NAME,OBJECT_KIND,REJECT_RETENTION_DAYS) "
                "VALUES ('main','retained','TABLE',30)"
            )
        )
    warehouse = build_warehouse_engine(project.config)
    with warehouse.begin() as conn:
        conn.execute(text("CREATE TABLE retained (id BIGINT)"))
        conn.execute(
            text(
                "CREATE TABLE retained__rejects (id BIGINT,PIPELINE_ID BIGINT,"
                "PIPELINE_RUN_ID BIGINT,"
                "TASK_RUN_ID BIGINT,CREATE_DATE TIMESTAMPTZ,CREATED_BY VARCHAR,"
                "REJECT_REASON VARCHAR,"
                "REJECT_COLUMN VARCHAR)"
            )
        )
        for key, stamp in (
            (1, datetime(2000, 1, 1, tzinfo=UTC)),
            (2, datetime(2100, 1, 1, tzinfo=UTC)),
        ):
            conn.execute(
                text(
                    "INSERT INTO retained__rejects VALUES "
                    "(:id,:pipeline,1,1,:stamp,'test','bad','id')"
                ),
                {"id": key, "pipeline": project.pipeline_id, "stamp": stamp},
            )
    warehouse.dispose()
    code, output = project.run("purge", fault=f"retention.{point}:kill")
    assert code == 137, output
    with project.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_RESOURCE_LOCKS SET LEASE_EXPIRES_AT=:past"),
            {"past": datetime(2000, 1, 1, tzinfo=UTC)},
        )
        requested = conn.execute(
            text(
                "SELECT ARGUMENTS AS arguments FROM AUD_ACTIONS WHERE OUTCOME='REQUESTED' "
                "AND CAST(ARGUMENTS AS TEXT) LIKE '%warehouse_retention%'"
            )
        ).all()
        assert requested
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) AS count FROM AUD_ACTIONS WHERE OUTCOME='SUCCESS' "
                    "AND CAST(ARGUMENTS AS TEXT) LIKE '%warehouse_retention%'"
                )
            ).scalar_one()
            == 0
        )
    code, output = project.run("purge")
    assert code == 0, output
    warehouse = build_warehouse_engine(project.config)
    try:
        with warehouse.connect() as conn:
            assert conn.execute(text("SELECT id AS id FROM retained__rejects")).scalar_one() == 2
    finally:
        warehouse.dispose()
