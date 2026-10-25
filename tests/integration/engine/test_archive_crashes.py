"""A purge killed at each commit boundary resumes its durable snapshot exactly once."""

import pytest
import yaml
from sqlalchemy import text

from etl_craft.core.enums import RunStatus
from etl_craft.services.retention import retaining
from etl_craft.warehouse.connection import build_warehouse_engine
from fixtures.metadata import upstream_run
from fixtures.retention import OLD

pytestmark = pytest.mark.chaos


@pytest.mark.parametrize(
    "point",
    [
        "after_prepare",
        "during_write",
        "after_warehouse_commit",
        "after_verify",
        "before_source_commit",
        "after_source_commit",
    ],
)
def test_killed_archive_resumes_without_lost_or_duplicate_rows(cli_project, point):
    project = cli_project
    path = project.config.config_path
    raw = yaml.safe_load(path.read_text())
    raw["Retention"] = {"Run_history_days": 90}
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    with project.engine.begin() as conn:
        run, _ = upstream_run(conn, project.pipeline_id, {project.task_id: (RunStatus.SUCCESS,)})
    with retaining(), project.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_PIPELINES_RUN_LOG SET END_DATE=:old WHERE PIPELINE_RUN_ID=:id"),
            {"old": OLD, "id": run},
        )
    warehouse = build_warehouse_engine(project.config)
    with warehouse.begin() as conn:
        conn.execute(text("CREATE SCHEMA history"))
    warehouse.dispose()
    code, output = project.run("purge", "--archive-to", "history", fault=f"archive.{point}:kill")
    assert code == 137, output
    with project.engine.begin() as conn:
        conn.execute(text("UPDATE AUD_RESOURCE_LOCKS SET LEASE_EXPIRES_AT=:past"), {"past": OLD})
    code, output = project.run("purge", "--archive-to", "history")
    assert code == 0, output
    with project.engine.connect() as conn:
        assert (
            conn.execute(text("SELECT COUNT(*) AS count FROM AUD_RETENTION_ARCHIVES")).scalar_one()
            == 0
        )
        assert (
            conn.execute(text("SELECT COUNT(*) AS count FROM AUD_PIPELINES_RUN_LOG")).scalar_one()
            == 0
        )
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) AS count FROM AUD_ACTIONS "
                    "WHERE CAST(ARGUMENTS AS TEXT) LIKE '%archive_batch%'"
                )
            ).scalar_one()
            > 0
        )
    warehouse = build_warehouse_engine(project.config)
    try:
        with warehouse.connect() as conn:
            assert (
                conn.execute(
                    text("SELECT COUNT(*) AS count FROM history.AUD_PIPELINES_RUN_LOG")
                ).scalar_one()
                == 1
            )
    finally:
        warehouse.dispose()
