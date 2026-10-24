"""The shipped multi-project demo runs through the public CLI against one Engine DB."""

import runpy
from pathlib import Path

import pytest
from sqlalchemy import text

pytestmark = pytest.mark.parametrize(
    "empty_engine_db",
    [pytest.param("postgresql", marks=pytest.mark.engine_postgres)],
    indirect=True,
)


def test_ten_projects_share_the_database_and_preserve_existing_metadata(cli_project):
    p = cli_project
    root = Path(__file__).resolve().parents[3] / "examples/demo"
    script = p.config.python_scripts_dir / "pool_demo.py"
    script.write_bytes((root / "python_scripts/pool_demo.py").read_bytes())
    demo = runpy.run_path(str(root / "ten_projects.py"))
    projects = demo["run_demo"](p.config.config_path)
    with p.engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT p.PROJECT_CODE AS project_code,q.PIPELINE_ID AS pipeline_id,"
                "q.PIPELINE_CODE AS pipeline_code,r.PIPELINE_RUN_ID AS run_id,r.STATUS AS status,"
                "t.TARGET_COUNT AS rows FROM CFG_PROJECTS p "
                "JOIN CFG_PIPELINES q ON q.PROJECT_ID=p.PROJECT_ID "
                "JOIN AUD_PIPELINES_RUN_LOG r ON r.PIPELINE_ID=q.PIPELINE_ID "
                "JOIN AUD_TASK_RUN_LOG t ON t.PIPELINE_RUN_ID=r.PIPELINE_RUN_ID "
                "WHERE q.PIPELINE_CODE='POOL_DEMO'"
            )
        ).all()
        original = conn.execute(
            text("SELECT ACTIVE_FLAG AS active FROM CFG_PIPELINES WHERE PIPELINE_ID=:id"),
            {"id": p.pipeline_id},
        ).scalar_one()
    assert {row.project_code for row in rows} == set(projects)
    assert len({row.pipeline_id for row in rows}) == 10
    assert len({row.run_id for row in rows}) == 10
    assert all(row.status == "SUCCESS" and row.rows == 1 for row in rows)
    assert original == "Y"
