"""The shipped end-of-run alert waits for the parallel business-rule branch."""

from pathlib import Path

import pytest
from sqlalchemy import text

from etl_craft.core.enums import RunStatus
from etl_craft.core.graph import TaskRunState, build_graph
from etl_craft.engine.queries import run_script
from etl_craft.engine.repository.dependencies import fetch_pipeline_graph


@pytest.mark.parametrize("quality_status", [RunStatus.SUCCESS, RunStatus.FAILED, RunStatus.SKIPPED])
def test_demo_alert_waits_for_quality_even_when_any_summary_finishes(engine_db, quality_status):
    path = Path(__file__).parents[3] / "examples/demo/metadata/support_insights.sql"
    with engine_db.engine.begin() as conn:
        run_script(conn, engine_db.dialect.split_statements(path.read_text()))
        pipeline = conn.execute(
            text("SELECT PIPELINE_ID AS id FROM CFG_PIPELINES WHERE PIPELINE_CODE='SUPPORT_DM'")
        ).scalar_one()
        data = fetch_pipeline_graph(conn, pipeline)
    graph = build_graph(data.tasks, data.same_pipeline_edges)
    ids = {node.task_code: node.task_id for node in data.tasks}
    states = {node.task_id: TaskRunState(RunStatus.SUCCESS, target_count=1) for node in data.tasks}
    states[ids["alert"]] = TaskRunState()
    states[ids["quality"]] = TaskRunState(RunStatus.IN_PROGRESS)
    assert ids["alert"] not in graph.ready(states)
    states[ids["quality"]] = TaskRunState(quality_status)
    assert ids["alert"] in graph.ready(states)
