"""Simultaneous initialization cannot finalize another caller's active run."""

import pytest
from sqlalchemy import text

from etl_craft.core.errors import RunStateError
from etl_craft.execution.pipeline import init_pipeline_run
from fixtures.metadata import add_pipeline
from fixtures.races import two_at_once


@pytest.mark.chaos
def test_simultaneous_initializers_leave_one_active_run(engine_db):
    db = engine_db
    with db.engine.begin() as conn:
        add_pipeline(conn, "P")

    def initialize():
        try:
            return init_pipeline_run(db.engine, db.config, "P")
        except RunStateError as error:
            return error

    results = two_at_once(initialize, initialize, at="etl_craft.execution.pipeline.check_gate_now")
    winners = [result for result in results if not isinstance(result, RunStateError)]
    refused = [result for result in results if isinstance(result, RunStateError)]
    assert len(winners) == len(refused) == 1
    assert "another process started" in str(refused[0])
    with db.engine.connect() as conn:
        statuses = conn.execute(text("SELECT STATUS FROM AUD_PIPELINES_RUN_LOG")).scalars().all()
        assert statuses.count("IN-PROGRESS") == 1
        assert all(status in {"IN-PROGRESS", "QUEUED"} for status in statuses)


def test_a_fault_between_run_creation_and_skipping_rolls_back(cli_project):
    project = cli_project
    with project.engine.begin() as conn:
        upstream = add_pipeline(conn, "UP")
        conn.execute(
            text(
                "INSERT INTO CFG_PIPELINE_DEPENDENCY (PIPELINE_ID, DEPENDS_ON_PIPELINE_ID, "
                "DEPENDENCY_TYPE) VALUES (:pipeline, :upstream, 'SUCCESS')"
            ),
            {"pipeline": project.pipeline_id, "upstream": upstream},
        )
    code, output = project.run("run", "--pipeline_code", "P", fault="pipeline.after_insert")
    assert code != 0, output
    with project.engine.connect() as conn:
        assert (
            conn.execute(text("SELECT STATUS FROM AUD_PIPELINES_RUN_LOG")).scalar_one() == "QUEUED"
        )
    code, output = project.run("run", "--pipeline_code", "P")
    assert code == 0, output
    project.wait_for("SELECT STATUS FROM AUD_PIPELINES_RUN_LOG", expected="SKIPPED")
