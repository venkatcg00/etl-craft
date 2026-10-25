"""Shared project history for purge and archive contract tests."""

from dataclasses import replace
from datetime import UTC, datetime

import pytest
from sqlalchemy import text

from etl_craft.config.model import Retention
from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.enums import ApiRole, RunStatus
from etl_craft.services.operations import OperationContext
from etl_craft.services.retention import retaining
from fixtures.metadata import add_pipeline, add_task, insert, upstream_run

OLD = datetime(2000, 1, 1, tzinfo=UTC)
FUTURE = datetime(2100, 1, 1, tzinfo=UTC)


@pytest.fixture
def scene(engine_db, tmp_path):
    db = engine_db
    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "retention")
        task = add_task(conn, pipeline, "load", handler="PYTHON")
        run, tasks = upstream_run(conn, pipeline, {task: (RunStatus.SUCCESS,)})
        attempt = insert(
            conn,
            "INSERT INTO AUD_TASK_ATTEMPTS "
            "(TASK_RUN_ID,ATTEMPT_NUMBER,STATUS,STARTED_AT,ENDED_AT) "
            "VALUES (:task,1,'SUCCESS',:old,:old)",
            "ATTEMPT_ID",
            task=tasks[task],
            old=OLD,
        )
    with retaining(), db.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_PIPELINES_RUN_LOG SET END_DATE=:old WHERE PIPELINE_RUN_ID=:run"),
            {"old": OLD, "run": run},
        )
    config = replace(db.config, retention=Retention(run_history_days=90), log_dir=tmp_path / "logs")
    ctx = OperationContext(db.engine, config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
    return ctx, pipeline, task, run, tasks[task], attempt


def count(ctx, table):
    with ctx.engine.connect() as conn:
        return conn.execute(text(f"SELECT COUNT(*) AS count FROM {table}")).scalar_one()
