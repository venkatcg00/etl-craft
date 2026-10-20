"""Upgrades retain old retry decisions without inventing missing failure evidence."""

from dataclasses import replace

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from etl_craft.engine import migrations, transitions
from fixtures.metadata import add_pipeline, add_task, insert
from fixtures.released_schema import SCHEMAS, install


@pytest.mark.parametrize("release", sorted(p.name for p in SCHEMAS.iterdir() if p.is_dir()))
def test_existing_attempts_keep_unknown_class_and_original_decision(
    empty_engine_db, release, monkeypatch
):
    db = empty_engine_db
    install(db, release)
    streams = migrations.migration_streams

    def before_classification(*args, **kwargs):
        return [
            replace(s, files=tuple(f for f in s.files if f.version < "0041"))
            for s in streams(*args, **kwargs)
        ]

    with monkeypatch.context() as previous:
        previous.setattr(migrations, "migration_streams", before_classification)
        migrations.apply_pending_migrations(db.engine)
    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "old_task")
        run = insert(
            conn,
            "INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID,STATUS,RUN_KEY,TRIGGER_KIND,RUN_DATE) "
            "VALUES (:pipeline,'IN-PROGRESS','retained-retry','MANUAL','2026-01-01')",
            "PIPELINE_RUN_ID",
            pipeline=pipeline,
        )
        task_run = transitions.find_or_create_task_run(conn, task, run).task_run_id
        conn.execute(
            text(
                "INSERT INTO AUD_TASK_ATTEMPTS (TASK_RUN_ID,ATTEMPT_NUMBER,STATUS,RETRYABLE) "
                "VALUES (:task_run,1,'FAILED',TRUE)"
            ),
            {"task_run": task_run},
        )
    assert migrations.apply_pending_migrations(db.engine)
    with db.engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT FAILURE_CLASS AS failure_class, FAILURE_SOURCE AS failure_source, "
                "FAILURE_CODE AS failure_code, RETRY_ON AS retry_on, RETRYABLE AS retryable "
                "FROM AUD_TASK_ATTEMPTS WHERE TASK_RUN_ID=:task_run"
            ),
            {"task_run": task_run},
        ).one()
    assert row.failure_class == "UNKNOWN"
    assert row.failure_source is None and row.failure_code is None
    assert row.retry_on is None
    assert row.retryable
    with pytest.raises(DBAPIError, match="immutable"), db.engine.begin() as conn:
        conn.execute(text("UPDATE AUD_TASK_ATTEMPTS SET FAILURE_CLASS='PERMANENT'"))
