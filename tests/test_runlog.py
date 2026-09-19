"""Tests for etl_craft.runlog against an in-memory SQLite stand-in.

The real Engine DB is always Postgres (per CLAUDE.md), and its own
behavioral guarantees — the partial unique indexes in particular — are
covered by sql/schema_test.sql against a real Postgres instance. This
suite instead exercises runlog.py's own control flow (find-or-create
races, short-circuiting, update-in-place) against a lightweight
SQLite schema that reproduces just the constraints runlog.py depends on.
"""

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from etl_craft.runlog import (
    RunLogError,
    find_or_create_active_run,
    find_or_create_task_run,
    resolve_run_for_task,
    update_task_run,
)

SCHEMA = """
CREATE TABLE AUD_PIPELINES_RUN_LOG (
    PIPELINE_RUN_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    PIPELINE_ID INTEGER NOT NULL,
    START_DATE TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    END_DATE TIMESTAMP,
    STATUS TEXT NOT NULL
);
CREATE UNIQUE INDEX ux_pipeline_run_one_active
    ON AUD_PIPELINES_RUN_LOG (PIPELINE_ID) WHERE STATUS = 'IN-PROGRESS';

CREATE TABLE AUD_TASK_RUN_LOG (
    TASK_RUN_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    TASK_ID INTEGER NOT NULL,
    PIPELINE_RUN_ID INTEGER NOT NULL,
    START_DATE TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    END_DATE TIMESTAMP,
    STATUS TEXT NOT NULL,
    SOURCE_COUNT INTEGER,
    TARGET_COUNT INTEGER,
    INSERT_COUNT INTEGER,
    UPDATE_COUNT INTEGER,
    DELETE_COUNT INTEGER,
    ERROR_MESSAGE TEXT,
    TASK_LOG TEXT
);
CREATE UNIQUE INDEX ux_task_run_one_per_pipeline_run
    ON AUD_TASK_RUN_LOG (TASK_ID, PIPELINE_RUN_ID);
"""


@pytest.fixture
def engine() -> Engine:
    """An in-memory SQLite engine with the audit tables runlog.py touches."""
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        for statement in SCHEMA.strip().split(";"):
            if statement.strip():
                conn.execute(text(statement))
    return engine


def test_find_or_create_active_run_mints_new_run_when_none_exists(engine):
    with engine.begin() as conn:
        run_id = find_or_create_active_run(conn, pipeline_id=1)
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT PIPELINE_ID, STATUS FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID = :id"
            ),
            {"id": run_id},
        ).one()
    assert row.PIPELINE_ID == 1
    assert row.STATUS == "IN-PROGRESS"


def test_find_or_create_active_run_reuses_existing_in_progress_run(engine):
    with engine.begin() as conn:
        first = find_or_create_active_run(conn, pipeline_id=1)
    with engine.begin() as conn:
        second = find_or_create_active_run(conn, pipeline_id=1)
    assert first == second


def test_find_or_create_active_run_is_independent_per_pipeline(engine):
    with engine.begin() as conn:
        run_for_1 = find_or_create_active_run(conn, pipeline_id=1)
        run_for_2 = find_or_create_active_run(conn, pipeline_id=2)
    assert run_for_1 != run_for_2


def test_find_or_create_active_run_mints_a_fresh_run_after_the_last_one_finished(engine):
    with engine.begin() as conn:
        first = find_or_create_active_run(conn, pipeline_id=1)
        conn.execute(
            text("UPDATE AUD_PIPELINES_RUN_LOG SET STATUS = 'SUCCESS' WHERE PIPELINE_RUN_ID = :id"),
            {"id": first},
        )
    with engine.begin() as conn:
        second = find_or_create_active_run(conn, pipeline_id=1)
    assert second != first


def test_resolve_run_for_task_binds_to_active_run(engine):
    with engine.begin() as conn:
        active = find_or_create_active_run(conn, pipeline_id=1)
    with engine.begin() as conn:
        resolved = resolve_run_for_task(conn, pipeline_id=1)
    assert resolved == active


def test_resolve_run_for_task_falls_back_to_latest_logged_run(engine):
    with engine.begin() as conn:
        run_id = find_or_create_active_run(conn, pipeline_id=1)
        conn.execute(
            text("UPDATE AUD_PIPELINES_RUN_LOG SET STATUS = 'SUCCESS' WHERE PIPELINE_RUN_ID = :id"),
            {"id": run_id},
        )
    with engine.begin() as conn:
        resolved = resolve_run_for_task(conn, pipeline_id=1)
    assert resolved == run_id
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT STATUS, END_DATE FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID = :id"),
            {"id": run_id},
        ).one()
    assert row.STATUS == "SUCCESS"  # status is left alone, not reopened
    assert row.END_DATE is not None  # but the date is touched


def test_resolve_run_for_task_raises_when_pipeline_never_ran(engine):
    with engine.begin() as conn, pytest.raises(RunLogError):
        resolve_run_for_task(conn, pipeline_id=999)


def test_find_or_create_task_run_creates_then_reuses_binding(engine):
    with engine.begin() as conn:
        run_id = find_or_create_active_run(conn, pipeline_id=1)
        first = find_or_create_task_run(conn, task_id=10, pipeline_run_id=run_id)
    assert first.status == "IN-PROGRESS"
    with engine.begin() as conn:
        second = find_or_create_task_run(conn, task_id=10, pipeline_run_id=run_id)
    assert second.task_run_id == first.task_run_id
    assert second.status == "IN-PROGRESS"


def test_find_or_create_task_run_reflects_updated_status(engine):
    with engine.begin() as conn:
        run_id = find_or_create_active_run(conn, pipeline_id=1)
        binding = find_or_create_task_run(conn, task_id=10, pipeline_run_id=run_id)
        update_task_run(conn, binding.task_run_id, status="SUCCESS", target_count=42)
    with engine.begin() as conn:
        rebound = find_or_create_task_run(conn, task_id=10, pipeline_run_id=run_id)
    assert rebound.task_run_id == binding.task_run_id
    assert rebound.status == "SUCCESS"
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT TARGET_COUNT, END_DATE FROM AUD_TASK_RUN_LOG WHERE TASK_RUN_ID = :id"),
            {"id": binding.task_run_id},
        ).one()
    assert row.TARGET_COUNT == 42
    assert row.END_DATE is not None


def test_update_task_run_leaves_unspecified_counts_untouched(engine):
    with engine.begin() as conn:
        run_id = find_or_create_active_run(conn, pipeline_id=1)
        binding = find_or_create_task_run(conn, task_id=10, pipeline_run_id=run_id)
        update_task_run(conn, binding.task_run_id, status="IN-PROGRESS", source_count=100)
        update_task_run(conn, binding.task_run_id, status="SUCCESS", target_count=99)
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT SOURCE_COUNT, TARGET_COUNT FROM AUD_TASK_RUN_LOG WHERE TASK_RUN_ID = :id"),
            {"id": binding.task_run_id},
        ).one()
    assert row.SOURCE_COUNT == 100  # untouched by the second call
    assert row.TARGET_COUNT == 99


def test_find_or_create_task_run_is_independent_per_task(engine):
    with engine.begin() as conn:
        run_id = find_or_create_active_run(conn, pipeline_id=1)
        a = find_or_create_task_run(conn, task_id=10, pipeline_run_id=run_id)
        b = find_or_create_task_run(conn, task_id=11, pipeline_run_id=run_id)
    assert a.task_run_id != b.task_run_id
