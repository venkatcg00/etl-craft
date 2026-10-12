"""Legacy normalization readiness includes inactive rows and never edits metadata."""

import pytest
from sqlalchemy import text

from etl_craft.engine.repository.normalization import normalization_problems
from etl_craft.services.validate import validate_on
from fixtures.metadata import add_pipeline


@pytest.mark.engine_sqlite
def test_readiness_does_not_decode_unrelated_inactive_settings(tmp_path):
    from fixtures.engine_db import apply_schema, sqlite_engine_db

    db = sqlite_engine_db(tmp_path)
    try:
        apply_schema(db.engine)
        with db.engine.begin() as conn:
            add_pipeline(conn, "P")
            conn.execute(
                text("UPDATE CFG_PIPELINES SET ACTIVE_FLAG='N', SCHEDULE_START_DATE='not-a-date'")
            )
        with db.engine.connect() as conn:
            assert normalization_problems(conn, catalog=None, dialect=None) == []
            assert validate_on(conn, db.config).findings == []
    finally:
        db.engine.dispose()


def test_duplicate_keys_and_qualified_source_aliases_are_reported_read_only(engine_db):
    db = engine_db
    with db.engine.begin() as conn:
        add_pipeline(conn, "P")
        conn.execute(text("UPDATE CFG_PIPELINES SET ACTIVE_FLAG='N'"))
        add_pipeline(conn, "P")
        conn.execute(
            text(
                "INSERT INTO CFG_SOURCE_COLUMNS "
                "(TABLE_NAME,COLUMN_NAME,DATA_TYPE,COLUMN_POSITION,ACTIVE_FLAG) VALUES "
                "('raw.orders','id','INT',1,'Y'), ('analytics.raw.orders','id','INT',1,'N')"
            )
        )
    with db.engine.connect() as conn:
        history_before = conn.execute(
            text("SELECT COUNT(*) FROM AUD_METADATA_CHANGES")
        ).scalar_one()
        problems = normalization_problems(conn, catalog="analytics", dialect="duckdb")
        assert len(problems) == 3
        assert any("CFG_PIPELINES identity ('P',)" in message for message in problems)
        assert any("CFG_SOURCE_COLUMNS identity" in message for message in problems)
        assert any("CFG_SOURCE_COLUMNS column position" in message for message in problems)
        assert all(
            "ACTIVE_FLAG=Y" in message and "ACTIVE_FLAG=N" in message for message in problems
        )
        assert all(
            "deactivating a duplicate does not resolve it" in message for message in problems
        )
        assert (
            conn.execute(text("SELECT COUNT(*) FROM AUD_METADATA_CHANGES")).scalar_one()
            == history_before
        )
        report = validate_on(conn, db.config)
        readiness = [
            finding for finding in report.findings if finding.where == "metadata normalization"
        ]
        assert readiness
        assert all(finding.status.value == "WARN" for finding in readiness)


def test_inactive_invalid_targets_are_reported_but_sql_and_paths_are_not_parsed(engine_db):
    from fixtures.metadata import add_task

    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        add_task(
            conn,
            pipeline,
            "task",
            TARGET_OBJECT="four.part.table.name",
            SOURCE_SQL="SELECT 1.0 AS number",
            SCRIPT_PATH="path.with.dots.py",
        )
        conn.execute(text("UPDATE CFG_TASK_PARAMETERS SET ACTIVE_FLAG='N'"))
    with engine_db.engine.connect() as conn:
        problems = normalization_problems(conn, catalog=None, dialect="postgres")
    assert len(problems) == 1
    assert "four.part.table.name" in problems[0]
    assert "CFG_TASK_PARAMETERS" in problems[0]
    assert "ACTIVE_FLAG=N" in problems[0]


def test_distinct_inactive_identity_is_not_a_duplicate(engine_db):
    from fixtures.metadata import add_task

    with engine_db.engine.begin() as conn:
        first = add_pipeline(conn, "P")
        second = add_pipeline(conn, "Q")
        add_task(conn, first, "same_code", SOURCE_SQL="SELECT 1")
        add_task(conn, second, "same_code", SOURCE_SQL="SELECT 1")
        conn.execute(text("UPDATE CFG_PIPELINES SET ACTIVE_FLAG='N' WHERE PIPELINE_CODE='Q'"))
    with engine_db.engine.connect() as conn:
        assert not normalization_problems(conn, catalog=None, dialect=None)
