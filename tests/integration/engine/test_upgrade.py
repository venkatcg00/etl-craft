"""An Engine DB made by an earlier release upgrades to exactly the schema a new one starts with.

``tests/fixtures/schemas/<version>/`` holds each release's packaged schema, as it was tagged.
Each is applied to an empty database, then ``migrate`` runs the packaged migrations; the result
must match a fresh initialization in columns, constraints, indexes and triggers, and retain
representative rows by named column.
"""

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError

from etl_craft.dialects.engine import build_engine
from etl_craft.engine.migrations import apply_pending_migrations
from etl_craft.engine.schema import init_db
from fixtures.catalog import snapshot
from fixtures.engine_db import engine_config, sqlite_engine_db
from fixtures.released_schema import install
from fixtures.upgrade_rows import assert_retained, seed

SCHEMAS = Path(__file__).parents[2] / "fixtures" / "schemas"
RELEASES = sorted(path.name for path in SCHEMAS.iterdir() if path.is_dir())


@pytest.mark.parametrize("release", RELEASES)
def test_an_earlier_engine_db_upgrades_to_the_current_schema(
    empty_engine_db, release, monkeypatch, tmp_path
):
    engine = empty_engine_db.engine
    folder = "postgres" if engine.dialect.name == "postgresql" else "sqlite"
    install(empty_engine_db, release)
    monkeypatch.delenv("ETL_CRAFT_MIGRATIONS_DIR", raising=False)
    monkeypatch.chdir(tmp_path)
    retained = seed(engine, tmp_path)

    upgrade_started = datetime.now(UTC)
    apply_pending_migrations(engine, project_default=tmp_path / "migrations")
    upgrade_ended = datetime.now(UTC)
    assert {"AUD_TASK_EVENTS", "AUD_EVENT_FILES", "AUD_EVENT_KEYS"} <= {
        name.upper() for name in inspect(engine).get_table_names()
    }
    with engine.connect() as conn:
        columns = conn.execute(text("SELECT * FROM AUD_EVENT_KEYS WHERE 1=0")).keys()
        assert {"task_id", "event_key", "task_run_id", "created_by", "created_at"} <= {
            name.lower() for name in columns
        }

    if folder == "postgres":
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA fresh"))
        fresh = build_engine(engine_config(replace(empty_engine_db.config.engine, schema="fresh")))
    else:
        directory = tmp_path / "fresh"
        directory.mkdir()
        fresh = sqlite_engine_db(directory).engine
    try:
        init_db(fresh)
        upgraded = snapshot(engine)
        for section, expected in snapshot(fresh).items():
            assert upgraded[section] == expected, section
    finally:
        fresh.dispose()
    assert_retained(engine, retained, (upgrade_started, upgrade_ended))
    # The partial unique indexes came along too: one open pause per pipeline.
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
                "VALUES ('P', 'P', 'FULL')"
            )
        )
    insert = text(
        "INSERT INTO AUD_PIPELINE_PAUSES (PIPELINE_ID, PAUSED_BY, REASON) "
        "SELECT PIPELINE_ID, 'op', 'why' FROM CFG_PIPELINES WHERE PIPELINE_CODE = 'P'"
    )
    with engine.begin() as conn:
        conn.execute(insert)
    with pytest.raises(IntegrityError), engine.begin() as conn:
        conn.execute(insert)
    # A second migrate has nothing left to do.
    assert apply_pending_migrations(engine, project_default=tmp_path / "migrations") == []


def test_signal_migration_keeps_existing_file_consumption_and_evidence(engine_db):
    from etl_craft.engine import transitions
    from etl_craft.engine.repository.events import consumed_files, consumed_keys, record_event
    from fixtures.metadata import add_pipeline, add_task, start_run

    db = engine_db
    with db.engine.begin() as conn:
        conn.execute(text("DROP TABLE AUD_EVENT_KEYS"))
        pipeline = add_pipeline(conn, "FILES")
        task = add_task(conn, pipeline, "wait", "EVENT_WAIT", EVENT_FILE="inbox/*")
        run = start_run(conn, pipeline)
        task_run = transitions.find_or_create_task_run(conn, task, run).task_run_id
        evidence = {"files": [{"path": "/kept/batch.csv", "size": 7}]}
        record_event(conn, task, task_run, evidence)
        migration = db.dialect.schema_path().parent / "migrations" / "0049_event_keys.sql"
        for sql in db.dialect.split_statements(migration.read_text()):
            conn.exec_driver_sql(sql)
    with db.engine.connect() as conn:
        assert consumed_files(conn, task) == {"/kept/batch.csv"}
        assert consumed_keys(conn, task, ["any-key"]) == set()
        row = conn.execute(
            text("SELECT EVIDENCE AS evidence FROM AUD_TASK_EVENTS WHERE TASK_RUN_ID=:id"),
            {"id": task_run},
        ).one()
        import json

        assert json.loads(row.evidence) == evidence
