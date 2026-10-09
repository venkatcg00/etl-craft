"""Every released Engine DB upgrades to the full catalog of a fresh initialization."""

from dataclasses import replace
from pathlib import Path

import pytest
from sqlalchemy import text

from etl_craft.dialects.engine import build_engine
from etl_craft.engine.migrations import apply_pending_migrations
from etl_craft.engine.queries import run_script
from etl_craft.engine.schema import init_db
from fixtures.catalog import snapshot
from fixtures.engine_db import engine_config, sqlite_engine_db
from fixtures.released_schema import install

SCHEMAS = Path(__file__).parents[2] / "fixtures" / "schemas"
RELEASES = sorted(path.name for path in SCHEMAS.iterdir() if path.is_dir())


@pytest.mark.parametrize("release", RELEASES)
def test_full_catalog_matches_fresh_initialization(empty_engine_db, release, tmp_path, monkeypatch):
    db = empty_engine_db
    folder = "postgres" if db.engine.dialect.name == "postgresql" else "sqlite"
    install(db, release)
    monkeypatch.delenv("ETL_CRAFT_MIGRATIONS_DIR", raising=False)
    apply_pending_migrations(db.engine)
    if folder == "postgres":
        with db.engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA fresh"))
        profile = replace(db.config.engine, schema="fresh")
        fresh = build_engine(engine_config(profile))
    else:
        directory = tmp_path / "fresh"
        directory.mkdir()
        fresh = sqlite_engine_db(directory).engine
    try:
        init_db(fresh)
        upgraded, expected = snapshot(db.engine), snapshot(fresh)
        for section in expected:
            if section == "tables":
                assert upgraded[section].keys() == expected[section].keys()
                for table in expected[section]:
                    for field in expected[section][table]:
                        assert upgraded[section][table][field] == expected[section][table][field], (
                            table,
                            field,
                        )
            else:
                assert upgraded[section] == expected[section], section
    finally:
        fresh.dispose()


@pytest.fixture
def before_backfill_constraint(empty_engine_db, monkeypatch):
    from etl_craft.engine import migrations

    db = empty_engine_db
    kind = "postgres" if db.engine.dialect.name == "postgresql" else "sqlite"
    with db.engine.begin() as conn:
        run_script(
            conn, db.dialect.split_statements((SCHEMAS / "0.1.0" / f"{kind}.sql").read_text())
        )
    monkeypatch.delenv("ETL_CRAFT_MIGRATIONS_DIR", raising=False)
    for migration in migrations.migration_streams(db.engine)[0].files:
        if migration.version < "0006":
            migrations._apply(db.engine, migration)
    original = migrations.migration_streams

    def streams(*args, **kwargs):
        return [
            replace(stream, files=tuple(f for f in stream.files if f.version < "0007"))
            if stream.source == migrations.ENGINE
            else stream
            for stream in original(*args, **kwargs)
        ]

    monkeypatch.setattr(migrations, "migration_streams", streams)
    return db


def seed_run_history(db):
    from fixtures.metadata import add_pipeline, add_task

    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "load")
        run = conn.execute(
            text(
                "INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS, SLA_STATUS, RUN_DATE, "
                "BACKFILL) VALUES (:pipeline, 'SUCCESS', 'MET', '2026-01-02', 'Y') "
                "RETURNING PIPELINE_RUN_ID AS pipeline_run_id"
            ),
            {"pipeline": pipeline},
        ).scalar_one()
        conn.execute(
            text(
                "INSERT INTO AUD_TASK_RUN_LOG (TASK_ID, PIPELINE_RUN_ID, STATUS, END_DATE) "
                "VALUES (:task, :run, 'SUCCESS', CURRENT_TIMESTAMP)"
            ),
            {"task": task, "run": run},
        )
        removed = conn.execute(
            text(
                "INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS) "
                "VALUES (:pipeline, 'SUCCESS') RETURNING PIPELINE_RUN_ID AS pipeline_run_id"
            ),
            {"pipeline": pipeline},
        ).scalar_one()
        conn.execute(
            text("DELETE FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID=:run"), {"run": removed}
        )
        conn.exec_driver_sql("CREATE INDEX custom_run_date ON AUD_PIPELINES_RUN_LOG (RUN_DATE)")
        conn.exec_driver_sql(
            "CREATE VIEW custom_runs AS SELECT PIPELINE_RUN_ID FROM AUD_PIPELINES_RUN_LOG"
        )
        conn.exec_driver_sql("CREATE TABLE custom_run_events (RUN_ID BIGINT)")
        if conn.dialect.name == "sqlite":
            conn.exec_driver_sql(
                "CREATE TRIGGER custom_run_insert AFTER INSERT ON AUD_PIPELINES_RUN_LOG "
                "BEGIN INSERT INTO custom_run_events VALUES (NEW.PIPELINE_RUN_ID); END"
            )
        else:
            conn.exec_driver_sql(
                "CREATE FUNCTION custom_run_event() RETURNS trigger LANGUAGE plpgsql AS $fn$ "
                "BEGIN INSERT INTO custom_run_events VALUES (NEW.PIPELINE_RUN_ID); "
                "RETURN NEW; END $fn$"
            )
            conn.exec_driver_sql(
                "CREATE TRIGGER custom_run_insert AFTER INSERT ON AUD_PIPELINES_RUN_LOG "
                "FOR EACH ROW EXECUTE FUNCTION custom_run_event()"
            )
    return pipeline, run, removed


def assert_history(db, pipeline, run, removed):
    with db.engine.begin() as conn:
        row = conn.execute(
            text(
                "SELECT PIPELINE_ID AS pipeline_id, STATUS AS status, "
                "SLA_STATUS AS sla_status, RUN_DATE AS run_date, BACKFILL AS backfill "
                "FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID=:run"
            ),
            {"run": run},
        ).one()
        assert (row.pipeline_id, row.status, row.sla_status, str(row.run_date), row.backfill) == (
            pipeline,
            "SUCCESS",
            "MET",
            "2026-01-02",
            "Y",
        )
        assert (
            conn.execute(text("SELECT PIPELINE_RUN_ID FROM AUD_TASK_RUN_LOG")).scalar_one() == run
        )
        assert conn.execute(text("SELECT PIPELINE_RUN_ID FROM custom_runs")).scalar_one() == run
        if conn.dialect.name == "sqlite":
            assert conn.exec_driver_sql("PRAGMA foreign_keys").scalar_one() == 1
            assert conn.exec_driver_sql("PRAGMA legacy_alter_table").scalar_one() == 0
            assert conn.exec_driver_sql("PRAGMA foreign_key_check").all() == []
        next_run = conn.execute(
            text(
                "INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS) "
                "VALUES (:pipeline, 'SUCCESS') RETURNING PIPELINE_RUN_ID AS pipeline_run_id"
            ),
            {"pipeline": pipeline},
        ).scalar_one()
        assert next_run > removed
        assert conn.execute(text("SELECT RUN_ID FROM custom_run_events")).scalar_one() == next_run


def test_backfill_constraint_preserves_history_references_objects_and_counter(
    before_backfill_constraint,
):
    db = before_backfill_constraint
    history = seed_run_history(db)
    original = snapshot(db.engine)
    assert apply_pending_migrations(db.engine) == ["0006_run_backfill_constraint.sql"]
    updated = snapshot(db.engine)
    section = "indexes" if db.engine.dialect.name == "postgresql" else "objects"
    assert updated[section] == original[section]
    assert_history(db, *history)


def test_backfill_constraint_failure_rolls_back_catalog_and_history(
    before_backfill_constraint, monkeypatch
):
    from etl_craft.core.errors import MigrationError
    from etl_craft.engine import migrations

    db = before_backfill_constraint
    history = seed_run_history(db)
    original = snapshot(db.engine)
    record = migrations._record

    def refuse_ledger(conn, migration):
        record(conn, migration)
        raise MigrationError("ledger refused")

    monkeypatch.setattr(migrations, "_record", refuse_ledger)
    with pytest.raises(MigrationError, match="ledger refused"):
        apply_pending_migrations(db.engine)
    assert snapshot(db.engine) == original
    with db.engine.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) FROM SCHEMA_MIGRATIONS WHERE VERSION="
                    "'0006_run_backfill_constraint.sql'"
                )
            ).scalar_one()
            == 0
        )
    monkeypatch.setattr(migrations, "_record", record)
    assert apply_pending_migrations(db.engine) == ["0006_run_backfill_constraint.sql"]
    assert_history(db, *history)


def test_backfill_constraint_preserves_or_refuses_extra_columns(before_backfill_constraint):
    from etl_craft.core.errors import MigrationError

    db = before_backfill_constraint
    seed_run_history(db)
    with db.engine.begin() as conn:
        conn.exec_driver_sql("ALTER TABLE AUD_PIPELINES_RUN_LOG ADD COLUMN OWNER VARCHAR")
        conn.exec_driver_sql("UPDATE AUD_PIPELINES_RUN_LOG SET OWNER='team'")
    if db.engine.dialect.name == "sqlite":
        with pytest.raises(MigrationError, match=r"AUD_PIPELINES_RUN_LOG.*owner.*would discard"):
            apply_pending_migrations(db.engine)
    else:
        apply_pending_migrations(db.engine)
    with db.engine.connect() as conn:
        assert conn.execute(text("SELECT OWNER FROM AUD_PIPELINES_RUN_LOG")).scalar_one() == "team"
