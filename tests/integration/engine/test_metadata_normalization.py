"""Legacy normalization readiness includes inactive rows and never edits metadata."""

import pytest
from sqlalchemy import text

from etl_craft.engine.repository.normalization import normalization_problems
from etl_craft.services.validate import validate_on
from fixtures.metadata import add_pipeline


@pytest.fixture
def legacy_identity_db(empty_engine_db, monkeypatch):
    from dataclasses import replace

    from etl_craft.engine import migrations
    from fixtures.released_schema import install

    db = empty_engine_db
    install(db, "0.2.0")
    original = migrations.migration_streams
    with monkeypatch.context() as patch:
        patch.delenv("ETL_CRAFT_MIGRATIONS_DIR", raising=False)
        patch.setattr(
            migrations,
            "migration_streams",
            lambda *args, **kwargs: [
                replace(stream, files=tuple(f for f in stream.files if f.version < "0027"))
                for stream in original(*args, **kwargs)
            ],
        )
        migrations.apply_pending_migrations(db.engine)
    return db


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


def test_duplicate_keys_and_qualified_source_aliases_are_reported_read_only(legacy_identity_db):
    db = legacy_identity_db
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


def test_inactive_invalid_targets_are_reported_but_sql_and_paths_are_not_parsed(legacy_identity_db):
    engine_db = legacy_identity_db
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


IDENTITY_ROWS = (
    ("CFG_PIPELINES", "PIPELINE_CODE,PIPELINE_NAME,REFRESH_TYPE", "'identity','Identity','FULL'"),
    ("CFG_TASKS", "PIPELINE_ID,TASK_CODE,TASK_TYPE,HANDLER", ":pipeline,'identity','ETL','SQL'"),
    ("CFG_TASK_PARAMETERS", "TASK_ID,PARAMETER_NAME", ":task,'identity'"),
    (
        "CFG_PIPELINE_DEPENDENCY",
        "PIPELINE_ID,DEPENDS_ON_PIPELINE_ID,DEPENDENCY_TYPE",
        ":pipeline,:upstream,'SUCCESS'",
    ),
    (
        "CFG_TASK_DEPENDENCY",
        "PIPELINE_ID,TASK_ID,DEPENDS_ON_TASK_ID,DEPENDENCY_TYPE",
        ":pipeline,:task,:other,'SUCCESS'",
    ),
    (
        "CFG_BUSINESS_RULES",
        "PIPELINE_ID,TASK_ID,BUSINESS_RULE_NAME,SEQUENCE_NUMBER,BUSINESS_RULE_TYPE,"
        "BUSINESS_RULE_KEY_COLUMN,TARGET_TABLE_ID,BUSINESS_RULE_SQL",
        ":pipeline,:task,'identity',1,'REPORT','id',:target,'SELECT 1'",
    ),
)


def identity_insert(conn, table, columns, values):
    from fixtures.metadata import add_task

    pipeline = add_pipeline(conn, "P")
    upstream = add_pipeline(conn, "Q")
    task = add_task(conn, pipeline, "first")
    other = add_task(conn, pipeline, "second")
    from fixtures.metadata import add_table

    params = {
        "pipeline": pipeline,
        "upstream": upstream,
        "task": task,
        "other": other,
    }
    if table == "CFG_BUSINESS_RULES":
        from sqlalchemy import inspect

        name = "cfg_tables" if conn.dialect.name == "postgresql" else "CFG_TABLES"
        if inspect(conn).has_table(name):
            params["target"] = add_table(conn, "raw.orders")
        else:
            columns = columns.replace("TARGET_TABLE_ID", "TARGET_TABLE")
            values = values.replace(":target", "'raw.orders'")
    insert = text(f"INSERT INTO {table} ({columns}) VALUES ({values})")
    conn.execute(insert, params)
    conn.execute(text(f"UPDATE {table} SET ACTIVE_FLAG='N'"))
    return insert, params


@pytest.mark.parametrize("table,columns,values", IDENTITY_ROWS)
def test_inactive_config_keys_cannot_be_reinserted(engine_db, table, columns, values):
    from sqlalchemy.exc import IntegrityError

    with engine_db.engine.begin() as conn:
        insert, params = identity_insert(conn, table, columns, values)
    with pytest.raises(IntegrityError), engine_db.engine.begin() as conn:
        conn.execute(insert, params)
    with engine_db.engine.begin() as conn:
        conn.execute(text(f"UPDATE {table} SET ACTIVE_FLAG='Y'"))
    with pytest.raises(IntegrityError), engine_db.engine.begin() as conn:
        conn.execute(insert, params)


@pytest.mark.parametrize("table,columns,values", IDENTITY_ROWS)
def test_identity_migration_refuses_duplicates_without_changing_schema_or_history(
    legacy_identity_db, table, columns, values
):
    from etl_craft.core.errors import MigrationError
    from etl_craft.engine.migrations import apply_pending_migrations
    from fixtures.catalog import snapshot

    db = legacy_identity_db
    with db.engine.begin() as conn:
        insert, params = identity_insert(conn, table, columns, values)
        conn.execute(insert, params)
    before = snapshot(db.engine)
    with db.engine.connect() as conn:
        history = conn.execute(text("SELECT COUNT(*) FROM AUD_METADATA_CHANGES")).scalar_one()
        ledger = conn.execute(text("SELECT COUNT(*) FROM SCHEMA_MIGRATIONS")).scalar_one()
    with pytest.raises(MigrationError, match=f"{table} identity") as failure:
        apply_pending_migrations(db.engine)
    assert "ACTIVE_FLAG=N" in str(failure.value)
    assert "ACTIVE_FLAG=Y" in str(failure.value)
    assert "deactivating a duplicate does not resolve it" in str(failure.value)
    assert snapshot(db.engine) == before
    with db.engine.connect() as conn:
        assert (
            conn.execute(text("SELECT COUNT(*) FROM AUD_METADATA_CHANGES")).scalar_one() == history
        )
        assert conn.execute(text("SELECT COUNT(*) FROM SCHEMA_MIGRATIONS")).scalar_one() == ledger


def test_identity_migration_preserves_rows_and_their_history(legacy_identity_db):
    from etl_craft.engine.migrations import apply_pending_migrations

    db = legacy_identity_db
    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        conn.execute(text("UPDATE CFG_PIPELINES SET ACTIVE_FLAG='N'"))
    with db.engine.connect() as conn:
        before = conn.execute(
            text("SELECT * FROM AUD_CFG_PIPELINES_HISTORY ORDER BY CHANGE_ID")
        ).all()
        row = conn.execute(text("SELECT * FROM CFG_PIPELINES")).one()
    assert apply_pending_migrations(db.engine) == [
        "0027_config_identities.sql",
        "0028_source_contracts.sql",
        "0029_rule_target_contracts.sql",
        "0030_task_target_contracts.sql",
    ]
    with db.engine.connect() as conn:
        assert conn.execute(text("SELECT * FROM CFG_PIPELINES")).one() == row
        assert (
            conn.execute(text("SELECT * FROM AUD_CFG_PIPELINES_HISTORY ORDER BY CHANGE_ID")).all()
            == before
        )
    with db.engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_PIPELINES SET ACTIVE_FLAG='Y' WHERE PIPELINE_ID=:id"),
            {"id": pipeline},
        )
    with db.engine.connect() as conn:
        assert conn.execute(text("SELECT PIPELINE_ID FROM CFG_PIPELINES")).scalar_one() == pipeline


@pytest.mark.parametrize("release", ["0.1.0", "0.2.0"])
def test_legacy_duplicate_preflight_runs_before_any_pending_engine_migration(
    empty_engine_db, release
):
    from etl_craft.core.errors import MigrationError
    from etl_craft.engine.migrations import apply_pending_migrations
    from fixtures.catalog import snapshot
    from fixtures.released_schema import install

    db = empty_engine_db
    install(db, release)
    with db.engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO CFG_PIPELINES (PIPELINE_CODE,PIPELINE_NAME,REFRESH_TYPE,ACTIVE_FLAG) "
                "VALUES ('P','Retired','FULL','N'), ('P','Replacement','FULL','Y')"
            )
        )
    before = snapshot(db.engine)
    with db.engine.connect() as conn:
        ledger = conn.execute(text("SELECT * FROM SCHEMA_MIGRATIONS")).all()
    with pytest.raises(MigrationError, match="CFG_PIPELINES identity"):
        apply_pending_migrations(db.engine)
    assert snapshot(db.engine) == before
    with db.engine.connect() as conn:
        assert conn.execute(text("SELECT * FROM SCHEMA_MIGRATIONS")).all() == ledger
