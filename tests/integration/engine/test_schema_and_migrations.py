"""``init-db`` and ``migrate`` against a real Engine DB, on SQLite and PostgreSQL."""

import threading

import pytest
from sqlalchemy import text

from etl_craft.core.errors import EngineDbError, MigrationError
from etl_craft.dialects.engine import for_engine
from etl_craft.dialects.engine.base import EngineDialect
from etl_craft.engine import migrations
from etl_craft.engine.migrations import apply_pending_migrations, mark_packaged_migrations_applied
from etl_craft.engine.schema import SENTINEL_TABLES, existing_engine_tables, init_db


def write(directory, name, sql):
    directory.mkdir(parents=True, exist_ok=True)
    (directory / name).write_text(sql, encoding="utf-8")


def ledger(engine):
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT SOURCE AS source, VERSION AS version FROM SCHEMA_MIGRATIONS")
        ).all()
    return [(row.source, row.version) for row in rows]


def table_exists(engine, name):
    return bool(for_engine(engine).existing_tables(engine, (name,)))


@pytest.fixture
def packaged(monkeypatch, tmp_path):
    """Point the packaged ENGINE stream at a directory the test controls, empty to start with."""
    directory = tmp_path / "packaged"
    directory.mkdir()
    monkeypatch.setattr(EngineDialect, "migrations_dir", lambda self: directory)
    return directory


@pytest.fixture
def initialized(empty_engine_db, packaged):
    init_db(empty_engine_db.engine)
    return empty_engine_db.engine


def test_init_db_creates_the_schema(empty_engine_db, packaged):
    engine = empty_engine_db.engine
    assert existing_engine_tables(engine) == []
    result = init_db(engine)
    assert result.statements > 16
    assert result.recorded_migrations == ()
    assert existing_engine_tables(engine) == sorted(SENTINEL_TABLES)


def test_init_db_refuses_a_database_that_already_has_the_schema(initialized):
    with pytest.raises(
        EngineDbError,
        match=r"already has Engine DB table\(s\) \['aud_pipelines_run_log', 'cfg_pipelines'\]",
    ):
        init_db(initialized)
    # --force applies it anyway, and the duplicate tables fail it as one transaction.
    with pytest.raises(EngineDbError, match="failed applying the"):
        init_db(initialized, force=True)
    assert existing_engine_tables(initialized) == sorted(SENTINEL_TABLES)


def test_init_db_records_the_packaged_migrations_it_includes(empty_engine_db, packaged):
    write(packaged, "0001_add_column.sql", "ALTER TABLE CFG_PIPELINES ADD COLUMN OWNER VARCHAR;")
    result = init_db(empty_engine_db.engine)
    assert result.recorded_migrations == ("0001_add_column.sql",)
    # Recorded, not run: the schema already includes it.
    assert ledger(empty_engine_db.engine) == [("ENGINE", "0001_add_column.sql")]
    assert mark_packaged_migrations_applied(empty_engine_db.engine) == ["0001_add_column.sql"]
    assert apply_pending_migrations(empty_engine_db.engine) == []


def test_migrate_with_nothing_pending(initialized, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv(migrations.MIGRATIONS_DIR_ENV_VAR, raising=False)
    assert apply_pending_migrations(initialized) == []


def test_migrate_applies_engine_then_project_files_in_order(initialized, packaged, tmp_path):
    project = tmp_path / "project"
    write(project, "0002_second.sql", "CREATE TABLE P2 (ID INT);")
    write(project, "0001_first.sql", "CREATE TABLE P1 (ID INT);\nINSERT INTO P1 VALUES (1);")
    write(packaged, "0001_engine.sql", "CREATE TABLE E1 (ID INT);")
    assert apply_pending_migrations(initialized, project) == [
        "0001_engine.sql",
        "0001_first.sql",
        "0002_second.sql",
    ]
    assert sorted(ledger(initialized)) == [
        ("ENGINE", "0001_engine.sql"),
        ("PROJECT", "0001_first.sql"),
        ("PROJECT", "0002_second.sql"),
    ]
    assert all(table_exists(initialized, name) for name in ("e1", "p1", "p2"))
    # A second run finds nothing to do; a new file is picked up.
    assert apply_pending_migrations(initialized, project) == []
    write(project, "0003_third.sql", "CREATE TABLE P3 (ID INT);")
    assert apply_pending_migrations(initialized, project) == ["0003_third.sql"]


def test_the_same_filename_in_both_streams_is_two_migrations(initialized, packaged, tmp_path):
    project = tmp_path / "project"
    write(packaged, "0001_x.sql", "CREATE TABLE E_X (ID INT);")
    write(project, "0001_x.sql", "CREATE TABLE P_X (ID INT);")
    assert apply_pending_migrations(initialized, project) == ["0001_x.sql", "0001_x.sql"]


def test_an_edited_applied_file_is_refused_before_anything_runs(initialized, tmp_path):
    project = tmp_path / "project"
    write(project, "0001_first.sql", "CREATE TABLE P1 (ID INT);")
    apply_pending_migrations(initialized, project)
    write(project, "0001_first.sql", "CREATE TABLE P1 (ID BIGINT);")
    write(project, "0002_second.sql", "CREATE TABLE P2 (ID INT);")
    with pytest.raises(MigrationError, match=r"'0001_first\.sql' has changed since it was applied"):
        apply_pending_migrations(initialized, project)
    assert not table_exists(initialized, "p2")


def test_a_missing_applied_file_is_refused(initialized, tmp_path):
    project = tmp_path / "project"
    write(project, "0001_first.sql", "CREATE TABLE P1 (ID INT);")
    apply_pending_migrations(initialized, project)
    (project / "0001_first.sql").unlink()
    with pytest.raises(MigrationError, match=r"'0001_first\.sql' is missing from"):
        apply_pending_migrations(initialized, project)


def test_applied_project_files_need_the_project_directory(initialized, tmp_path, monkeypatch):
    project = tmp_path / "project"
    write(project, "0001_first.sql", "CREATE TABLE P1 (ID INT);")
    apply_pending_migrations(initialized, project)
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv(migrations.MIGRATIONS_DIR_ENV_VAR, raising=False)
    with pytest.raises(MigrationError, match="no project migrations directory is configured"):
        apply_pending_migrations(initialized)


def test_a_failing_file_rolls_back_with_its_record_and_stops_the_run(initialized, tmp_path):
    project = tmp_path / "project"
    write(project, "0001_bad.sql", "CREATE TABLE HALF (ID INT);\nSELECT * FROM NO_SUCH_TABLE;")
    write(project, "0002_after.sql", "CREATE TABLE AFTER_BAD (ID INT);")
    with pytest.raises(MigrationError, match=r"0001_bad\.sql failed to apply"):
        apply_pending_migrations(initialized, project)
    assert not table_exists(initialized, "half")
    assert not table_exists(initialized, "after_bad")
    assert ledger(initialized) == []


def test_sql_is_run_as_written(initialized, tmp_path):
    # Colons and percent signs inside literals are not bind parameters.
    project = tmp_path / "project"
    write(
        project,
        "0001_literals.sql",
        "CREATE TABLE NOTES (TXT VARCHAR);\n"
        "INSERT INTO NOTES VALUES ('at 12:30 :name 100% done; really');",
    )
    apply_pending_migrations(initialized, project)
    with initialized.connect() as conn:
        assert conn.execute(text("SELECT TXT AS txt FROM NOTES")).scalar_one() == (
            "at 12:30 :name 100% done; really"
        )


def test_concurrent_runs_apply_each_file_once(initialized, tmp_path):
    project = tmp_path / "project"
    write(project, "0001_first.sql", "CREATE TABLE P1 (ID INT);")
    results, errors = [], []

    def run():
        try:
            results.append(apply_pending_migrations(initialized, project))
        except Exception as error:  # pragma: no cover - surfaced by the assertion below
            errors.append(error)

    threads = [threading.Thread(target=run) for _ in range(3)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(60)
    assert errors == []
    assert sorted(results) == [[], [], ["0001_first.sql"]]


def test_migrate_on_a_database_without_the_schema(empty_engine_db, tmp_path):
    with pytest.raises(MigrationError, match="could not read SCHEMA_MIGRATIONS"):
        apply_pending_migrations(empty_engine_db.engine)
