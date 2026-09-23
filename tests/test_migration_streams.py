"""Regression tests for engine/project migration stream isolation."""

from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from etl_craft.migrate import (
    ENGINE_MIGRATION_SOURCE,
    PROJECT_MIGRATION_SOURCE,
    MigrationError,
    apply_pending_migrations,
)
from etl_craft.packaged_sql import packaged_migrations_dir


@pytest.fixture
def migration_engine(postgres_engine: Engine) -> Engine:
    """Create an isolated blank database for migration-ledger regressions."""
    url = postgres_engine.url
    admin = create_engine(
        url.set(database="postgres").render_as_string(hide_password=False),
        isolation_level="AUTOCOMMIT",
    )
    database_name = f"etl_craft_migration_{uuid4().hex[:16]}"
    target = create_engine(url.set(database=database_name).render_as_string(hide_password=False))
    try:
        with admin.connect() as conn:
            conn.execute(text(f"CREATE DATABASE {database_name}"))
        yield target
    finally:
        target.dispose()
        with admin.connect() as conn:
            conn.execute(text(f"DROP DATABASE IF EXISTS {database_name}"))
        admin.dispose()


def _write_migration(directory, name: str, body: str) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / name).write_text(body, encoding="utf-8")


def _table_exists(engine: Engine, table_name: str) -> bool:
    with engine.connect() as conn:
        return (
            conn.execute(text("SELECT to_regclass(:name)"), {"name": table_name}).scalar()
            is not None
        )


def test_project_stream_does_not_mask_packaged_stream_or_same_filename(
    migration_engine: Engine, tmp_path, monkeypatch
) -> None:
    """ENGINE runs first and its filename may also exist in PROJECT."""
    package_dir = tmp_path / "package"
    project_dir = tmp_path / "project"
    version = "0001_shared_name.sql"
    _write_migration(
        package_dir,
        version,
        "CREATE TABLE migration_stream_engine_probe (id int);",
    )
    _write_migration(
        project_dir,
        version,
        "CREATE TABLE migration_stream_project_probe (id int);",
    )
    monkeypatch.setattr("etl_craft.migrate.packaged_migrations_dir", lambda: package_dir)

    applied = apply_pending_migrations(migration_engine, project_dir)

    assert applied == [version, version]
    assert _table_exists(migration_engine, "migration_stream_engine_probe")
    assert _table_exists(migration_engine, "migration_stream_project_probe")
    with migration_engine.connect() as conn:
        records = conn.execute(
            text("SELECT SOURCE, VERSION, CHECKSUM FROM SCHEMA_MIGRATIONS " "ORDER BY SOURCE")
        ).all()
    assert [(row.source, row.version) for row in records] == [
        (ENGINE_MIGRATION_SOURCE, version),
        (PROJECT_MIGRATION_SOURCE, version),
    ]
    assert all(len(row.checksum) == 64 for row in records)


def test_changed_applied_file_fails_before_later_project_migrations_run(
    migration_engine: Engine, tmp_path, monkeypatch
) -> None:
    """A content edit is detected before a pending file can change the database."""
    package_dir = tmp_path / "package"
    project_dir = tmp_path / "project"
    first = "0001_project_baseline.sql"
    later = "0002_must_not_run.sql"
    package_dir.mkdir()
    _write_migration(
        project_dir,
        first,
        "CREATE TABLE migration_checksum_baseline (id int);",
    )
    monkeypatch.setattr("etl_craft.migrate.packaged_migrations_dir", lambda: package_dir)

    assert apply_pending_migrations(migration_engine, project_dir) == [first]
    _write_migration(
        project_dir,
        first,
        "CREATE TABLE migration_checksum_baseline (id int, changed int);",
    )
    _write_migration(
        project_dir,
        later,
        "CREATE TABLE migration_checksum_must_not_run (id int);",
    )

    with pytest.raises(MigrationError, match="checksum mismatch"):
        apply_pending_migrations(migration_engine, project_dir)

    assert not _table_exists(migration_engine, "migration_checksum_must_not_run")
    with migration_engine.connect() as conn:
        later_record = conn.execute(
            text(
                "SELECT 1 FROM SCHEMA_MIGRATIONS " "WHERE SOURCE = :source AND VERSION = :version"
            ),
            {"source": PROJECT_MIGRATION_SOURCE, "version": later},
        ).scalar_one_or_none()
    assert later_record is None


def test_legacy_engine_records_are_adopted_without_reapplying_them(
    migration_engine: Engine, tmp_path, monkeypatch
) -> None:
    """A pre-stream ledger remains usable when upgrading to the new runner."""
    package_dir = tmp_path / "package"
    version = "0001_add_run_condition.sql"
    _write_migration(
        package_dir,
        version,
        "CREATE TABLE migration_legacy_should_not_be_created (id int);",
    )
    ledger_migration = "0004_migration_streams_and_checksums.sql"
    _write_migration(
        package_dir,
        ledger_migration,
        (packaged_migrations_dir() / ledger_migration).read_text(encoding="utf-8"),
    )
    monkeypatch.setattr("etl_craft.migrate.packaged_migrations_dir", lambda: package_dir)
    with migration_engine.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE SCHEMA_MIGRATIONS ("
                "VERSION VARCHAR PRIMARY KEY, "
                "APPLIED_AT TIMESTAMPTZ NOT NULL DEFAULT now())"
            )
        )
        conn.execute(
            text("INSERT INTO SCHEMA_MIGRATIONS (VERSION) VALUES (:version)"),
            {"version": version},
        )

    # Adoption assigns the old record to ENGINE without executing its body.
    # The actual 0004 ledger migration then runs against the upgraded table,
    # proving the package migration is safe on the old one-column ledger.
    assert apply_pending_migrations(migration_engine) == [ledger_migration]
    assert not _table_exists(migration_engine, "migration_legacy_should_not_be_created")
    with migration_engine.connect() as conn:
        record = conn.execute(
            text("SELECT SOURCE, CHECKSUM FROM SCHEMA_MIGRATIONS " "WHERE VERSION = :version"),
            {"version": version},
        ).one()
    assert record.source == ENGINE_MIGRATION_SOURCE
    assert len(record.checksum) == 64
