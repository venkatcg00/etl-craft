"""Finding and reading migration streams, without an Engine DB."""

from pathlib import Path

import pytest
from sqlalchemy import create_engine

from etl_craft.core.errors import MigrationError
from etl_craft.core.text import sha256_hex
from etl_craft.dialects.engine.base import EngineDialect
from etl_craft.engine import migrations

pytestmark = pytest.mark.unit


@pytest.fixture
def engine():
    engine = create_engine("sqlite://")
    yield engine
    engine.dispose()


@pytest.fixture
def packaged(monkeypatch, tmp_path):
    directory = tmp_path / "packaged"
    directory.mkdir()
    monkeypatch.setattr(EngineDialect, "migrations_dir", lambda self: directory)
    return directory


def test_project_directory_lookup_order(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv(migrations.MIGRATIONS_DIR_ENV_VAR, raising=False)
    project = tmp_path / "etl-craft" / "migrations"
    assert migrations.resolve_project_migrations_dir() is None
    # The project's migrations/ counts only once it exists.
    assert migrations.resolve_project_migrations_dir(None, project) is None
    project.mkdir(parents=True)
    assert migrations.resolve_project_migrations_dir(None, project) == project
    monkeypatch.setenv(migrations.MIGRATIONS_DIR_ENV_VAR, "/from/env")
    assert migrations.resolve_project_migrations_dir(None, project) == Path("/from/env")
    assert migrations.resolve_project_migrations_dir("explicit", project) == Path("explicit")


def test_a_stream_reads_sql_files_in_order_with_their_checksums(tmp_path):
    (tmp_path / "0002_b.sql").write_bytes(b"SELECT 2;")
    (tmp_path / "0001_a.sql").write_bytes(b"SELECT 1;")
    (tmp_path / "notes.md").write_text("ignored", encoding="utf-8")
    (tmp_path / "0003_dir.sql").mkdir()
    stream = migrations.read_stream("PROJECT", tmp_path)
    assert [f.version for f in stream.files] == ["0001_a.sql", "0002_b.sql"]
    assert stream.files[0].checksum == sha256_hex(b"SELECT 1;")
    assert stream.files[0].source == "PROJECT"


def test_an_unreadable_file(tmp_path):
    (tmp_path / "0001_bad.sql").write_bytes(b"\xff\xfe not utf-8")
    with pytest.raises(MigrationError, match="could not read migration"):
        migrations.read_stream("PROJECT", tmp_path)


def test_the_packaged_directory_must_exist(engine, monkeypatch, tmp_path):
    monkeypatch.setattr(EngineDialect, "migrations_dir", lambda self: tmp_path / "missing")
    with pytest.raises(MigrationError, match="missing its SQL files"):
        migrations.migration_streams(engine)


def test_a_named_project_directory_must_exist(engine, packaged, tmp_path):
    with pytest.raises(MigrationError, match="does not exist — pass --migrations-dir"):
        migrations.migration_streams(engine, tmp_path / "nope")


def test_the_packaged_directory_is_not_read_twice_as_a_project(engine, packaged):
    streams = migrations.migration_streams(engine, packaged)
    assert [stream.source for stream in streams] == ["ENGINE"]


def test_verify_ledger_accepts_matching_files(tmp_path):
    (tmp_path / "0001_a.sql").write_bytes(b"SELECT 1;")
    stream = migrations.read_stream("PROJECT", tmp_path)
    migrations.verify_ledger({("PROJECT", "0001_a.sql"): sha256_hex(b"SELECT 1;")}, [stream])


@pytest.mark.parametrize(
    "name, sql",
    [
        (
            "0099_named_without_python.sql",
            "-- etl-craft: rebuild-metadata\n-- etl-craft: check-metadata-codes\nSELECT 1;",
        ),
        ("0005_metadata_codes.sql", "SELECT 1;"),
    ],
)
@pytest.mark.parametrize("newline", ["\n", "\r\n"])
def test_engine_markers_preserve_the_original_checksum(tmp_path, name, sql, newline):
    payload = sql.replace("\n", newline).encode()
    (tmp_path / name).write_bytes(payload)
    stream = migrations.read_stream(migrations.ENGINE, tmp_path)
    (migration,) = stream.files
    assert migration.markers == {"rebuild-metadata", "check-metadata-codes"}
    assert migration.checksum == sha256_hex(payload)
    migrations.verify_ledger({(migrations.ENGINE, name): sha256_hex(payload)}, [stream])


@pytest.mark.parametrize(
    "source, marker, message",
    [
        (migrations.ENGINE, "unknown", "unknown migration markers"),
        (migrations.ENGINE, "bad_marker_1", "unknown migration markers"),
        (migrations.PROJECT, "rebuild-metadata", "ENGINE-only"),
    ],
)
def test_invalid_migration_markers_are_refused(tmp_path, source, marker, message):
    (tmp_path / "0099_marked.sql").write_text(f"-- etl-craft: {marker}\nSELECT 1;")
    with pytest.raises(MigrationError, match=message):
        migrations.read_stream(source, tmp_path)
