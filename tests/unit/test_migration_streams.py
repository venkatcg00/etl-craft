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
    assert migrations.resolve_project_migrations_dir() is None
    (tmp_path / "sql" / "migrations").mkdir(parents=True)
    assert migrations.resolve_project_migrations_dir() == tmp_path / "sql" / "migrations"
    monkeypatch.setenv(migrations.MIGRATIONS_DIR_ENV_VAR, "/from/env")
    assert migrations.resolve_project_migrations_dir() == Path("/from/env")
    assert migrations.resolve_project_migrations_dir("explicit") == Path("explicit")


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
