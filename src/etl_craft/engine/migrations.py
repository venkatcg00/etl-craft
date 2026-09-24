"""``migrate``: bringing an existing Engine DB up to date.

Two ordered streams of ``*.sql`` files: the packaged ``ENGINE`` stream, which always runs
first, and the team's optional ``PROJECT`` stream. ``SCHEMA_MIGRATIONS`` records each applied
file by stream, filename and SHA-256. Files are applied in filename order within their stream,
each in its own transaction together with its ledger row, so a failure rolls both back and
stops before any later file. Before anything runs, every applied file must still be present and
unchanged: a released migration file is never edited.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.core.errors import MigrationError
from etl_craft.core.text import sha256_hex
from etl_craft.dialects.engine import for_engine
from etl_craft.engine import locks
from etl_craft.engine.queries import run_script, statement

logger = logging.getLogger(__name__)

MIGRATIONS_DIR_ENV_VAR = "ETL_CRAFT_MIGRATIONS_DIR"
ENGINE = "ENGINE"
PROJECT = "PROJECT"


@dataclass(frozen=True)
class MigrationFile:
    """One migration file, read once: its stream, path, SQL and checksum."""

    source: str
    path: Path
    sql: str
    checksum: str

    @property
    def version(self) -> str:
        """The file's name, which identifies it within its stream."""
        return self.path.name


@dataclass(frozen=True)
class Stream:
    """The migration files of one stream, in the order they apply."""

    source: str
    directory: Path
    files: tuple[MigrationFile, ...]


def resolve_project_migrations_dir(explicit: Path | str | None = None) -> Path | None:
    """Return the project migrations directory, or ``None`` when there is no project stream.

    ``--migrations-dir`` first, then ``$ETL_CRAFT_MIGRATIONS_DIR``, then ``./sql/migrations``
    when that directory exists.
    """
    if explicit is not None:
        return Path(explicit)
    from_env = os.environ.get(MIGRATIONS_DIR_ENV_VAR)
    if from_env:
        return Path(from_env)
    local = Path.cwd() / "sql" / "migrations"
    return local if local.is_dir() else None


def read_stream(source: str, directory: Path) -> Stream:
    """Read every ``*.sql`` file in ``directory``, in filename order."""
    files = []
    for path in sorted(p for p in directory.glob("*.sql") if p.is_file()):
        try:
            payload = path.read_bytes()
            sql = payload.decode("utf-8")
        except (OSError, UnicodeDecodeError) as error:
            raise MigrationError(f"could not read migration {str(path)!r}: {error}") from error
        files.append(MigrationFile(source, path, sql, sha256_hex(payload)))
    return Stream(source, directory, tuple(files))


def migration_streams(engine: Engine, project_dir: Path | str | None = None) -> list[Stream]:
    """Return the packaged ENGINE stream, followed by the PROJECT stream when there is one."""
    package_dir = for_engine(engine).migrations_dir()
    if not package_dir.is_dir():
        raise MigrationError(
            f"packaged migrations directory {str(package_dir)!r} does not exist — the "
            "installed etl-craft package is missing its SQL files"
        )
    streams = [read_stream(ENGINE, package_dir)]
    project = resolve_project_migrations_dir(project_dir)
    if project is None:
        return streams
    if not project.is_dir():
        raise MigrationError(
            f"migrations directory {str(project)!r} does not exist — pass --migrations-dir, "
            f"set ${MIGRATIONS_DIR_ENV_VAR}, or run from a directory containing sql/migrations/"
        )
    if project.resolve() != package_dir.resolve():
        streams.append(read_stream(PROJECT, project))
    return streams


def _load_ledger(conn: Connection) -> dict[tuple[str, str], str]:
    try:
        rows = conn.execute(statement(conn, "applied_migrations")).all()
    except SQLAlchemyError as error:
        raise MigrationError(
            f"could not read SCHEMA_MIGRATIONS — is this an Engine DB? Run `etl-craft init-db` "
            f"on an empty database first. ({error})"
        ) from error
    return {(str(row.source), str(row.version)): str(row.checksum) for row in rows}


def verify_ledger(ledger: dict[tuple[str, str], str], streams: list[Stream]) -> None:
    """Raise ``MigrationError`` unless every applied file is present and unchanged."""
    files = {(f.source, f.version): f for stream in streams for f in stream.files}
    directories = {stream.source: stream.directory for stream in streams}
    for (source, version), checksum in sorted(ledger.items()):
        migration = files.get((source, version))
        if migration is None:
            if source not in directories:
                raise MigrationError(
                    f"applied {source.lower()} migration {version!r} cannot be verified because "
                    "no project migrations directory is configured — pass --migrations-dir (or "
                    f"set ${MIGRATIONS_DIR_ENV_VAR})"
                )
            raise MigrationError(
                f"applied {source.lower()} migration {version!r} is missing from "
                f"{str(directories[source])!r}; restore the original file"
            )
        if checksum != migration.checksum:
            raise MigrationError(
                f"applied {source.lower()} migration {version!r} has changed since it was "
                "applied: migration files are never edited. Restore the original file and add "
                "a new migration."
            )


def _record(conn: Connection, migration: MigrationFile) -> None:
    conn.execute(
        statement(conn, "record_migration"),
        {"source": migration.source, "version": migration.version, "checksum": migration.checksum},
    )


def _apply(engine: Engine, migration: MigrationFile) -> None:
    dialect = for_engine(engine)
    try:
        with engine.begin() as conn:
            dialect.begin_ddl_transaction(conn)
            run_script(conn, dialect.split_statements(migration.sql))
            _record(conn, migration)
    except Exception as error:
        raise MigrationError(f"{migration.version} failed to apply: {error}") from error


def apply_pending_migrations(
    engine: Engine, project_dir: Path | str | None = None, *, wait_seconds: float = 0
) -> list[str]:
    """Apply every pending ENGINE, then PROJECT, migration; return the filenames applied.

    Concurrent runs are serialized by the ``migrate`` lock, so a second one waits and then finds
    nothing left to do.
    """
    streams = migration_streams(engine, project_dir)
    applied: list[str] = []
    with locks.MIGRATE.hold(engine, wait_seconds):
        with engine.connect() as conn:
            ledger = _load_ledger(conn)
        verify_ledger(ledger, streams)
        for stream in streams:
            for migration in stream.files:
                if (migration.source, migration.version) in ledger:
                    continue
                _apply(engine, migration)
                logger.info("applied %s migration %s", migration.source, migration.version)
                applied.append(migration.version)
    return applied


def mark_packaged_migrations_applied(engine: Engine) -> list[str]:
    """Record the packaged ENGINE migrations as applied without running them.

    ``init-db`` calls this: the packaged schema already includes every packaged migration. A
    project's migrations are not in the schema, so they stay pending.
    """
    stream = migration_streams(engine)[0]
    if not stream.files:
        return []
    with locks.MIGRATE.hold(engine), engine.begin() as conn:
        ledger = _load_ledger(conn)
        verify_ledger(ledger, [stream])
        for migration in stream.files:
            if (migration.source, migration.version) not in ledger:
                _record(conn, migration)
    return [migration.version for migration in stream.files]
