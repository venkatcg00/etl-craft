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
import re
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.core.actor import migration as current_migration
from etl_craft.core.errors import MigrationError
from etl_craft.core.text import is_metadata_code, sha256_hex
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


def resolve_project_migrations_dir(
    explicit: Path | str | None = None, project_default: Path | None = None
) -> Path | None:
    """Return the project migrations directory, or ``None`` when there is no project stream.

    ``--migrations-dir`` first, then ``$ETL_CRAFT_MIGRATIONS_DIR``, then ``project_default``
    (``migrations/`` in the project directory) when that directory exists.
    """
    if explicit is not None:
        return Path(explicit)
    from_env = os.environ.get(MIGRATIONS_DIR_ENV_VAR)
    if from_env:
        return Path(from_env)
    if project_default is not None and project_default.is_dir():
        return project_default
    return None


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


def migration_streams(
    engine: Engine, project_dir: Path | str | None = None, project_default: Path | None = None
) -> list[Stream]:
    """Return the packaged ENGINE stream, followed by the PROJECT stream when there is one."""
    package_dir = for_engine(engine).migrations_dir()
    if not package_dir.is_dir():
        raise MigrationError(
            f"packaged migrations directory {str(package_dir)!r} does not exist — the "
            "installed etl-craft package is missing its SQL files"
        )
    streams = [read_stream(ENGINE, package_dir)]
    project = resolve_project_migrations_dir(project_dir, project_default)
    if project is None:
        return streams
    if not project.is_dir():
        raise MigrationError(
            f"migrations directory {str(project)!r} does not exist — pass --migrations-dir, "
            f"set ${MIGRATIONS_DIR_ENV_VAR}, or keep them in migrations/ in the project directory"
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
    token = current_migration.set(migration.version if migration.source == PROJECT else "")
    try:
        with dialect.migration_transaction(
            engine,
            rebuild_metadata=migration.sql
            if migration.source == ENGINE
            and migration.version
            in (
                "0005_metadata_codes.sql",
                "0006_run_backfill_constraint.sql",
                "0007_identity.sql",
                "0008_actors_and_audit_guards.sql",
            )
            else None,
        ) as conn:
            statements = dialect.split_statements(migration.sql)
            if migration.source == PROJECT:
                if dialect.name == "sqlite":
                    from etl_craft.dialects.engine.sqlite.audit import refresh_metadata_triggers
                else:
                    from etl_craft.dialects.engine.postgres.audit import refresh_metadata_triggers

                for sql in statements:
                    run_script(conn, [sql])
                    if re.search(r"\b(?:ALTER|CREATE)\s+TABLE\b", sql, re.IGNORECASE):
                        refresh_metadata_triggers(conn)
            else:
                run_script(conn, statements)
            _record(conn, migration)
    except Exception as error:
        raise MigrationError(f"{migration.version} failed to apply: {error}") from error
    finally:
        current_migration.reset(token)


def pending_migrations(
    engine: Engine,
    project_dir: Path | str | None = None,
    *,
    project_default: Path | None = None,
) -> list[str]:
    """Return the migrations ``migrate`` would apply, without applying any.

    Raises ``MigrationError`` as ``migrate`` would, for an applied file that is missing or was
    edited.
    """
    streams = migration_streams(engine, project_dir, project_default)
    with engine.connect() as conn:
        ledger = _load_ledger(conn)
    verify_ledger(ledger, streams)
    return [
        f"{migration.source.lower()}/{migration.version}"
        for stream in streams
        for migration in stream.files
        if (migration.source, migration.version) not in ledger
    ]


def _check_metadata_codes(conn: Connection) -> None:
    invalid = [
        f"{row.object} (id {row.object_id}) = {row.code!r}"
        for row in conn.execute(statement(conn, "metadata_codes"))
        if not is_metadata_code(row.code)
    ]
    if invalid:
        raise MigrationError(
            "metadata code migration cannot run: "
            + "; ".join(invalid)
            + "; rename these codes to start with an ASCII letter and contain only letters, "
            "digits and underscores, at most 128 characters, then run migrate again"
        )


def apply_pending_migrations(
    engine: Engine,
    project_dir: Path | str | None = None,
    *,
    project_default: Path | None = None,
    wait_seconds: float = 0,
) -> list[str]:
    """Apply every pending ENGINE, then PROJECT, migration; return the filenames applied.

    ``project_dir`` is ``--migrations-dir``; ``project_default`` is the project's
    ``migrations/``, used when neither it nor ``$ETL_CRAFT_MIGRATIONS_DIR`` is given.

    Concurrent runs are serialized by the ``migrate`` lock, so a second one waits and then finds
    nothing left to do.
    """
    streams = migration_streams(engine, project_dir, project_default)
    applied: list[str] = []
    with locks.MIGRATE.hold(engine, wait_seconds):
        with engine.connect() as conn:
            ledger = _load_ledger(conn)
        verify_ledger(ledger, streams)
        if any(
            file.source == ENGINE
            and file.version == "0005_metadata_codes.sql"
            and (file.source, file.version) not in ledger
            for stream in streams
            for file in stream.files
        ):
            with engine.connect() as conn:
                _check_metadata_codes(conn)
        for stream in streams:
            for migration in stream.files:
                if (migration.source, migration.version) in ledger:
                    continue
                _apply(engine, migration)
                logger.info("applied %s migration %s", migration.source, migration.version)
                applied.append(migration.version)
    return applied


def mark_packaged_migrations_applied(conn: Connection) -> list[str]:
    """Record the packaged ENGINE migrations as applied without running them.

    ``init-db`` calls this: the packaged schema already includes every packaged migration. A
    project's migrations are not in the schema, so they stay pending. The caller holds the
    MIGRATE lock and owns the transaction that creates the schema and records this ledger.
    """
    stream = migration_streams(conn.engine)[0]
    if not stream.files:
        return []
    ledger = _load_ledger(conn)
    verify_ledger(ledger, [stream])
    for migration in stream.files:
        if (migration.source, migration.version) not in ledger:
            _record(conn, migration)
    return [migration.version for migration in stream.files]
