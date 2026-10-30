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
import tomllib
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Unpack

from sqlalchemy import inspect
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.core.actor import migration as current_migration
from etl_craft.core.errors import MigrationError
from etl_craft.core.text import sha256_hex, strip_comments_and_literals
from etl_craft.dialects.engine import for_engine
from etl_craft.dialects.engine.tables import AUDIT_TABLES, OWNED_CONFIG_TABLES
from etl_craft.engine import locks
from etl_craft.engine.migration_steps import (
    MARKERS,
    NO_TARGET,
    STEPS,
    MarkerStep,
    Target,
    TargetOptions,
)
from etl_craft.engine.queries import run_script, statement

logger = logging.getLogger(__name__)

MIGRATIONS_DIR_ENV_VAR = "ETL_CRAFT_MIGRATIONS_DIR"
ENGINE = "ENGINE"
PROJECT = "PROJECT"
PACKAGED_MARKERS = Path(__file__).parents[1] / "dialects/engine/migration-markers.toml"


@dataclass(frozen=True)
class MigrationFile:
    """One migration file, read once: its stream, path, SQL and checksum."""

    source: str
    path: Path
    sql: str
    checksum: str
    markers: frozenset[str] = frozenset()
    convert_at: int | None = None

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


def read_stream(source: str, directory: Path, dialect: str = "") -> Stream:
    """Read every ``*.sql`` file in ``directory``, in filename order.

    ``dialect`` names the Engine DB (``postgresql``, ``sqlite``) whose ``convert-at``
    positions apply to the packaged stream.
    """
    registry = tomllib.loads(PACKAGED_MARKERS.read_text()) if source == ENGINE else {}
    defaults = registry.get("markers", {})
    positions = registry.get("convert-at", {}).get(dialect, {})
    files = []
    for path in sorted(p for p in directory.glob("*.sql") if p.is_file()):
        try:
            payload = path.read_bytes()
            sql = payload.decode("utf-8")
        except (OSError, UnicodeDecodeError) as error:
            raise MigrationError(f"could not read migration {str(path)!r}: {error}") from error
        markers = frozenset(
            marker.strip() for marker in re.findall(r"^-- etl-craft: (.*)$", sql, re.MULTILINE)
        )
        markers |= {marker for marker, names in defaults.items() if path.name in names}
        if markers and source != ENGINE:
            raise MigrationError(f"{path.name}: etl-craft migration markers are ENGINE-only")
        if markers - MARKERS:
            raise MigrationError(
                f"{path.name}: unknown migration markers {sorted(markers - MARKERS)}; "
                f"use {sorted(MARKERS)}"
            )
        files.append(
            MigrationFile(source, path, sql, sha256_hex(payload), markers, positions.get(path.name))
        )
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
    streams = [read_stream(ENGINE, package_dir, for_engine(engine).name)]
    project = resolve_project_migrations_dir(project_dir, project_default)
    if project is None:
        return streams
    if not project.is_dir():
        raise MigrationError(
            f"migrations directory {str(project)!r} does not exist — give `migrations_dir`, "
            f"set ${MIGRATIONS_DIR_ENV_VAR}, or keep them in migrations/ in the project directory"
        )
    if project.resolve() != package_dir.resolve():
        streams.append(read_stream(PROJECT, project))
    return streams


def _project_ledger(conn: Connection) -> bool:
    """Read released ledgers until the packaged project migration adds their scope."""
    return "project_id" in {
        name.lower() for name in for_engine(conn).column_names(conn, "SCHEMA_MIGRATIONS")
    }


def _load_ledger(conn: Connection) -> dict[tuple[str, str], str]:
    try:
        query = "scoped_applied_migrations" if _project_ledger(conn) else "applied_migrations"
        rows = conn.execute(statement(conn, query)).all()
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
                    "no project migrations directory is configured — give `migrations_dir` (or "
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
        statement(
            conn,
            "record_project_migration"
            if migration.source == PROJECT and _project_ledger(conn)
            else "record_migration",
        ),
        {"source": migration.source, "version": migration.version, "checksum": migration.checksum},
    )


def _apply(engine: Engine, migration: MigrationFile, target: Target = NO_TARGET) -> None:
    dialect = for_engine(engine)
    steps = [step for marker, step in STEPS.items() if marker in migration.markers]
    token = current_migration.set(
        migration.version
        if migration.source == PROJECT
        else f"engine/{migration.version}"
        if any(step.names_migration for step in steps)
        else ""
    )
    try:
        with dialect.migration_transaction(
            engine,
            rebuild_metadata=migration.sql
            if migration.source == ENGINE and "rebuild-metadata" in migration.markers
            else None,
        ) as conn:
            if migration.source == PROJECT:
                _run_project(conn, migration, target)
            else:
                _run_engine(conn, migration, steps, target)
            _record(conn, migration)
    except Exception as error:
        raise MigrationError(f"{migration.version} failed to apply: {error}") from error
    finally:
        current_migration.reset(token)


def _run_project(conn: Connection, migration: MigrationFile, target: Target) -> None:
    """Run a team migration inside its project, refreshing guards after each table change."""
    from etl_craft.engine.project_migrations import ProjectScope
    from etl_craft.engine.repository.pipelines import current_project_id
    from etl_craft.engine.repository.source_contracts import resolve_project_sources

    dialect = for_engine(conn)
    scope = ProjectScope(conn, current_project_id(), migration.version)
    dialect.project_view(conn, scope.project_id)
    for number, sql in enumerate(dialect.split_statements(migration.sql), start=1):
        scope.check(number, sql)
        scope.run(number, sql)
        if re.search(r"\b(?:ALTER|CREATE)\s+TABLE\b", sql, re.IGNORECASE):
            dialect.refresh_metadata_triggers(conn)
    dialect.project_view(conn, None)
    resolve_project_sources(conn, target.catalog, target.warehouse_dialect)


def _run_engine(
    conn: Connection, migration: MigrationFile, steps: list[MarkerStep], target: Target
) -> None:
    """Run a packaged migration with its markers' checks and conversion at its position."""
    dialect = for_engine(conn)
    states = [(step, step.check(conn, target)) for step in steps if step.check is not None]
    statements = dialect.split_statements(migration.sql)
    at = len(statements) if migration.convert_at is None else migration.convert_at
    run_script(conn, statements[:at])
    for step, state in states:
        if step.convert is not None and state is not None:
            step.convert(conn, state, target)
    run_script(conn, statements[at:])
    dialect.refresh_metadata_triggers(conn)


def pending_migrations(
    engine: Engine,
    project_dir: Path | str | None = None,
    *,
    project_default: Path | None = None,
    connection: Connection | None = None,
) -> list[str]:
    """Return the migrations ``migrate`` would apply, without applying any.

    Raises ``MigrationError`` as ``migrate`` would, for an applied file that is missing or was
    edited.
    """
    return [
        f"{migration.source.lower()}/{migration.version}"
        for migration in pending_files(
            engine, project_dir, project_default=project_default, connection=connection
        )
    ]


def pending_files(
    engine: Engine,
    project_dir: Path | str | None = None,
    *,
    project_default: Path | None = None,
    connection: Connection | None = None,
) -> list[MigrationFile]:
    """Return the files ``migrate`` would apply, in order, after verifying the applied ones."""
    streams = migration_streams(engine, project_dir, project_default)
    with nullcontext(connection) if connection is not None else engine.connect() as conn:
        ledger = _load_ledger(conn)
    verify_ledger(ledger, streams)
    return [
        migration
        for stream in streams
        for migration in stream.files
        if (migration.source, migration.version) not in ledger
    ]


def apply_pending_migrations(
    engine: Engine,
    project_dir: Path | str | None = None,
    *,
    project_default: Path | None = None,
    wait_seconds: float = 0,
    **target_options: Unpack[TargetOptions],
) -> list[str]:
    """Apply every pending ENGINE, then PROJECT, migration; return the filenames applied.

    ``project_dir`` is ``--migrations-dir``; ``project_default`` is the project's
    ``migrations/``, used when neither it nor ``$ETL_CRAFT_MIGRATIONS_DIR`` is given.

    Concurrent runs are serialized by the ``migrate`` lock, so a second one waits and then finds
    nothing left to do.
    """
    streams = migration_streams(engine, project_dir, project_default)
    target = Target(**target_options)
    applied: list[str] = []
    with locks.MIGRATE.hold(engine, wait_seconds):
        with engine.connect() as conn:
            ledger = _load_ledger(conn)
        verify_ledger(ledger, streams)
        pending = [
            file
            for stream in streams
            for file in stream.files
            if (file.source, file.version) not in ledger
        ]
        with engine.connect() as conn:
            _check_package_collisions(conn, pending)
        for marker, step in STEPS.items():
            if step.check is not None and any(marker in file.markers for file in pending):
                with engine.connect() as conn:
                    step.check(conn, target)
        for stream in streams:
            for migration in stream.files:
                if (migration.source, migration.version) in ledger:
                    continue
                _apply(engine, migration, target)
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


def _check_package_collisions(conn: Connection, pending: list[MigrationFile]) -> None:
    """Refuse conflicting package table creation before any pending migration can write."""
    present = {name.upper() for name in inspect(conn).get_table_names()}
    owned = OWNED_CONFIG_TABLES | AUDIT_TABLES
    for file in pending:
        if file.source != ENGINE:
            continue
        for sql in for_engine(conn).split_statements(file.sql):
            sql = strip_comments_and_literals(sql).strip()
            dropped = re.match(r'DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?"?(\w+)"?', sql, re.IGNORECASE)
            if dropped:
                present.discard(dropped[1].upper())
            created = re.match(
                r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"?(\w+)"?', sql, re.IGNORECASE
            )
            if created and created[1].upper() in owned and created[1].upper() in present:
                raise MigrationError(
                    f"engine/{file.version} creates {created[1]}, but that table already exists; "
                    "rename the conflicting team table before upgrading. "
                    "No pending migration was applied."
                )
