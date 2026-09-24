"""`etl-craft migrate` — apply pending sql/migrations/*.sql files to an existing Engine DB.

[ADDITION] Closes CLAUDE.md open question #7 ("No migration tooling
(Alembic or otherwise) has been discussed... on the assumption schema
review happens before that matters"), per explicit permission ("you may
implement the migration mechanism as well"). Deliberately a small,
Alembic-*lite* runner, not a full migration framework — see
sql/migrations/README.md for the file convention this reads, and
schema.sql's own SCHEMA_MIGRATIONS table comment for why a fresh install
(via schema.sql) and an empty SCHEMA_MIGRATIONS table are consistent by
construction (no migration file has ever existed for anything already
baked into schema.sql — see that file's own note on this).

Packaged engine migrations and a team's optional project migrations are two
ordered streams.  Engine files always run first; selecting a project directory
adds its files rather than hiding the package.  Each stream is keyed by source,
filename, and SHA-256 checksum in SCHEMA_MIGRATIONS.  A pending file runs
inside its own transaction together with its bookkeeping row — a failure rolls
both back and stops before any later file runs.

[DEVIATION, 2026-09-20, E2-05] Three plumbing fixes, all found by the
iteration-2 review rather than by anything failing:

  * `DEFAULT_MIGRATIONS_DIR` used to be `Path(__file__).parents[2] / "sql" /
    "migrations"`, which resolves to the repo checkout — and to whatever sits
    above `site-packages` once installed, where the wheel shipped no `sql/`
    at all (E2-13). `Path.glob` on a nonexistent directory yields nothing
    without error, so `migrate` printed "already up to date" and silently
    skipped a team's migrations. The packaged directory is now always the
    ENGINE stream; an explicit `--migrations-dir`, `$ETL_CRAFT_MIGRATIONS_DIR`,
    or `./sql/migrations` adds the PROJECT stream.
  * `SELECT VERSION FROM SCHEMA_MIGRATIONS` raised a raw `ProgrammingError`
    from outside the try against any database predating that table, so the
    CLI produced a traceback and nothing could bootstrap it. The table is
    created if absent.
  * Two concurrent `migrate` runs were unserialized. A Postgres advisory lock
    now spans the whole run, so the second waits rather than double-applying.
  * A one-column ledger meant a customer's filename could mask a future
    package migration.  The ledger now namespaces ENGINE and PROJECT rows and
    checks the content hash of every already-applied file before any new SQL
    is executed.

[CHOICE] A migration file is split into individual `;`-terminated
statements and each is executed separately, rather than handed to the
driver as one multi-statement string — psycopg3's extended query protocol
(what SQLAlchemy's `text()` normally goes through) doesn't reliably support
more than one statement per call the way `psql`'s own simple-query mode
does. The split is a plain string split on `;`, not a real SQL parse (same
limitation this codebase already accepts elsewhere for the same reason,
per Non-goals) — a statement containing a literal semicolon inside a
string literal would be mis-split. Acceptable for a deliberately small
tool; flagged rather than silently assumed safe.
"""

from __future__ import annotations

import hashlib
import os
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from etl_craft.locks import engine_lock
from etl_craft.packaged_sql import packaged_migrations_dir

MIGRATIONS_DIR_ENV_VAR = "ETL_CRAFT_MIGRATIONS_DIR"
ENGINE_MIGRATION_SOURCE = "ENGINE"
PROJECT_MIGRATION_SOURCE = "PROJECT"
LEGACY_MIGRATION_SOURCE = "LEGACY"
# Arbitrary but fixed: the key both `migrate` runs agree on. Postgres advisory
# locks are namespaced only by the integer itself, so it is written here once
# rather than computed, and chosen far from anything a team's own SQL is
# likely to pick.
_ADVISORY_LOCK_KEY = 8_241_007


class MigrationError(Exception):
    """Raised when a migration file fails to apply."""


@dataclass(frozen=True)
class MigrationFile:
    """One immutable migration payload prepared for an application run."""

    source: str
    path: Path
    sql: str
    checksum: str

    @property
    def version(self) -> str:
        """Return the filename used as this migration's version."""
        return self.path.name


# These are the packaged files that existed before the ledger gained a source
# column.  They are the only legacy rows that can safely be recognized as
# engine-owned by filename alone.  Do not add later engine migrations here:
# a legacy row with a later name may be a customer's old migration, and must
# fail loudly rather than silently suppressing a future engine migration.
_PRE_STREAM_ENGINE_VERSIONS = frozenset(
    {
        "0001_add_run_condition.sql",
        "0002_column_lineage_and_docs.sql",
        "0003_task_attempt_count.sql",
    }
)


def resolve_migrations_dir(explicit: Path | str | None = None) -> Path:
    """Resolve which migrations directory to read, most-specific first.

    [DEVIATION, 2026-09-20, E2-05] See this module's own docstring: the old
    package-relative default silently resolved to a nonexistent path once
    installed, and `migrate` then reported success having applied nothing.
    """
    if explicit is not None:
        return Path(explicit)
    from_env = os.environ.get(MIGRATIONS_DIR_ENV_VAR)
    if from_env:
        return Path(from_env)
    local = Path.cwd() / "sql" / "migrations"
    if local.is_dir():
        return local
    return packaged_migrations_dir()


def resolve_project_migrations_dir(explicit: Path | str | None = None) -> Path | None:
    """Return the optional project migration directory, without the package fallback.

    `resolve_migrations_dir` remains for callers that need the historical
    single-directory answer.  Application needs a different answer now:
    packaged engine migrations are always one stream, while an explicit,
    environment-selected, or local directory is a second project stream.
    Returning ``None`` means there is no project stream to apply.
    """
    if explicit is not None:
        return Path(explicit)
    from_env = os.environ.get(MIGRATIONS_DIR_ENV_VAR)
    if from_env:
        return Path(from_env)
    local = Path.cwd() / "sql" / "migrations"
    return local if local.is_dir() else None


def split_statements(sql_text: str) -> list[str]:
    """Split a SQL file into statements on `;`, respecting quotes and comments.

    [DEVIATION, 2026-09-20, E2-05] This used to be `sql_text.split(";")`, a
    documented limitation that turned out to be the *first* thing anyone would
    hit: `schema.sql`'s own trigger functions are `CREATE FUNCTION ... $$ ...
    ; ... $$` bodies, so any migration touching them — and `init-db` applying
    the schema at all — would be shredded mid-body. A literal semicolon inside
    an ordinary string literal broke it too.

    Deliberately a small scanner, not a SQL parser (Non-goals rules one out):
    it tracks single-quoted strings with their `''` escape, dollar-quoted
    bodies including tagged `$tag$` ones, `--` line comments and `/* */` block
    comments, and splits on any `;` outside all of them. That is the whole
    grammar a statement splitter needs, and nothing here tries to understand
    the statements themselves.
    """
    statements: list[str] = []
    current: list[str] = []
    i = 0
    length = len(sql_text)
    while i < length:
        ch = sql_text[i]
        rest = sql_text[i:]

        if rest.startswith("--"):
            end = sql_text.find("\n", i)
            end = length if end == -1 else end
            current.append(sql_text[i:end])
            i = end
            continue

        if rest.startswith("/*"):
            end = sql_text.find("*/", i + 2)
            end = length if end == -1 else end + 2
            current.append(sql_text[i:end])
            i = end
            continue

        if ch == "'":
            end = i + 1
            while end < length:
                if sql_text[end] == "'":
                    if end + 1 < length and sql_text[end + 1] == "'":
                        end += 2
                        continue
                    end += 1
                    break
                end += 1
            current.append(sql_text[i:end])
            i = end
            continue

        if ch == "$":
            tag = _dollar_tag_at(sql_text, i)
            if tag is not None:
                close = sql_text.find(tag, i + len(tag))
                end = length if close == -1 else close + len(tag)
                current.append(sql_text[i:end])
                i = end
                continue

        if ch == ";":
            statements.append("".join(current))
            current = []
            i += 1
            continue

        current.append(ch)
        i += 1

    statements.append("".join(current))
    return [stmt.strip() for stmt in statements if stmt.strip() and not _is_only_comments(stmt)]


def _dollar_tag_at(sql_text: str, index: int) -> str | None:
    """Return the dollar-quote tag starting at `index` (e.g. "$$", "$fn$"), or None."""
    end = sql_text.find("$", index + 1)
    if end == -1:
        return None
    body = sql_text[index + 1 : end]
    if body and not (body[0].isalpha() or body[0] == "_"):
        return None
    if not all(c.isalnum() or c == "_" for c in body):
        return None
    return sql_text[index : end + 1]


def _is_only_comments(statement: str) -> bool:
    """Report whether `statement` holds nothing but comments and whitespace."""
    stripped = re.sub(r"/\*.*?\*/", "", statement, flags=re.DOTALL)
    stripped = re.sub(r"--[^\n]*", "", stripped)
    return not stripped.strip()


# Kept as the private name the rest of this package already imports.
_split_statements = split_statements


def split_sqlite_statements(sql_text: str) -> list[str]:
    """Split a SQLite script into statements, keeping trigger bodies whole.

    [ADDITION, 2026-09-24] `split_statements` knows Postgres's quoting, not
    SQLite's `CREATE TRIGGER ... BEGIN ...; ...; END;`, whose inner semicolons
    it would cut at. SQLite ships the answer itself: `complete_statement`
    reports whether text so far forms whole statements, so a `;` only ends a
    statement once SQLite agrees it does.
    """
    statements: list[str] = []
    current: list[str] = []
    for ch in sql_text:
        current.append(ch)
        if ch == ";" and sqlite3.complete_statement("".join(current)):
            statements.append("".join(current))
            current = []
    statements.append("".join(current))
    return [
        stmt.strip().rstrip(";").strip()
        for stmt in statements
        if stmt.strip() and not _is_only_comments(stmt)
    ]


def statements_for(engine: Engine, sql_text: str) -> list[str]:
    """Split `sql_text` with the splitter that understands `engine`'s dialect."""
    if engine.dialect.name == "sqlite":
        return split_sqlite_statements(sql_text)
    return split_statements(sql_text)


def begin_ddl_transaction(conn: Connection) -> None:
    """Make DDL on `conn` transactional, which it is not by default on SQLite.

    Python's sqlite3 module opens a transaction implicitly only before
    INSERT/UPDATE/DELETE, so DDL would otherwise autocommit statement by
    statement and a failed migration would leave half its changes behind.
    Postgres needs nothing: DDL is transactional there already.
    """
    if conn.dialect.name == "sqlite":
        conn.exec_driver_sql("BEGIN IMMEDIATE")


def _ensure_bookkeeping_table(engine: Engine) -> None:
    """Create or upgrade the migration ledger before reading it.

    The original ledger had ``VERSION`` as its sole primary key.  That made a
    project migration named ``0004_add_thing.sql`` indistinguishable from a
    future engine migration with the same filename, and selecting a project
    directory hid every packaged migration altogether.  The ledger now keeps
    the source and SHA-256 checksum alongside a filename, with ``(SOURCE,
    VERSION)`` as the key.

    This bootstrap lives here as well as in the packaged ledger migration:
    the runner must be able to read and classify the old table *before* it can
    decide whether that migration is pending.  It is deliberately limited to
    this tool-owned bookkeeping table.  Existing records become ``LEGACY`` and
    are adopted only when their ownership can be proved safely.
    """
    if engine.dialect.name == "sqlite":
        # A SQLite Engine DB is never older than the (SOURCE, VERSION,
        # CHECKSUM) ledger -- schema_sqlite.sql was written after it -- so
        # there is no legacy shape to upgrade, only a missing table to create.
        with engine.begin() as conn:
            conn.exec_driver_sql(
                "CREATE TABLE IF NOT EXISTS SCHEMA_MIGRATIONS ("
                "SOURCE VARCHAR NOT NULL DEFAULT 'LEGACY', "
                "VERSION VARCHAR NOT NULL, "
                "CHECKSUM VARCHAR(64), "
                "APPLIED_AT TIMESTAMP NOT NULL "
                "DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'), "
                "PRIMARY KEY (SOURCE, VERSION))"
            )
        return
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql(
                "CREATE TABLE IF NOT EXISTS SCHEMA_MIGRATIONS ("
                "SOURCE VARCHAR NOT NULL DEFAULT 'LEGACY', "
                "VERSION VARCHAR NOT NULL, "
                "CHECKSUM VARCHAR(64), "
                "APPLIED_AT TIMESTAMPTZ NOT NULL DEFAULT now(), "
                "PRIMARY KEY (SOURCE, VERSION))"
            )
            # `IF NOT EXISTS` makes this safe for both the original, one-key
            # ledger and a database initialized from the current schema.
            conn.exec_driver_sql(
                "ALTER TABLE SCHEMA_MIGRATIONS ADD COLUMN IF NOT EXISTS SOURCE VARCHAR"
            )
            conn.exec_driver_sql(
                "ALTER TABLE SCHEMA_MIGRATIONS ADD COLUMN IF NOT EXISTS CHECKSUM VARCHAR(64)"
            )
            conn.exec_driver_sql(
                "UPDATE SCHEMA_MIGRATIONS SET SOURCE = 'LEGACY' WHERE SOURCE IS NULL"
            )
            conn.exec_driver_sql(
                "ALTER TABLE SCHEMA_MIGRATIONS ALTER COLUMN SOURCE SET DEFAULT 'LEGACY'"
            )
            conn.exec_driver_sql("ALTER TABLE SCHEMA_MIGRATIONS ALTER COLUMN SOURCE SET NOT NULL")
            _ensure_composite_primary_key(conn)
    except Exception as exc:
        raise MigrationError(f"failed preparing SCHEMA_MIGRATIONS: {exc}") from exc


def _ensure_composite_primary_key(conn: Connection) -> None:
    """Replace the legacy VERSION-only primary key when it is still present."""
    rows = conn.execute(
        text(
            "SELECT kcu.constraint_name, kcu.column_name "
            "FROM information_schema.table_constraints AS tc "
            "JOIN information_schema.key_column_usage AS kcu "
            "ON tc.constraint_name = kcu.constraint_name "
            "AND tc.table_schema = kcu.table_schema "
            "WHERE tc.table_schema = current_schema() "
            "AND tc.table_name = 'schema_migrations' "
            "AND tc.constraint_type = 'PRIMARY KEY' "
            "ORDER BY kcu.ordinal_position"
        )
    ).all()
    columns = tuple(row[1].lower() for row in rows)
    if columns == ("source", "version"):
        return
    if rows:
        constraint_name = str(rows[0][0])
        conn.exec_driver_sql(
            "ALTER TABLE SCHEMA_MIGRATIONS DROP CONSTRAINT " f"{_quote_identifier(constraint_name)}"
        )
    conn.exec_driver_sql(
        "ALTER TABLE SCHEMA_MIGRATIONS "
        "ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (SOURCE, VERSION)"
    )


def _quote_identifier(identifier: str) -> str:
    """Quote a database identifier obtained from PostgreSQL's own catalog."""
    return '"' + identifier.replace('"', '""') + '"'


def _read_migration_files(source: str, directory: Path) -> list[MigrationFile]:
    """Read immutable migration payloads and calculate their content hashes."""
    migrations: list[MigrationFile] = []
    for path in sorted(p for p in directory.glob("*.sql") if p.is_file()):
        try:
            payload = path.read_bytes()
            sql = payload.decode("utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            raise MigrationError(f"could not read migration {str(path)!r}: {exc}") from exc
        migrations.append(
            MigrationFile(
                source=source,
                path=path,
                sql=sql,
                checksum=hashlib.sha256(payload).hexdigest(),
            )
        )
    return migrations


def _migration_streams(
    explicit: Path | str | None,
    dialect: str = "postgresql",
) -> list[tuple[str, Path, list[MigrationFile]]]:
    """Return packaged engine migrations followed by the optional project stream."""
    package_dir = packaged_migrations_dir(dialect)
    if not package_dir.is_dir():
        raise MigrationError(
            f"packaged migrations directory {str(package_dir)!r} does not exist — "
            "the installed etl-craft package is missing its sql data files"
        )
    streams = [
        (
            ENGINE_MIGRATION_SOURCE,
            package_dir,
            _read_migration_files(ENGINE_MIGRATION_SOURCE, package_dir),
        )
    ]

    project_dir = resolve_project_migrations_dir(explicit)
    if project_dir is None:
        return streams
    if not project_dir.is_dir():
        raise MigrationError(
            f"migrations directory {str(project_dir)!r} does not exist — pass "
            f"--migrations-dir, set ${MIGRATIONS_DIR_ENV_VAR}, or run from a directory "
            "containing sql/migrations/"
        )
    # An explicit packaged directory used to mean "apply the package".  Keep
    # that useful spelling without treating the same files as customer code.
    if project_dir.resolve() == package_dir.resolve():
        return streams
    streams.append(
        (
            PROJECT_MIGRATION_SOURCE,
            project_dir,
            _read_migration_files(PROJECT_MIGRATION_SOURCE, project_dir),
        )
    )
    return streams


def _load_ledger(conn: Connection) -> dict[tuple[str, str], str | None]:
    """Read source-scoped migration records from the upgraded ledger."""
    rows = conn.execute(text("SELECT SOURCE, VERSION, CHECKSUM FROM SCHEMA_MIGRATIONS")).all()
    return {(str(row[0]), str(row[1])): row[2] for row in rows}


def _stream_maps(
    streams: list[tuple[str, Path, list[MigrationFile]]],
) -> tuple[dict[str, dict[str, MigrationFile]], dict[str, Path]]:
    """Index each stream by filename and retain its directory for diagnostics."""
    migration_map: dict[str, dict[str, MigrationFile]] = {}
    directories: dict[str, Path] = {}
    for source, directory, migrations in streams:
        migration_map[source] = {migration.version: migration for migration in migrations}
        directories[source] = directory
    return migration_map, directories


def _validate_recorded_migrations(
    ledger: dict[tuple[str, str], str | None],
    migration_map: dict[str, dict[str, MigrationFile]],
    directories: dict[str, Path],
) -> None:
    """Reject removed or changed files before any migration body can run."""
    valid_sources = {ENGINE_MIGRATION_SOURCE, PROJECT_MIGRATION_SOURCE, LEGACY_MIGRATION_SOURCE}
    unknown_sources = {source for source, _version in ledger} - valid_sources
    if unknown_sources:
        rendered = ", ".join(sorted(repr(source) for source in unknown_sources))
        raise MigrationError(f"SCHEMA_MIGRATIONS contains unknown migration source(s): {rendered}")

    for (source, version), recorded_checksum in ledger.items():
        if source == LEGACY_MIGRATION_SOURCE:
            continue
        migration = migration_map.get(source, {}).get(version)
        if migration is None:
            if source == PROJECT_MIGRATION_SOURCE and source not in directories:
                raise MigrationError(
                    f"applied project migration {version!r} cannot be verified because no "
                    "project migrations directory is configured — restore the project directory "
                    f"or pass --migrations-dir (or ${MIGRATIONS_DIR_ENV_VAR})"
                )
            directory = directories.get(source)
            raise MigrationError(
                f"applied {source.lower()} migration {version!r} is missing from "
                f"{str(directory)!r}; restore the original immutable migration file"
            )
        if recorded_checksum is None:
            raise MigrationError(
                f"applied {source.lower()} migration {version!r} has no checksum — "
                "restore it from the original release or classify it as a legacy migration"
            )
        if recorded_checksum != migration.checksum:
            raise MigrationError(
                f"checksum mismatch for applied {source.lower()} migration {version!r}: "
                "migration files are immutable; restore the original file and add a new migration"
            )


def _legacy_adoption_plan(
    ledger: dict[tuple[str, str], str | None],
    migration_map: dict[str, dict[str, MigrationFile]],
) -> list[MigrationFile]:
    """Choose only unambiguous legacy rows to promote into a named stream."""
    plan: list[MigrationFile] = []
    for (source, version), legacy_checksum in sorted(ledger.items()):
        if source != LEGACY_MIGRATION_SOURCE:
            continue
        candidates = [
            candidate_source
            for candidate_source in (ENGINE_MIGRATION_SOURCE, PROJECT_MIGRATION_SOURCE)
            if version in migration_map.get(candidate_source, {})
        ]
        if not candidates:
            continue
        if len(candidates) > 1:
            raise MigrationError(
                f"legacy migration {version!r} exists in both ENGINE and PROJECT streams; "
                "rename the project migration and resolve the legacy ledger row before retrying"
            )
        candidate_source = candidates[0]
        if (
            candidate_source == ENGINE_MIGRATION_SOURCE
            and version not in _PRE_STREAM_ENGINE_VERSIONS
        ):
            raise MigrationError(
                f"legacy migration {version!r} cannot be safely classified as an engine "
                "migration; restore the project migration directory and classify or rename it"
            )
        migration = migration_map[candidate_source][version]
        if (candidate_source, version) in ledger:
            raise MigrationError(
                f"migration {version!r} has both a legacy record and a {candidate_source} "
                "record; resolve the duplicate ledger entries before retrying"
            )
        if legacy_checksum is not None and legacy_checksum != migration.checksum:
            raise MigrationError(
                f"checksum mismatch for legacy migration {version!r}; restore the original "
                "file before assigning it to a migration stream"
            )
        plan.append(migration)
    return plan


def _adopt_legacy_records(
    conn: Connection,
    ledger: dict[tuple[str, str], str | None],
    plan: list[MigrationFile],
) -> None:
    """Move proven legacy records into their stream and seed their checksum."""
    for migration in plan:
        conn.execute(
            text(
                "UPDATE SCHEMA_MIGRATIONS "
                "SET SOURCE = :source, CHECKSUM = :checksum "
                "WHERE SOURCE = :legacy_source AND VERSION = :version"
            ),
            {
                "source": migration.source,
                "checksum": migration.checksum,
                "legacy_source": LEGACY_MIGRATION_SOURCE,
                "version": migration.version,
            },
        )
        del ledger[(LEGACY_MIGRATION_SOURCE, migration.version)]
        ledger[(migration.source, migration.version)] = migration.checksum


def _record_migration(conn: Connection, migration: MigrationFile) -> None:
    """Write one successful migration's source, version, and immutable hash."""
    conn.execute(
        text(
            "INSERT INTO SCHEMA_MIGRATIONS (SOURCE, VERSION, CHECKSUM) "
            "VALUES (:source, :version, :checksum)"
        ),
        {
            "source": migration.source,
            "version": migration.version,
            "checksum": migration.checksum,
        },
    )


def _apply_migration(engine: Engine, migration: MigrationFile) -> None:
    """Execute a single frozen payload and record it in one transaction."""
    try:
        with engine.begin() as conn:
            begin_ddl_transaction(conn)
            for statement in statements_for(engine, migration.sql):
                # These are complete, trusted DDL statements from the frozen
                # payload above, with no bind parameters.  `text()` would
                # re-parse ordinary literals such as ':name' as parameters.
                conn.exec_driver_sql(statement)
            _record_migration(conn, migration)
    except Exception as exc:
        raise MigrationError(f"{migration.version} failed to apply: {exc}") from exc


def mark_packaged_migrations_applied(engine: Engine) -> list[str]:
    """Record packaged migrations after `init-db`, without executing their bodies.

    This is intentionally limited to the packaged engine stream.  A project's
    migrations are not reflected in schema.sql and therefore remain pending
    after a fresh install.  Existing engine records are checksum-verified;
    changed package files never get silently accepted by ``ON CONFLICT``.
    """
    package_dir = packaged_migrations_dir(engine.dialect.name)
    if not package_dir.is_dir():
        return []
    migrations = _read_migration_files(ENGINE_MIGRATION_SOURCE, package_dir)
    if not migrations:
        return []
    # engine_lock holds its advisory lock on a connection of its own, never
    # the ledger's: a ledger SELECT on the lock's connection would retain an
    # ACCESS SHARE lock until the end of its transaction and make 0004's
    # ALTER TABLE block behind itself. On SQLite it is a file lock instead.
    with engine_lock(engine, _ADVISORY_LOCK_KEY, "migrate"):
        _ensure_bookkeeping_table(engine)
        migration_map = {ENGINE_MIGRATION_SOURCE: {m.version: m for m in migrations}}
        directories = {ENGINE_MIGRATION_SOURCE: package_dir}
        with engine.begin() as ledger_conn:
            ledger = _load_ledger(ledger_conn)
            _validate_recorded_migrations(ledger, migration_map, directories)
            plan = _legacy_adoption_plan(ledger, migration_map)
            _adopt_legacy_records(ledger_conn, ledger, plan)
            for migration in migrations:
                if (migration.source, migration.version) not in ledger:
                    _record_migration(ledger_conn, migration)
                    ledger[(migration.source, migration.version)] = migration.checksum
    return [migration.version for migration in migrations]


def apply_pending_migrations(engine: Engine, migrations_dir: Path | str | None = None) -> list[str]:
    """Apply pending ENGINE migrations, then the optional PROJECT stream.

    A project directory no longer replaces the package directory.  Each stream
    has its own ledger namespace, so matching filenames are legal after the
    streams have been established.  Before any SQL executes, every previously
    applied current-stream file is checked for presence and SHA-256 integrity.
    """
    streams = _migration_streams(migrations_dir, engine.dialect.name)
    migration_map, directories = _stream_maps(streams)

    applied: list[str] = []
    # One advisory lock covers validation, legacy adoption, and both streams.
    # Per-file transactions still ensure a failed file rolls back its own DDL
    # and ledger record without preventing earlier successful files from being
    # durably recorded.
    with engine_lock(engine, _ADVISORY_LOCK_KEY, "migrate"):
        _ensure_bookkeeping_table(engine)
        with engine.begin() as ledger_conn:
            ledger = _load_ledger(ledger_conn)
            _validate_recorded_migrations(ledger, migration_map, directories)
            plan = _legacy_adoption_plan(ledger, migration_map)
            _adopt_legacy_records(ledger_conn, ledger, plan)

        for _source, _directory, migrations in streams:
            for migration in migrations:
                if (migration.source, migration.version) in ledger:
                    continue
                _apply_migration(engine, migration)
                ledger[(migration.source, migration.version)] = migration.checksum
                applied.append(migration.version)
    return applied
