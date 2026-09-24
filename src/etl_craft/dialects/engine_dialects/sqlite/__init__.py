"""SQLite Engine DB -- the default: one file, nothing to install.

Owns ``schema.sql`` (a table-for-table translation of the PostgreSQL schema)
and its own ``migrations/`` stream. Suited to local development and
single-machine deployments; PostgreSQL remains the production recommendation
because a SQLite file cannot be reached by orchestrator workers on other hosts
and serializes every write.
"""

from __future__ import annotations

import sqlite3
import sys
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any

from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import URL, Connection, Engine

from etl_craft.dialects.engine_dialects import EngineDialect, LockTimeout
from etl_craft.dialects.engine_dialects.postgres import is_only_comments

if TYPE_CHECKING:
    from etl_craft.config import ConnectionProfile, ConnectorConfig

JDBC_PREFIX = "jdbc:sqlite:"
# RETURNING (used by runlog.py and friends) needs 3.35. Checked at connect time
# so an old system library fails with a sentence rather than a syntax error.
MIN_SQLITE_VERSION = (3, 35, 0)
# How long a writer waits for another process's write to finish. Engine DB
# writes are short (E2-80 removed the long ones), so this is only ever reached
# by something genuinely wedged.
SQLITE_BUSY_TIMEOUT_MS = 60_000

# How often a waiter re-tries a held file lock. Short: holders are tasks and
# migrations, and the wait is bounded by the caller anyway.
_FILE_LOCK_POLL_SECONDS = 0.1


class SqliteEngineDialect(EngineDialect):
    """SQLite: one file, file locks, DDL made transactional explicitly."""

    name = "sqlite"
    directory = Path(__file__).parent
    jdbc_prefix = JDBC_PREFIX
    auth_fields: dict[str, tuple[str, ...]] = {"none": ()}
    verified_auth_modes = frozenset({"none"})

    def build_engine(
        self, config: ConnectorConfig, profile: ConnectionProfile, **engine_kwargs: Any
    ) -> Engine:
        """Build the Engine for a `jdbc:sqlite:` profile -- no credentials, one file."""
        from etl_craft.db import ConnectionError_

        if profile.auth_mode != "none":
            raise ConnectionError_(
                f"profile {profile.name!r}: a SQLite Engine DB has nothing to authenticate, "
                f"so auth_mode must be 'none', got {profile.auth_mode!r}"
            )
        database = resolve_sqlite_path(profile.jdbc_url, config.config_path)
        return create_engine(
            URL.create("sqlite", database=database),
            creator=_sqlite_creator(database),
            # detect_types already returns datetimes; without this SQLAlchemy
            # would re-parse them as strings for DateTime-typed columns
            # (cloning.py's reflected tables).
            native_datetime=True,
            **engine_kwargs,
        )

    def split_statements(self, sql_text: str) -> list[str]:
        """Split a script into statements, keeping trigger bodies whole.

        [ADDITION, 2026-09-24] The Postgres splitter knows Postgres's quoting,
        not SQLite's `CREATE TRIGGER ... BEGIN ...; ...; END;`, whose inner
        semicolons it would cut at. SQLite ships the answer itself:
        `complete_statement` reports whether text so far forms whole
        statements, so a `;` only ends a statement once SQLite agrees it does.
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
            if stmt.strip() and not is_only_comments(stmt)
        ]

    def begin_ddl_transaction(self, conn: Connection) -> None:
        """Open the transaction Python's sqlite3 would not open before DDL.

        It opens one implicitly only before INSERT/UPDATE/DELETE, so DDL would
        otherwise autocommit statement by statement and a failed migration
        would leave half its changes behind.
        """
        conn.exec_driver_sql("BEGIN IMMEDIATE")

    def duration_seconds_sql(self) -> str:
        """Return END_DATE - START_DATE in seconds; julianday() reads the stored UTC text."""
        return "(julianday(END_DATE) - julianday(START_DATE)) * 86400.0"

    def existing_tables(self, engine: Engine, names: tuple[str, ...]) -> list[str]:
        """Return which of `names` exist; SQLite's catalog is sqlite_master."""
        present = {name.lower(): name for name in inspect(engine).get_table_names()}
        return sorted(present[name] for name in names if name in present)

    @contextmanager
    def lock(self, engine: Engine, key: int, name: str, wait_seconds: int = 0) -> Iterator[None]:
        """Hold an OS file lock beside the database file.

        A faithful substitute for an advisory lock, for the same reason SQLite
        is acceptable as an Engine DB at all: every process that could contend
        is on the machine holding the file. It queues rather than spinning on
        the protected resource, and the OS releases it if its holder dies.
        """
        del key  # Postgres's advisory-lock key; the lock file name plays that role here.
        database = engine.url.database
        if not database:
            raise LockTimeout(f"cannot lock {name}: the SQLite Engine DB has no file path")
        with _file_lock(f"{database}.{name}.lock", wait_seconds):
            yield

    def ensure_migration_ledger(self, engine: Engine) -> None:
        """Create SCHEMA_MIGRATIONS if absent.

        A SQLite Engine DB is never older than the (SOURCE, VERSION, CHECKSUM)
        ledger -- its schema was written after it -- so there is no legacy
        shape to upgrade, only a missing table to create.
        """
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

    def prepare_fork(self, engine: Engine) -> None:
        """Close idle pooled connections: SQLite forbids carrying one across fork().

        Its per-process lock and mutex state would be copied into the child,
        which then opens the same file and can deadlock on it. Found as an
        intermittent hang of a forked task child, never seen on Postgres.
        """
        engine.dispose()


@contextmanager
def _file_lock(path: str, wait_seconds: int) -> Iterator[None]:
    deadline = time.monotonic() + wait_seconds if wait_seconds else None
    handle = open(path, "a+b")  # noqa: SIM115 - closed in the finally below
    try:
        while not _try_lock(handle):
            if deadline is not None and time.monotonic() >= deadline:
                raise LockTimeout(f"timed out after {wait_seconds}s waiting for {path}")
            time.sleep(_FILE_LOCK_POLL_SECONDS)
        try:
            yield
        finally:
            _unlock(handle)
    finally:
        handle.close()


if sys.platform == "win32":  # pragma: no cover - exercised on Windows only
    import msvcrt

    def _try_lock(handle: IO[bytes]) -> bool:
        handle.seek(0)
        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            return False
        return True

    def _unlock(handle: IO[bytes]) -> None:
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)

else:
    import fcntl

    def _try_lock(handle: IO[bytes]) -> bool:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        return True

    def _unlock(handle: IO[bytes]) -> None:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def resolve_sqlite_path(jdbc_url: str, config_path: Path | None = None) -> str:
    """Return the database path a `jdbc:sqlite:` URL names.

    [CHOICE] A relative path resolves against the directory holding
    craft-connector.yml, not the current directory. The config itself is found
    by an upward search (E2-06) and every spawned task re-reads it via
    `--config`, so resolving against the cwd would let two invocations from
    different directories silently open two different Engine DBs -- two
    unrelated run histories, with nothing erroring.
    """
    from etl_craft.db import ConnectionError_

    raw = jdbc_url.strip()[len(JDBC_PREFIX) :]
    if not raw or raw == ":memory:":
        raise ConnectionError_(
            "an in-memory SQLite Engine DB cannot work: every task runs in its own process, "
            "so each would see an empty database. Give jdbc:sqlite: a file path."
        )
    path = Path(raw).expanduser()
    if not path.is_absolute() and config_path is not None:
        path = Path(config_path).resolve().parent / path
    return str(path)


def _adapt_datetime(value: datetime) -> str:
    # One text format for every timestamp, always UTC, so SQLite's plain text
    # comparison orders them correctly -- the tracker's "newer than" checks
    # and every ORDER BY START_DATE depend on that. Naive values are the
    # engine's own UTC instants.
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat(sep=" ", timespec="microseconds")


def _convert_timestamp(value: bytes) -> datetime:
    parsed = datetime.fromisoformat(value.decode())
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _sqlite_creator(database: str) -> Callable[[], Any]:
    from etl_craft.db import ConnectionError_

    if sqlite3.sqlite_version_info < MIN_SQLITE_VERSION:
        raise ConnectionError_(
            f"the SQLite library is {sqlite3.sqlite_version}; the Engine DB needs "
            f"{'.'.join(map(str, MIN_SQLITE_VERSION))} or newer"
        )
    # Process-wide registrations, and idempotent. TIMESTAMP is the declared
    # type every Engine DB timestamp column carries in this dialect's
    # schema.sql, so reads come back as aware datetimes exactly as they do
    # from Postgres.
    sqlite3.register_adapter(datetime, _adapt_datetime)
    sqlite3.register_converter("TIMESTAMP", _convert_timestamp)

    def _connect() -> Any:
        Path(database).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(
            database,
            timeout=SQLITE_BUSY_TIMEOUT_MS / 1000,
            detect_types=sqlite3.PARSE_DECLTYPES,
            # Each pooled connection stays in the thread that checked it out;
            # the pool itself hands them between threads (business_rules.py's
            # parallel waves), which the stdlib check would otherwise refuse.
            check_same_thread=False,
        )
        # WAL lets readers proceed while one process writes, which is the
        # whole concurrency story for parallel task subprocesses. Foreign keys
        # are off by default in SQLite and every CFG_/AUD_ FK depends on them.
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    return _connect
