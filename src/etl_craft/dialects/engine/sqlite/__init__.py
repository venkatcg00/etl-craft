"""SQLite: the default Engine DB, one file and nothing to install.

It suits local development and single-machine deployments. PostgreSQL remains the production
recommendation: a SQLite file cannot be reached by workers on other hosts, and every write is
serialized.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Connection, Engine

from etl_craft.config.auth import engine_for_jdbc_url
from etl_craft.core.enums import AuthMode
from etl_craft.core.errors import ConfigurationError, LockTimeoutError
from etl_craft.core.filelock import file_lock
from etl_craft.core.text import is_only_comments
from etl_craft.dialects.engine.base import EngineDialect

if TYPE_CHECKING:
    from etl_craft.config import ConnectionProfile, ConnectorConfig

JDBC_PREFIX = "jdbc:sqlite:"
MIN_SQLITE_VERSION = (3, 35, 0)
"""RETURNING needs SQLite 3.35; an older library is refused when connecting."""

BUSY_TIMEOUT_MS = 60_000
"""How long a writer waits for another process's write; Engine DB writes are short."""


class SqliteEngineDialect(EngineDialect):
    """SQLite: one file, file locks, DDL made transactional explicitly."""

    spec = engine_for_jdbc_url(JDBC_PREFIX)
    directory = Path(__file__).parent

    def build_engine(
        self, config: ConnectorConfig, profile: ConnectionProfile, **engine_kwargs: Any
    ) -> Engine:
        """Build the engine for a ``jdbc:sqlite:`` profile; there is nothing to authenticate."""
        if profile.auth_mode != AuthMode.NONE:
            raise ConfigurationError(
                f"profile {profile.name!r}: a SQLite Engine DB has nothing to authenticate, "
                f"so auth_mode must be 'none', got {profile.auth_mode!r}"
            )
        database = resolve_sqlite_path(profile.jdbc_url, config.config_path)
        return create_engine(
            URL.create("sqlite", database=database),
            creator=sqlite_creator(database),
            # The connection already returns datetimes; SQLAlchemy must not parse them again.
            native_datetime=True,
            **engine_kwargs,
        )

    def split_statements(self, sql_text: str) -> list[str]:
        """Split a script into statements, keeping trigger bodies whole.

        A ``;`` ends a statement only when SQLite's own ``complete_statement`` agrees, so the
        semicolons inside ``CREATE TRIGGER ... BEGIN ...; ...; END;`` do not.
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
            stmt.strip().rstrip(";").strip() for stmt in statements if not is_only_comments(stmt)
        ]

    def begin_ddl_transaction(self, conn: Connection) -> None:
        """Open the transaction Python's sqlite3 does not open before DDL.

        It opens one implicitly only before INSERT, UPDATE and DELETE, so DDL would otherwise
        commit statement by statement and a failed script would leave half its changes.
        """
        conn.exec_driver_sql("BEGIN IMMEDIATE")

    def duration_seconds_sql(self) -> str:
        """Return ``END_DATE - START_DATE`` in seconds; julianday reads the stored UTC text."""
        return "(julianday(END_DATE) - julianday(START_DATE)) * 86400.0"

    @contextmanager
    def lock(self, engine: Engine, key: int, name: str, wait_seconds: float = 0) -> Iterator[None]:
        """Hold an OS file lock beside the database file.

        Every process that could contend runs on the machine holding the file, and the OS
        releases the lock if its holder dies. The lock file's name plays the part of ``key``.
        """
        database = engine.url.database
        if not database:
            raise LockTimeoutError(f"cannot lock {name}: the SQLite Engine DB has no file path")
        with ExitStack() as held:
            try:
                held.enter_context(file_lock(f"{database}.{name}.lock", wait_seconds))
            except LockTimeoutError as error:
                raise LockTimeoutError(
                    f"timed out after {wait_seconds:g}s waiting for {name}"
                ) from error
            yield


def resolve_sqlite_path(jdbc_url: str, config_path: Path | None = None) -> str:
    """Return the database file a ``jdbc:sqlite:`` URL names.

    A relative path resolves against the directory holding ``craft-connector.yml``, not the
    working directory, so every command and task process opens the same file wherever it
    starts. An in-memory database is refused: each task runs in its own process and would see
    an empty one.
    """
    raw = jdbc_url.strip()[len(JDBC_PREFIX) :]
    if not raw or raw == ":memory:":
        raise ConfigurationError(
            "an in-memory SQLite Engine DB cannot work: every task runs in its own process, "
            "so each would see an empty database. Give jdbc:sqlite: a file path."
        )
    path = Path(raw).expanduser()
    if not path.is_absolute() and config_path is not None:
        path = Path(config_path).resolve().parent / path
    return str(path)


def _adapt_datetime(value: datetime) -> str:
    # One UTC text format for every timestamp, so plain text comparison orders them. Naive
    # values are the engine's own UTC instants.
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat(sep=" ", timespec="microseconds")


def _convert_timestamp(value: bytes) -> datetime:
    parsed = datetime.fromisoformat(value.decode())
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def sqlite_creator(database: str) -> Callable[[], Any]:
    """Return a function that opens one configured connection to ``database``.

    Each connection uses WAL, so readers proceed while one process writes; waits up to
    ``BUSY_TIMEOUT_MS`` for another writer; and enforces foreign keys, which SQLite leaves off
    by default. ``TIMESTAMP`` columns read back as timezone-aware datetimes.
    """
    if sqlite3.sqlite_version_info < MIN_SQLITE_VERSION:
        raise ConfigurationError(
            f"the SQLite library is {sqlite3.sqlite_version}; the Engine DB needs "
            f"{'.'.join(map(str, MIN_SQLITE_VERSION))} or newer"
        )
    # Process-wide and idempotent registrations.
    sqlite3.register_adapter(datetime, _adapt_datetime)
    sqlite3.register_converter("TIMESTAMP", _convert_timestamp)

    def connect() -> Any:
        folder = Path(database).parent
        try:
            folder.mkdir(parents=True, exist_ok=True)
        except OSError as error:
            raise ConfigurationError(
                f"the SQLite Engine DB {database} cannot be created: its folder {folder} could "
                f"not be made ({error.strerror}); point jdbc:sqlite: at a writable path"
            ) from error
        conn = sqlite3.connect(
            database,
            timeout=BUSY_TIMEOUT_MS / 1000,
            detect_types=sqlite3.PARSE_DECLTYPES,
            # The pool hands connections between threads; each is used by one at a time.
            check_same_thread=False,
        )
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    return connect
