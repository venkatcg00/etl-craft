"""Engine DB dialects: PostgreSQL (recommended for production) and SQLite (the default).

Each dialect is a directory holding its module, its full ``schema.sql`` (what
``init-db`` applies to an empty database) and its ``migrations/`` stream (what
``migrate`` applies to an existing one). Callers never branch on the Engine DB
themselves: they ask ``for_engine(engine)`` and call the primitive they need.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import AbstractContextManager
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy.engine import Connection, Engine

if TYPE_CHECKING:
    from etl_craft.config import ConnectionProfile, ConnectorConfig


class LockTimeout(Exception):
    """Raised when a cross-process lock could not be taken within the caller's bound."""


class EngineDialect:
    """What differs between Engine DBs. Every method has one caller-visible meaning."""

    #: SQLAlchemy's dialect name for this database.
    name: str = ""
    #: The directory holding this dialect's schema.sql and migrations/.
    directory: Path = Path()
    #: The JDBC URL prefix that selects this dialect.
    jdbc_prefix: str = ""
    #: auth_mode -> the profile fields it needs (beyond jdbc_url). Its keys are
    #: the auth modes this Engine DB accepts.
    auth_fields: dict[str, tuple[str, ...]] = {}
    #: The auth modes run against a real server in this project. The others are
    #: built from the vendor's documentation and untested: they can be used,
    #: but success is not guaranteed (`doctor` says so).
    verified_auth_modes: frozenset[str] = frozenset()

    @property
    def auth_modes(self) -> frozenset[str]:
        """Return the auth modes this Engine DB accepts."""
        return frozenset(self.auth_fields)

    def build_engine(
        self, config: ConnectorConfig, profile: ConnectionProfile, **engine_kwargs: Any
    ) -> Engine:
        """Build a SQLAlchemy Engine for `profile`."""
        raise NotImplementedError

    def schema_path(self) -> Path:
        """Return the full schema a fresh install applies."""
        return self.directory / "schema.sql"

    def migrations_dir(self) -> Path:
        """Return the packaged ENGINE migration stream."""
        return self.directory / "migrations"

    def split_statements(self, sql_text: str) -> list[str]:
        """Split a script into statements this database will accept one at a time."""
        raise NotImplementedError

    def begin_ddl_transaction(self, conn: Connection) -> None:
        """Make DDL on `conn` part of the surrounding transaction."""
        return None

    def duration_seconds_sql(self) -> str:
        """Return SQL for END_DATE - START_DATE, in seconds."""
        raise NotImplementedError

    def existing_tables(self, engine: Engine, names: tuple[str, ...]) -> list[str]:
        """Return which of `names` (lower-case) already exist as tables."""
        raise NotImplementedError

    def lock(
        self, engine: Engine, key: int, name: str, wait_seconds: int = 0
    ) -> AbstractContextManager[None]:
        """Hold a named cross-process lock; `wait_seconds` 0 waits indefinitely."""
        raise NotImplementedError

    def ensure_migration_ledger(self, engine: Engine) -> None:
        """Create SCHEMA_MIGRATIONS if absent, upgrading an older shape where one exists."""
        raise NotImplementedError

    def prepare_fork(self, engine: Engine) -> None:
        """Make `engine` safe to fork: nothing to do unless the database says otherwise."""
        return None


@cache
def _registry() -> dict[str, EngineDialect]:
    # One shared instance per dialect. Imported here, not at module level:
    # each dialect module imports this one for the base class.
    from etl_craft.dialects.engine_dialects.postgres import PostgresEngineDialect
    from etl_craft.dialects.engine_dialects.sqlite import SqliteEngineDialect

    dialects: list[EngineDialect] = [PostgresEngineDialect(), SqliteEngineDialect()]
    return {dialect.name: dialect for dialect in dialects}


def for_name(dialect_name: str) -> EngineDialect:
    """Return the Engine DB dialect for a SQLAlchemy dialect name."""
    dialect = _registry().get(dialect_name.split("+", 1)[0])
    if dialect is None:
        raise ValueError(
            f"{dialect_name!r} is not a supported Engine DB -- use PostgreSQL or SQLite"
        )
    return dialect


def for_jdbc_url(jdbc_url: str) -> EngineDialect:
    """Return the Engine DB dialect a JDBC URL names."""
    lowered = jdbc_url.strip().lower()
    for dialect in _registry().values():
        if lowered.startswith(dialect.jdbc_prefix):
            return dialect
    raise ValueError(
        f"{jdbc_url!r} is not a supported Engine DB URL -- use jdbc:postgresql://... "
        "(recommended for production) or jdbc:sqlite:<path>"
    )


def for_engine(engine: Engine | Connection) -> EngineDialect:
    """Return the Engine DB dialect `engine` is connected to."""
    return for_name(engine.dialect.name)


def all_dialects() -> Iterator[EngineDialect]:
    """Yield every supported Engine DB dialect."""
    yield from _registry().values()
