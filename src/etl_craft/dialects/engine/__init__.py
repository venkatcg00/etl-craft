"""Engine DB dialects: PostgreSQL (recommended for production) and SQLite (the default)."""

from __future__ import annotations

from collections.abc import Iterator
from functools import cache
from typing import TYPE_CHECKING, Any

from sqlalchemy.engine import Connection, Engine

from etl_craft.config.auth import engine_for_jdbc_url
from etl_craft.core.errors import ConfigurationError
from etl_craft.dialects.engine.base import EngineDialect

if TYPE_CHECKING:
    from etl_craft.config import ConnectionProfile, ConnectorConfig

__all__ = [
    "EngineDialect",
    "all_dialects",
    "build_engine",
    "for_engine",
    "for_jdbc_url",
    "for_name",
]


@cache
def _registry() -> dict[str, EngineDialect]:
    from etl_craft.dialects.engine.postgres import PostgresEngineDialect
    from etl_craft.dialects.engine.sqlite import SqliteEngineDialect

    dialects: list[EngineDialect] = [PostgresEngineDialect(), SqliteEngineDialect()]
    return {dialect.name: dialect for dialect in dialects}


def all_dialects() -> Iterator[EngineDialect]:
    """Yield every Engine DB dialect."""
    yield from _registry().values()


def for_name(dialect_name: str) -> EngineDialect:
    """Return the dialect for a SQLAlchemy name such as ``postgresql+psycopg``."""
    dialect = _registry().get(dialect_name.split("+", 1)[0])
    if dialect is None:
        raise ConfigurationError(
            f"{dialect_name!r} is not a supported Engine DB — use PostgreSQL or SQLite"
        )
    return dialect


def for_jdbc_url(jdbc_url: str) -> EngineDialect:
    """Return the dialect a JDBC URL names; raises ``ConfigurationError`` for any other."""
    return for_name(engine_for_jdbc_url(jdbc_url).name)


def for_engine(engine: Engine | Connection) -> EngineDialect:
    """Return the dialect ``engine`` is connected to."""
    return for_name(engine.dialect.name)


def build_engine(
    config: ConnectorConfig, profile: ConnectionProfile | None = None, **engine_kwargs: Any
) -> Engine:
    """Build a SQLAlchemy engine for ``profile``, by default the active Engine profile."""
    profile = profile or config.engine.active
    return for_jdbc_url(profile.jdbc_url).build_engine(config, profile, **engine_kwargs)
