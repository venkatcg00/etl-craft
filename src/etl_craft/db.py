"""Build a SQLAlchemy Engine for the Engine DB from a craft-connector.yml profile.

The Engine DB is PostgreSQL (recommended for production) or SQLite (the
default for local and single-machine use). Everything that differs between
the two -- connecting, locking, splitting a script, the schema itself -- lives
in ``dialects/engine_dialects/<name>/``; this module only picks the dialect a
profile's JDBC URL names and asks it for an Engine.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from sqlalchemy.engine import Engine

from etl_craft.config import ConfigError, ConnectionProfile, ConnectorConfig


class ConnectionError_(ConfigError):
    """Raised when a JDBC URL or auth_mode can't be turned into a connection."""


SQLITE_JDBC_PREFIX = "jdbc:sqlite:"


def is_sqlite_url(jdbc_url: str) -> bool:
    """Whether `jdbc_url` names a SQLite Engine DB."""
    return jdbc_url.strip().lower().startswith(SQLITE_JDBC_PREFIX)


def resolve_sqlite_path(jdbc_url: str, config_path: Path | None = None) -> str:
    """Return the file a `jdbc:sqlite:` URL names, relative paths beside the config."""
    from etl_craft.dialects.engine_dialects.sqlite import resolve_sqlite_path as resolve

    return resolve(jdbc_url, config_path)


def build_engine(
    config: ConnectorConfig, profile: ConnectionProfile | None = None, **engine_kwargs: Any
) -> Engine:
    """Build a SQLAlchemy Engine for `profile` (default: the active Engine profile)."""
    from etl_craft.dialects.engine_dialects import for_jdbc_url

    profile = profile or config.postgres.active
    try:
        dialect = for_jdbc_url(profile.jdbc_url)
    except ValueError as exc:
        raise ConnectionError_(str(exc)) from exc
    return dialect.build_engine(config, profile, **engine_kwargs)
