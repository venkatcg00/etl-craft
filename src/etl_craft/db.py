"""Build a SQLAlchemy Engine for the Postgres Engine DB from a craft-connector.yml profile."""

# Per CLAUDE.md "Connections & auth": auth is a small registry of functions
# keyed by each profile's auth_mode, handed to `create_engine(creator=...)`.
# Every mode goes through `creator` uniformly here (not just token/sso) so
# there's one connection-construction path regardless of auth_mode.
#
# [ADDITION] `token` and `sso` mint/refresh short-lived credentials per
# CLAUDE.md, but the concrete provider (which cloud, which SDK call) isn't
# specified anywhere — that's necessarily team-specific. Left as a clearly
# marked extension point (NotImplementedError) rather than guessed at.

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any
from urllib.parse import parse_qsl

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from etl_craft.config import ConfigError, ConnectionProfile, ConnectorConfig, resolve_secret

# [DEVIATION, 2026-09-20, E2-10] The query string is captured now, not
# discarded. This pattern had no `query` group and stopped the database
# capture at "?", so `jdbc:postgresql://host/db?sslmode=require` connected
# **without** TLS, silently. warehouse.py's own translator parsed and
# forwarded query parameters all along, which made the Engine DB — the one
# that is always Postgres and always required — the weaker of the two.
_JDBC_POSTGRES_RE = re.compile(
    r"^jdbc:postgresql://(?P<host>[^:/]+)(:(?P<port>\d+))?/(?P<database>[^?]+)"
    r"(\?(?P<query>.*))?$"
)

DEFAULT_PORT = 5432

# Comfortably under a typical short-lived credential's lifetime, per
# CLAUDE.md's guidance for token/sso profiles; unused until those modes are
# implemented, but declared here so the setting has one obvious home.
TOKEN_POOL_RECYCLE_SECONDS = 15 * 60


class ConnectionError_(ConfigError):
    """Raised when a JDBC URL or auth_mode can't be turned into a connection."""


def parse_jdbc_postgres(jdbc_url: str) -> dict[str, Any]:
    """Split a `jdbc:postgresql://host[:port]/database` URL into its parts."""
    match = _JDBC_POSTGRES_RE.match(jdbc_url)
    if not match:
        raise ConnectionError_(f"not a recognized jdbc:postgresql:// URL: {jdbc_url!r}")
    port = int(match["port"]) if match["port"] else DEFAULT_PORT
    query = dict(parse_qsl(match["query"])) if match["query"] else {}
    return {
        "host": match["host"],
        "port": port,
        "database": match["database"],
        "query": query,
    }


def _password_creator(profile: ConnectionProfile, secret: str) -> Callable[[], Any]:
    parts = parse_jdbc_postgres(profile.jdbc_url)

    def _connect() -> Any:
        import psycopg

        return psycopg.connect(
            host=parts["host"],
            port=parts["port"],
            dbname=parts["database"],
            user=profile.user,
            password=secret,
            # Forwarded, not dropped: sslmode and friends are part of the URL
            # a team wrote down, and silently ignoring sslmode=require is
            # worse than failing on it (E2-10).
            **parts["query"],
        )

    return _connect


def _key_file_creator(profile: ConnectionProfile, secret: str) -> Callable[[], Any]:
    parts = parse_jdbc_postgres(profile.jdbc_url)
    key_file = profile.extra.get("key_file")
    if not key_file:
        raise ConnectionError_(
            f"profile {profile.name!r}: auth_mode=key_file requires a key_file path in extra"
        )

    def _connect() -> Any:
        import psycopg

        return psycopg.connect(
            host=parts["host"],
            port=parts["port"],
            dbname=parts["database"],
            user=profile.user,
            sslkey=key_file,
            # str, not bytes: psycopg's own signature is str | int | None, and
            # libpq treats it as text. Encoding it was accepted at runtime but
            # wrong by the driver's contract (caught by mypy, E2-29).
            sslpassword=secret if secret else None,
            **parts["query"],
        )

    return _connect


def _token_creator(profile: ConnectionProfile, secret: str) -> Callable[[], Any]:
    raise NotImplementedError(
        "auth_mode='token' has no concrete implementation yet — the credential-minting "
        "provider (which cloud/SDK) is team-specific and unspecified in CLAUDE.md."
    )


def _sso_creator(profile: ConnectionProfile, secret: str) -> Callable[[], Any]:
    raise NotImplementedError(
        "auth_mode='sso' has no concrete implementation yet — the credential-minting "
        "provider is team-specific and unspecified in CLAUDE.md."
    )


AUTH_REGISTRY: dict[str, Callable[[ConnectionProfile, str], Callable[[], Any]]] = {
    "password": _password_creator,
    "key_file": _key_file_creator,
    "token": _token_creator,
    "sso": _sso_creator,
}


def build_engine(
    config: ConnectorConfig, profile: ConnectionProfile | None = None, **engine_kwargs: Any
) -> Engine:
    """Build a SQLAlchemy Engine for `profile` (default: config.postgres.active)."""
    profile = profile or config.postgres.active
    creator_factory = AUTH_REGISTRY.get(profile.auth_mode)
    if creator_factory is None:
        raise ConnectionError_(f"unknown auth_mode: {profile.auth_mode!r}")
    secret = resolve_secret(config, profile)
    creator = creator_factory(profile, secret)
    engine_kwargs.setdefault("pool_pre_ping", True)
    return create_engine("postgresql+psycopg://", creator=creator, **engine_kwargs)
