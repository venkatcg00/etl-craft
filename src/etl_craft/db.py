"""Build a SQLAlchemy Engine for the Engine DB from a craft-connector.yml profile.

[DEVIATION, 2026-09-24] The Engine DB is no longer "always Postgres". Per
explicit instruction ("implement sqlite as default engine and postgres as
recommended for production"), a `jdbc:sqlite:<path>` profile with
`auth_mode: none` is accepted too, and it is what `etl-craft setup` writes
when no ENGINE_JDBC_URL is given. Postgres stays the recommended production
Engine DB: SQLite is one file on one machine, so it cannot be shared by
orchestrator workers on other hosts, and it serializes every write.
"""

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
import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl

from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine

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


SQLITE_JDBC_PREFIX = "jdbc:sqlite:"
# What `etl-craft setup` writes when no ENGINE_JDBC_URL is given. Relative, so
# it resolves next to craft-connector.yml (see resolve_sqlite_path).
DEFAULT_SQLITE_JDBC_URL = "jdbc:sqlite:etl-craft-engine.db"
# RETURNING (used by runlog.py and friends) needs 3.35; ON CONFLICT ... DO
# UPDATE with a partial-index target needs 3.24. Checked at connect time so an
# old system library fails with a sentence rather than a syntax error.
MIN_SQLITE_VERSION = (3, 35, 0)
# How long a writer waits for another process's write to finish. Engine DB
# writes are short (E2-80 removed the long ones), so this is only ever
# reached by something genuinely wedged.
SQLITE_BUSY_TIMEOUT_MS = 60_000


def is_sqlite_url(jdbc_url: str) -> bool:
    """Whether `jdbc_url` names a SQLite Engine DB."""
    return jdbc_url.strip().lower().startswith(SQLITE_JDBC_PREFIX)


def is_sqlite_engine(engine: Engine) -> bool:
    """Whether `engine` is connected to a SQLite Engine DB."""
    return engine.dialect.name == "sqlite"


def resolve_sqlite_path(jdbc_url: str, config_path: Path | None = None) -> str:
    """Return the database path a `jdbc:sqlite:` URL names.

    [CHOICE] A relative path resolves against the directory holding
    craft-connector.yml, not the current directory. The config itself is found
    by an upward search (E2-06) and every spawned task re-reads it via
    `--config`, so resolving against the cwd would let two invocations from
    different directories silently open two different Engine DBs -- two
    unrelated run histories, with nothing erroring.
    """
    raw = jdbc_url.strip()[len(SQLITE_JDBC_PREFIX) :]
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
    if sqlite3.sqlite_version_info < MIN_SQLITE_VERSION:
        raise ConnectionError_(
            f"the SQLite library is {sqlite3.sqlite_version}; the Engine DB needs "
            f"{'.'.join(map(str, MIN_SQLITE_VERSION))} or newer"
        )
    # Process-wide registrations, and idempotent. TIMESTAMP is the declared
    # type every Engine DB timestamp column carries in schema_sqlite.sql, so
    # reads come back as aware datetimes exactly as they do from Postgres.
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
    if is_sqlite_url(profile.jdbc_url):
        return _build_sqlite_engine(config, profile, **engine_kwargs)
    creator_factory = AUTH_REGISTRY.get(profile.auth_mode)
    if creator_factory is None:
        raise ConnectionError_(f"unknown auth_mode: {profile.auth_mode!r}")
    secret = resolve_secret(config, profile)
    creator = creator_factory(profile, secret)
    engine_kwargs.setdefault("pool_pre_ping", True)
    if profile.auth_mode in {"token", "sso"}:  # pragma: no cover - modes not implemented yet
        # CLAUDE.md: for these, pool_recycle should sit comfortably under the
        # credential's real lifetime, so a checked-out connection always has
        # meaningful life left. TOKEN_POOL_RECYCLE_SECONDS was declared for
        # this and then never used (E2-35).
        engine_kwargs.setdefault("pool_recycle", TOKEN_POOL_RECYCLE_SECONDS)
    # [DEVIATION, 2026-09-20, E2-24] A real URL, minus the password. The blank
    # "postgresql+psycopg://" this replaced kept every secret out of a logged
    # engine URL — a good goal — but left `engine.url` empty, which has already
    # broken two real things: cloning's same-database guard and ClickHouse's
    # table-engine reflection. SQLAlchemy never logs a password it was not
    # given, so omitting just the password preserves the original goal while
    # making engine.url truthful.
    parts = parse_jdbc_postgres(profile.jdbc_url)
    url = URL.create(
        "postgresql+psycopg",
        username=profile.user,
        host=parts["host"],
        port=parts["port"],
        database=parts["database"],
        query=parts["query"],
    )
    return create_engine(url, creator=creator, **engine_kwargs)


def _build_sqlite_engine(
    config: ConnectorConfig, profile: ConnectionProfile, **engine_kwargs: Any
) -> Engine:
    """Build the Engine for a `jdbc:sqlite:` profile -- no credentials, one file."""
    if profile.auth_mode != "none":
        raise ConnectionError_(
            f"profile {profile.name!r}: a SQLite Engine DB has nothing to authenticate, so "
            f"auth_mode must be 'none', got {profile.auth_mode!r}"
        )
    database = resolve_sqlite_path(profile.jdbc_url, config.config_path)
    return create_engine(
        URL.create("sqlite", database=database),
        creator=_sqlite_creator(database),
        # detect_types above already returns datetimes; without this SQLAlchemy
        # would try to re-parse them as strings for DateTime-typed columns
        # (cloning.py's reflected tables).
        native_datetime=True,
        **engine_kwargs,
    )
