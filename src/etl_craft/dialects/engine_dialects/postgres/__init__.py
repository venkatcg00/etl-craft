"""PostgreSQL Engine DB -- the recommended production Engine DB.

Owns ``schema.sql`` (the authoritative full definition, with its trigger
functions), ``schema_test.sql`` (its self-asserting constraint tests) and the
``migrations/`` stream every already-deployed Postgres Engine DB is carried
forward with.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qsl

from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import URL, Connection, Engine
from sqlalchemy.exc import OperationalError

from etl_craft.dialects.engine_dialects import EngineDialect, LockTimeout

if TYPE_CHECKING:
    from etl_craft.config import ConnectionProfile, ConnectorConfig

# [DEVIATION, 2026-09-20, E2-10] The query string is captured, not discarded.
# This pattern once had no `query` group and stopped the database capture at
# "?", so `jdbc:postgresql://host/db?sslmode=require` connected **without**
# TLS, silently.
_JDBC_POSTGRES_RE = re.compile(
    r"^jdbc:postgresql://(?P<host>[^:/]+)(:(?P<port>\d+))?/(?P<database>[^?]+)"
    r"(\?(?P<query>.*))?$"
)

DEFAULT_PORT = 5432

# Comfortably under a typical short-lived credential's lifetime, per
# CLAUDE.md's guidance for token/sso profiles; unused until those modes are
# implemented, but declared here so the setting has one obvious home.
TOKEN_POOL_RECYCLE_SECONDS = 15 * 60


class PostgresEngineDialect(EngineDialect):
    """PostgreSQL: transactional DDL, advisory locks, one database shared by every worker."""

    name = "postgresql"
    directory = Path(__file__).parent
    jdbc_prefix = "jdbc:postgresql:"
    auth_modes = frozenset({"password", "key_file"})

    def build_engine(
        self, config: ConnectorConfig, profile: ConnectionProfile, **engine_kwargs: Any
    ) -> Engine:
        """Build the Engine through psycopg, with the password never rendered into its URL."""
        from etl_craft.config import resolve_secret
        from etl_craft.db import ConnectionError_

        creator_factory = _AUTH_REGISTRY.get(profile.auth_mode)
        if creator_factory is None:
            raise ConnectionError_(
                f"auth_mode {profile.auth_mode!r} is not valid for a PostgreSQL Engine DB -- "
                f"use one of {sorted(self.auth_modes)}"
            )
        creator = creator_factory(profile, resolve_secret(config, profile))
        engine_kwargs.setdefault("pool_pre_ping", True)
        # [DEVIATION, 2026-09-20, E2-24] A real URL, minus the password.
        # SQLAlchemy never logs a password it was not given, so omitting just
        # the password keeps secrets out of logs while making engine.url true.
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

    def split_statements(self, sql_text: str) -> list[str]:
        """Split on `;`, respecting quotes, dollar-quoted bodies and comments."""
        return split_statements(sql_text)

    def duration_seconds_sql(self) -> str:
        """Return END_DATE - START_DATE in seconds."""
        return "EXTRACT(EPOCH FROM (END_DATE - START_DATE))"

    def existing_tables(self, engine: Engine, names: tuple[str, ...]) -> list[str]:
        """Return which of `names` exist in the current schema."""
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                    "WHERE TABLE_SCHEMA = CURRENT_SCHEMA() AND LOWER(TABLE_NAME) IN :names"
                ).bindparams(bindparam("names", expanding=True)),
                {"names": list(names)},
            ).all()
        return sorted(row[0] for row in rows)

    @contextmanager
    def lock(self, engine: Engine, key: int, name: str, wait_seconds: int = 0) -> Iterator[None]:
        """Hold a transaction-scoped advisory lock, on a connection of its own."""
        with engine.begin() as lock_conn:
            if wait_seconds:
                # No bind parameter: SET takes a literal. wait_seconds is an int
                # from config/limits, never user text. Postgres's lock_timeout
                # does apply to pg_advisory_xact_lock -- verified, not assumed.
                lock_conn.execute(text(f"SET LOCAL lock_timeout = '{int(wait_seconds)}s'"))
            try:
                lock_conn.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": key})
            except OperationalError as exc:
                raise LockTimeout(f"timed out after {wait_seconds}s waiting for {name}") from exc
            # The caller's own work runs on other connections; this one exists
            # only to hold the lock until the transaction ends.
            yield

    def ensure_migration_ledger(self, engine: Engine) -> None:
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
        quoted = '"' + constraint_name.replace('"', '""') + '"'
        conn.exec_driver_sql(f"ALTER TABLE SCHEMA_MIGRATIONS DROP CONSTRAINT {quoted}")
    conn.exec_driver_sql(
        "ALTER TABLE SCHEMA_MIGRATIONS "
        "ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (SOURCE, VERSION)"
    )


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
    return [stmt.strip() for stmt in statements if stmt.strip() and not is_only_comments(stmt)]


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


def is_only_comments(statement: str) -> bool:
    """Report whether `statement` holds nothing but comments and whitespace."""
    stripped = re.sub(r"/\*.*?\*/", "", statement, flags=re.DOTALL)
    stripped = re.sub(r"--[^\n]*", "", stripped)
    return not stripped.strip()


def parse_jdbc_postgres(jdbc_url: str) -> dict[str, Any]:
    """Split a `jdbc:postgresql://host[:port]/database[?query]` URL into its parts."""
    from etl_craft.db import ConnectionError_

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
    from etl_craft.db import ConnectionError_

    parts = parse_jdbc_postgres(profile.jdbc_url)
    key_file = profile.extra.get("key_file")
    if not key_file:
        raise ConnectionError_(
            f"profile {profile.name!r}: auth_mode=key_file requires a key_file path"
        )

    def _connect() -> Any:
        import psycopg

        return psycopg.connect(
            host=parts["host"],
            port=parts["port"],
            dbname=parts["database"],
            user=profile.user,
            sslkey=key_file,
            # str, not bytes: psycopg's own signature is str | int | None
            # (caught by mypy, E2-29).
            sslpassword=secret if secret else None,
            **parts["query"],
        )

    return _connect


_AUTH_REGISTRY: dict[str, Callable[[ConnectionProfile, str], Callable[[], Any]]] = {
    "password": _password_creator,
    "key_file": _key_file_creator,
}
