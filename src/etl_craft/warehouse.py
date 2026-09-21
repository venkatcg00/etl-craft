"""Build a SQLAlchemy Engine for the Data DB / warehouse (CLAUDE.md's [Warehouse] section).

Unlike db.py's Engine DB connector — pinned to Postgres, no exceptions — the
Data DB can be any SQLAlchemy-supported relational engine.

[DEVIATION, 2026-09-20] **Postgres and DuckDB are the two supported
warehouses**, per explicit decision: "DuckDB is our warehouse now ... duckdb
and postgresql are the ones we want to majorly support". Both are exercised
by the test suite against real databases. ClickHouse was briefly a third and
is gone: it is too far from ANSI for the SQL-action vocabulary to hold there
(no `UPDATE` at all, mandatory table ENGINE clauses, session-scoped temporary
tables), and pretending otherwise produced per-dialect branches that nothing
ran.

Anything else — Snowflake, Databricks, BigQuery, Redshift, ... — remains an
optional extra a team installs itself, discovered through SQLAlchemy's entry
points, never imported here. That constraint is what rules out db.py's
approach of hand-writing a `psycopg.connect` call per auth_mode: there is no
single driver to import.

Instead, `_password_creator` below builds a real `sqlalchemy.engine.URL`
from the profile (never handed to `create_engine` directly, so a checked-out
connection's password is never rendered into a logged/echoed engine URL —
same spirit as db.py's empty-URL-plus-creator approach) and, at each
pool-checkout, asks that URL's own resolved dialect to turn itself into raw
a live connection through its own `create_connect_args` + `connect` pair.
This works for whatever dialect is actually installed, without this module
ever importing a specific driver.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine
from sqlalchemy.exc import OperationalError

from etl_craft.config import ConnectionProfile, ConnectorConfig, resolve_secret
from etl_craft.db import ConnectionError_

# DuckDB is embedded, so its URL names a file rather than a server.
# `jdbc:duckdb:` alone means an in-memory database.
_DUCKDB_URL_RE = re.compile(r"^jdbc:duckdb:(?P<path>.*)$")

# The catalog name qualify() interpolates unquoted into database.schema.table.
_SAFE_CATALOG = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_JDBC_URL_RE = re.compile(
    r"^jdbc:(?P<scheme>[a-zA-Z0-9_+-]+)://(?P<host>[^:/?]+)(:(?P<port>\d+))?/(?P<database>[^?]+)"
    r"(\?(?P<query>.*))?$"
)

# [ADDITION] Deliberately small and non-exhaustive, not a full JDBC-vendor
# catalog: per CLAUDE.md's Non-goals, this module never imports a
# third-party dialect directly, so there's nothing to gain from hardcoding
# entries for warehouses no one has confirmed using yet. A JDBC scheme
# absent from this map is passed through unchanged as the SQLAlchemy
# dialect name — correct whenever the two names already match (e.g.
# "oracle", "mssql"), and the two entries below cover the common case where
# they don't (JDBC's bare "postgresql"/"mysql" vs. SQLAlchemy's
# driver-qualified dialect string). A vendor whose JDBC URL shape isn't
# `scheme://host[:port]/database[?query]` at all (e.g. Snowflake's
# account-identifier host, Oracle's `thin:@` form) isn't handled by this
# translator and would need its own parsing added when that vendor is
# actually chosen — not guessed at now.
JDBC_SCHEME_TO_SQLALCHEMY_DIALECT: dict[str, str] = {
    "postgresql": "postgresql+psycopg",
    "mysql": "mysql+pymysql",
}


def translate_jdbc_url(jdbc_url: str) -> tuple[str, dict[str, Any]]:
    """Split a JDBC URL into (dialect, parts). Handles DuckDB's file form too."""
    duckdb = _DUCKDB_URL_RE.match(jdbc_url)
    if duckdb:
        # [ADDITION, 2026-09-20] DuckDB is embedded: its JDBC URL is
        # `jdbc:duckdb:<path>` (or bare `jdbc:duckdb:` for in-memory) with no
        # host, port or query string — exactly the "vendor whose JDBC URL
        # shape isn't scheme://host[:port]/database at all" case this
        # translator's own comment flagged as needing its own parsing once
        # such a vendor was actually chosen. It has been.
        #
        # `database` is the catalog name DuckDB derives from the file stem
        # (`/data/warehouse.duckdb` -> `warehouse`), which is what
        # qualify()'s three-part `catalog.schema.table` form needs. An
        # in-memory database's catalog is `memory`.
        path = duckdb["path"] or ""
        if not path:
            # [DEVIATION, 2026-09-21, E2-63] The bare form is in-memory, and
            # now genuinely is. This used to fall through with path="" so the
            # creator below reached for `database` instead -- the literal
            # string "memory" -- and built `duckdb:///memory`, which DuckDB
            # reads as *a file named `memory` in the current working
            # directory*. Reproduced: two task subprocesses against
            # `jdbc:duckdb:` left a 274 KB file called `memory` in the repo
            # root and the second saw the first's data, which a real
            # in-memory database could not have shared. Each process also
            # starts wherever it happened to start, so cwd differences
            # between the orchestrator, a task subprocess and an Airflow
            # worker could produce several unrelated "warehouses".
            return "duckdb", {
                "host": None,
                "port": None,
                "path": ":memory:",
                "database": "memory",
                "query": {},
            }
        stem = Path(path).stem
        # [ADDITION, 2026-09-21, E2-62] The catalog name is the file stem, and
        # qualify() interpolates it unquoted into `catalog.schema.table`. A
        # hyphen is not an exotic filename, but `my-warehouse.public.t` is a
        # parser error that never mentions the file -- so check it here, where
        # it is derived, rather than letting every SQL action fail obscurely.
        # validate's own identifier check cannot catch this: it checks CFG_
        # values, and this one comes from craft-connector.yml.
        #
        # [CHOICE] Reject rather than quote. Quoting would make the catalog
        # case-sensitive and diverge from how the Postgres path builds the
        # same name.
        if not _SAFE_CATALOG.match(stem):
            raise ConnectionError_(
                f"DuckDB warehouse file {path!r} gives the catalog name {stem!r}, which is not "
                "a usable SQL identifier — it is interpolated unquoted into "
                "database.schema.table. Rename the file to use only letters, digits and "
                "underscores, starting with a letter or underscore."
            )
        return "duckdb", {
            "host": None,
            "port": None,
            "path": path,
            "database": stem,
            "query": {},
        }

    match = _JDBC_URL_RE.match(jdbc_url)
    if not match:
        raise ConnectionError_(
            f"not a recognized JDBC URL: {jdbc_url!r} — expected "
            "jdbc:<dialect>://host[:port]/database or jdbc:duckdb:<path>"
        )
    dialect = JDBC_SCHEME_TO_SQLALCHEMY_DIALECT.get(match["scheme"], match["scheme"])
    port = int(match["port"]) if match["port"] else None
    query = dict(parse_qsl(match["query"])) if match["query"] else {}
    return dialect, {
        "host": match["host"],
        "port": port,
        "database": match["database"],
        "query": query,
    }


def _dbapi_connect(url: URL) -> Any:
    """Open one raw DBAPI connection for `url` via its own resolved dialect.

    [DEVIATION, 2026-09-20] Calls `dialect.connect(...)`, not
    `dbapi.connect(...)`. The original went straight to the DBAPI on the
    reasoning that this is what `DefaultDialect.connect()` does internally —
    true, but it bypasses any dialect that *overrides* `connect()`, and
    overriding it is exactly how a dialect does its own setup work.

    DuckDB exposed this the moment it was tried: `duckdb_engine`'s `DBAPI`
    class has no `connect` attribute at all (an `AttributeError` at the first
    pool checkout), because its dialect's own `connect()` is what parses the
    URL's config, preloads extensions and wraps the connection. Going through
    the dialect is both more correct and strictly more general — and it still
    imports no driver.
    """
    dialect_cls = url.get_dialect()
    # `dbapi=` is a DefaultDialect kwarg, not on the Dialect base that
    # get_dialect() is typed as returning. Every real dialect subclasses
    # DefaultDialect, so this is a stub gap rather than a live hazard.
    dialect = dialect_cls(dbapi=dialect_cls.import_dbapi())  # type: ignore[call-arg]
    cargs, cparams = dialect.create_connect_args(url)
    return dialect.connect(*cargs, **cparams)


def _password_creator(profile: ConnectionProfile, secret: str) -> Callable[[], Any]:
    dialect_name, parts = translate_jdbc_url(profile.jdbc_url)
    url = URL.create(
        drivername=dialect_name,
        username=profile.user,
        password=secret,
        host=parts["host"],
        port=parts["port"],
        database=parts["database"],
        query=parts["query"],
    )

    def _connect() -> Any:
        return _dbapi_connect(url)

    return _connect


def _none_creator(profile: ConnectionProfile, secret: str) -> Callable[[], Any]:
    """Connect with no credentials at all — for an embedded warehouse like DuckDB.

    [ADDITION, 2026-09-20] DuckDB is a file, not a server: there is no user to
    be and no password to present, so requiring one would mean inventing a
    secret that authenticates nothing. `auth_mode: none` says that plainly.
    `[Email]` already uses the same value for the same reason, so this is an
    existing vocabulary rather than a new one.

    `secret` is accepted and ignored to keep one registry signature.
    """
    del secret
    dialect_name, parts = translate_jdbc_url(profile.jdbc_url)
    url = URL.create(
        drivername=dialect_name,
        database=parts.get("path") or parts["database"],
    )

    def _connect() -> Any:
        return _dbapi_connect(url)

    return _connect


def _key_file_creator(profile: ConnectionProfile, secret: str) -> Callable[[], Any]:
    raise NotImplementedError(
        "auth_mode='key_file' has no generic Data DB implementation — how a private-key/cert "
        "credential maps to DBAPI connect args is genuinely dialect-specific (Postgres SSL "
        "client certs and, say, Snowflake private-key auth share nothing), and CLAUDE.md "
        "doesn't pin a warehouse dialect down yet to build against."
    )


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


WAREHOUSE_AUTH_REGISTRY: dict[str, Callable[[ConnectionProfile, str], Callable[[], Any]]] = {
    "none": _none_creator,
    "password": _password_creator,
    "key_file": _key_file_creator,
    "token": _token_creator,
    "sso": _sso_creator,
}


def build_data_engine(
    config: ConnectorConfig, profile: ConnectionProfile | None = None, **engine_kwargs: Any
) -> Engine:
    """Build a SQLAlchemy Engine for the Data DB (default profile: config.warehouse.active)."""
    if profile is None:
        if config.warehouse is None:
            raise ConnectionError_(
                "no [Warehouse] section configured in craft-connector.yml — nothing to connect to"
            )
        profile = config.warehouse.active
    creator_factory = WAREHOUSE_AUTH_REGISTRY.get(profile.auth_mode)
    if creator_factory is None:
        raise ConnectionError_(f"unknown auth_mode: {profile.auth_mode!r}")
    # auth_mode='none' has no secret to resolve — asking for one would mean
    # inventing a variable that authenticates nothing.
    secret = "" if profile.auth_mode == "none" else resolve_secret(config, profile)
    creator = creator_factory(profile, secret)
    dialect_name, parts = translate_jdbc_url(profile.jdbc_url)
    engine_kwargs.setdefault("pool_pre_ping", True)
    # [DEVIATION, 2026-09-20, E2-24] A real URL, minus the password. The blank
    # "dialect://" this replaced kept secrets out of a logged engine URL — a
    # good goal — but left engine.url empty, which broke cloning's
    # same-database guard and ClickHouse's table-engine reflection, both
    # documented in cloning.py. SQLAlchemy never logs a password it was not
    # given, so omitting only the password preserves the goal.
    url = URL.create(
        dialect_name,
        username=profile.user or None,
        host=parts["host"],
        port=parts["port"],
        database=parts.get("path") or parts["database"],
        query=parts["query"],
    )
    return create_engine(url, creator=creator, **engine_kwargs)


# [ADDITION, 2026-09-21, E2-61] Warehouses that permit exactly one writing OS
# process at a time. DuckDB is embedded: its state is a file plus the writing
# process's buffers, and it takes an exclusive lock -- a second process is
# refused outright ("IO Error: Could not set lock on file"), and so is a
# *read-only* connection while a writer holds it. Verified directly.
#
# That collides with the engine's core execution model, which is one
# subprocess per ready task: with Max_parallel_tasks defaulting to 8, any wave
# holding two SQL/BUSINESS_RULES tasks would fail all but one, and the same
# applies under Airflow, whose parallel tasks are separate processes too.
SINGLE_WRITER_DIALECTS = frozenset({"duckdb"})

# Arbitrary but fixed, and deliberately distinct from migrate.py's own key:
# every process coordinating Data DB access has to agree on it.
_WAREHOUSE_ADVISORY_LOCK_KEY = 0x657463_7761

# How long a read-only verb (validate, doctor) waits for a busy single-writer
# warehouse before giving up. Deliberately short: these are interactive
# commands someone runs *because* something looks wrong, so a clear "a task is
# using it" beats a long silent hang.
READ_ONLY_WAIT_SECONDS = 30


def is_in_memory(config: ConnectorConfig) -> bool:
    """Whether the configured warehouse is an in-memory DuckDB database."""
    if config.warehouse is None:
        return False
    try:
        dialect_name, parts = translate_jdbc_url(config.warehouse.active.jdbc_url)
    except ConnectionError_:
        return False
    return dialect_name == "duckdb" and parts.get("path") == ":memory:"


def is_single_writer(config: ConnectorConfig) -> bool:
    """Whether the configured warehouse admits only one writing process at a time."""
    if config.warehouse is None:
        return False
    try:
        dialect_name, _ = translate_jdbc_url(config.warehouse.active.jdbc_url)
    except ConnectionError_:
        return False
    return dialect_name.split("+", 1)[0] in SINGLE_WRITER_DIALECTS


@contextmanager
def data_db(
    config: ConnectorConfig,
    engine_db: Engine | None = None,
    *,
    wait_seconds: int = 0,
    **engine_kwargs: Any,
) -> Iterator[Engine]:
    """Open the Data DB for one unit of work, serializing it when the warehouse is single-writer.

    [ADDITION, 2026-09-21, E2-61] The one way the engine reaches the Data DB.
    For Postgres -- and any other warehouse that accepts concurrent writers --
    this is exactly the previous `build_data_engine(...)` / `dispose()` pairing
    and costs nothing: no lock is taken and waves stay fully parallel.

    For a single-writer warehouse it additionally holds a Postgres advisory
    lock in the *Engine DB* for the duration, so concurrent tasks queue
    instead of erroring. The Engine DB is the right place for it: CLAUDE.md
    makes a valid Engine DB connection the one hard runtime dependency of
    every action, so it is reachable from every process that could contend --
    including Airflow workers on other machines, where the engine does not own
    the process model at all and therefore cannot serialize by spawning less.
    `migrate.py` already coordinates concurrent runs the same way.

    [CHOICE] Queueing, not retrying. Per explicit decision the subprocess-per-
    task model stays (it is what crash detection and "local runs mirror an
    orchestrator" are built on), so the contention is real and has to be
    waited out. An advisory lock queues fairly and cannot starve a waiter the
    way a retry loop on DuckDB's own IOException would.

    `wait_seconds` bounds the wait so a wedged holder cannot block a caller
    forever; 0 means wait indefinitely. Postgres's `lock_timeout` does apply
    to `pg_advisory_xact_lock` -- verified, not assumed.
    """
    if engine_db is None or not is_single_writer(config):
        data_engine = build_data_engine(config, **engine_kwargs)
        try:
            yield data_engine
        finally:
            data_engine.dispose()
        return

    with engine_db.begin() as lock_conn:
        if wait_seconds:
            # No bind parameter: SET takes a literal. wait_seconds is an int
            # from config/limits, never user text.
            lock_conn.execute(text(f"SET LOCAL lock_timeout = '{int(wait_seconds)}s'"))
        try:
            lock_conn.execute(
                text("SELECT pg_advisory_xact_lock(:key)"),
                {"key": _WAREHOUSE_ADVISORY_LOCK_KEY},
            )
        except OperationalError as exc:
            raise ConnectionError_(
                f"timed out after {wait_seconds}s waiting for the Data DB: the configured "
                "warehouse allows only one writing process at a time, and another task is "
                "still using it"
            ) from exc
        data_engine = build_data_engine(config, **engine_kwargs)
        try:
            yield data_engine
        finally:
            data_engine.dispose()
