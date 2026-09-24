"""Build a SQLAlchemy Engine for the warehouse (CLAUDE.md's [Warehouse] section).

Unlike db.py's Engine DB connector — pinned to Postgres, no exceptions — the
warehouse can be any SQLAlchemy-supported relational engine.

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

Instead, the connection creator below (`_creator_for`) builds a real
`sqlalchemy.engine.URL` from the profile (never handed to `create_engine`
directly, so a checked-out connection's credential is never rendered into a
logged/echoed engine URL) and, at each pool checkout, asks that URL's own
resolved dialect to turn itself into a live connection through its own
`create_connect_args` + `connect` pair. How the credential is presented --
URL username/password, URL query, or driver connect args -- is each warehouse
dialect's own `present()` (2026-09-24).
This works for whatever dialect is actually installed, without this module
ever importing a specific driver.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import replace
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.config import ConnectionProfile, ConnectorConfig, profile_secret
from etl_craft.credentials import MINTED_AUTH_MODES, MINTED_CREDENTIAL_POOL_RECYCLE_SECONDS
from etl_craft.db import ConnectionError_
from etl_craft.dialects import warehouse_dialects
from etl_craft.dialects.engine_dialects import LockTimeout, for_engine
from etl_craft.dialects.warehouse_dialects.base import (
    AUTH_MODES,
    WarehouseDialect,
    parse_generic_jdbc,
)

_JDBC_SCHEME_RE = re.compile(r"^jdbc:(?P<scheme>[a-zA-Z0-9_+-]+):")


def _scheme(jdbc_url: str) -> str:
    match = _JDBC_SCHEME_RE.match(jdbc_url)
    if not match:
        raise ConnectionError_(
            f"not a recognized JDBC URL: {jdbc_url!r} — expected jdbc:<vendor>:..."
        )
    return match["scheme"].lower()


def translate_jdbc_url(jdbc_url: str) -> tuple[str, dict[str, Any]]:
    """Split a JDBC URL into (SQLAlchemy dialect name, parts), via that vendor's dialect.

    [DEVIATION, 2026-09-24] The per-vendor parsers live in each warehouse
    dialect's own module now. A scheme no dialect claims takes the generic
    `scheme://host[:port]/database[?query]` parser -- which is what makes "any
    sql tool over plain iceberg" need no code at all, only a dialect on the path.
    """
    dialect = warehouse_dialects.for_scheme(_scheme(jdbc_url))
    if dialect is None:
        return parse_generic_jdbc(jdbc_url)
    return dialect.parse_jdbc(jdbc_url)


def warehouse_dialect(config: ConnectorConfig) -> WarehouseDialect:
    """Return the dialect the configured warehouse connection and default format select."""
    if config.warehouse is None:
        raise ConnectionError_("no Warehouse section configured in craft-connector.yml")
    dialect_name, _ = translate_jdbc_url(config.warehouse.active.jdbc_url)
    try:
        return warehouse_dialects.resolve(dialect_name, config.warehouse_table_format)
    except warehouse_dialects.UnsupportedWarehouse as exc:
        raise ConnectionError_(str(exc)) from exc


def active_catalog(config: ConnectorConfig) -> str:
    """Return the catalog/database the active warehouse profile writes into.

    It is the first part of every `catalog.schema.table` name the engine builds;
    the schema comes from CFG_TASK_PARAMETERS.TARGET_OBJECT, never the profile.
    """
    if config.warehouse is None:
        raise ValueError("no Warehouse section configured in craft-connector.yml")
    profile = config.warehouse.active
    named = warehouse_dialect(config).catalog_name(profile.extra)
    if named:
        return named
    _, parts = translate_jdbc_url(profile.jdbc_url)
    # `catalog` where the two differ: Snowflake and Trino carry database and
    # schema in one `database/schema` segment, and only the catalog half is
    # wanted here.
    database = parts.get("catalog") or parts["database"]
    if not database:
        raise ValueError(
            "the active warehouse profile's jdbc_url names no catalog/database, so "
            "TARGET_OBJECT's schema.table cannot be resolved to a full name — add one "
            "(e.g. ConnCatalog=<catalog> for Databricks, db=<database> for Snowflake)"
        )
    return str(database)


#: Warehouse.Name -> the separate token connection fields it accepts.
PREFERRED_CONNECTION_FIELDS: dict[str, tuple[str, ...]] = {
    dialect.display_name.lower(): dialect.preferred_fields
    for dialect in warehouse_dialects.ALL
    if dialect.preferred_fields and dialect.table_format == "native"
}


def preferred_connection_url(name: str, fields: Mapping[str, str]) -> str:
    """Translate separate cloud connection fields into a credential-free JDBC URL."""
    dialect_name = warehouse_dialects.NAMES.get(name.lower())
    if dialect_name is None or name.lower() not in PREFERRED_CONNECTION_FIELDS:
        raise ConnectionError_("Separate token connection fields require Databricks or Snowflake")
    return warehouse_dialects.resolve(dialect_name, "native").preferred_connection_url(fields)


def _dbapi_connect(url: URL, extra: dict[str, Any] | None = None) -> Any:
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
    if extra:
        # Applied after create_connect_args deliberately: credentials that
        # must never travel in a URL go here. Snowflake's own dialect refuses
        # `private_key_file` in a URL query string "for safety reasons" and
        # tells you to use connect_args — this is that path, and it means the
        # key path and passphrase are never rendered into anything loggable.
        cparams.update(extra)
    return dialect.connect(*cargs, **cparams)


def _creator_for(auth_mode: str) -> Callable[..., Callable[[], Any]]:
    """Build the connection-creator factory for one auth mode.

    [DEVIATION, 2026-09-24] One creator for every mode. How a credential is
    presented -- URL username/password, URL query (Trino), or driver connect
    args (Snowflake's key pair and authenticators, psycopg's SSL and OAuth
    settings) -- is each warehouse dialect's own `present()`, so a new mode or
    vendor is a change to that vendor's file. The profile is checked when the
    engine is built, so a missing field fails before the first connection;
    `present()` runs once per new connection, which keeps a minted credential
    (oauth) fresh.

    What the history of this function established still holds: the URL built
    here carries the credential and is never the Engine's own (logged) URL;
    an embedded warehouse (a DuckDB file) keeps the file form, and a server
    keeps its host, port and query even with auth_mode none (a local Trino
    once resolved the literal hostname "none" when they were dropped).
    """

    def factory(
        profile: ConnectionProfile, secret: str, dialect: WarehouseDialect | None = None
    ) -> Callable[[], Any]:
        if profile.auth_mode != auth_mode:
            profile = replace(profile, auth_mode=auth_mode)
        dialect_name, parts = translate_jdbc_url(profile.jdbc_url)
        if dialect is None:
            dialect = warehouse_dialects.resolve(dialect_name, "native")
        dialect.check_profile(profile)
        chosen = dialect

        def _connect() -> Any:
            presented = chosen.present(profile, secret, parts)
            if parts.get("path"):
                # Embedded: the path *is* the database, and there is no server.
                url = URL.create(drivername=dialect_name, database=parts["path"])
            else:
                url = URL.create(
                    drivername=dialect_name,
                    username=presented.username,
                    password=presented.password,
                    host=parts["host"],
                    port=parts["port"],
                    database=parts["database"],
                    query={**parts["query"], **presented.query},
                )
            if presented.connect_args:
                return _dbapi_connect(url, dict(presented.connect_args))
            return _dbapi_connect(url)

        return _connect

    return factory


WAREHOUSE_AUTH_REGISTRY: dict[str, Callable[..., Callable[[], Any]]] = {
    auth_mode: _creator_for(auth_mode) for auth_mode in AUTH_MODES
}


def build_warehouse_engine(
    config: ConnectorConfig, profile: ConnectionProfile | None = None, **engine_kwargs: Any
) -> Engine:
    """Build a SQLAlchemy Engine for the warehouse (default profile: config.warehouse.active)."""
    if profile is None:
        if config.warehouse is None:
            raise ConnectionError_(
                "no [Warehouse] section configured in craft-connector.yml — nothing to connect to"
            )
        profile = config.warehouse.active
    creator_factory = WAREHOUSE_AUTH_REGISTRY.get(profile.auth_mode)
    if creator_factory is None:
        raise ConnectionError_(f"unknown auth_mode: {profile.auth_mode!r}")
    # A mode with nothing to present (none, sts, a secret-less sso) resolves
    # no secret — asking for one would mean inventing a variable.
    secret = profile_secret(config, profile)
    dialect = warehouse_dialect(config)
    base_creator = creator_factory(profile, secret, dialect)

    def creator() -> Any:
        # Per-connection setup a dialect needs before any statement -- DuckDB
        # over Iceberg attaches its catalog here. A no-op everywhere else.
        dbapi_connection = base_creator()
        dialect.on_connect(dbapi_connection, profile, secret)
        return dbapi_connection

    dialect_name, parts = translate_jdbc_url(profile.jdbc_url)
    engine_kwargs.setdefault("pool_pre_ping", True)
    if profile.auth_mode in MINTED_AUTH_MODES:
        engine_kwargs.setdefault("pool_recycle", MINTED_CREDENTIAL_POOL_RECYCLE_SECONDS)
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


# Arbitrary but fixed, and deliberately distinct from migrate.py's own key:
# every process coordinating warehouse access has to agree on it.
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
        dialect = warehouse_dialect(config)
        _, parts = translate_jdbc_url(config.warehouse.active.jdbc_url)
    except ConnectionError_:
        return False
    # DuckDB over Iceberg runs in memory by design: its data lives in the catalog.
    return dialect.key == "duckdb" and parts.get("path") == ":memory:"


def is_single_writer(config: ConnectorConfig) -> bool:
    """Whether the configured warehouse admits only one writing process at a time."""
    if config.warehouse is None:
        return False
    try:
        return warehouse_dialect(config).single_writer
    except ConnectionError_:
        return False


@contextmanager
def open_warehouse(
    config: ConnectorConfig,
    engine_db: Engine | None = None,
    *,
    wait_seconds: int = 0,
    **engine_kwargs: Any,
) -> Iterator[Engine]:
    """Open the warehouse for one unit of work, serializing it when the warehouse is single-writer.

    [ADDITION, 2026-09-21, E2-61] The one way the engine reaches the warehouse.
    For Postgres -- and any other warehouse that accepts concurrent writers --
    this is exactly the previous `build_warehouse_engine(...)` / `dispose()` pairing
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
    with single_writer_lock(config, engine_db, wait_seconds=wait_seconds):
        warehouse_engine = build_warehouse_engine(config, **engine_kwargs)
        try:
            yield warehouse_engine
        finally:
            warehouse_engine.dispose()


@contextmanager
def single_writer_lock(
    config: ConnectorConfig, engine_db: Engine | None = None, *, wait_seconds: int = 0
) -> Iterator[None]:
    """Serialize warehouse access when the warehouse admits one writing process; else a no-op.

    [ADDITION, 2026-09-22, E2-81] Split out of `open_warehouse` so a caller can take
    the queueing without opening a warehouse engine of its own. HANDLER=PYTHON
    is exactly that caller: it is the *ingestion* handler -- CLAUDE.md's own
    rule is that "the team's own script is responsible for fetching and
    including pipeline_run_id in whatever it inserts", so writing to the
    warehouse is its whole purpose -- but the engine opens no warehouse
    connection for it, the team's script does, in its own process. Before
    this, an ingestion task in the same wave as any SQL task raced for
    DuckDB's file lock and whichever lost failed with the raw "Could not set
    lock on file" that E2-61 exists to prevent, in the handler most likely to
    be doing the writing.

    The engine cannot make a team's script take the lock, but it can hold it
    *around* the script for exactly the same reason it holds it around a SQL
    action -- the point is the queueing, not the engine object.

    A no-op when no [Warehouse] is configured, so wrapping a PYTHON task in it
    never invents a requirement the task did not previously have.
    """
    if engine_db is None or not is_single_writer(config):
        yield
        return

    # [DEVIATION, 2026-09-24] Through the Engine DB dialect's own lock rather
    # than a literal pg_advisory_xact_lock, so a SQLite Engine DB queues the
    # same way (a file lock beside the database; SQLite has no advisory locks).
    try:
        with for_engine(engine_db).lock(
            engine_db, _WAREHOUSE_ADVISORY_LOCK_KEY, "warehouse", wait_seconds=wait_seconds
        ):
            yield
    except LockTimeout as exc:
        raise ConnectionError_(
            f"timed out after {wait_seconds}s waiting for the warehouse: the configured "
            "warehouse allows only one writing process at a time, and another task is "
            "still using it"
        ) from exc


# Trino catalogs whose connector genuinely stores Iceberg tables. Trino is the
# one supported engine where the table format is a property of the *catalog*
# rather than the connection, so it is the one where "is this Iceberg-backed?"
# can be asked and answered rather than assumed.
ICEBERG_CONNECTORS = frozenset({"iceberg"})


def verify_iceberg_catalog(config: ConnectorConfig, warehouse_engine: Engine) -> str | None:
    """Check the warehouse really stores Iceberg; return a problem string, or None.

    [ADDITION, 2026-09-22, E2-69] `sql_actions._is_iceberg_backed` decides from
    the dialect name alone, which for Trino is an assumption rather than a
    fact: the format comes from the catalog, and a Trino deployment routinely
    has several. `jdbc:trino://host:8080/hive/analytics` is a perfectly valid
    [Warehouse] URL that the engine would treat as Iceberg-backed -- computed
    ROW_ID instead of an identity column, no primary key expected -- while
    actually creating Hive tables. Everything "succeeds" and the lakehouse
    invariant is silently false.

    That is the same failure the Snowflake path refuses to allow, and it was
    decided differently for Trino only because the dialect name happened to be
    the only thing consulted. So verify it once, here, where `doctor` can fail
    with something a human can act on.

    Returns None when there is nothing to check -- a warehouse whose format is
    fixed by the connection rather than a catalog.
    """
    if warehouse_engine.dialect.name != "trino":
        return None
    # [DEVIATION, 2026-09-22, E2-72] Deliberately does NOT consult
    # config.warehouse_table_format. This answers one question -- is this
    # catalog an Iceberg catalog -- and *when to ask* belongs to the caller,
    # which is validate, because only validate can see the per-task
    # TABLE_FORMAT overrides. Filtering here as well short-circuited on the
    # warehouse default and silently skipped a task that had overridden it:
    # E2-72 again, one layer in. Caught by its own regression test.
    _, parts = (
        translate_jdbc_url(config.warehouse.active.jdbc_url) if config.warehouse else ("", {})
    )
    catalog = (parts or {}).get("catalog")
    if not catalog:
        return None
    try:
        with warehouse_engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT connector_name FROM system.metadata.catalogs "
                    "WHERE catalog_name = :name"
                ),
                {"name": catalog},
            ).first()
    except SQLAlchemyError as exc:
        return f"could not check whether catalog {catalog!r} is an Iceberg catalog: {exc}"
    if row is None:
        return f"catalog {catalog!r} does not exist on this Trino server"
    connector = str(row[0])
    if connector not in ICEBERG_CONNECTORS:
        return (
            f"catalog {catalog!r} is a {connector!r} catalog, not an Iceberg catalog — the "
            "engine would create tables there while treating them as Iceberg, so the lakehouse "
            "invariant would be silently false. Point [Warehouse] at an Iceberg catalog."
        )
    return None
