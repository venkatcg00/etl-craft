"""Connecting to the warehouse.

The warehouse can be any database SQLAlchemy has a dialect for. Its driver is never imported
here: at each new connection, the creator builds a SQLAlchemy URL carrying the credential and
asks that URL's own dialect to connect. That URL is never the engine's logged one, so a
credential never appears in logs. How the credential is presented (URL username and password,
URL query, or driver arguments) is each warehouse dialect's ``present``.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.config import ConnectionProfile, ConnectorConfig, profile_secret
from etl_craft.config.targets import WarehouseUrl, parse_warehouse_url
from etl_craft.core.errors import ConfigurationError, LockTimeoutError
from etl_craft.dialects import credentials
from etl_craft.dialects.engine import for_engine
from etl_craft.dialects.warehouse import WarehouseDialect, resolve

logger = logging.getLogger(__name__)

READ_ONLY_WAIT_SECONDS = 30
"""How long a read-only check (``validate``, ``doctor``) waits for a single-writer warehouse."""

WAREHOUSE_LOCK_KEY = 0x657463_7761
"""The Engine DB lock that queues writers of a single-writer warehouse."""

ICEBERG_CONNECTORS = frozenset({"iceberg"})


def _active_profile(config: ConnectorConfig) -> ConnectionProfile:
    if config.warehouse is None:
        raise ConfigurationError(
            "no Warehouse section configured in craft-connector.yml — nothing to connect to"
        )
    return config.warehouse.active


def warehouse_dialect(config: ConnectorConfig) -> WarehouseDialect:
    """Return the dialect the active warehouse profile and the default table format select."""
    url = parse_warehouse_url(_active_profile(config).jdbc_url)
    return resolve(url.dialect, config.warehouse_table_format)


def build_warehouse_engine(config: ConnectorConfig, **engine_kwargs: Any) -> Engine:
    """Build a SQLAlchemy engine for the active warehouse profile.

    The profile is checked against its dialect first, so a missing auth field fails before
    any connection. Each new connection authenticates afresh and runs the dialect's
    ``on_connect``; pools using a minted credential are recycled before it expires.
    """
    profile = _active_profile(config)
    dialect = warehouse_dialect(config)
    dialect.check_profile(profile)
    url = parse_warehouse_url(profile.jdbc_url)
    secret = profile_secret(config, profile)
    connect = warehouse_creator(dialect, profile, secret, url)

    def creator() -> Any:
        dbapi_connection = connect()
        dialect.on_connect(dbapi_connection, profile, secret)
        return dbapi_connection

    engine_kwargs.setdefault("pool_pre_ping", True)
    if profile.auth_mode in credentials.MINTED_AUTH_MODES:
        engine_kwargs.setdefault("pool_recycle", credentials.MINTED_CREDENTIAL_POOL_RECYCLE_SECONDS)
    logged_url = URL.create(
        url.dialect,
        username=profile.user or None,
        host=url.host,
        port=url.port,
        database=url.path or url.database,
        query=url.query,
    )
    return create_engine(logged_url, creator=creator, **engine_kwargs)


def warehouse_creator(
    dialect: WarehouseDialect, profile: ConnectionProfile, secret: str, url: WarehouseUrl
) -> Callable[[], Any]:
    """Return a function that opens one authenticated DBAPI connection to the warehouse.

    An embedded warehouse (a DuckDB file) is addressed by its path; a server keeps its host,
    port and query whatever the auth mode.
    """

    def connect() -> Any:
        presented = dialect.present(profile, secret, url)
        if url.path:
            connection_url = URL.create(drivername=url.dialect, database=url.path)
        else:
            connection_url = URL.create(
                drivername=url.dialect,
                username=presented.username,
                password=presented.password,
                host=url.host,
                port=url.port,
                database=url.database,
                query={**url.query, **presented.query},
            )
        return dbapi_connect(connection_url, dict(presented.connect_args))

    return connect


def dbapi_connect(url: URL, extra: dict[str, Any] | None = None) -> Any:
    """Open one raw DBAPI connection through ``url``'s own SQLAlchemy dialect.

    The dialect's ``connect`` is used rather than the driver's, because a dialect may do its
    own setup there (DuckDB's parses its configuration and loads extensions). ``extra`` is
    added after the dialect's own connect arguments, for settings that must never travel in a
    URL, such as a private key's path and passphrase.
    """
    dialect_cls = url.get_dialect()
    # `dbapi=` belongs to DefaultDialect, which every real dialect subclasses.
    dialect = dialect_cls(dbapi=dialect_cls.import_dbapi())  # type: ignore[call-arg]
    cargs, cparams = dialect.create_connect_args(url)
    if extra:
        cparams.update(extra)
    return dialect.connect(*cargs, **cparams)


def is_single_writer(config: ConnectorConfig) -> bool:
    """Whether the configured warehouse admits only one writing process at a time."""
    if config.warehouse is None:
        return False
    try:
        return warehouse_dialect(config).single_writer
    except ConfigurationError:
        return False


def is_in_memory(config: ConnectorConfig) -> bool:
    """Whether the configured warehouse is an in-memory DuckDB database.

    Every task runs in its own process, so each would see its own empty database; ``doctor``
    refuses such a configuration.
    """
    if config.warehouse is None:
        return False
    try:
        dialect = warehouse_dialect(config)
        url = parse_warehouse_url(config.warehouse.active.jdbc_url)
    except ConfigurationError:
        return False
    return dialect.key == "duckdb" and url.path == ":memory:"


@contextmanager
def single_writer_lock(
    config: ConnectorConfig, engine_db: Engine | None = None, *, wait_seconds: float = 0
) -> Iterator[None]:
    """Queue behind other writers when the warehouse admits one writing process; else nothing.

    The lock is held in the Engine DB, which every process that could contend can reach,
    including workers on other machines. Waiters queue in order rather than retrying against
    the warehouse's own lock error. ``wait_seconds`` 0 waits indefinitely; otherwise a
    ``LockTimeoutError`` says another task is still using the warehouse. A task whose own
    script writes to the warehouse is wrapped in this lock too, without an engine being opened
    for it.
    """
    if engine_db is None or not is_single_writer(config):
        yield
        return
    try:
        with for_engine(engine_db).lock(
            engine_db, WAREHOUSE_LOCK_KEY, "warehouse", wait_seconds=wait_seconds
        ):
            yield
    except LockTimeoutError as error:
        raise LockTimeoutError(
            f"timed out after {wait_seconds:g}s waiting for the warehouse: it allows only one "
            "writing process at a time, and another task is still using it"
        ) from error


@contextmanager
def open_warehouse(
    config: ConnectorConfig,
    engine_db: Engine | None = None,
    *,
    wait_seconds: float = 0,
    **engine_kwargs: Any,
) -> Iterator[Engine]:
    """Open the warehouse for one unit of work, queued behind other writers where needed.

    This is how the engine reaches the warehouse. For a warehouse that accepts concurrent
    writers it takes no lock, so waves stay fully parallel. The engine is disposed afterwards.
    """
    with single_writer_lock(config, engine_db, wait_seconds=wait_seconds):
        warehouse_engine = build_warehouse_engine(config, **engine_kwargs)
        try:
            yield warehouse_engine
        finally:
            warehouse_engine.dispose()


def verify_iceberg_catalog(config: ConnectorConfig, warehouse_engine: Engine) -> str | None:
    """Check that a Trino warehouse's catalog stores Iceberg tables; return the problem, or None.

    Trino's table format comes from the catalog, and a deployment often has several. A URL
    naming a Hive catalog would have the engine create Hive tables while treating them as
    Iceberg, so ``doctor`` checks it. Every other warehouse returns ``None``: its format is
    fixed by the connection.
    """
    if warehouse_engine.dialect.name != "trino" or config.warehouse is None:
        return None
    catalog = parse_warehouse_url(config.warehouse.active.jdbc_url).catalog
    if not catalog:
        return None
    try:
        with warehouse_engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT connector_name AS connector_name FROM system.metadata.catalogs "
                    "WHERE catalog_name = :name"
                ),
                {"name": catalog},
            ).first()
    except SQLAlchemyError as error:
        return f"could not check whether catalog {catalog!r} is an Iceberg catalog: {error}"
    if row is None:
        return f"catalog {catalog!r} does not exist on this Trino server"
    connector = str(row.connector_name)
    if connector not in ICEBERG_CONNECTORS:
        return (
            f"catalog {catalog!r} is a {connector!r} catalog, not an Iceberg catalog — the "
            "engine would create tables there while treating them as Iceberg. Point the "
            "Warehouse at an Iceberg catalog."
        )
    return None
