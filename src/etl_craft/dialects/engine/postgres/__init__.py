"""PostgreSQL: the recommended Engine DB for production, shared by every agent."""

from __future__ import annotations

import os
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import ExitStack, contextmanager
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar

import psycopg
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Connection, Engine
from sqlalchemy.engine.interfaces import DBAPIConnection
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import NullPool

from etl_craft.config.auth import POSTGRES_AUTH_FIELDS, engine_for_jdbc_url
from etl_craft.config.managed_engine import validate_managed_engine
from etl_craft.core.actor import Actor
from etl_craft.core.definitions import Definition
from etl_craft.core.enums import AuthMode, ManagedProvider
from etl_craft.core.errors import ConfigurationError, EngineDbError, LockTimeoutError
from etl_craft.core.outage import OutageBudget
from etl_craft.core.text import JdbcUrl, parse_jdbc_url, public_url_query
from etl_craft.dialects import credentials
from etl_craft.dialects.engine.base import EngineDialect
from etl_craft.dialects.engine.cloning import refresh_postgres
from etl_craft.dialects.engine.connections import SerialConnections
from etl_craft.dialects.engine.postgres.audit import refresh_metadata_triggers

if TYPE_CHECKING:
    from etl_craft.config import ConnectionProfile, ConnectorConfig

DEFAULT_PORT = 5432
C = TypeVar("C")


class PostgresEngineDialect(EngineDialect):
    """PostgreSQL: transactional DDL and advisory locks."""

    def admitted_query(self, sql: str, definition: Definition, *, name: str = "") -> str:
        """Expand an admitted definition through PostgreSQL composite row types."""
        from etl_craft.dialects.engine.definitions import projection

        return projection(sql, definition, postgres=True, name=name)

    supports_api_replicas = True
    archive_lock_suffix = " FOR UPDATE"
    spec = engine_for_jdbc_url("jdbc:postgresql:")
    directory = Path(__file__).parent

    def listen(self, conn: Connection, channels: tuple[str, ...]) -> None:
        """Quote each notification channel as an identifier, never as SQL input."""
        for channel in channels:
            quoted = conn.dialect.identifier_preparer.quote_identifier(channel)
            conn.exec_driver_sql(f"LISTEN {quoted}")

    def notifications(self, conn: Connection, timeout: float) -> Iterator[tuple[str, str]]:
        """Read committed notifications from this dedicated session connection."""
        driver = conn.connection.driver_connection
        assert isinstance(driver, psycopg.Connection)
        for notification in driver.notifies(timeout=timeout):
            yield notification.channel, notification.payload

    def stored_name(self, table: str) -> str:
        """Return ``table`` in lower case, as PostgreSQL folds unquoted names."""
        return table.lower()

    def snapshot(self, conn: Connection) -> Connection:
        """Read at REPEATABLE READ, so concurrent writes stay out of the snapshot."""
        return conn.execution_options(isolation_level="REPEATABLE READ")

    def drop_trigger(self, conn: Connection, trigger: str, table: str) -> None:
        """Drop ``trigger`` from ``table``."""
        quoted = conn.dialect.identifier_preparer.quote(trigger)
        conn.exec_driver_sql(f"DROP TRIGGER {quoted} ON {self.stored_name(table)}")

    def record_config_conversion(
        self, conn: Connection, table: str, primary: str, *, migration: str, actor: Actor
    ) -> None:
        """Record the conversion, then move the change-id sequence past it."""
        super().record_config_conversion(conn, table, primary, migration=migration, actor=actor)
        self._advance_change_id(conn)

    def record_config_baseline(
        self, conn: Connection, tables: tuple[str, ...], *, migration: str, actor: Actor
    ) -> None:
        """Record the baseline, then move the change-id sequence past it."""
        super().record_config_baseline(conn, tables, migration=migration, actor=actor)
        self._advance_change_id(conn)

    def _advance_change_id(self, conn: Connection) -> None:
        conn.execute(
            text(f"SELECT setval('etl_craft_change_id', {self._last_change_id(conn)}+1, false)")
        )

    def legacy_source_id_high_water(self, conn: Connection) -> int:
        """Return the legacy id sequence's last handed-out value."""
        sequence = conn.execute(
            text(
                "SELECT pg_get_serial_sequence('cfg_source_columns','source_column_id') AS sequence"
            )
        ).scalar_one()
        last, called = conn.execute(
            text(f"SELECT last_value AS last_value, is_called AS is_called FROM {sequence}")
        ).one()
        return int(last) if called else int(last) - 1

    def set_column_id_high_water(self, conn: Connection, high_water: int) -> None:
        """Set ``CFG_COLUMNS``' identity sequence to continue after ``high_water``."""
        conn.execute(
            text(
                "SELECT setval(pg_get_serial_sequence('cfg_columns','column_id'), :value, :called)"
                " AS value"
            ),
            {"value": max(1, high_water), "called": high_water > 0},
        )

    def refresh_metadata_triggers(self, conn: Connection) -> None:
        """Install this database's metadata capture and write guards."""
        refresh_metadata_triggers(conn)
        refresh_postgres(conn)

    def build_engine(
        self,
        config: ConnectorConfig,
        profile: ConnectionProfile,
        *,
        supervised: bool = False,
        **engine_kwargs: Any,
    ) -> Engine:
        """Build the engine through psycopg; the password never appears in its URL.

        Every new connection authenticates afresh, so a minted credential (``oauth``, ``sts``)
        is current; pooled connections using one are recycled before it expires. Once the
        database has been reached, a refused connection is retried for the outage budget.
        """
        from etl_craft.config import profile_secret

        validate_managed_engine(profile)
        url = parse_postgres_url(profile.jdbc_url)
        creator = riding_out_outages(
            postgres_creator(profile, profile_secret(config, profile), url),
            config.limits.engine_db_outage_seconds,
            reached=supervised,
        )
        serial = SerialConnections() if supervised else None
        if serial is not None:
            creator = serial.creator(creator)
        engine_kwargs.setdefault("pool_pre_ping", True)
        if profile.auth_mode in credentials.MINTED_AUTH_MODES:
            engine_kwargs.setdefault(
                "pool_recycle", credentials.MINTED_CREDENTIAL_POOL_RECYCLE_SECONDS
            )
        sqlalchemy_url = URL.create(
            "postgresql+psycopg",
            username=profile.user,
            host=url.host,
            port=url.port,
            database=url.database,
            query=public_url_query(url.query),
        )
        engine = create_engine(sqlalchemy_url, creator=creator, **engine_kwargs)
        if serial is not None:
            serial.attach(engine)
        return engine

    def tag_process_connection(self, driver: DBAPIConnection, identity: str) -> None:
        """Retain capacity while the owner or an orphaned local task still has a session."""
        cursor = driver.cursor()
        try:
            cursor.execute(
                "SELECT set_config('application_name', %s, false) AS application_name",
                (f"etl-craft-process-{identity}",),
            )
            driver.commit()
        finally:
            cursor.close()

    def session_connection(self, engine: Engine, config: ConnectorConfig) -> Connection:
        """Use the direct session address for ownership and LISTEN, outside the request pool."""
        from etl_craft.config import profile_secret

        if getattr(engine.dialect, "default_isolation_level", None) is None:
            with engine.connect():
                pass
        profile = config.engine
        profile = replace(profile, jdbc_url=profile.listen_jdbc_url or profile.jdbc_url)
        creator = postgres_creator(
            profile, profile_secret(config, profile), parse_postgres_url(profile.jdbc_url)
        )
        try:
            return Connection(engine, connection=NullPool(creator=creator).connect())
        except psycopg.OperationalError as error:
            raise OperationalError(None, None, error, connection_invalidated=True) from error

    def max_connections(self, conn: Connection) -> int | None:
        """Return the server's ``max_connections``."""
        return int(conn.execute(text("SHOW max_connections")).scalar_one())

    def schema_problem(self, conn: Connection, schema: str) -> str | None:
        """Return why the Engine schema cannot be used: it must already exist."""
        found = conn.execute(
            text("SELECT 1 FROM information_schema.schemata WHERE schema_name = :schema"),
            {"schema": schema.lower()},
        ).first()
        if found is None:
            database = conn.execute(text("SELECT current_database()")).scalar_one()
            return (
                f"the Engine schema {schema!r} does not exist in database {database!r}; create it "
                f"first (CREATE SCHEMA {schema.lower()}) — etl-craft does not create PostgreSQL "
                "schemas"
            )
        return None

    @contextmanager
    def lock(self, engine: Engine, key: int, name: str, wait_seconds: float = 0) -> Iterator[None]:
        """Hold a transaction-scoped advisory lock, on a connection of its own.

        The lock ends with that connection's transaction, so it is released even when the
        holder's process dies. The caller's own work runs on other connections.
        """
        with ExitStack() as held:
            try:
                lock_conn = held.enter_context(engine.begin())
                if wait_seconds:
                    # SET takes a literal, not a bind parameter; the value is a number.
                    timeout_ms = max(int(wait_seconds * 1000), 1)
                    lock_conn.execute(text(f"SET LOCAL lock_timeout = {timeout_ms}"))
                lock_conn.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": key})
            except OperationalError as error:
                if getattr(error.orig, "sqlstate", None) == "55P03":
                    waited = f"; waited {wait_seconds:g}s" if wait_seconds else ""
                    raise LockTimeoutError(f"timed out waiting for {name}{waited}") from error
                raise EngineDbError(f"failed acquiring lock {name}: {error.orig}") from error
            yield


def parse_postgres_url(jdbc_url: str) -> JdbcUrl:
    """Split a ``jdbc:postgresql://host[:port]/database[?query]`` URL; the port defaults to 5432.

    The query string is kept, so ``sslmode=require`` reaches the driver.
    """
    url = parse_jdbc_url(jdbc_url, default_port=DEFAULT_PORT)
    if url.scheme.lower() != "postgresql" or not url.database:
        raise ConfigurationError(
            f"not a recognized jdbc:postgresql://host[:port]/database URL: {jdbc_url!r}"
        )
    return url


def psycopg_auth_kwargs(
    auth_mode: str,
    *,
    user: str,
    secret: str,
    extra: Mapping[str, Any],
    host: str,
    port: int,
) -> dict[str, Any]:
    """Return the psycopg connect arguments that authenticate ``user`` by ``auth_mode``.

    Shared by the Engine DB and the PostgreSQL warehouse, so they authenticate the same way.
    Called once per new connection, which keeps a minted credential current.
    """
    if auth_mode in {AuthMode.PASSWORD, AuthMode.TOKEN}:
        return {"password": secret}
    if auth_mode == AuthMode.KEY_FILE:
        kwargs: dict[str, Any] = {
            "sslkey": str(extra["key_file"]),
            "sslpassword": secret or None,
        }
        if extra.get("cert_file"):
            kwargs["sslcert"] = str(extra["cert_file"])
        return kwargs
    if auth_mode == AuthMode.OAUTH:
        token = credentials.client_credentials_token(
            str(extra["token_url"]), str(extra["client_id"]), secret, extra.get("scope")
        )
        return {"password": token}
    if auth_mode == AuthMode.STS:
        token = credentials.aws_rds_auth_token(
            host, port, user, str(extra["region"]), extra.get("role_arn")
        )
        # RDS refuses an IAM token over an unencrypted connection; a URL that names its own
        # sslmode still wins, because the URL's query is applied after these.
        return {"password": token, "sslmode": "require"}
    if auth_mode == AuthMode.SSO:
        kwargs = {"oauth_issuer": str(extra["issuer"]), "oauth_client_id": str(extra["client_id"])}
        if secret:
            kwargs["oauth_client_secret"] = secret
        if extra.get("scope"):
            kwargs["oauth_scope"] = str(extra["scope"])
        return kwargs
    raise ConfigurationError(
        f"Auth_mode {auth_mode!r} is not available for PostgreSQL — use one of "
        f"{', '.join(sorted(POSTGRES_AUTH_FIELDS))}"
    )


def riding_out_outages(
    connect: Callable[[], C],
    budget_seconds: float,
    *,
    reached: bool = False,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> Callable[[], C]:
    """Retry refused connections within the budget, once the database has been reached.

    A database never reached fails at once, so a wrong URL or password is reported without a
    wait; ``reached`` says it was reached already, by the supervisor of a task process. Every
    connection shares one outage, so once its budget is spent the process's other connections
    fail at once instead of each waiting again.
    """
    import psycopg

    outage = OutageBudget(budget_seconds, clock)

    def open_connection() -> C:
        nonlocal reached
        while True:
            try:
                connection = connect()
            except psycopg.OperationalError as error:
                if not reached:
                    raise
                sleep(outage.failed("connecting to the Engine DB", error))
                continue
            reached = True
            outage.recovered("connecting to the Engine DB")
            return connection

    return open_connection


def postgres_creator(profile: ConnectionProfile, secret: str, url: JdbcUrl) -> Callable[[], Any]:
    """Return a function that opens one authenticated psycopg connection for ``profile``."""
    port = url.port
    assert port is not None  # parse_postgres_url fills in the default

    def connect() -> Any:
        import psycopg

        if profile.managed_provider == ManagedProvider.NONE:
            auth = psycopg_auth_kwargs(
                profile.auth_mode,
                user=profile.user,
                secret=secret,
                extra=profile.extra,
                host=url.host,
                port=port,
            )
        else:
            from etl_craft.dialects.managed_postgres import database_token

            auth = {"password": database_token(profile, secret)}
        # The URL's own query comes last, so a setting the team wrote (sslmode) wins.
        settings = {
            "host": url.host,
            "port": port,
            "dbname": url.database,
            "user": profile.user,
            **auth,
            **url.query,
        }
        if identity := os.environ.get("ETL_CRAFT_PROCESS_RESERVATION"):
            settings["application_name"] = f"etl-craft-process-{identity}"
        if profile.schema:
            # Every Engine DB table is created and read in the profile's schema.
            options = f"{settings.get('options', '')} -c search_path={profile.schema}"
            settings["options"] = options.strip()
        return psycopg.connect(**settings)

    return connect
