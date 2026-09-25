"""PostgreSQL: the recommended Engine DB for production, shared by every worker."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Connection, Engine
from sqlalchemy.exc import OperationalError

from etl_craft.config.auth import POSTGRES_AUTH_FIELDS, engine_for_jdbc_url
from etl_craft.core.enums import AuthMode
from etl_craft.core.errors import ConfigurationError, LockTimeoutError
from etl_craft.core.text import JdbcUrl, parse_jdbc_url
from etl_craft.dialects import credentials
from etl_craft.dialects.engine.base import EngineDialect

if TYPE_CHECKING:
    from etl_craft.config import ConnectionProfile, ConnectorConfig

DEFAULT_PORT = 5432


class PostgresEngineDialect(EngineDialect):
    """PostgreSQL: transactional DDL and advisory locks."""

    spec = engine_for_jdbc_url("jdbc:postgresql:")
    directory = Path(__file__).parent

    def build_engine(
        self, config: ConnectorConfig, profile: ConnectionProfile, **engine_kwargs: Any
    ) -> Engine:
        """Build the engine through psycopg; the password never appears in its URL.

        Every new connection authenticates afresh, so a minted credential (``oauth``, ``sts``)
        is current; pooled connections using one are recycled before it expires.
        """
        from etl_craft.config import profile_secret

        if profile.auth_mode not in POSTGRES_AUTH_FIELDS:
            raise ConfigurationError(
                f"auth_mode {profile.auth_mode!r} is not valid for a PostgreSQL Engine DB — "
                f"use one of {sorted(POSTGRES_AUTH_FIELDS)}"
            )
        require_auth_fields(profile, POSTGRES_AUTH_FIELDS[profile.auth_mode])
        url = parse_postgres_url(profile.jdbc_url)
        creator = postgres_creator(profile, profile_secret(config, profile), url)
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
            query=url.query,
        )
        return create_engine(sqlalchemy_url, creator=creator, **engine_kwargs)

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

    def duration_seconds_sql(self) -> str:
        """Return ``END_DATE - START_DATE`` in seconds."""
        return "EXTRACT(EPOCH FROM (END_DATE - START_DATE))"

    @contextmanager
    def lock(self, engine: Engine, key: int, name: str, wait_seconds: float = 0) -> Iterator[None]:
        """Hold a transaction-scoped advisory lock, on a connection of its own.

        The lock ends with that connection's transaction, so it is released even when the
        holder's process dies. The caller's own work runs on other connections.
        """
        with engine.begin() as lock_conn:
            if wait_seconds:
                # SET takes a literal, not a bind parameter; the value is a number.
                timeout_ms = max(int(wait_seconds * 1000), 1)
                lock_conn.execute(text(f"SET LOCAL lock_timeout = {timeout_ms}"))
            try:
                lock_conn.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": key})
            except OperationalError as error:
                raise LockTimeoutError(
                    f"timed out after {wait_seconds:g}s waiting for {name}"
                ) from error
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


def require_auth_fields(profile: ConnectionProfile, fields: tuple[str, ...]) -> None:
    """Raise ``ConfigurationError`` unless ``profile`` has every field its auth mode needs.

    ``user`` and ``secret`` are checked by the loader; this covers the auth mode's own fields.
    """
    for name in fields:
        if name in {"user", "secret"}:
            continue
        if not profile.extra.get(name):
            raise ConfigurationError(
                f"profile {profile.name!r}: auth_mode={profile.auth_mode} requires a "
                f"`{name}:` value in the profile"
            )


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
        f"auth_mode {auth_mode!r} is not available for PostgreSQL — use one of "
        f"{sorted(POSTGRES_AUTH_FIELDS)}"
    )


def postgres_creator(profile: ConnectionProfile, secret: str, url: JdbcUrl) -> Callable[[], Any]:
    """Return a function that opens one authenticated psycopg connection for ``profile``."""
    port = url.port
    assert port is not None  # parse_postgres_url fills in the default

    def connect() -> Any:
        import psycopg

        auth = psycopg_auth_kwargs(
            profile.auth_mode,
            user=profile.user,
            secret=secret,
            extra=profile.extra,
            host=url.host,
            port=port,
        )
        # The URL's own query comes last, so a setting the team wrote (sslmode) wins.
        settings = {
            "host": url.host,
            "port": port,
            "dbname": url.database,
            "user": profile.user,
            **auth,
            **url.query,
        }
        if profile.schema:
            # Every Engine DB table is created and read in the profile's schema.
            options = f"{settings.get('options', '')} -c search_path={profile.schema}"
            settings["options"] = options.strip()
        return psycopg.connect(**settings)

    return connect
