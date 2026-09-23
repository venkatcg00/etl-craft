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
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine
from sqlalchemy.exc import OperationalError, SQLAlchemyError

from etl_craft.config import ConnectionProfile, ConnectorConfig, resolve_secret
from etl_craft.db import ConnectionError_

PREFERRED_CONNECTION_FIELDS = {
    "databricks": ("jdbc_url", "catalog", "schema", "token"),
    "snowflake": ("user", "account", "database", "schema", "warehouse", "role", "token"),
}


def preferred_connection_url(name: str, fields: Mapping[str, str]) -> str:
    """Translate separate cloud connection fields into a credential-free JDBC URL."""
    name = name.lower()
    if name not in PREFERRED_CONNECTION_FIELDS:
        raise ConnectionError_("Separate token connection fields require Databricks or Snowflake")
    for key in PREFERRED_CONNECTION_FIELDS[name]:
        if key != "token" and not fields.get(key):
            raise ConnectionError_(f"{name} connection requires {key}")
    if name == "databricks":
        for key in ("catalog", "schema"):
            if not _SAFE_CATALOG.fullmatch(fields[key]):
                raise ConnectionError_(f"Databricks {key} must be an unquoted SQL identifier")
        if not fields["jdbc_url"].startswith("jdbc:databricks://"):
            raise ConnectionError_("Databricks jdbc_url must start with jdbc:databricks://")
        # The separate fields take precedence over defaults in the copied URL.
        return (
            fields["jdbc_url"].rstrip(";")
            + f";ConnCatalog={fields['catalog']};ConnSchema={fields['schema']}"
        )
    account = fields["account"]
    if not re.fullmatch(r"[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*", account):
        raise ConnectionError_("Snowflake account must be an account identifier, not a URL")
    if account.endswith(".snowflakecomputing.com"):
        account = account.removesuffix(".snowflakecomputing.com")
    query = {
        "db": fields["database"],
        "schema": fields["schema"],
        "warehouse": fields["warehouse"],
        "role": fields["role"],
    }
    return f"jdbc:snowflake://{account}.snowflakecomputing.com/?{urlencode(query)}"


# DuckDB is embedded, so its URL names a file rather than a server.
# `jdbc:duckdb:` alone means an in-memory database.
_DUCKDB_URL_RE = re.compile(r"^jdbc:duckdb:(?P<path>.*)$")

# The catalog name qualify() interpolates unquoted into database.schema.table.
_SAFE_CATALOG = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_JDBC_SCHEME_RE = re.compile(r"^jdbc:(?P<scheme>[a-zA-Z0-9_+-]+):")

_DATABRICKS_URL_RE = re.compile(
    r"^jdbc:databricks://(?P<host>[^:/;]+)(:(?P<port>\d+))?"
    r"(/(?P<schema>[^;]*))?(;(?P<params>.*))?$"
)

_SNOWFLAKE_URL_RE = re.compile(
    r"^jdbc:snowflake://(?P<host>[^:/?]+)(:(?P<port>\d+))?/?(\?(?P<query>.*))?$"
)

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


def _parse_duckdb(jdbc_url: str) -> tuple[str, dict[str, Any]]:
    """Parse `jdbc:duckdb:<path>` — a file, not a server."""
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
    raise ConnectionError_(f"not a recognized DuckDB JDBC URL: {jdbc_url!r}")


def _parse_databricks(jdbc_url: str) -> tuple[str, dict[str, Any]]:
    """Parse Databricks' semicolon-parameter JDBC form.

    [ADDITION, 2026-09-22] `jdbc:databricks://<host>:443/<schema>;httpPath=...;
    ConnCatalog=...` — semicolon-separated parameters after the path, not a
    query string, so the generic parser cannot read it.

    Only the parameters the SQLAlchemy dialect actually consumes are carried
    over (`http_path`, `catalog`, `schema`, verified against
    `create_connect_args`). Transport/auth parameters a JDBC driver needs and
    this one does not — `AuthMech`, `transportMode`, `ssl`, `UID`, `PWD` — are
    dropped rather than passed through, since `PWD` in particular would put
    the token in the URL, which this module goes out of its way to avoid.
    """
    match = _DATABRICKS_URL_RE.match(jdbc_url)
    if not match:
        raise ConnectionError_(
            f"not a recognized Databricks JDBC URL: {jdbc_url!r} — expected "
            "jdbc:databricks://<host>:443/<schema>;httpPath=/sql/1.0/warehouses/<id>"
        )
    params: dict[str, str] = {}
    for chunk in (match["params"] or "").split(";"):
        if "=" in chunk:
            key, _, value = chunk.partition("=")
            params[key.strip().lower()] = value.strip()

    http_path = params.get("httppath")
    if not http_path:
        raise ConnectionError_(
            f"Databricks JDBC URL {jdbc_url!r} has no httpPath — it names the SQL warehouse "
            "or cluster to run against (e.g. httpPath=/sql/1.0/warehouses/<id>)"
        )
    query = {"http_path": http_path}
    catalog = params.get("conncatalog") or params.get("catalog")
    schema = params.get("connschema") or params.get("schema") or match["schema"]
    if catalog:
        query["catalog"] = catalog
    if schema and schema != "default":
        query["schema"] = schema
    return "databricks", {
        "host": match["host"],
        "port": int(match["port"]) if match["port"] else None,
        # qualify()'s three-part name needs the Unity Catalog catalog here.
        "database": catalog or "",
        "query": query,
    }


def _parse_snowflake(jdbc_url: str) -> tuple[str, dict[str, Any]]:
    """Parse Snowflake's account-host JDBC form.

    [ADDITION, 2026-09-22] `jdbc:snowflake://<account>.snowflakecomputing.com/
    ?db=<db>&schema=<schema>&warehouse=<wh>&role=<role>` — the path is empty
    and the database lives in the query string, which the generic parser (it
    requires a non-empty path segment) cannot read.

    The SQLAlchemy dialect takes database and schema as a two-segment
    `database/schema` path and splits them itself — verified against
    `create_connect_args`, which produced `database='MYDB', schema='PUBLIC'`.
    """
    match = _SNOWFLAKE_URL_RE.match(jdbc_url)
    if not match:
        raise ConnectionError_(
            f"not a recognized Snowflake JDBC URL: {jdbc_url!r} — expected "
            "jdbc:snowflake://<account>.snowflakecomputing.com/?db=<db>&schema=<schema>"
        )
    query = dict(parse_qsl(match["query"] or ""))
    database = query.pop("db", "") or query.pop("database", "")
    schema = query.pop("schema", "")
    # With a fully qualified host the Snowflake dialect does not derive the
    # required account argument. Preserve the endpoint and supply it explicitly.
    query.setdefault("account", match["host"].split(".", 1)[0])
    if not database:
        raise ConnectionError_(
            f"Snowflake JDBC URL {jdbc_url!r} has no db= parameter — it names the database "
            "qualify() resolves schema.table against"
        )
    return "snowflake", {
        "host": match["host"],
        "port": int(match["port"]) if match["port"] else None,
        "database": f"{database}/{schema}" if schema else database,
        # The catalog half of qualify()'s three-part name.
        "catalog": database,
        "query": query,
    }


def _parse_generic(jdbc_url: str) -> tuple[str, dict[str, Any]]:
    """Parse the ordinary `jdbc:<scheme>://host[:port]/database[?query]` form."""
    match = _JDBC_URL_RE.match(jdbc_url)
    if not match:
        raise ConnectionError_(
            f"not a recognized JDBC URL: {jdbc_url!r} — expected "
            "jdbc:<dialect>://host[:port]/database or jdbc:duckdb:<path>"
        )
    dialect = JDBC_SCHEME_TO_SQLALCHEMY_DIALECT.get(match["scheme"], match["scheme"])
    port = int(match["port"]) if match["port"] else None
    query = dict(parse_qsl(match["query"])) if match["query"] else {}
    database = match["database"]
    return dialect, {
        "host": match["host"],
        "port": port,
        "database": database,
        # Trino (and any other engine whose path is `catalog/schema`) names
        # the catalog first; qualify() wants that half alone.
        "catalog": database.split("/", 1)[0],
        "query": query,
    }


# [ADDITION, 2026-09-22] A parser per vendor whose JDBC URL is not the ordinary
# `scheme://host[:port]/database[?query]` shape, which this module's own
# comment always said would be needed "when that vendor is actually chosen".
# Three now are. A scheme absent from this map takes the generic parser, which
# is what makes "any sql tool over plain iceberg" need no code here at all —
# only a dialect on the path.
JDBC_PARSERS: dict[str, Callable[[str], tuple[str, dict[str, Any]]]] = {
    "duckdb": _parse_duckdb,
    "databricks": _parse_databricks,
    "snowflake": _parse_snowflake,
}


def translate_jdbc_url(jdbc_url: str) -> tuple[str, dict[str, Any]]:
    """Split a JDBC URL into (dialect name, parts), via that vendor's own parser."""
    scheme_match = _JDBC_SCHEME_RE.match(jdbc_url)
    if not scheme_match:
        raise ConnectionError_(
            f"not a recognized JDBC URL: {jdbc_url!r} — expected jdbc:<vendor>:..."
        )
    parser = JDBC_PARSERS.get(scheme_match["scheme"].lower(), _parse_generic)
    return parser(jdbc_url)


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
    """Connect with no credentials — an embedded warehouse, or an unauthenticated server.

    [ADDITION, 2026-09-20] DuckDB is a file, not a server: there is no user to
    be and no password to present, so requiring one would mean inventing a
    secret that authenticates nothing. `auth_mode: none` says that plainly.
    `[Email]` already uses the same value for the same reason, so this is an
    existing vocabulary rather than a new one.

    [DEVIATION, 2026-09-22] It is no longer only about embedded warehouses.
    The first version built a URL from the file path alone, dropping host and
    port — correct for DuckDB and silently wrong for anything else, which
    surfaced the moment a Trino cluster with authentication disabled (an
    ordinary local/dev setup) tried to connect and the driver resolved the
    literal hostname "none". A server profile keeps its host, port, user and
    query; only a pathless one falls back to the file form.

    `secret` is accepted and ignored to keep one registry signature.
    """
    del secret
    dialect_name, parts = translate_jdbc_url(profile.jdbc_url)
    if parts.get("path"):
        # Embedded: the path *is* the database, and there is no server.
        url = URL.create(drivername=dialect_name, database=parts["path"])
    else:
        url = URL.create(
            drivername=dialect_name,
            username=profile.user or None,
            host=parts["host"],
            port=parts["port"],
            database=parts["database"],
            query=parts["query"],
        )

    def _connect() -> Any:
        return _dbapi_connect(url)

    return _connect


# How each dialect names a private-key credential in its own connect args.
# Deliberately per-dialect: this was left unimplemented for years precisely
# because "Postgres SSL client certs and Snowflake private-key auth share
# nothing", which is still true -- there is no generic mapping, only a
# per-vendor one, and now there is a vendor to write.
KEY_FILE_CONNECT_ARGS: dict[str, tuple[str, str]] = {
    # (path argument, passphrase argument)
    "snowflake": ("private_key_file", "private_key_file_pwd"),
}


def _key_file_creator(profile: ConnectionProfile, secret: str) -> Callable[[], Any]:
    """Connect with a private key — Snowflake's key-pair (RSA) authentication.

    [DEVIATION, 2026-09-22] Implemented for Snowflake. **This is the
    corporate-standard way to authenticate a Snowflake service account**:
    Snowflake has been moving service accounts off single-factor passwords,
    and key-pair is what automation is expected to use. `password` still works
    and is fine for a human exploring an account.

    The profile names the key file (`key_file:` in its `extra`, the same
    convention db.py's Postgres key_file mode already uses) and the secret is
    the key's passphrase — empty if the key is unencrypted. The key itself
    never goes in craft-connector.yml, and neither value is ever rendered into
    a URL: they are injected after `create_connect_args`, which is exactly
    what Snowflake's own dialect insists on.

    For CI, write the key from a secret store to a file in a setup step and
    point `key_file` at it -- there is deliberately no inline-PEM mode, which
    would mean parsing the key here and taking a crypto dependency the engine
    does not otherwise need.
    """
    dialect_name, parts = translate_jdbc_url(profile.jdbc_url)
    base_dialect = dialect_name.split("+", 1)[0]
    arg_names = KEY_FILE_CONNECT_ARGS.get(base_dialect)
    if arg_names is None:
        raise NotImplementedError(
            f"auth_mode='key_file' has no implementation for dialect {base_dialect!r} — how a "
            "private-key credential maps to DBAPI connect args is genuinely vendor-specific "
            "(Postgres SSL client certs and Snowflake key-pair auth share nothing). "
            f"Implemented so far: {sorted(KEY_FILE_CONNECT_ARGS)}."
        )
    key_file = profile.extra.get("key_file")
    if not key_file:
        raise ConnectionError_(
            f"profile {profile.name!r}: auth_mode=key_file requires a `key_file:` path in the "
            "profile — the private key itself is never stored in craft-connector.yml"
        )
    path_arg, passphrase_arg = arg_names
    extra: dict[str, Any] = {path_arg: str(key_file)}
    if secret:
        extra[passphrase_arg] = secret

    url = URL.create(
        drivername=dialect_name,
        username=profile.user,
        host=parts["host"],
        port=parts["port"],
        database=parts["database"],
        query=parts["query"],
    )

    def _connect() -> Any:
        return _dbapi_connect(url, extra)

    return _connect


# Warehouses whose "token" is a long-lived bearer credential presented in the
# password position, rather than something minted per connection. Databricks
# personal access tokens work exactly this way -- the dialect's own
# create_connect_args maps username/password onto server_hostname/access_token
# (verified directly).
_STATIC_TOKEN_USERNAMES: dict[str, str] = {"databricks": "token"}


def _token_creator(profile: ConnectionProfile, secret: str) -> Callable[[], Any]:
    """Connect with a bearer token.

    [DEVIATION, 2026-09-22] Implemented, where it previously raised
    NotImplementedError on the grounds that "the credential-minting provider
    is team-specific". That reasoning still holds for tokens a provider mints
    per connection (an OAuth2 client-credentials exchange, an STS
    AssumeRole) -- none of which is specified -- but it conflated those with
    the far commoner case: a long-lived token the team already has, presented
    like a password. Databricks personal access tokens are exactly that, and
    the engine cannot reach Databricks at all without it.

    So this handles the static case and nothing more. A minted/refreshed token
    remains unimplemented and is a genuinely different mechanism, which is why
    `pool_recycle` guidance exists for it in CLAUDE.md.
    """
    dialect_name, parts = translate_jdbc_url(profile.jdbc_url)
    base_dialect = dialect_name.split("+", 1)[0]
    username = profile.user or _STATIC_TOKEN_USERNAMES.get(base_dialect)
    if not username:
        raise ConnectionError_(
            f"auth_mode='token' needs a `user` for dialect {base_dialect!r} — it is sent in "
            "the username position alongside the token. Databricks uses the literal 'token'."
        )
    url = URL.create(
        drivername=dialect_name,
        username=username,
        password=secret,
        host=parts["host"],
        port=parts["port"],
        database=parts["database"],
        query=parts["query"],
    )

    def _connect() -> Any:
        return _dbapi_connect(url)

    return _connect


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
                f"timed out after {wait_seconds}s waiting for the warehouse: the configured "
                "warehouse allows only one writing process at a time, and another task is "
                "still using it"
            ) from exc
        yield


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
