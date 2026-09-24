"""The warehouse-dialect interface every warehouse module implements.

A warehouse dialect is one database *and* one table format: ``databricks`` and
``databricks_iceberg`` are separate modules because what CREATE TABLE emits,
and which ALTER keyword a later rename needs, differ between them. The engine's
SQL actions call only what is declared here, so a new warehouse is a new module
rather than a new branch in every action.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import parse_qsl

from sqlalchemy import text
from sqlalchemy.engine import Connection

if TYPE_CHECKING:
    from etl_craft.config import CloningConfig, ConnectionProfile

SurrogateKey = Literal["identity", "sequence", "computed"]

# CAST target type for each engine-managed audit column, when a column needs
# to exist with zero rows (SETUP_TABLE's empty-shape creation) -- an untyped
# NULL literal would otherwise default to whatever the dialect's "unknown"
# type is, which some engines reject outright in a persisted CREATE TABLE AS
# SELECT.
# [DEVIATION, 2026-09-20, E2-31/E2-33] Two corrections here. CREATE_DATE and
# UPDATE_DATE were plain TIMESTAMP while the engine writes datetime.now(UTC) --
# every warehouse-side audit timestamp silently lost its offset. And bare
# VARCHAR with no length is rejected in DDL by several dialects, so
# CREATED_BY/UPDATED_BY carry one.
AUDIT_COLUMN_TYPES: dict[str, str] = {
    "HASH_KEY": "VARCHAR(32)",
    "CREATE_DATE": "TIMESTAMP WITH TIME ZONE",
    "UPDATE_DATE": "TIMESTAMP WITH TIME ZONE",
    "CREATED_BY": "VARCHAR(255)",
    "UPDATED_BY": "VARCHAR(255)",
    "DELETE_FLAG": "VARCHAR(1)",
    "ACTIVE_FLAG": "VARCHAR(1)",
}

_JDBC_URL_RE = re.compile(
    r"^jdbc:(?P<scheme>[a-zA-Z0-9_+-]+)://(?P<host>[^:/?]+)(:(?P<port>\d+))?"
    r"/(?P<database>[^?]*)(\?(?P<query>.*))?$"
)
SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

#: Every authentication type a craft-connector.yml profile can choose, in the
#: order the docs list them. Which ones a warehouse accepts is its own
#: `auth_fields`.
AUTH_MODES = ("none", "password", "token", "key_file", "oauth", "sso", "sts")


@dataclass(frozen=True)
class Presented:
    """How one new connection presents its credential to the driver.

    `username`/`password` go into the SQLAlchemy URL built inside the
    connection creator (never the Engine's own, logged URL); `query` joins that
    URL's query string, for drivers that read auth settings there (Trino);
    `connect_args` are handed to the DBAPI connect call after the dialect's own
    create_connect_args, for settings that must never travel in a URL.
    """

    username: str | None = None
    password: str | None = None
    query: Mapping[str, str] = field(default_factory=dict)
    connect_args: Mapping[str, Any] = field(default_factory=dict)


class WarehouseDialect:
    """What differs between warehouses. Defaults are the ANSI behaviour Postgres follows."""

    #: This module's name, e.g. "databricks_iceberg".
    key: str = ""
    #: The value Warehouse.Name uses for this database, e.g. "Databricks".
    display_name: str = ""
    #: SQLAlchemy's dialect name, as a connected Engine reports it.
    sqlalchemy_name: str = ""
    #: "native" or "iceberg".
    table_format: str = "native"

    #: Whether CREATE TEMPORARY TABLE exists and behaves. Trino has none;
    #: Databricks refuses DROP on a name it shares with a temp table.
    temporary_tables: bool = True
    #: Whether an unqualified name resolves somewhere sensible. Databricks'
    #: session has no default schema, so scratch tables are qualified there.
    default_schema: bool = True
    #: Whether UPDATE/DELETE accept a target alias. Trino rejects one.
    mutation_alias: bool = True
    #: Whether ALTER TABLE ... RENAME TO takes (or needs) a qualified new name.
    qualified_rename: bool = False
    #: How ROW_ID is generated: an identity column, a sequence default, or
    #: computed per insert where the format has neither (Iceberg).
    surrogate_key: SurrogateKey = "identity"
    #: Whether the database enforces a PRIMARY KEY, which validate relies on.
    enforces_primary_keys: bool = True
    #: Whether only one OS process may write at a time (E2-61, DuckDB files).
    single_writer: bool = False
    #: The type text values are cast to before hashing.
    string_type: str = "VARCHAR"
    #: Whether a task may override Warehouse.Table_format. Not on DuckDB,
    #: where the format *is* the connection (a file, or an Iceberg catalog).
    per_task_format: bool = True

    # -- connection ----------------------------------------------------------

    def parse_jdbc(self, jdbc_url: str) -> tuple[str, dict[str, Any]]:
        """Split a JDBC URL into (SQLAlchemy dialect name, parts)."""
        return parse_generic_jdbc(jdbc_url)

    #: How a private-key credential is named in this dialect's connect args,
    #: as (path argument, passphrase argument); None where unsupported.
    key_file_connect_args: tuple[str, str] | None = None
    #: The username a static bearer token authenticates as, where fixed.
    token_username: str | None = None
    #: auth_mode -> the profile fields it needs (beyond the connection itself).
    #: Its keys are the auth modes this warehouse accepts; the loader refuses
    #: any other, and a missing field, before anything connects.
    auth_fields: Mapping[str, tuple[str, ...]] = {
        "none": (),
        "password": ("user", "secret"),
        "token": ("user", "secret"),
    }
    #: The auth modes run against a live service in this project. The others
    #: follow the vendor's documentation and are untested: they can be used,
    #: but success is not guaranteed (`doctor` says so).
    verified_auth_modes: frozenset[str] = frozenset()

    #: Whether a bearer token (token, oauth) is sent under a username, which
    #: must then come from the profile or `token_username`.
    bearer_needs_user: bool = True

    @property
    def auth_modes(self) -> frozenset[str]:
        """Return the auth modes this warehouse accepts."""
        return frozenset(self.auth_fields)

    def check_profile(self, profile: ConnectionProfile) -> None:
        """Raise unless `profile` can authenticate here, before anything connects."""
        from etl_craft.db import ConnectionError_

        mode = profile.auth_mode
        if mode not in self.auth_fields:
            raise ConnectionError_(
                f"auth_mode {mode!r} is not available for a {self.display_name or self.key} "
                f"warehouse -- use one of {sorted(self.auth_modes)}"
            )
        for name in self.auth_fields[mode]:
            if name in {"user", "secret"} or profile.extra.get(name):
                continue
            noun = "path" if name.endswith("_file") else "value"
            raise ConnectionError_(
                f"profile {profile.name!r}: auth_mode={mode} requires a `{name}:` {noun} in the "
                "profile — a private key or credential itself is never stored in "
                "craft-connector.yml"
            )
        if (
            mode in {"token", "oauth"}
            and self.bearer_needs_user
            and not (profile.user or self.token_username)
        ):
            raise ConnectionError_(
                f"auth_mode='{mode}' needs a `user` for dialect {self.key!r} — it is sent in "
                "the username position alongside the token. Databricks uses the literal 'token'."
            )

    def present(
        self, profile: ConnectionProfile, secret: str, parts: Mapping[str, Any]
    ) -> Presented:
        """Return how one new connection authenticates, for `profile.auth_mode`.

        Called once per connection, so a credential minted here (oauth) is
        fresh. The ANSI defaults: a user and password, and a bearer token in
        the password position under the dialect's fixed token username.
        """
        from etl_craft.db import ConnectionError_

        mode = profile.auth_mode
        user = profile.user or None
        if mode == "none":
            return Presented(username=user)
        if mode == "password":
            return Presented(username=user, password=secret)
        if mode == "token":
            return self.present_bearer(secret, user)
        if mode == "oauth":
            return self.present_bearer(self.oauth_token(profile, secret, parts), user)
        if mode == "key_file" and self.key_file_connect_args is not None:
            path_arg, passphrase_arg = self.key_file_connect_args
            connect_args: dict[str, Any] = {path_arg: str(profile.extra["key_file"])}
            if secret:
                connect_args[passphrase_arg] = secret
            return Presented(username=user, connect_args=connect_args)
        raise ConnectionError_(
            f"auth_mode {mode!r} is not available for a {self.display_name or self.key} "
            f"warehouse -- use one of {sorted(self.auth_modes)}"
        )

    def present_bearer(self, token: str, user: str | None) -> Presented:
        """Present a bearer token in the password position."""
        from etl_craft.db import ConnectionError_

        username = user or self.token_username
        if not username:
            raise ConnectionError_(
                f"auth_mode='token' needs a `user` for dialect {self.key!r} — it is sent in "
                "the username position alongside the token. Databricks uses the literal 'token'."
            )
        return Presented(username=username, password=token)

    def oauth_token(self, profile: ConnectionProfile, secret: str, parts: Mapping[str, Any]) -> str:
        """Mint an access token by the profile's client-credentials grant."""
        from etl_craft.credentials import client_credentials_token

        return client_credentials_token(
            str(profile.extra["token_url"]),
            str(profile.extra["client_id"]),
            secret,
            profile.extra.get("scope"),
        )

    #: The separate connection fields this warehouse accepts in place of one
    #: packed JDBC URL (the tested shape for Databricks and Snowflake).
    preferred_fields: tuple[str, ...] = ()

    def preferred_connection_url(self, fields: Mapping[str, str]) -> str:
        """Build a credential-free JDBC URL from separate connection fields."""
        from etl_craft.db import ConnectionError_

        raise ConnectionError_(
            f"{self.display_name} does not take separate token connection fields -- "
            "only Databricks and Snowflake do"
        )

    def require_preferred_fields(self, fields: Mapping[str, str]) -> None:
        """Raise unless every non-secret preferred field is present."""
        from etl_craft.db import ConnectionError_

        for key in self.preferred_fields:
            if key != "token" and not fields.get(key):
                raise ConnectionError_(f"{self.display_name} connection requires {key}")

    def catalog_name(self, profile_extra: dict[str, str]) -> str | None:
        """Return the catalog for catalog.schema.table where the URL does not name it."""
        return None

    def on_connect(self, dbapi_connection: Any, profile: ConnectionProfile, secret: str) -> None:
        """Run per-connection setup a dialect needs before any statement (none by default)."""
        return None

    def load_table_metadata(self, conn: Connection, schema: str, table: str) -> None:
        """Make information_schema.columns describe `schema.table` (it already does by default)."""
        return None

    # -- DDL -----------------------------------------------------------------

    def create_table_clause(self) -> str:
        """Return the clause between `CREATE TABLE <name>` and `AS <select>`."""
        return ""

    def create_table_as(
        self, conn: Connection, qualified_name: str, select_sql: str, params: dict[str, str]
    ) -> None:
        """Issue CREATE TABLE ... AS SELECT in this dialect's table format."""
        clause = self.create_table_clause()
        prefix = f"CREATE TABLE {qualified_name}"
        statement = f"{prefix} {clause} AS {select_sql}" if clause else f"{prefix} AS {select_sql}"
        conn.execute(text(statement))

    def mirror_table_ddl(self, name: str, column_ddl: str, cloning: CloningConfig) -> str | None:
        """Return DDL for a cloning mirror, or None to let SQLAlchemy create a plain table."""
        clause = self.create_table_clause()
        return f"CREATE TABLE {name} ({column_ddl}) {clause}" if clause else None

    def task_storage_problem(self, params: dict[str, str]) -> str | None:
        """Return why a task's storage parameters cannot work here, or None."""
        return None

    def cloning_storage_problem(self, cloning: CloningConfig) -> str | None:
        """Return why cloning cannot create mirrors here as configured, or None."""
        return None

    def alter_table_keyword(self) -> str:
        """Return the keyword that alters a table this dialect created."""
        return "ALTER TABLE"

    def audit_column_type(self, column: str) -> str:
        """Return the DDL type for an engine-managed audit column."""
        return AUDIT_COLUMN_TYPES[column]

    def scratch_table_keyword(self) -> str:
        """Return `TEMPORARY TABLE`, or `TABLE` where temporary tables are unusable."""
        return "TEMPORARY TABLE" if self.temporary_tables else "TABLE"

    # -- expressions ---------------------------------------------------------

    def hash_expression(self, values: list[str]) -> str:
        """MD5 over NULL-safe, `|`-joined text of `values`, as 32 hex characters.

        COALESCE to empty string per value stops one NULL from collapsing the
        whole concatenation to NULL, which would make every NULL-containing row
        hash identically regardless of its other values.
        """
        return f"MD5({self._hash_input(values)})"

    def _hash_input(self, values: list[str]) -> str:
        return " || '|' || ".join(
            f"COALESCE(CAST({value} AS {self.string_type}), '')" for value in values
        )

    def scalar_source_value(self, expression: str) -> str:
        """Wrap a value read by a correlated scalar subquery, where the dialect requires it."""
        return expression


def parse_generic_jdbc(jdbc_url: str) -> tuple[str, dict[str, Any]]:
    """Parse the ordinary `jdbc:<scheme>://host[:port]/database[?query]` form."""
    from etl_craft.db import ConnectionError_

    match = _JDBC_URL_RE.match(jdbc_url)
    if not match:
        raise ConnectionError_(
            f"not a recognized JDBC URL: {jdbc_url!r} — expected "
            "jdbc:<dialect>://host[:port]/database or jdbc:duckdb:<path>"
        )
    scheme = match["scheme"]
    dialect = {"postgresql": "postgresql+psycopg", "mysql": "mysql+pymysql"}.get(scheme, scheme)
    query = dict(parse_qsl(match["query"])) if match["query"] else {}
    database = match["database"]
    return dialect, {
        "host": match["host"],
        "port": int(match["port"]) if match["port"] else None,
        "database": database,
        # Trino (and any other engine whose path is `catalog/schema`) names the
        # catalog first; qualify() wants that half alone.
        "catalog": database.split("/", 1)[0],
        "query": query,
    }
