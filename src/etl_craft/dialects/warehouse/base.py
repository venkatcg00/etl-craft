"""The interface every warehouse dialect implements.

A warehouse dialect is one database and one table format: ``databricks`` and
``databricks_iceberg`` are separate dialects because what CREATE TABLE writes, and which ALTER
keyword a later change needs, differ between them. The SQL actions call only what is declared
here, so a new warehouse is a new module rather than a new branch in every action.

What a dialect accepts (its name, table format and auth modes) is its ``spec`` from
``config.auth``, the same table the configuration loader checks against.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from sqlalchemy import text
from sqlalchemy.engine import Connection

from etl_craft.config.auth import AuthFields, WarehouseSpec
from etl_craft.core.enums import AuthMode, TableFormat
from etl_craft.core.errors import ConfigurationError
from etl_craft.dialects import credentials

if TYPE_CHECKING:
    from etl_craft.config import CloningConfig, ConnectionProfile
    from etl_craft.config.targets import WarehouseUrl

SurrogateKey = Literal["identity", "sequence", "computed"]

AUDIT_COLUMN_TYPES: dict[str, str] = {
    "HASH_KEY": "VARCHAR(32)",
    "CREATE_DATE": "TIMESTAMP WITH TIME ZONE",
    "UPDATE_DATE": "TIMESTAMP WITH TIME ZONE",
    "CREATED_BY": "VARCHAR(255)",
    "UPDATED_BY": "VARCHAR(255)",
    "DELETE_FLAG": "VARCHAR(1)",
    "ACTIVE_FLAG": "VARCHAR(1)",
}
"""The DDL type of each engine-managed audit column, used to create a column with no rows."""


@dataclass(frozen=True)
class Presented:
    """How one new connection presents its credential to the driver.

    ``username`` and ``password`` go into the SQLAlchemy URL built inside the connection
    creator, never the engine's own logged URL. ``query`` joins that URL's query string, for
    drivers that read auth settings there (Trino). ``connect_args`` are passed to the driver
    after the dialect's own connect arguments, for settings that must never travel in a URL.
    """

    username: str | None = None
    password: str | None = None
    query: Mapping[str, str] = field(default_factory=dict)
    connect_args: Mapping[str, Any] = field(default_factory=dict)


STORAGE_PARAMETERS = ("EXTERNAL_LOCATION", "EXTERNAL_VOLUME", "BASE_LOCATION", "CATALOG")
"""Task parameters that place a table's data files; each dialect accepts only its own."""


class WarehouseDialect:
    """What differs between warehouses; the defaults are the ANSI behaviour PostgreSQL follows."""

    spec: WarehouseSpec

    # The STORAGE_PARAMETERS a task may set on this warehouse and table format.
    storage_parameters: frozenset[str] = frozenset()

    # Whether CREATE TEMPORARY TABLE exists and behaves. Trino has none; Databricks refuses
    # DROP on a name it shares with a temporary table.
    temporary_tables: bool = True
    # Whether an unqualified name resolves somewhere sensible. A Databricks session has no
    # default schema, so scratch tables are qualified there.
    default_schema: bool = True
    # Whether UPDATE and DELETE accept an alias for their target. Trino rejects one.
    mutation_alias: bool = True
    # Whether ALTER TABLE ... RENAME TO needs a qualified new name.
    qualified_rename: bool = False
    # How ROW_ID is generated: an identity column, a sequence default, or computed per insert
    # where the table format has neither (Iceberg).
    surrogate_key: SurrogateKey = "identity"
    # Whether the database enforces a PRIMARY KEY, which business rules rely on.
    enforces_primary_keys: bool = True
    # Whether only one process may write at a time (a DuckDB file).
    single_writer: bool = False
    # The type values are cast to before hashing.
    string_type: str = "VARCHAR"
    # How a private key is named in the driver's connect arguments: (path, passphrase).
    key_file_connect_args: tuple[str, str] | None = None
    # The username a bearer token is sent under, where the driver fixes it.
    token_username: str | None = None
    # Whether a bearer token (token, oauth) is sent under a username.
    bearer_needs_user: bool = True

    def __init__(self, spec: WarehouseSpec | None = None) -> None:
        """Use ``spec`` instead of the class's own, for a warehouse without its own dialect."""
        if spec is not None:
            self.spec = spec

    @property
    def key(self) -> str:
        """This dialect's name, such as ``databricks_iceberg``."""
        return self.spec.key

    @property
    def display_name(self) -> str:
        """The ``Warehouse.Name`` for this database, such as ``Databricks``."""
        return self.spec.display_name

    @property
    def sqlalchemy_name(self) -> str:
        """SQLAlchemy's name for this database, as a connected engine reports it."""
        return self.spec.sqlalchemy_name

    @property
    def table_format(self) -> TableFormat:
        """The table format this dialect writes."""
        return self.spec.table_format

    @property
    def per_task_format(self) -> bool:
        """Whether a task may choose another table format than the warehouse's own."""
        return self.spec.per_task_format

    @property
    def auth_fields(self) -> AuthFields:
        """The profile fields each auth mode needs; its keys are the modes accepted."""
        return self.spec.auth_fields

    @property
    def auth_modes(self) -> frozenset[str]:
        """The auth modes this warehouse accepts."""
        return self.spec.auth_modes

    # Connecting

    def check_profile(self, profile: ConnectionProfile) -> None:
        """Raise ``ConfigurationError`` unless ``profile`` can authenticate here."""
        mode = profile.auth_mode
        label = self.display_name or self.key
        if mode not in self.auth_fields:
            raise ConfigurationError(
                f"auth_mode {mode!r} is not available for a {label} warehouse — use one of "
                f"{sorted(self.auth_modes)}"
            )
        for name in self.auth_fields[mode]:
            if name in {"user", "secret"} or profile.extra.get(name):
                continue
            noun = "path" if name.endswith("_file") else "value"
            raise ConfigurationError(
                f"profile {profile.name!r}: auth_mode={mode} requires a `{name}:` {noun} in the "
                "profile — a private key or credential itself is never stored in "
                "craft-connector.yml"
            )
        if (
            mode in {AuthMode.TOKEN, AuthMode.OAUTH}
            and self.bearer_needs_user
            and not (profile.user or self.token_username)
        ):
            raise ConfigurationError(self._bearer_user_message(mode))

    def present(self, profile: ConnectionProfile, secret: str, url: WarehouseUrl) -> Presented:
        """Return how one new connection authenticates by ``profile.auth_mode``.

        Called once per connection, so a credential minted here is current. The defaults: a
        user and password, a bearer token in the password position, and a private key through
        ``key_file_connect_args``.
        """
        mode = profile.auth_mode
        user = profile.user or None
        if mode == AuthMode.NONE:
            return Presented(username=user)
        if mode == AuthMode.PASSWORD:
            return Presented(username=user, password=secret)
        if mode == AuthMode.TOKEN:
            return self.present_bearer(secret, user)
        if mode == AuthMode.OAUTH:
            return self.present_bearer(self.oauth_token(profile, secret, url), user)
        if mode == AuthMode.KEY_FILE and self.key_file_connect_args is not None:
            path_arg, passphrase_arg = self.key_file_connect_args
            connect_args: dict[str, Any] = {path_arg: str(profile.extra["key_file"])}
            if secret:
                connect_args[passphrase_arg] = secret
            return Presented(username=user, connect_args=connect_args)
        raise ConfigurationError(
            f"auth_mode {mode!r} is not available for a {self.display_name or self.key} "
            f"warehouse — use one of {sorted(self.auth_modes)}"
        )

    def present_bearer(self, token: str, user: str | None) -> Presented:
        """Present a bearer token in the password position."""
        username = user or self.token_username
        if not username:
            raise ConfigurationError(self._bearer_user_message(AuthMode.TOKEN))
        return Presented(username=username, password=token)

    def _bearer_user_message(self, mode: str) -> str:
        return (
            f"auth_mode='{mode}' needs a `user` for dialect {self.key!r} — it is sent in the "
            "username position alongside the token"
        )

    def oauth_token(self, profile: ConnectionProfile, secret: str, url: WarehouseUrl) -> str:
        """Mint an access token by the profile's client-credentials grant."""
        return credentials.client_credentials_token(
            str(profile.extra["token_url"]),
            str(profile.extra["client_id"]),
            secret,
            profile.extra.get("scope"),
        )

    def on_connect(self, dbapi_connection: Any, profile: ConnectionProfile, secret: str) -> None:
        """Run the setup a new connection needs before any statement; none by default."""
        return None

    def load_table_metadata(self, conn: Connection, schema: str, table: str) -> None:
        """Make information_schema.columns describe ``schema.table``; it already does by default."""
        return None

    # DDL

    def create_table_clause(self) -> str:
        """Return the clause between ``CREATE TABLE <name>`` and ``AS <select>``."""
        return ""

    def create_table_as(
        self, conn: Connection, qualified_name: str, select_sql: str, params: Mapping[str, str]
    ) -> None:
        """Run CREATE TABLE ... AS SELECT in this dialect's table format.

        ``params`` are the task's parameters, for dialects that read storage settings there.
        """
        clause = self.create_table_clause()
        prefix = f"CREATE TABLE {qualified_name}"
        statement = f"{prefix} {clause} AS {select_sql}" if clause else f"{prefix} AS {select_sql}"
        conn.execute(text(statement))

    def mirror_table_ddl(self, name: str, column_ddl: str, cloning: CloningConfig) -> str | None:
        """Return the DDL for a cloning mirror, or ``None`` for a plain CREATE TABLE."""
        clause = self.create_table_clause()
        return f"CREATE TABLE {name} ({column_ddl}) {clause}" if clause else None

    def task_storage_problem(self, params: Mapping[str, str]) -> str | None:
        """Return why a task's storage parameters cannot work here, or ``None``."""
        return None

    def unsupported_storage_problem(self, params: Mapping[str, str]) -> str | None:
        """Return which storage parameters the task sets that do not apply here, or ``None``.

        Such a parameter would be ignored, leaving the table somewhere the author did not
        intend.
        """
        given = [name for name in STORAGE_PARAMETERS if (params.get(name) or "").strip()]
        extra = [name for name in given if name not in self.storage_parameters]
        if not extra:
            return None
        accepted = ", ".join(sorted(self.storage_parameters)) or "none"
        return (
            f"{', '.join(extra)} does not apply to {self.display_name} with the "
            f"{self.table_format} table format, and would be ignored; storage parameters here: "
            f"{accepted}"
        )

    def cloning_storage_problem(self, cloning: CloningConfig) -> str | None:
        """Return why cloning cannot create mirrors here as configured, or ``None``."""
        return None

    def alter_table_keyword(self) -> str:
        """Return the keyword that alters a table this dialect created."""
        return "ALTER TABLE"

    def audit_column_type(self, column: str) -> str:
        """Return the DDL type of an engine-managed audit column."""
        return AUDIT_COLUMN_TYPES[column]

    def scratch_table_keyword(self) -> str:
        """Return ``TEMPORARY TABLE``, or ``TABLE`` where temporary tables are unusable."""
        return "TEMPORARY TABLE" if self.temporary_tables else "TABLE"

    # Expressions

    def hash_expression(self, values: list[str]) -> str:
        """Return MD5 over the NULL-safe, ``|``-joined text of ``values``, as 32 hex characters.

        Each value is COALESCEd to an empty string, so one NULL does not turn the whole
        concatenation NULL and make every row containing a NULL hash the same.
        """
        return f"MD5({self._hash_input(values)})"

    def _hash_input(self, values: list[str]) -> str:
        return " || '|' || ".join(
            f"COALESCE(CAST({value} AS {self.string_type}), '')" for value in values
        )

    def scalar_source_value(self, expression: str) -> str:
        """Wrap a value read by a correlated scalar subquery, where the dialect requires it."""
        return expression
