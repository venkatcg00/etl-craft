"""The interface every warehouse dialect implements.

A warehouse dialect is one database and one table format: ``databricks`` and
``databricks_iceberg`` are separate dialects because what CREATE TABLE writes, and which ALTER
keyword a later change needs, differ between them. The SQL actions call only what is declared
here, so a new warehouse is a new module rather than a new branch in every action.

What a dialect accepts (its name, table format and auth modes) is its ``spec`` from
``config.auth``, the same table the configuration loader checks against.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from sqlalchemy import text
from sqlalchemy.engine import Connection

from etl_craft.config.auth import AuthFields, WarehouseSpec
from etl_craft.core.enums import AuthMode, TableFormat
from etl_craft.core.errors import ConfigurationError, HandlerError
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


ReplaceStrategy = Literal["transactional", "create_or_replace", "copy_and_restore"]


class WarehouseDialect:
    """What differs between warehouses; the defaults are the ANSI behaviour PostgreSQL follows."""

    spec: WarehouseSpec

    # The STORAGE_PARAMETERS a task may set on this warehouse and table format.
    storage_parameters: frozenset[str] = frozenset()

    # Whether CREATE TEMPORARY TABLE exists and behaves. Trino has none; Databricks refuses
    # DROP on a name it shares with a temporary table.
    temporary_tables: bool = True
    replace_strategy: ReplaceStrategy = "copy_and_restore"
    """How a replacement protects an existing target from a failed write."""

    # Whether UPDATE and DELETE accept an alias for their target. Trino rejects one.
    mutation_alias: bool = True
    # Whether ALTER TABLE ... RENAME TO needs a qualified new name.
    qualified_rename: bool = False
    # How ROW_ID is generated: an identity column, a sequence default, or computed per insert
    # where the table format has neither (Iceberg).
    surrogate_key: SurrogateKey = "identity"
    identity_in_create: bool = False
    """Whether ROW_ID must be declared when creating the table, rather than added later."""
    # Whether the database enforces a PRIMARY KEY, which business rules rely on.
    enforces_primary_keys: bool = True
    # Whether only one process may write at a time (a DuckDB file).
    single_writer: bool = False
    # The type values are cast to before hashing.
    string_type: str = "VARCHAR"
    # The case unquoted identifiers fold to ("lower" or "upper"), or None where the database
    # keeps them as written and matches them case-insensitively.
    identifier_case: str | None = None
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

    def identity_table_ddl(self, target: str, columns: str, params: Mapping[str, str]) -> str:
        """Create an empty table with the supplied columns and a generated ROW_ID."""
        raise NotImplementedError(f"{self.key} does not declare identity columns in CREATE")

    def identity_replacement_ddl(
        self,
        conn: Connection,
        target: str,
        candidate: str,
        columns: str,
        params: Mapping[str, str],
        *,
        existing: bool,
    ) -> tuple[str, str]:
        """Return candidate creation and atomic publication SQL for an identity table."""
        raise NotImplementedError(f"{self.key} does not publish identity table replacements")

    def row_id_generated(self, conn: Connection, target: str) -> bool:
        """Whether inserts into this existing table omit ROW_ID and let its default fill it."""
        return self.surrogate_key != "computed"

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

    def replacement_comment(self, conn: Connection, target: str) -> str | None:
        """Capture the comment before a transactional replacement removes the old object."""
        return None

    def preserve_replacement_properties(
        self, conn: Connection, target: str, candidate: str
    ) -> None:
        """Refuse promotion unless the dialect can preserve the target's protected properties."""
        raise HandlerError(
            f"{target}: this warehouse cannot preserve replacement properties; use OVERWRITE_TABLE"
        )

    def backup_table(self, conn: Connection, backup: str, target: str) -> None:
        """Keep a durable row copy without reusing the target's configured storage path."""
        self.create_table_as(conn, backup, f"SELECT * FROM {target}", {})

    def replacement_ddl(
        self,
        conn: Connection,
        target: str,
        select_sql: str,
        params: Mapping[str, str],
        *,
        existing: bool,
    ) -> str:
        """Render a single atomic replacement, or refuse when the dialect cannot do it."""
        raise HandlerError(f"{self.display_name} cannot atomically replace {target}")

    overwrite_uses_ctas: bool = False
    """Whether an overwrite publishes a replacement snapshot with CTAS."""

    def overwrite_statement(self, target: str, columns: str, select_sql: str) -> str:
        """Replace rows without a separate truncate, where the warehouse supports it."""
        raise HandlerError(f"{self.display_name} cannot atomically overwrite {target}")

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

    def full_column_types(self, conn: Connection, table: str) -> dict[str, str]:
        """Read reusable DDL types, including nested types and declared sizes."""
        rows = conn.execute(text(f"DESCRIBE TABLE {table}")).all()
        return {
            str(row[0]).lower(): str(row[1])
            for row in rows
            if row[0] and not str(row[0]).startswith("#")
        }

    def full_column_type(self, conn: Connection, table: str, column: str) -> str:
        """Return a column's complete warehouse type, refusing missing metadata."""
        columns = self.full_column_types(conn, table)
        if column.lower() not in columns:
            raise HandlerError(f"{table}.{column}: cannot read the complete column type")
        return columns[column.lower()]

    def column_addition_problem(self, data_type: str) -> str | None:
        """Return why this type cannot be added without rebuilding, or None."""
        return None

    def same_column_type(self, source: str, target: str) -> bool:
        """Compare types as this warehouse stores and reports them."""
        return (
            self.evolution_column_type(source).strip().casefold()
            == self.evolution_column_type(target).strip().casefold()
        )

    def evolution_column_type(self, data_type: str) -> str:
        """Return the full stage type in the target table format's DDL representation."""
        return data_type

    def audit_column_type(self, column: str) -> str:
        """Return the DDL type of an engine-managed audit column."""
        return AUDIT_COLUMN_TYPES[column]

    def scratch_table_keyword(self) -> str:
        """Return ``TEMPORARY TABLE``, or ``TABLE`` where temporary tables are unusable."""
        return "TEMPORARY TABLE" if self.temporary_tables else "TABLE"

    hash_metadata_columns = "column_name, data_type, numeric_precision, numeric_scale"

    # Expressions

    def hash_expression(self, values: list[str], types: list[str] | None = None) -> str:
        """Return the version-2 MD5 of typed, NULL-tagged, length-prefixed values."""
        return f"MD5({self._hash_input(values, types)})"

    def _hash_input(self, values: list[str], types: list[str] | None = None) -> str:
        kinds = types if types is not None else ["VARCHAR"] * len(values)
        parts = []
        for value, kind in zip(values, kinds, strict=True):
            rendered = self.canonical_text(value, kind)
            parts.append(
                f"CASE WHEN {value} IS NULL THEN 'N' ELSE 'V' || "
                f"CAST(LENGTH({rendered}) AS {self.string_type}) || ':' || {rendered} END"
            )
        return " || ".join(parts)

    def canonical_text(self, value: str, kind: str) -> str:
        """Render supported scalar values independently of session output formats."""
        kind = kind.upper()
        base = re.split(r"[ (]", kind)[0]
        if base in {"FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "REAL", "BINARY_FLOAT", "BINARY_DOUBLE"}:
            raise HandlerError(
                f"MERGE_COMPARE_COLUMNS includes {value} ({kind}): floats have no stable text "
                "form; cast to DECIMAL in the SELECT"
            )
        if base in {
            "TIMESTAMP",
            "TIMESTAMPTZ",
            "TIMESTAMP_NS",
            "TIMESTAMP_MS",
            "TIMESTAMP_S",
            "TIMESTAMP_NTZ",
            "TIMESTAMP_LTZ",
            "TIMESTAMP_TZ",
            "DATETIME",
        }:
            return self.timestamp_text(value, kind)
        if base in {"BOOLEAN", "BOOL"}:
            return f"CASE WHEN {value} THEN 'true' ELSE 'false' END"
        if base in {"DECIMAL", "NUMERIC", "NUMBER"}:
            if not re.fullmatch(r"(?:DECIMAL|NUMERIC|NUMBER)\(\d+,\s*\d+\)", kind):
                raise HandlerError(
                    f"{value} has {kind} without a declared scale; cast to DECIMAL(p,s)"
                )
            return self.decimal_text(value, kind)
        if base == "DATE":
            return self.date_text(value)
        if base not in {
            "VARCHAR",
            "CHAR",
            "CHARACTER",
            "TEXT",
            "STRING",
            "BPCHAR",
            "TINYINT",
            "SMALLINT",
            "INTEGER",
            "INT",
            "BIGINT",
            "HUGEINT",
            "UTINYINT",
            "USMALLINT",
            "UINTEGER",
            "UBIGINT",
        }:
            raise HandlerError(
                f"MERGE_COMPARE_COLUMNS includes {value} ({kind}): unsupported "
                "canonical type; cast to TEXT or DECIMAL in the SELECT"
            )
        return f"CAST({value} AS {self.string_type})"

    def decimal_text(self, value: str, kind: str) -> str:
        """Keep the decimal's declared scale, including trailing zeroes."""
        return f"CAST(CAST({value} AS {kind}) AS {self.string_type})"

    def date_text(self, value: str) -> str:
        """Render dates in ISO format."""
        return f"CAST({value} AS {self.string_type})"

    def timestamp_text(self, value: str, kind: str) -> str:
        """Render a UTC timestamp with exactly six fractional digits."""
        raise NotImplementedError(f"{self.key} does not declare timestamp canonicalization")

    update_uses_merge: bool = False
    """Whether a joined update needs MERGE rather than UPDATE ... FROM."""

    def update_from_stage(
        self,
        target: str,
        stage: str,
        keys: tuple[str, ...],
        assignments: Mapping[str, str],
        condition: str,
    ) -> str:
        """Update matching target rows from a deduplicated stage using aliases t and s."""
        match = " AND ".join(f"t.{key} = s.{key}" for key in keys)
        values = ", ".join(f"{column} = {value}" for column, value in assignments.items())
        if self.update_uses_merge:
            return (
                f"MERGE INTO {target} t USING {stage} s ON {match} "
                f"WHEN MATCHED AND ({condition}) THEN UPDATE SET {values}"
            )
        return f"UPDATE {target} t SET {values} FROM {stage} s WHERE {match} AND ({condition})"

    def prepare_update_stage(self, stage: str, keys: tuple[str, ...]) -> tuple[str, ...]:
        """Statements that prepare a stage for joined updates on its merge keys."""
        return ()
