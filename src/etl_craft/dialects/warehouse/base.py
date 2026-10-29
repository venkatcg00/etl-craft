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
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from sqlalchemy import TextClause, text
from sqlalchemy.engine import Connection, CursorResult, Engine
from sqlalchemy.engine.interfaces import DBAPIConnection

from etl_craft.config.auth import WarehouseSpec
from etl_craft.core.enums import AuthMode, IcebergMaintenance, TableFormat
from etl_craft.core.errors import ConfigurationError, SqlGuardError
from etl_craft.core.failures import Failure
from etl_craft.core.text import as_subquery
from etl_craft.dialects import credentials
from etl_craft.dialects.failure_evidence import classify
from etl_craft.dialects.warehouse.metadata import TableMetadata

if TYPE_CHECKING:
    from etl_craft.config import CloningConfig, ConnectionProfile
    from etl_craft.config.targets import WarehouseUrl

SurrogateKey = Literal["identity", "sequence", "computed"]


def unbounded_varchar(data_type: str) -> str:
    """Render string types for Iceberg storage that does not retain VARCHAR bounds."""
    return re.sub(r"\bVARCHAR\s*\(\s*\d+\s*\)", "VARCHAR", data_type, flags=re.IGNORECASE)


AUDIT_COLUMN_TYPES: dict[str, str] = {
    "TASK_RUN_ID": "BIGINT",
    "PIPELINE_ID": "BIGINT",
    "HASH_KEY": "VARCHAR(32)",
    "CREATE_DATE": "TIMESTAMP WITH TIME ZONE",
    "UPDATE_DATE": "TIMESTAMP WITH TIME ZONE",
    "CREATED_BY": "VARCHAR(255)",
    "UPDATED_BY": "VARCHAR(255)",
    "DELETE_FLAG": "VARCHAR(1)",
    "ACTIVE_FLAG": "VARCHAR(1)",
}
"""The DDL type of each engine-managed audit column, used to create a column with no rows."""

_DECLARED_LENGTH = re.compile(r"(?:n?var)?char(?:acter)?(?: varying)?\s*\((\d+)\)", re.IGNORECASE)
"""A character type with a declared length, such as ``VARCHAR(10)`` or ``character(3)``."""


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

    def classify_failure(self, error: BaseException) -> Failure:
        """Classify the driver error using DB-API types and SQLSTATE."""
        return classify(error)

    spec: WarehouseSpec
    catalog_columns = True
    sqlalchemy_driver: str | None = None

    dml_rowcounts = False

    def affected_rows(self, result: CursorResult[Any]) -> int:
        """Require an exact DML count from a driver that promises to supply it."""
        if result.rowcount < 0:
            raise SqlGuardError(
                f"{self.spec.display_name}: DML returned rowcount={result.rowcount}; "
                "expected an affected-row count; use a driver that reports DML counts"
            )
        return result.rowcount

    # The STORAGE_PARAMETERS a task may set on this warehouse and table format.
    storage_parameters: frozenset[str] = frozenset()

    # The sqlglot dialect this warehouse's SQL is read in, for lineage and the SQL checks.
    sqlglot_dialect: str | None = None
    # How CREATE VIEW marks a secure view: Snowflake's SECURE keyword, PostgreSQL's
    # security_barrier option, or not at all.
    secure_view: Literal["keyword", "security_barrier"] | None = None
    # Whether CREATE VIEW works where this dialect's tables live.
    views: bool = True
    # How a view is replaced with its grants kept: CREATE OR REPLACE keeps them here; Snowflake
    # keeps them only with COPY GRANTS, and Databricks only when the view is altered in place.
    view_replacement: Literal["or_replace", "copy_grants", "alter"] = "or_replace"

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
    process_local_queries: bool = False
    """Embedded query execution ends with the owning task process."""

    single_writer: bool = False
    atomic_protection: bool = False
    """Whether business writes and protection evidence share one atomic transaction."""
    # The type values are cast to before hashing.
    string_type: str = "VARCHAR"
    # The case unquoted identifiers fold to ("lower" or "upper"), or None where the database
    # keeps them as written and matches them case-insensitively.
    identifier_case: str | None = None
    # How a private key is named in the driver's connect arguments: (path, passphrase).
    key_file_connect_args: tuple[str, str] | None = None
    # The query naming a catalog's connector (:name), where the URL's catalog sets the format.
    catalog_connector_sql: str | None = None
    # The username a bearer token is sent under, where the driver fixes it.
    token_username: str | None = None
    # Whether a bearer token (token, oauth) is sent under a username.
    bearer_needs_user: bool = True

    def protection_lock_sql(self, target: str) -> str | None:
        """Exclude uncoordinated DML while protected data is checked and changed."""
        return None

    def identity_table_ddl(self, target: str, columns: str, params: Mapping[str, str]) -> str:
        """Create an empty table with the supplied columns and a generated ROW_ID."""
        raise NotImplementedError(f"{self.spec.key} does not declare identity columns in CREATE")

    def identity_setup_sql(self, target: str) -> tuple[str, ...]:
        """Prepare a separately declared generator only where the warehouse requires it."""
        return ()

    def describe_select(self, conn: Connection, sql: str) -> list[tuple[str, str]] | None:
        """Return complete source types without staging when the dialect can describe queries."""
        return None

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
        raise NotImplementedError(f"{self.spec.key} does not publish identity table replacements")

    def existing_table_format(self, conn: Connection, target: str) -> TableFormat:
        """Read a table's format, or use the sole format supported by this dialect."""
        return self.spec.table_format

    def row_id_generated(self, conn: Connection, target: str) -> bool:
        """Whether inserts into this existing table omit ROW_ID and let its default fill it."""
        return self.surrogate_key != "computed"

    # Connecting

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
            f"Auth_mode {mode!r} is not available for a {self.spec.display_name or self.spec.key} "
            f"warehouse — use one of {', '.join(sorted(self.spec.auth_modes))}"
        )

    def present_bearer(self, token: str, user: str | None) -> Presented:
        """Present a bearer token in the password position."""
        username = user or self.token_username
        if not username:
            raise ConfigurationError(self._bearer_user_message(AuthMode.TOKEN))
        return Presented(username=username, password=token)

    def _bearer_user_message(self, mode: str) -> str:
        return (
            f"auth_mode='{mode}' needs a `user` for dialect {self.spec.key!r} — it is sent in the "
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

    session_sql: str | None = None
    """Setup SQL for each new warehouse connection."""

    session_connect_args: ClassVar[Mapping[str, Any]] = {}
    """Session options negotiated at login, without a setup statement."""

    def add_column_sql(self, table: str, column: str, kind: str, *, target: bool) -> str:
        """Temporary native stages use ordinary ALTER, independently of target storage."""
        keyword = self.alter_table_keyword() if target else "ALTER TABLE"
        return f"{keyword} {table} ADD COLUMN {column} {kind}"

    session_fencing = False
    """Whether a new lock holder can stop an expired holder's warehouse statements.

    Cluster execution needs it: a statement keeps running on the server after its client
    dies, so a lock passes on only once the old holder's tagged statements are confirmed gone.
    """

    def event_probe_sql(self, sql: str, max_rows: int) -> str:
        """Bound returned event rows at the server, including one overflow sentinel."""
        return f"SELECT * FROM {as_subquery(sql)} AS etl_craft_event LIMIT {max_rows}"

    def session_tag_args(self, tag: str) -> Mapping[str, Any]:
        """Driver connect arguments that label every statement of a session with ``tag``."""
        return {}

    def stop_tagged(self, conn: Connection, tag: str) -> None:
        """Ask the server to stop every statement labelled ``tag``."""
        raise ConfigurationError(
            f"a {self.spec.display_name} warehouse cannot stop another session's statements; "
            "use local execution (Deployment.Profile LAPTOP or CONTROLLER)"
        )

    def running_tagged(self, conn: Connection, tag: str) -> int:
        """Count the statements labelled ``tag`` that the server has not finished."""
        raise ConfigurationError(
            f"a {self.spec.display_name} warehouse cannot list another session's statements; "
            "use local execution (Deployment.Profile LAPTOP or CONTROLLER)"
        )

    def on_connect(self, dbapi_connection: Any, profile: ConnectionProfile, secret: str) -> None:
        """Apply this dialect's session settings, closing its setup cursor on failure too."""
        if self.session_sql is None:
            return
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute(self.session_sql)
        finally:
            cursor.close()

    def script_autocommit(self, engine: Engine, driver: DBAPIConnection) -> None:
        """Enable native autocommit without SQLAlchemy starting an implicit transaction."""
        engine.dialect.set_isolation_level(driver, "AUTOCOMMIT")

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
        raise SqlGuardError(
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
        raise SqlGuardError(f"{self.spec.display_name} cannot atomically replace {target}")

    overwrite_uses_ctas: bool = False
    """Whether an overwrite publishes a replacement snapshot with CTAS."""

    def overwrite_statement(self, target: str, columns: str, select_sql: str) -> str:
        """Replace rows without a separate truncate, where the warehouse supports it."""
        raise SqlGuardError(f"{self.spec.display_name} cannot atomically overwrite {target}")

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
            f"{', '.join(extra)} does not apply to {self.spec.display_name} with the "
            f"{self.spec.table_format} table format, and would be ignored; "
            "storage parameters here: "
            f"{accepted}"
        )

    def cloning_storage_problem(self, cloning: CloningConfig) -> str | None:
        """Return why cloning cannot create mirrors here as configured, or ``None``."""
        return None

    def view_ddl(self, qualified_name: str, select_sql: str, *, secure: bool, exists: bool) -> str:
        """Return the statement that creates or replaces the view over ``select_sql``.

        ``exists`` says whether the view is there already; replacing it keeps its grants.
        """
        if not self.views:
            raise SqlGuardError(
                f"CREATE_VIEW {qualified_name}: {self.spec.display_name} cannot create views "
                "where its tables live; use OVERWRITE_TABLE to keep the result as a table"
            )
        if secure and self.secure_view is None:
            raise SqlGuardError(
                f"CREATE_VIEW {qualified_name}: {self.spec.display_name} has no secure views; "
                "set SECURE_VIEW to false or remove it"
            )
        if self.view_replacement == "alter":
            verb = "ALTER VIEW" if exists else "CREATE VIEW"
            return f"{verb} {qualified_name} AS {select_sql}"
        if secure and self.secure_view == "security_barrier":
            return (
                f"CREATE OR REPLACE VIEW {qualified_name} WITH (security_barrier = true) "
                f"AS {select_sql}"
            )
        secure_keyword = "SECURE " if secure else ""
        grants = " COPY GRANTS" if self.view_replacement == "copy_grants" else ""
        return f"CREATE OR REPLACE {secure_keyword}VIEW {qualified_name}{grants} AS {select_sql}"

    def relation_type_query(self, table: str) -> tuple[str, dict[str, str | None]]:
        """Read information_schema's table type for a schema- or catalog-qualified name."""
        parts = table.split(".")
        where = "lower(table_name) = lower(:table) AND lower(table_schema) = lower(:schema)"
        catalog = parts[0] if len(parts) == 3 else None
        if catalog is not None:
            where += " AND lower(table_catalog) = lower(:catalog)"
        return (
            f"SELECT table_type FROM information_schema.tables WHERE {where}",
            {"table": parts[-1], "schema": parts[-2], "catalog": catalog},
        )

    def relation_types_query(self, tables: tuple[str, ...]) -> tuple[str, dict[str, str | None]]:
        """Read a set of relation kinds in one statement, preserving input identity."""
        queries = []
        parameters: dict[str, str | None] = {}
        for index, table in enumerate(tables):
            query, values = self.relation_type_query(table)
            for name, value in values.items():
                query = query.replace(f":{name}", f":{name}_{index}")
                parameters[f"{name}_{index}"] = value
            queries.append(f"SELECT {index} AS position, table_type FROM ({query}) relation")
        return " UNION ALL ".join(queries), parameters

    def alter_table_keyword(self) -> str:
        """Return the keyword that alters a table this dialect created."""
        return "ALTER TABLE"

    def key_counts(self, stage: str, keys: tuple[str, ...], target: str | None = None) -> str:
        """Count invalid and duplicate keys against the same staged snapshot."""
        listed = ", ".join(f"{key} AS k{i}" for i, key in enumerate(keys))
        grouped = ", ".join(keys)
        nulls = " OR ".join(f"k{i} IS NULL" for i in range(len(keys)))
        new = ""
        if target is not None:
            match = " AND ".join(f"t.{key}=s.{key}" for key in keys)
            new = (
                f", (SELECT COUNT(*) FROM {stage} s WHERE NOT EXISTS "
                f"(SELECT 1 FROM {target} t WHERE {match})) AS new_keys"
            )
        return (
            "SELECT COALESCE(SUM(n), 0) AS source_rows, "
            f"COALESCE(SUM(CASE WHEN {nulls} THEN n ELSE 0 END), 0) AS null_keys, "
            f"COALESCE(SUM(CASE WHEN n > 1 THEN 1 ELSE 0 END), 0) AS duplicate_keys {new} "
            f"FROM (SELECT {listed}, COUNT(*) AS n FROM {stage} GROUP BY {grouped}) counted"
        )

    def column_metadata_query(
        self, conn: Connection, table: str, columns: str
    ) -> tuple[str, dict[str, str | None]]:
        """Read information-schema columns for a bare, schema- or catalog-qualified table."""
        parts = table.split(".")
        name = parts[-1]
        schema = parts[-2] if len(parts) >= 2 else None
        catalog = parts[0] if len(parts) == 3 else None
        where = "lower(table_name) = lower(:table)"
        if schema is not None:
            self.load_table_metadata(conn, schema, name)
            where += " AND lower(table_schema) = lower(:schema)"
        if catalog is not None:
            where += " AND lower(table_catalog) = lower(:catalog)"
        return (
            f"SELECT {columns} FROM information_schema.columns "
            f"WHERE {where} ORDER BY ordinal_position",
            {"table": name, "schema": schema, "catalog": catalog},
        )

    def read_metadata(self, conn: Connection, table: str) -> TableMetadata:
        """Read ordered types and nullability from the dialect's metadata source."""
        query, parameters = self.column_metadata_query(
            conn, table, self.hash_metadata_columns + ", is_nullable"
        )
        return TableMetadata(
            tuple(
                (str(row[0]), str(row[1]), row[2], row[3], str(row[4]))
                for row in conn.execute(text(query), parameters)
            )
        )

    def task_metadata_query(
        self, conn: Connection, target: str, related: tuple[str, ...]
    ) -> tuple[str, dict[str, str | None]]:
        """Read target columns and related relation kinds in a single catalog snapshot."""
        query, parameters = self.column_metadata_query(
            conn, target, self.hash_metadata_columns + ", is_nullable"
        )
        fields = []
        for index, name in enumerate(related):
            sql, values = self.relation_type_query(name)
            for key, value in values.items():
                sql = sql.replace(f":{key}", f":{key}_{index}")
                parameters[f"{key}_{index}"] = value
            fields.append(f"({sql}) AS relation_{index}")
        return (
            f"SELECT columns.*, {', '.join(fields)} FROM (SELECT 1 AS present) one_row "
            f"LEFT JOIN ({query}) columns ON 1=1",
            parameters,
        )

    def hashed_stage(self, select_sql: str, hashed: str) -> str:
        """Materialize the source and its hash in the same CTAS."""
        return f"SELECT etl_src.*, {hashed} AS HASH_KEY FROM {as_subquery(select_sql)} etl_src"

    def new_table_insert(self, target: str, columns: list[str], select_sql: str) -> str:
        """Populate an explicitly declared table with the source and admitted audit ids."""
        return (
            f"INSERT INTO {target} ({', '.join(columns)}, "
            "PIPELINE_RUN_ID, PIPELINE_ID, TASK_RUN_ID) "
            "SELECT etl_src.*, :pipeline_run_id, :pipeline_id, :task_run_id "
            f"FROM {as_subquery(select_sql)} etl_src"
        )

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
            raise SqlGuardError(f"{table}.{column}: cannot read the complete column type")
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

    def retention_maintenance(
        self, table: str, action: IcebergMaintenance, days: int
    ) -> TextClause | None:
        """Return supported Iceberg maintenance SQL, or refuse it through the caller."""
        return None

    def archive_type_matches(self, actual: str, expected: str) -> bool:
        """Compare the lossless archive types against physical warehouse metadata."""
        return self.same_column_type(actual, expected)

    def evolution_column_type(self, data_type: str) -> str:
        """Return the full stage type in the target table format's DDL representation."""
        return data_type

    safe_cast = "TRY_CAST"
    """How ``reject_checks`` tests a cast without failing, named in its step's log line."""

    def reject_checks(self, value: str, kind: str) -> list[tuple[str, str]]:
        """Return each condition under which a non-NULL ``value`` cannot go into type ``kind``.

        Each comes with its reason, for ``REJECT_LIMIT``: the cast fails, or the text is longer
        than a declared length, which some casts would truncate rather than refuse.
        """
        checks = [(f"{self.safe_cast}({value} AS {kind}) IS NULL", f"does not cast to {kind}")]
        length = _DECLARED_LENGTH.fullmatch(kind.strip())
        if length is not None:
            checks.append(
                (
                    f"LENGTH(CAST({value} AS {self.string_type})) > {length[1]}",
                    f"is longer than {length[1]} characters",
                )
            )
        return checks

    def is_bigint_type(self, data_type: str) -> bool:
        """Whether a stored column has the signed BIGINT type used for task-run ids."""
        return self.same_column_type(data_type, "BIGINT")

    timestamp_default_precision = 6

    def timestamp_precision(self, data_type: str) -> int | None:
        """Read fractional-second capacity from an already loaded column definition."""
        kind = data_type.upper().strip()
        if not kind.startswith(("TIMESTAMP", "TIMESTAMPTZ")):
            return None
        match = re.search(r"\((\d+)\)", kind)
        if match:
            return int(match[1])
        return {"TIMESTAMP_S": 0, "TIMESTAMP_MS": 3, "TIMESTAMP_NS": 9}.get(
            kind, self.timestamp_default_precision
        )

    def timestamp_precision_problem(self, table: str, column: str, kind: str) -> str | None:
        """Refuse storage that cannot preserve the Engine DB's microsecond write stamp."""
        precision = self.timestamp_precision(kind)
        if precision is not None and precision >= 6:
            return None
        return (
            f"{table}.{column}: type {kind} has timestamp precision {precision!r}; "
            "expected at least 6 fractional digits (microseconds). Apply a project migration "
            "to widen the column to TIMESTAMP(6) or the dialect's equivalent before running; "
            "etl-craft does not alter existing timestamp columns"
        )

    def audit_column_type(self, column: str) -> str:
        """Return the DDL type of an engine-managed audit column."""
        return AUDIT_COLUMN_TYPES[column]

    def audit_column_contract(self, column: str) -> str:
        """Return an engine column's type as its storage format reports it."""
        return self.evolution_column_type(self.audit_column_type(column))

    def scratch_table_keyword(self) -> str:
        """Return ``TEMPORARY TABLE``, or ``TABLE`` where temporary tables are unusable."""
        return "TEMPORARY TABLE" if self.temporary_tables else "TABLE"

    def scratch_ddl(self, name: str, body: str, *, immutable: bool = False) -> tuple[str, str]:
        """Return the scratch relation and its creation statement."""
        return name, f"CREATE {self.scratch_table_keyword()} {name} AS {body}"

    def temporary_stage_unavailable(self, error: BaseException | None) -> bool:
        """Whether the server explicitly refused session temporary tables."""
        return False

    metadata_types_complete = False
    """Whether information_schema retains reusable type sizes and nested declarations."""

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
            raise SqlGuardError(
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
                raise SqlGuardError(
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
            raise SqlGuardError(
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
        raise NotImplementedError(f"{self.spec.key} does not declare timestamp canonicalization")

    update_uses_merge: bool = False
    """Whether a joined update needs MERGE rather than UPDATE ... FROM."""

    def row_id_base_sql(self, target: str) -> str | None:
        """Inline the allocation only where INSERT may read its own target."""
        return None

    def merge_source(self, target: str, stage: str, keys: tuple[str, ...]) -> str | None:
        """Return a source carrying identifiers when the dialect can allocate within MERGE."""
        return None

    def supports_insert_merge(self, conn: Connection) -> bool:
        """Whether one MERGE can update and insert source rows."""
        return True

    def update_from_stage(
        self,
        target: str,
        stage: str,
        keys: tuple[str, ...],
        assignments: Mapping[str, str],
        condition: str,
        insert: tuple[str, str] | None = None,
    ) -> str:
        """Update matching target rows from a deduplicated stage using aliases t and s."""
        match = " AND ".join(f"t.{key} = s.{key}" for key in keys)
        values = ", ".join(f"{column} = {value}" for column, value in assignments.items())
        if self.update_uses_merge or insert is not None:
            sql = (
                f"MERGE INTO {target} t USING {stage} s ON {match} "
                f"WHEN MATCHED AND ({condition}) THEN UPDATE SET {values}"
            )
            if insert is not None:
                columns, expressions = insert
                sql += f" WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({expressions})"
            return sql
        return f"UPDATE {target} t SET {values} FROM {stage} s WHERE {match} AND ({condition})"

    def prepare_update_stage(self, stage: str, keys: tuple[str, ...]) -> tuple[str, ...]:
        """Statements that prepare a stage for joined updates on its merge keys."""
        return ()
