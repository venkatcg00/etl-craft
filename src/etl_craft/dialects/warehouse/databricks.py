"""Databricks warehouse, Delta tables."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import replace
from typing import TYPE_CHECKING, Any, ClassVar

from sqlalchemy import text
from sqlalchemy.engine import Connection, CursorResult, Engine
from sqlalchemy.engine.interfaces import DBAPIConnection
from sqlalchemy.exc import SQLAlchemyError
from sqlglot import exp

from etl_craft.config.auth import warehouse_by_key
from etl_craft.core.enums import AuthMode, FailureClass, TableFormat
from etl_craft.core.errors import ConfigurationError, SqlGuardError
from etl_craft.core.failures import Failure
from etl_craft.dialects import credentials
from etl_craft.dialects.failure_evidence import driver_error, state_class
from etl_craft.dialects.warehouse.base import (
    Presented,
    ReplaceStrategy,
    SurrogateKey,
    WarehouseDialect,
)
from etl_craft.dialects.warehouse.metadata import TableMetadata
from etl_craft.dialects.warehouse.replacement import identity_replacement, replacement_ddl

if TYPE_CHECKING:
    from etl_craft.config import CloningConfig, ConnectionProfile
    from etl_craft.config.targets import WarehouseUrl


_TEMPORARY_TABLE_UNAVAILABLE = frozenset(
    {
        "TEMP_TABLE_CREATION_LEGACY_WITH_QUERY",
        "TEMP_TABLE_CREATION_NOT_SUPPORTED_IN_COMPUTE",
        "TEMP_TABLE_NOT_SUPPORTED_WITH_DATABRICKS_JOBS",
        "TEMP_TABLE_NOT_SUPPORTED_WITH_HMS",
        "TEMP_TABLE_REQUIRES_DATABRICKS_ENV",
        "TEMP_TABLE_REQUIRES_DELTA",
        "TEMP_TABLE_REQUIRES_UC",
    }
)


class DatabricksWarehouse(WarehouseDialect):
    """Databricks, Delta tables. A session has no default schema, so scratch tables are named."""

    session_fencing = True

    def session_tag_args(self, tag: str) -> Mapping[str, Any]:
        """Apply one holder identity to every statement in the session."""
        return {"query_tags": {"etl_craft_holder": tag}}

    def stop_tagged(self, conn: Connection, tag: str) -> None:
        """Request cancellation of every unfinished query belonging to this holder."""
        from etl_craft.dialects.warehouse.databricks_queries import active, request

        for identity in active(conn, tag):
            request(conn, f"statements/{identity}/cancel", method="POST")

    def running_tagged(self, conn: Connection, tag: str) -> int:
        """Count unfinished tagged statements; a cancel response alone is no acknowledgement."""
        from etl_craft.dialects.warehouse.databricks_queries import active

        return len(active(conn, tag))

    def script_autocommit(self, engine: Engine, driver: DBAPIConnection) -> None:
        """Keep the native driver's autocommit outside explicit file transactions."""
        return None

    def classify_failure(self, error: BaseException) -> Failure:
        """Thrift server errors carry Spark SQLSTATE in their diagnostic message."""
        error = driver_error(error)
        failure = super().classify_failure(error)
        match = re.search(r"SQLSTATE[:=] ?([0-9A-Z]{5})", str(error), re.IGNORECASE)
        if match:
            state = match[1].upper()
            return replace(failure, kind=state_class(state), code=state)
        context = getattr(error, "context", {})
        status = str(context.get("http-code", "")) if isinstance(context, dict) else ""
        if status in {"401", "403"}:
            return replace(failure, kind=FailureClass.PERMANENT, code=status)
        if status in {"408", "429", "500", "502", "503", "504"}:
            return replace(failure, kind=FailureClass.TRANSIENT, code=status)
        return failure

    spec = warehouse_by_key("databricks")
    catalog_columns = False
    metadata_types_complete = True
    dml_rowcounts = True

    def read_metadata(self, conn: Connection, table: str) -> TableMetadata:
        """DESCRIBE JSON supplies complete types, nullability and table properties together."""
        try:
            result = conn.execute(text(f"DESCRIBE TABLE EXTENDED {table} AS JSON")).scalar_one()
        except SQLAlchemyError as error:
            if self.classify_failure(error).code == "42P01":
                return TableMetadata(())
            raise
        try:
            description = json.loads(str(result))
            columns = tuple(
                (
                    str(column["name"]),
                    self.json_type(column["type"]),
                    None,
                    None,
                    "YES" if column["nullable"] else "NO",
                )
                for column in description["columns"]
            )
            storage = (
                None
                if "VIEW" in str(description["type"]).upper()
                else self.metadata_format(
                    table, description["provider"], description.get("table_properties")
                )
            )
        except (KeyError, TypeError, ValueError) as error:
            raise SqlGuardError(
                f"{table}: DESCRIBE returned invalid column metadata; expected complete JSON "
                "types and nullability; repair the table's metadata before writing"
            ) from error
        return TableMetadata(columns, table_format=storage)

    def json_type(self, description: object) -> str:
        """Render only the documented structured type metadata, including nested fields."""
        if not isinstance(description, dict):
            raise SqlGuardError(
                "Databricks column type: expected DESCRIBE JSON type metadata; repair the table"
            )
        name = str(description["name"]).upper()
        if name == "DECIMAL":
            return f"DECIMAL({int(description['precision'])},{int(description['scale'])})"
        if name in {"CHAR", "VARCHAR"}:
            return f"{name}({int(description['length'])})"
        if name == "ARRAY":
            return f"ARRAY<{self.json_type(description['element_type'])}>"
        if name == "MAP":
            key, value = description["key_type"], description["value_type"]
            return f"MAP<{self.json_type(key)},{self.json_type(value)}>"
        if name == "STRUCT":
            fields = (
                f"{exp.to_identifier(field['name'], quoted=True).sql(dialect='databricks')}:"
                f"{self.json_type(field['type'])}"
                for field in description["fields"]
            )
            return f"STRUCT<{','.join(fields)}>"
        if name in {
            "TINYINT",
            "SMALLINT",
            "INT",
            "BIGINT",
            "FLOAT",
            "DOUBLE",
            "STRING",
            "BINARY",
            "BOOLEAN",
            "DATE",
            "TIMESTAMP_LTZ",
            "TIMESTAMP_NTZ",
            "VARIANT",
            "VOID",
        }:
            return name
        raise SqlGuardError(
            f"Databricks column type {name!r}: expected a supported DESCRIBE JSON type; "
            "cast the column to a supported type or migrate it explicitly"
        )

    def affected_rows(self, result: CursorResult[Any]) -> int:
        """Read either the protocol's modified count or SQL's explicit DML result."""
        if result.rowcount >= 0:
            return result.rowcount
        row = result.mappings().one()
        if "num_affected_rows" in row:
            return int(row["num_affected_rows"])
        return super().affected_rows(result)

    sqlglot_dialect = "databricks"
    replace_strategy: ReplaceStrategy = "create_or_replace"
    storage_parameters = frozenset({"EXTERNAL_LOCATION"})
    update_uses_merge = True
    temporary_tables = False

    def scratch_ddl(self, name: str, body: str, *, immutable: bool = False) -> tuple[str, str]:
        """Use session tables only for stages that need neither ALTER nor DELETE."""
        if immutable:
            bare = name.rsplit(".", 1)[-1]
            return bare, f"CREATE TEMPORARY TABLE {bare} AS {body}"
        return super().scratch_ddl(name, body)

    def temporary_stage_unavailable(self, error: BaseException | None) -> bool:
        """Only documented compute/catalog capability refusals permit persistent staging."""
        message = "" if error is None else str(driver_error(error))
        primary = re.match(
            r"\[(?:RequestId=[0-9a-fA-F-]+ ErrorClass=)?([A-Z][A-Z0-9_.]*)\]",
            message.lstrip(),
        )
        return primary is not None and primary[1] in _TEMPORARY_TABLE_UNAVAILABLE

    qualified_rename = True
    surrogate_key: SurrogateKey = "identity"
    identity_in_create = True
    enforces_primary_keys = False
    string_type = "STRING"
    token_username = "token"
    view_replacement = "alter"

    def identity_table_ddl(self, target: str, columns: str, params: Mapping[str, str]) -> str:
        """Declare Delta's identity before any rows are inserted."""
        location = (params.get("EXTERNAL_LOCATION") or "").strip()
        return (
            f"CREATE TABLE {target} ({columns}, ROW_ID BIGINT GENERATED ALWAYS AS IDENTITY) "
            f"{self.delta_clause(location)}"
        )

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
        """Prepare independent identity rows, then deep-clone them into the target atomically."""
        if existing and params.get("EXTERNAL_LOCATION"):
            raise SqlGuardError(
                f"{target}: replacement cannot change EXTERNAL_LOCATION; use OVERWRITE_TABLE"
            )
        ddl = (
            str(conn.execute(text(f"SHOW CREATE TABLE {target}")).scalar_one())
            if existing
            else self.identity_table_ddl(target, columns, params)
        )
        return identity_replacement(
            ddl,
            target,
            candidate,
            f"{columns}, ROW_ID BIGINT GENERATED ALWAYS AS IDENTITY",
            "databricks",
        )

    def existing_table_format(self, conn: Connection, target: str) -> TableFormat:
        """Distinguish ordinary Delta from Delta exposing Iceberg metadata through UniForm."""
        detail = conn.execute(text(f"DESCRIBE DETAIL {target}")).mappings().one()
        return self.metadata_format(target, detail["format"], detail.get("properties"))

    def metadata_format(self, target: str, format: object, properties: object) -> TableFormat:
        """Apply the same Delta and UniForm checks to either metadata response."""
        provider = str(format).lower()
        if provider != "delta":
            raise SqlGuardError(
                f"{target}: existing storage format is {provider!r}; this warehouse writes "
                "Delta or Delta with UniForm. Migrate the table explicitly or use another target"
            )
        unreadable = f"{target}: cannot read table properties to determine its format"
        try:
            if isinstance(properties, str):
                properties = json.loads(properties)
            # A mapping, or the key and value pairs some drivers return.
            if isinstance(properties, Mapping):
                properties = list(properties.items())
            if not isinstance(properties, (list, tuple)):
                raise SqlGuardError(unreadable)
            mapping = {str(key): value for key, value in properties}
        except (TypeError, ValueError) as error:
            raise SqlGuardError(unreadable) from error
        enabled = str(mapping.get("delta.universalFormat.enabledFormats", ""))
        return (
            TableFormat.ICEBERG
            if "iceberg" in {value.strip().lower() for value in enabled.split(",")}
            else TableFormat.NATIVE
        )

    def row_id_generated(self, conn: Connection, target: str) -> bool:
        """Keep older computed ROW_ID tables writable until explicitly replaced."""
        ddl = str(conn.execute(text(f"SHOW CREATE TABLE {target}")).scalar_one())
        return bool(
            re.search(
                r"\b`?ROW_ID`?\s+BIGINT\s+(?:NOT NULL\s+)?"
                r"GENERATED\s+(?:ALWAYS|BY DEFAULT)\s+AS\s+IDENTITY\b",
                ddl,
                re.I,
            )
        )

    def oauth_token(self, profile: ConnectionProfile, secret: str, url: WarehouseUrl) -> str:
        """Mint a workspace access token for a service principal (OAuth machine-to-machine).

        The token URL defaults to the workspace's own endpoint and the scope to ``all-apis``.
        """
        token_url = profile.extra.get("token_url") or f"https://{url.host}/oidc/v1/token"
        return credentials.client_credentials_token(
            str(token_url),
            str(profile.extra["client_id"]),
            secret,
            profile.extra.get("scope") or "all-apis",
        )

    def present(self, profile: ConnectionProfile, secret: str, url: WarehouseUrl) -> Presented:
        """Browser SSO is the connector's own flow; the other modes are a bearer token."""
        if profile.auth_mode == AuthMode.SSO:
            connect_args: dict[str, Any] = {"auth_type": "databricks-oauth"}
            if profile.extra.get("client_id"):
                connect_args["oauth_client_id"] = str(profile.extra["client_id"])
            return Presented(connect_args=connect_args)
        return super().present(profile, secret, url)

    def create_table_clause(self) -> str:
        """Name Delta explicitly rather than relying on the workspace default."""
        return self.delta_clause()

    def delta_clause(self, location: str = "") -> str:
        """Return ``USING DELTA``, with ``LOCATION`` for an external table, and table properties.

        Databricks expects LOCATION before TBLPROPERTIES.
        """
        clause = "USING DELTA"
        if location:
            clause += f" LOCATION '{location}'"
        properties = self.table_properties()
        return f"{clause} {properties}" if properties else clause

    def table_properties(self) -> str:
        """Return the TBLPROPERTIES clause this table format needs; none for plain Delta."""
        return ""

    def create_table_as(
        self, conn: Connection, qualified_name: str, select_sql: str, params: Mapping[str, str]
    ) -> None:
        """Run CREATE TABLE ... AS SELECT; an ``EXTERNAL_LOCATION`` parameter makes it external.

        ``EXTERNAL_LOCATION`` is the table's own path in cloud storage (``s3://``,
        ``abfss://``, ``gs://``), which Unity Catalog must cover with an external location.
        Without it, the table is managed and stored where its catalog or schema says.
        """
        problem = self.task_storage_problem(params)
        if problem:
            raise SqlGuardError(problem)
        location = (params.get("EXTERNAL_LOCATION") or "").strip()
        conn.execute(
            text(f"CREATE TABLE {qualified_name} {self.delta_clause(location)} AS {select_sql}")
        )

    def task_storage_problem(self, params: Mapping[str, str]) -> str | None:
        """Refuse an ``EXTERNAL_LOCATION`` containing a quote; it is written into the DDL."""
        location = params.get("EXTERNAL_LOCATION") or ""
        if "'" in location:
            return f"CFG_TASK_PARAMETERS.EXTERNAL_LOCATION must not contain a quote: {location!r}"
        return None

    def mirror_table_ddl(self, name: str, column_ddl: str, cloning: CloningConfig) -> str | None:
        """Create a cloning mirror; it is external under the Cloning ``Base_location`` if set."""
        problem = self.cloning_storage_problem(cloning)
        if problem:
            raise ConfigurationError(problem)
        base = cloning.base_location.rstrip("/")
        location = f"{base}/{name}" if base else ""
        return f"CREATE TABLE {name} ({column_ddl}) {self.delta_clause(location)}"

    def cloning_storage_problem(self, cloning: CloningConfig) -> str | None:
        """Refuse a ``Base_location`` containing a quote; it is written into the DDL."""
        if "'" in cloning.base_location:
            return f"Cloning Base_location must not contain a quote: {cloning.base_location!r}"
        return None

    def audit_column_type(self, column: str) -> str:
        """Use Databricks' TIMESTAMP, which is always UTC, for audit instants."""
        if column in {"CREATE_DATE", "UPDATE_DATE"}:
            return "TIMESTAMP"
        return super().audit_column_type(column)

    def timestamp_text(self, value: str, kind: str) -> str:
        """Convert the session's local representation of instants to a naive UTC value."""
        utc = (
            value
            if kind == "TIMESTAMP_NTZ"
            else f"CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'UTC', CAST({value} AS TIMESTAMP_NTZ))"
        )
        return f"REPLACE(DATE_FORMAT({utc}, 'yyyy-MM-dd HH:mm:ss.SSSSSS'), ' ', 'T')"

    session_connect_args: ClassVar[Mapping[str, Any]] = {
        "session_configuration": {"timezone": "UTC"}
    }

    def replacement_ddl(
        self,
        conn: Connection,
        target: str,
        select_sql: str,
        params: Mapping[str, str],
        *,
        existing: bool,
    ) -> str:
        """Preserve the existing table's declared properties in one CTAS replacement."""
        if existing:
            if params.get("EXTERNAL_LOCATION"):
                raise SqlGuardError(
                    f"{target}: replacement cannot change EXTERNAL_LOCATION; use OVERWRITE_TABLE"
                )
            ddl = str(conn.execute(text(f"SHOW CREATE TABLE {target}")).scalar_one())
            return replacement_ddl(ddl, target, select_sql, "databricks")
        location = (params.get("EXTERNAL_LOCATION") or "").strip()
        return f"CREATE OR REPLACE TABLE {target} {self.delta_clause(location)} AS {select_sql}"

    def overwrite_statement(self, target: str, columns: str, select_sql: str) -> str:
        """Commit a complete Delta overwrite with one statement."""
        return f"INSERT OVERWRITE TABLE {target} ({columns}) {select_sql}"
