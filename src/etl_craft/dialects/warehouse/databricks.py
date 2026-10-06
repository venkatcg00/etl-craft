"""Databricks warehouse, Delta tables."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from etl_craft.config.auth import warehouse_by_key
from etl_craft.core.enums import AuthMode
from etl_craft.core.errors import ConfigurationError, HandlerError
from etl_craft.dialects import credentials
from etl_craft.dialects.warehouse.base import (
    Presented,
    ReplaceStrategy,
    SurrogateKey,
    WarehouseDialect,
)
from etl_craft.dialects.warehouse.replacement import replacement_ddl

if TYPE_CHECKING:
    from etl_craft.config import CloningConfig, ConnectionProfile
    from etl_craft.config.targets import WarehouseUrl


class DatabricksWarehouse(WarehouseDialect):
    """Databricks, Delta tables. A session has no default schema, so scratch tables are named."""

    spec = warehouse_by_key("databricks")
    replace_strategy: ReplaceStrategy = "create_or_replace"
    storage_parameters = frozenset({"EXTERNAL_LOCATION"})
    update_uses_merge = True
    temporary_tables = False
    qualified_rename = True
    surrogate_key: SurrogateKey = "computed"
    enforces_primary_keys = False
    string_type = "STRING"
    token_username = "token"

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
            raise HandlerError(problem)
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

    def on_connect(self, dbapi_connection: Any, profile: ConnectionProfile, secret: str) -> None:
        """Pin every new warehouse session to UTC."""
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("SET TIME ZONE 'UTC'")
        finally:
            cursor.close()

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
                raise HandlerError(
                    f"{target}: replacement cannot change EXTERNAL_LOCATION; use OVERWRITE_TABLE"
                )
            ddl = str(conn.execute(text(f"SHOW CREATE TABLE {target}")).scalar_one())
            return replacement_ddl(ddl, target, select_sql, "databricks")
        location = (params.get("EXTERNAL_LOCATION") or "").strip()
        return f"CREATE OR REPLACE TABLE {target} {self.delta_clause(location)} AS {select_sql}"

    def overwrite_statement(self, target: str, columns: str, select_sql: str) -> str:
        """Commit a complete Delta overwrite with one statement."""
        return f"INSERT OVERWRITE TABLE {target} ({columns}) {select_sql}"
