"""Snowflake warehouse, ordinary tables."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from etl_craft.config.auth import warehouse_by_key
from etl_craft.core.enums import AuthMode, TableFormat
from etl_craft.core.errors import HandlerError
from etl_craft.dialects.warehouse.base import (
    Presented,
    ReplaceStrategy,
    SurrogateKey,
    WarehouseDialect,
)
from etl_craft.dialects.warehouse.replacement import identity_replacement, replacement_ddl

if TYPE_CHECKING:
    from etl_craft.config import ConnectionProfile
    from etl_craft.config.targets import WarehouseUrl


class SnowflakeWarehouse(WarehouseDialect):
    """Snowflake, ordinary tables."""

    spec = warehouse_by_key("snowflake")
    replace_strategy: ReplaceStrategy = "create_or_replace"
    identifier_case = "upper"
    qualified_rename = True
    surrogate_key: SurrogateKey = "identity"
    identity_in_create = True
    enforces_primary_keys = False
    key_file_connect_args = ("private_key_file", "private_key_file_pwd")

    def identity_table_ddl(self, target: str, columns: str, params: Mapping[str, str]) -> str:
        """Use an ordered native identity; inserts omit the managed key."""
        return (
            f"CREATE TABLE {target} ({columns}, "
            "ROW_ID BIGINT AUTOINCREMENT START 1 INCREMENT 1 ORDER)"
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
        """Clone a fully populated identity candidate, preserving the target's grants."""
        ddl = (
            str(
                conn.execute(
                    text("SELECT GET_DDL('TABLE', :target)"), {"target": target}
                ).scalar_one()
            )
            if existing
            else self.identity_table_ddl(target, columns, params)
        )
        return identity_replacement(
            ddl,
            target,
            candidate,
            f"{columns}, ROW_ID BIGINT AUTOINCREMENT START 1 INCREMENT 1 ORDER",
            "snowflake",
        )

    def is_bigint_type(self, data_type: str) -> bool:
        """Recognize native and Iceberg BIGINT's Snowflake numeric representations."""
        return data_type.upper().replace(" ", "") in {"BIGINT", "NUMBER(38,0)", "NUMBER(19,0)"}

    def existing_table_format(self, conn: Connection, target: str) -> TableFormat:
        """Read Snowflake's native/Iceberg flag in the target's own database."""
        catalog, schema, table = target.split(".")
        flag = str(
            conn.execute(
                text(
                    f"SELECT is_iceberg FROM {catalog}.information_schema.tables "
                    "WHERE lower(table_schema) = lower(:schema) "
                    "AND lower(table_name) = lower(:table)"
                ),
                {"schema": schema, "table": table},
            ).scalar_one()
        ).upper()
        if flag not in {"YES", "NO"}:
            raise HandlerError(f"{target}: cannot determine table format from IS_ICEBERG={flag!r}")
        return TableFormat.ICEBERG if flag == "YES" else TableFormat.NATIVE

    def row_id_generated(self, conn: Connection, target: str) -> bool:
        """Recognize actual native identity metadata, including targets created before this run."""
        rows = conn.execute(text(f"DESCRIBE TABLE {target}")).mappings()
        return any(
            str(row["name"]).lower() == "row_id"
            and bool(re.search(r"AUTOINCREMENT|IDENTITY", str(row["default"]), re.I))
            for row in rows
        )

    def present(self, profile: ConnectionProfile, secret: str, url: WarehouseUrl) -> Presented:
        """Name the connector's own authenticator for oauth, sso and sts."""
        user = profile.user or None
        mode = profile.auth_mode
        if mode == AuthMode.OAUTH:
            connect_args: dict[str, Any] = {
                "authenticator": "OAUTH_CLIENT_CREDENTIALS",
                "oauth_client_id": str(profile.extra["client_id"]),
                "oauth_client_secret": secret,
                "oauth_token_request_url": str(profile.extra["token_url"]),
            }
            if profile.extra.get("scope"):
                connect_args["oauth_scope"] = str(profile.extra["scope"])
            return Presented(username=user, connect_args=connect_args)
        if mode == AuthMode.SSO:
            return Presented(username=user, connect_args={"authenticator": "externalbrowser"})
        if mode == AuthMode.STS:
            return Presented(
                username=user,
                connect_args={
                    "authenticator": "WORKLOAD_IDENTITY",
                    "workload_identity_provider": "AWS",
                },
            )
        return super().present(profile, secret, url)

    def timestamp_text(self, value: str, kind: str) -> str:
        """Format UTC instants and naive UTC values without session format defaults."""
        utc = (
            f"CONVERT_TIMEZONE('UTC', {value})"
            if kind.startswith(("TIMESTAMP_TZ", "TIMESTAMP_LTZ"))
            else value
        )
        return f"TO_CHAR(CAST({utc} AS TIMESTAMP_NTZ), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6')"

    def date_text(self, value: str) -> str:
        """Ignore DATE_OUTPUT_FORMAT."""
        return f"TO_CHAR({value}, 'YYYY-MM-DD')"

    def decimal_text(self, value: str, kind: str) -> str:
        """Keep every declared fractional digit without integer padding."""
        precision, scale = [int(n) for n in kind[kind.index("(") + 1 : -1].split(",")]
        model = "FM" + "9" * max(precision - scale - 1, 0) + "0"
        if scale:
            model += "." + "0" * scale
        return f"TO_CHAR(CAST({value} AS {kind}), '{model}')"

    session_sql = (
        "ALTER SESSION SET TIMEZONE = 'UTC', "
        "TIMESTAMP_OUTPUT_FORMAT = 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6'"
    )

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
            ddl = str(
                conn.execute(
                    text("SELECT GET_DDL('TABLE', :target)"), {"target": target}
                ).scalar_one()
            )
            return replacement_ddl(ddl, target, select_sql, "snowflake")
        return f"CREATE OR REPLACE TABLE {target} AS {select_sql}"

    def overwrite_statement(self, target: str, columns: str, select_sql: str) -> str:
        """Commit deletion and insertion together, retaining the table definition."""
        return f"INSERT OVERWRITE INTO {target} ({columns}) {select_sql}"
