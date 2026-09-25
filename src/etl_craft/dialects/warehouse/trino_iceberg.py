"""Trino warehouse over an Iceberg catalog."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.engine import Connection

from etl_craft.config.auth import warehouse_by_key
from etl_craft.core.enums import AuthMode
from etl_craft.core.errors import HandlerError
from etl_craft.dialects.warehouse.base import Presented, SurrogateKey, WarehouseDialect

if TYPE_CHECKING:
    from etl_craft.config import ConnectionProfile
    from etl_craft.config.targets import WarehouseUrl


class TrinoIcebergWarehouse(WarehouseDialect):
    """Trino, Iceberg catalog: no temporary tables, no alias on UPDATE or DELETE targets."""

    spec = warehouse_by_key("trino_iceberg")
    storage_parameters = frozenset({"EXTERNAL_LOCATION"})
    temporary_tables = False
    mutation_alias = False
    qualified_rename = True
    surrogate_key: SurrogateKey = "computed"
    enforces_primary_keys = False
    bearer_needs_user = False

    def present(self, profile: ConnectionProfile, secret: str, url: WarehouseUrl) -> Presented:
        """Hand the credential to the Trino client through the URL query it reads."""
        user = profile.user or None
        mode = profile.auth_mode
        if mode == AuthMode.TOKEN:
            return Presented(username=user, query={"access_token": secret})
        if mode == AuthMode.OAUTH:
            token = self.oauth_token(profile, secret, url)
            return Presented(username=user, query={"access_token": token})
        if mode == AuthMode.SSO:
            return Presented(username=user, query={"externalAuthentication": "true"})
        if mode == AuthMode.KEY_FILE:
            return Presented(
                username=user,
                query={
                    "cert": str(profile.extra["cert_file"]),
                    "key": str(profile.extra["key_file"]),
                },
            )
        return super().present(profile, secret, url)

    def create_table_as(
        self, conn: Connection, qualified_name: str, select_sql: str, params: Mapping[str, str]
    ) -> None:
        """Run CREATE TABLE ... AS SELECT; ``EXTERNAL_LOCATION`` is the Iceberg table's location.

        Without it, the catalog places the table under its schema's location.
        """
        problem = self.task_storage_problem(params)
        if problem:
            raise HandlerError(problem)
        location = (params.get("EXTERNAL_LOCATION") or "").strip()
        clause = f" WITH (location = '{location}')" if location else ""
        conn.execute(text(f"CREATE TABLE {qualified_name}{clause} AS {select_sql}"))

    def task_storage_problem(self, params: Mapping[str, str]) -> str | None:
        """Refuse an ``EXTERNAL_LOCATION`` containing a quote; it is written into the DDL."""
        location = params.get("EXTERNAL_LOCATION") or ""
        if "'" in location:
            return f"EXTERNAL_LOCATION must not contain a quote: {location!r}"
        return None

    def hash_expression(self, values: list[str]) -> str:
        """Hex-encode md5(), which takes and returns varbinary on Trino."""
        return f"lower(to_hex(md5(to_utf8({self._hash_input(values)}))))"
