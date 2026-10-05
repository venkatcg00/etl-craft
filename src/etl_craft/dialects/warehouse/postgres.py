"""PostgreSQL warehouse, native tables."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from etl_craft.config.auth import warehouse_by_key
from etl_craft.dialects.engine.postgres import DEFAULT_PORT, psycopg_auth_kwargs
from etl_craft.dialects.warehouse.base import Presented, WarehouseDialect

if TYPE_CHECKING:
    from etl_craft.config import ConnectionProfile
    from etl_craft.config.targets import WarehouseUrl


class PostgresWarehouse(WarehouseDialect):
    """PostgreSQL, native tables."""

    spec = warehouse_by_key("postgres")
    identifier_case = "lower"

    def present(self, profile: ConnectionProfile, secret: str, url: WarehouseUrl) -> Presented:
        """Authenticate through psycopg's own arguments, exactly as the Engine DB does.

        A setting the URL's query already names (sslmode) is left to the URL.
        """
        kwargs = psycopg_auth_kwargs(
            profile.auth_mode,
            user=profile.user,
            secret=secret,
            extra=profile.extra,
            host=str(url.host),
            port=int(url.port or DEFAULT_PORT),
        )
        connect_args = {name: value for name, value in kwargs.items() if name not in url.query}
        return Presented(username=profile.user or None, connect_args=connect_args)

    def timestamp_text(self, value: str, kind: str) -> str:
        """Normalize instants to UTC; naive timestamps already represent UTC."""
        utc = (
            f"({value} AT TIME ZONE 'UTC')"
            if "WITH TIME ZONE" in kind or kind == "TIMESTAMPTZ"
            else value
        )
        return f"TO_CHAR({utc}, 'YYYY-MM-DD\"T\"HH24:MI:SS.US')"

    def date_text(self, value: str) -> str:
        """Use ISO dates even when DateStyle differs."""
        return f"TO_CHAR({value}, 'YYYY-MM-DD')"

    def on_connect(self, dbapi_connection: Any, profile: ConnectionProfile, secret: str) -> None:
        """Pin every new warehouse session to UTC."""
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("SET TimeZone = 'UTC'")
        finally:
            cursor.close()
        dbapi_connection.commit()

    def prepare_update_stage(self, stage: str, keys: tuple[str, ...]) -> tuple[str, ...]:
        """Give the planner merge-key access and current temporary-stage statistics."""
        return (f"CREATE INDEX ON {stage} ({', '.join(keys)})", f"ANALYZE {stage}")
