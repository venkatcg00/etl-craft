"""PostgreSQL warehouse, native tables."""

from __future__ import annotations

from typing import TYPE_CHECKING

from etl_craft.config.auth import warehouse_by_key
from etl_craft.dialects.engine.postgres import DEFAULT_PORT, psycopg_auth_kwargs
from etl_craft.dialects.warehouse.base import Presented, WarehouseDialect

if TYPE_CHECKING:
    from etl_craft.config import ConnectionProfile
    from etl_craft.config.targets import WarehouseUrl


class PostgresWarehouse(WarehouseDialect):
    """PostgreSQL, native tables."""

    spec = warehouse_by_key("postgres")

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
