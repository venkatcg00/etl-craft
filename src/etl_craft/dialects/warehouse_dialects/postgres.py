"""PostgreSQL warehouse -- native tables only, and the one with no caveats.

The reference launch path. Every primitive is the ANSI default in base.py:
temporary tables, an identity-column ROW_ID with an enforced primary key,
MD5 as hex text.

There is deliberately no ``postgres_iceberg``: PostgreSQL has no Iceberg
tables without a third-party extension this project neither ships nor tests,
so asking for ``Table_format: iceberg`` on Postgres is a configuration error
rather than a silently ignored setting.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from etl_craft.dialects.engine_dialects.postgres import (
    DEFAULT_PORT,
    POSTGRES_AUTH_FIELDS,
    POSTGRES_VERIFIED_AUTH_MODES,
    psycopg_auth_kwargs,
)
from etl_craft.dialects.warehouse_dialects.base import Presented, WarehouseDialect

if TYPE_CHECKING:
    from etl_craft.config import ConnectionProfile


class PostgresWarehouse(WarehouseDialect):
    """PostgreSQL, native tables."""

    key = "postgres"
    display_name = "Postgres"
    sqlalchemy_name = "postgresql"
    # The same ways in as a PostgreSQL Engine DB: one implementation for both.
    auth_fields = POSTGRES_AUTH_FIELDS
    verified_auth_modes = POSTGRES_VERIFIED_AUTH_MODES

    def present(
        self, profile: ConnectionProfile, secret: str, parts: Mapping[str, Any]
    ) -> Presented:
        """Authenticate through psycopg's own arguments, exactly as the Engine DB does."""
        kwargs = psycopg_auth_kwargs(
            profile.auth_mode,
            user=profile.user,
            secret=secret,
            extra=profile.extra,
            host=str(parts["host"]),
            port=int(parts["port"] or DEFAULT_PORT),
        )
        # A setting the URL names itself (sslmode=verify-full) wins over a
        # default an auth mode supplies.
        query = parts.get("query") or {}
        connect_args = {name: value for name, value in kwargs.items() if name not in query}
        return Presented(username=profile.user or None, connect_args=connect_args)
