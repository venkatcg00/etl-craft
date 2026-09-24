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

from etl_craft.dialects.warehouse_dialects.base import WarehouseDialect


class PostgresWarehouse(WarehouseDialect):
    """PostgreSQL, native tables."""

    key = "postgres"
    display_name = "Postgres"
    sqlalchemy_name = "postgresql"
