"""Every warehouse dialect, and the one a connection and table format select."""

from __future__ import annotations

from functools import cache

from etl_craft.config.auth import warehouse_spec
from etl_craft.dialects.warehouse.base import WarehouseDialect
from etl_craft.dialects.warehouse.databricks import DatabricksWarehouse
from etl_craft.dialects.warehouse.databricks_iceberg import DatabricksIcebergWarehouse
from etl_craft.dialects.warehouse.duckdb import DuckDBWarehouse
from etl_craft.dialects.warehouse.duckdb_iceberg import DuckDBIcebergWarehouse
from etl_craft.dialects.warehouse.postgres import PostgresWarehouse
from etl_craft.dialects.warehouse.snowflake import SnowflakeWarehouse
from etl_craft.dialects.warehouse.snowflake_iceberg import SnowflakeIcebergWarehouse
from etl_craft.dialects.warehouse.trino_iceberg import TrinoIcebergWarehouse


@cache
def all_dialects() -> tuple[WarehouseDialect, ...]:
    """Return one instance of every warehouse dialect."""
    return (
        PostgresWarehouse(),
        DuckDBWarehouse(),
        DuckDBIcebergWarehouse(),
        TrinoIcebergWarehouse(),
        DatabricksWarehouse(),
        DatabricksIcebergWarehouse(),
        SnowflakeWarehouse(),
        SnowflakeIcebergWarehouse(),
    )


def for_key(key: str) -> WarehouseDialect:
    """Return the dialect registered under ``key``, such as ``trino_iceberg``."""
    for dialect in all_dialects():
        if dialect.spec.key == key:
            return dialect
    raise LookupError(f"no warehouse dialect named {key!r}")


def resolve(sqlalchemy_name: str, table_format: str) -> WarehouseDialect:
    """Choose the dialect for a connection's SQLAlchemy name and a table format.

    Trino is Iceberg whichever format is asked for, because its catalog decides; PostgreSQL has
    no Iceberg tables; an unsupported database is a ``ConfigurationError``.
    """
    return for_key(warehouse_spec(sqlalchemy_name, table_format).key)
