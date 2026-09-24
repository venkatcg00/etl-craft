"""Warehouse dialects: one module per database and table format.

    postgres            native tables (the reference launch path)
    duckdb              native tables in a local file (local development)
    duckdb_iceberg      DuckDB compute over an Iceberg REST catalog
    trino_iceberg       Trino over an Iceberg catalog
    databricks          Delta tables
    databricks_iceberg  Delta tables with UniForm (Iceberg-readable)
    snowflake           ordinary Snowflake tables
    snowflake_iceberg   Snowflake Iceberg tables

A task's dialect is chosen from the warehouse's connection *and* the task's
resolved TABLE_FORMAT (its own CFG_TASK_PARAMETERS value, else
Warehouse.Table_format). There is no ``postgres_iceberg``: PostgreSQL has no
Iceberg tables without an extension this project neither ships nor tests.
"""

from __future__ import annotations

from etl_craft.dialects.warehouse_dialects.base import WarehouseDialect
from etl_craft.dialects.warehouse_dialects.databricks import DatabricksWarehouse
from etl_craft.dialects.warehouse_dialects.databricks_iceberg import DatabricksIcebergWarehouse
from etl_craft.dialects.warehouse_dialects.duckdb import DuckDBWarehouse
from etl_craft.dialects.warehouse_dialects.duckdb_iceberg import DuckDBIcebergWarehouse
from etl_craft.dialects.warehouse_dialects.postgres import PostgresWarehouse
from etl_craft.dialects.warehouse_dialects.snowflake import SnowflakeWarehouse
from etl_craft.dialects.warehouse_dialects.snowflake_iceberg import SnowflakeIcebergWarehouse
from etl_craft.dialects.warehouse_dialects.trino_iceberg import TrinoIcebergWarehouse

ALL: tuple[WarehouseDialect, ...] = (
    PostgresWarehouse(),
    DuckDBWarehouse(),
    DuckDBIcebergWarehouse(),
    TrinoIcebergWarehouse(),
    DatabricksWarehouse(),
    DatabricksIcebergWarehouse(),
    SnowflakeWarehouse(),
    SnowflakeIcebergWarehouse(),
)

_BY_KEY = {dialect.key: dialect for dialect in ALL}

# JDBC scheme -> the dialect that parses that vendor's URL. The parser is the
# same for both formats of one vendor.
_BY_SCHEME = {
    "postgresql": _BY_KEY["postgres"],
    "duckdb": _BY_KEY["duckdb"],
    "trino": _BY_KEY["trino_iceberg"],
    "databricks": _BY_KEY["databricks"],
    "snowflake": _BY_KEY["snowflake"],
}

#: Warehouse.Name values, lower-cased, to the SQLAlchemy dialect each implies.
NAMES = {
    "postgres": "postgresql",
    "duckdb": "duckdb",
    "trino": "trino",
    "databricks": "databricks",
    "snowflake": "snowflake",
}


class UnsupportedWarehouse(ValueError):
    """Raised when a warehouse and table format combination has no dialect."""


def for_key(key: str) -> WarehouseDialect:
    """Return the dialect module registered under `key`."""
    return _BY_KEY[key]


def for_scheme(scheme: str) -> WarehouseDialect | None:
    """Return the dialect that parses JDBC URLs with `scheme`, if one does."""
    return _BY_SCHEME.get(scheme.lower())


def resolve(sqlalchemy_name: str, table_format: str) -> WarehouseDialect:
    """Choose the dialect for a warehouse connection and a resolved table format.

    Trino is Iceberg whichever format is asked for -- the catalog decides, and
    ``validate`` checks it is an Iceberg catalog. A SQLAlchemy dialect this
    registry does not know is treated as a plain ANSI warehouse ("any SQL tool
    over plain Iceberg"), which is what its generic parser already assumed.
    """
    name = sqlalchemy_name.split("+", 1)[0]
    if name == "trino":
        return _BY_KEY["trino_iceberg"]
    if name == "postgresql" and table_format == "iceberg":
        raise UnsupportedWarehouse(
            "PostgreSQL tables are always native: TABLE_FORMAT iceberg is not available on a "
            "Postgres warehouse (there is no postgres_iceberg dialect)"
        )
    for dialect in ALL:
        if dialect.sqlalchemy_name == name and dialect.table_format == table_format:
            return dialect
    generic = WarehouseDialect()
    generic.key = name
    generic.display_name = name
    generic.sqlalchemy_name = name
    generic.table_format = table_format
    generic.surrogate_key = "computed"
    generic.enforces_primary_keys = False
    return generic
