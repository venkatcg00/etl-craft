"""Warehouse dialects: one per database and table format.

    postgres            native tables
    duckdb              native tables in a local file
    duckdb_iceberg      DuckDB compute over an Iceberg REST catalog
    trino_iceberg       Trino over an Iceberg catalog
    databricks          Delta tables
    databricks_iceberg  Delta tables with UniForm, readable as Iceberg
    snowflake           ordinary Snowflake tables
    snowflake_iceberg   Snowflake Iceberg tables

A task's dialect comes from the warehouse connection and the task's table format: its own
``TABLE_FORMAT`` parameter, else ``Warehouse.Table_format``. The vendors' SQLAlchemy dialects are
optional extras; nothing here imports them.
"""

from etl_craft.dialects.warehouse.base import AUDIT_COLUMN_TYPES, Presented, WarehouseDialect
from etl_craft.dialects.warehouse.registry import all_dialects, for_key, resolve

__all__ = [
    "AUDIT_COLUMN_TYPES",
    "Presented",
    "WarehouseDialect",
    "all_dialects",
    "for_key",
    "resolve",
]
