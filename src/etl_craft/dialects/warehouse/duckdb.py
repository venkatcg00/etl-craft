"""DuckDB warehouse, native tables in one file."""

from __future__ import annotations

from etl_craft.config.auth import warehouse_by_key
from etl_craft.dialects.warehouse.base import SurrogateKey, WarehouseDialect


class DuckDBWarehouse(WarehouseDialect):
    """DuckDB, native tables in one file.

    Only one process may write a DuckDB file at a time, so warehouse access is queued.
    """

    spec = warehouse_by_key("duckdb")
    surrogate_key: SurrogateKey = "sequence"
    single_writer = True
