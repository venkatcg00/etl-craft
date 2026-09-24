"""Databricks warehouse, Delta tables readable as Iceberg."""

from __future__ import annotations

from etl_craft.config.auth import warehouse_by_key
from etl_craft.dialects.warehouse.databricks import DatabricksWarehouse


class DatabricksIcebergWarehouse(DatabricksWarehouse):
    """Databricks, Delta tables with UniForm, so external engines read them as Iceberg."""

    spec = warehouse_by_key("databricks_iceberg")

    def table_properties(self) -> str:
        """Enable UniForm's Iceberg metadata on the Delta table."""
        return (
            "TBLPROPERTIES ('delta.enableIcebergCompatV2' = 'true', "
            "'delta.universalFormat.enabledFormats' = 'iceberg')"
        )
