"""Databricks SQL, Iceberg-readable tables: managed Delta with UniForm enabled.

[DEVIATION, 2026-09-23] Not `USING ICEBERG`, per explicit instruction ("it
should be uniform tables in databricks"). `USING ICEBERG` works too, but makes
a Unity-Catalog-managed Iceberg table with no Delta log at all. UniForm keeps
Delta as the format Databricks reads and writes -- so every UPDATE/MERGE the
engine issues is unchanged -- while generating Iceberg metadata beside it for
external engines. `DESCRIBE DETAIL` reports `format: 'delta'` plus
`delta.universalFormat.enabledFormats: iceberg`, verified live.
"""

from __future__ import annotations

from etl_craft.dialects.warehouse_dialects.databricks import DatabricksWarehouse


class DatabricksIcebergWarehouse(DatabricksWarehouse):
    """Databricks, Delta tables with UniForm (Iceberg-readable)."""

    key = "databricks_iceberg"
    table_format = "iceberg"

    def create_table_clause(self) -> str:
        """Enable UniForm so external engines can read the table as Iceberg."""
        return (
            "USING DELTA TBLPROPERTIES "
            "('delta.enableIcebergCompatV2' = 'true', "
            "'delta.universalFormat.enabledFormats' = 'iceberg')"
        )
