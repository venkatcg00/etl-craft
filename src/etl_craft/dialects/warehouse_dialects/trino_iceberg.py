"""Trino over an Iceberg catalog.

On Trino the table format is a property of the *catalog* in the connection,
not of the statement: a table created in an Iceberg catalog is Iceberg by
construction, so CREATE TABLE takes no format clause (adding one is a syntax
error). ``validate`` checks the catalog really is Iceberg (E2-69). Every
difference below was found by running the action vocabulary against a real
Trino/Iceberg stack, not by reading documentation.
"""

from __future__ import annotations

from etl_craft.dialects.warehouse_dialects.base import WarehouseDialect


class TrinoIcebergWarehouse(WarehouseDialect):
    """Trino, Iceberg catalog."""

    key = "trino_iceberg"
    display_name = "Trino"
    sqlalchemy_name = "trino"
    table_format = "iceberg"
    # No temporary tables at all ("mismatched input" on CREATE TEMPORARY TABLE).
    # The stage is uniquely named per task run and dropped on every path, so
    # an ordinary table behaves the same.
    temporary_tables = False
    # `UPDATE tbl t SET` and `DELETE FROM tbl t` are syntax errors; the target
    # is qualified by its own table name instead (E2-65).
    mutation_alias = False
    qualified_rename = True
    # Iceberg has no identity columns, sequences or constraints.
    surrogate_key = "computed"
    enforces_primary_keys = False

    def hash_expression(self, values: list[str]) -> str:
        """Hex-encode md5(), which takes and returns varbinary on Trino."""
        return f"lower(to_hex(md5(to_utf8({self._hash_input(values)}))))"
