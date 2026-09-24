"""Snowflake warehouse, Iceberg tables."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.engine import Connection

from etl_craft.config.auth import warehouse_by_key
from etl_craft.core.errors import ConfigurationError, HandlerError
from etl_craft.core.text import is_safe_identifier
from etl_craft.dialects.warehouse.snowflake import SnowflakeWarehouse

if TYPE_CHECKING:
    from etl_craft.config import CloningConfig

SNOWFLAKE_MANAGED_VOLUME = "SNOWFLAKE_MANAGED"
"""The external volume that means Snowflake's own managed storage."""

SNOWFLAKE_CATALOG = "SNOWFLAKE"
"""The catalog that means Snowflake manages the Iceberg metadata itself."""


class SnowflakeIcebergWarehouse(SnowflakeWarehouse):
    """Snowflake, Iceberg tables: on Snowflake-managed storage unless a volume is named.

    A task names its storage with the ``EXTERNAL_VOLUME`` and ``BASE_LOCATION`` parameters,
    since a team may point different targets at different volumes; cloning mirrors use the
    Cloning section's ``External_volume`` and ``Base_location``. A task's ``CATALOG`` parameter
    names a catalog integration for a table in an externally managed Iceberg catalog; the
    default is Snowflake's own catalog. Whether Snowflake accepts writes to an external catalog
    depends on the integration.
    """

    spec = warehouse_by_key("snowflake_iceberg")

    def create_table_as(
        self, conn: Connection, qualified_name: str, select_sql: str, params: Mapping[str, str]
    ) -> None:
        """Run CREATE ICEBERG TABLE, format version 2, on the task's volume or Snowflake's."""
        problem = self.task_storage_problem(params)
        if problem:
            raise HandlerError(problem)
        external_volume = _task_volume(params)
        catalog = _task_catalog(params)
        base_location = (params.get("BASE_LOCATION") or "").strip()
        for value, name in ((external_volume, "EXTERNAL_VOLUME"), (base_location, "BASE_LOCATION")):
            if "'" in value:
                raise HandlerError(
                    f"CFG_TASK_PARAMETERS.{name} must not contain a quote: {value!r}"
                )
        location_clause = (
            f"BASE_LOCATION = '{base_location}' "
            if external_volume != SNOWFLAKE_MANAGED_VOLUME
            else ""
        )
        conn.execute(
            text(
                f"CREATE ICEBERG TABLE {qualified_name} "
                f"EXTERNAL_VOLUME = '{external_volume}' "
                "ICEBERG_VERSION = 2 "
                f"CATALOG = '{catalog}' "
                f"{location_clause}"
                f"AS {select_sql}"
            )
        )

    def task_storage_problem(self, params: Mapping[str, str]) -> str | None:
        """Check the task's storage parameters fit together.

        A customer ``EXTERNAL_VOLUME`` needs ``BASE_LOCATION``; Snowflake-managed storage needs
        neither. A ``CATALOG`` other than Snowflake's is a plain identifier and needs a
        customer volume, because an external catalog's data never lives in managed storage.
        """
        volume = _task_volume(params)
        catalog = _task_catalog(params)
        if catalog != SNOWFLAKE_CATALOG:
            if not is_safe_identifier(catalog):
                return (
                    f"CFG_TASK_PARAMETERS.CATALOG={catalog!r} must name a Snowflake catalog "
                    "integration, a plain identifier"
                )
            if volume == SNOWFLAKE_MANAGED_VOLUME:
                return (
                    f"an Iceberg table in the external catalog {catalog!r} needs an "
                    "EXTERNAL_VOLUME: Snowflake-managed storage is only for Snowflake's own "
                    "catalog"
                )
        if volume != SNOWFLAKE_MANAGED_VOLUME and not (params.get("BASE_LOCATION") or "").strip():
            return (
                f"an Iceberg table on Snowflake with a customer EXTERNAL_VOLUME ({volume!r}) "
                "needs BASE_LOCATION, the path within that volume — add it as a "
                "CFG_TASK_PARAMETERS value, or drop EXTERNAL_VOLUME to use Snowflake-managed "
                "storage"
            )
        return None

    def mirror_table_ddl(self, name: str, column_ddl: str, cloning: CloningConfig) -> str | None:
        """Mirror into an Iceberg table on the volume the Cloning section names.

        A mirror nothing else in the lakehouse could read would defeat its purpose, so both
        settings are required rather than falling back to an ordinary table.
        """
        problem = self.cloning_storage_problem(cloning)
        if problem:
            raise ConfigurationError(problem)
        for value, label in (
            (cloning.external_volume, "Cloning External_volume"),
            (cloning.base_location, "Cloning Base_location"),
        ):
            if "'" in value:
                raise ConfigurationError(f"{label} must not contain a quote: {value!r}")
        return (
            f"CREATE ICEBERG TABLE {name} ({column_ddl}) "
            f"EXTERNAL_VOLUME = '{cloning.external_volume}' CATALOG = 'SNOWFLAKE' "
            f"BASE_LOCATION = '{cloning.base_location}/{name}'"
        )

    def cloning_storage_problem(self, cloning: CloningConfig) -> str | None:
        """Require both External_volume and Base_location for Iceberg cloning mirrors."""
        missing = [
            label
            for label, value in (
                ("Cloning External_volume", cloning.external_volume),
                ("Cloning Base_location", cloning.base_location),
            )
            if not value
        ]
        if missing:
            return (
                "Cloning is enabled and mirrors would be Iceberg tables, but "
                f"{', '.join(missing)} is not set in craft-connector.yml"
            )
        return None

    def alter_table_keyword(self) -> str:
        """Return ALTER ICEBERG TABLE: Snowflake refuses plain ALTER TABLE on an Iceberg table."""
        return "ALTER ICEBERG TABLE"

    def audit_column_type(self, column: str) -> str:
        """Use TIMESTAMP_NTZ(6) for audit instants: Iceberg tables here take no zoned timestamp.

        The engine writes only UTC instants, so storing them without a zone loses nothing.
        """
        if column in {"CREATE_DATE", "UPDATE_DATE"}:
            return "TIMESTAMP_NTZ(6)"
        return super().audit_column_type(column)


def _task_volume(params: Mapping[str, str]) -> str:
    return (params.get("EXTERNAL_VOLUME") or "").strip() or SNOWFLAKE_MANAGED_VOLUME


def _task_catalog(params: Mapping[str, str]) -> str:
    return (params.get("CATALOG") or "").strip() or SNOWFLAKE_CATALOG
