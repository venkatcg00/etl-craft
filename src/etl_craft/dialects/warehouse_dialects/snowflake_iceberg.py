"""Snowflake Iceberg tables.

A different statement, not a clause: `CREATE ICEBERG TABLE ... EXTERNAL_VOLUME
... CATALOG = 'SNOWFLAKE'`, and `ALTER ICEBERG TABLE` for anything that alters
one afterwards. Every rule below was verified against a real account.

Worth stating plainly, because they are easily confused: Snowflake *hybrid*
tables and *Iceberg* tables are different features, and a table cannot be both.
Iceberg is what "iceberg" means here.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.engine import Connection

from etl_craft.dialects.warehouse_dialects.snowflake import SnowflakeWarehouse

if TYPE_CHECKING:
    from etl_craft.config import CloningConfig

#: Snowflake's reserved EXTERNAL_VOLUME value meaning "store this table's Iceberg
#: data in Snowflake's own internal storage" -- no customer bucket, so no
#: BASE_LOCATION and no cloud IAM setup. Verified live: DDL, INSERT and CTAS all
#: succeed, and `SHOW TABLES` reports `is_iceberg: 'Y'`. This is what makes
#: Snowflake Iceberg support zero-config.
SNOWFLAKE_MANAGED_VOLUME = "SNOWFLAKE_MANAGED"


class SnowflakeIcebergWarehouse(SnowflakeWarehouse):
    """Snowflake, Iceberg tables (Snowflake-managed storage unless a volume is named)."""

    key = "snowflake_iceberg"
    table_format = "iceberg"

    def create_table_as(
        self, conn: Connection, qualified_name: str, select_sql: str, params: dict[str, str]
    ) -> None:
        """Issue CREATE ICEBERG TABLE, on the task's own volume or Snowflake's.

        EXTERNAL_VOLUME/BASE_LOCATION are ordinary CFG_TASK_PARAMETERS, because a
        team can legitimately point different targets at different volumes. A
        task naming a customer volume must pair it with BASE_LOCATION; a task
        naming neither gets SNOWFLAKE_MANAGED. `ICEBERG_VERSION = 2` is explicit
        per the original instruction to target format v2.
        """
        from etl_craft.execution import HandlerError

        external_volume = (params.get("EXTERNAL_VOLUME") or "").strip() or SNOWFLAKE_MANAGED_VOLUME
        base_location = (params.get("BASE_LOCATION") or "").strip()
        if external_volume != SNOWFLAKE_MANAGED_VOLUME and not base_location:
            raise HandlerError(
                f"snowflake needs CREATE ICEBERG TABLE with BASE_LOCATION alongside a customer "
                f"EXTERNAL_VOLUME ({external_volume!r}) — add it as a CFG_TASK_PARAMETERS value, "
                "or drop EXTERNAL_VOLUME entirely to use Snowflake's own managed storage "
                f"({SNOWFLAKE_MANAGED_VOLUME!r}) instead."
            )
        # Both are interpolated into DDL, so a quote is refused rather than escaped
        # (E2-84/E2-85).
        for value, name in ((external_volume, "EXTERNAL_VOLUME"), (base_location, "BASE_LOCATION")):
            if "'" in value:
                raise HandlerError(
                    f"CFG_TASK_PARAMETERS.{name} must not contain a quote: {value!r}"
                )
        # SNOWFLAKE_MANAGED needs no BASE_LOCATION -- there is no bucket to place
        # a path within; supplying one alongside it is what fails, verified live.
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
                "CATALOG = 'SNOWFLAKE' "
                f"{location_clause}"
                f"AS {select_sql}"
            )
        )

    def mirror_table_ddl(self, name: str, column_ddl: str, cloning: CloningConfig) -> str | None:
        """Mirror into an Iceberg table on the volume Cloning names (both settings required)."""
        external_volume = cloning.external_volume
        base_location = cloning.base_location
        if not external_volume or not base_location:
            raise ValueError(
                "Cloning to snowflake needs CREATE ICEBERG TABLE with an EXTERNAL_VOLUME and "
                "BASE_LOCATION — set External_volume and Base_location in the Cloning section "
                "of craft-connector.yml. Refusing rather than mirroring into a non-Iceberg "
                "table nothing else in the lakehouse could read."
            )
        for value, label in (
            (external_volume, "Cloning External_volume"),
            (base_location, "Cloning Base_location"),
        ):
            if "'" in value:
                raise ValueError(f"{label} must not contain a quote: {value!r}")
        return (
            f"CREATE ICEBERG TABLE {name} ({column_ddl}) "
            f"EXTERNAL_VOLUME = '{external_volume}' CATALOG = 'SNOWFLAKE' "
            f"BASE_LOCATION = '{base_location}/{name}'"
        )

    def task_storage_problem(self, params: dict[str, str]) -> str | None:
        """Require BASE_LOCATION with a customer EXTERNAL_VOLUME; managed storage needs neither."""
        volume = (params.get("EXTERNAL_VOLUME") or "").strip() or SNOWFLAKE_MANAGED_VOLUME
        if volume != SNOWFLAKE_MANAGED_VOLUME and not (params.get("BASE_LOCATION") or "").strip():
            return (
                "an Iceberg table on snowflake with a customer EXTERNAL_VOLUME needs "
                "BASE_LOCATION — use Snowflake-managed storage or specify the path within "
                "that volume"
            )
        return None

    def cloning_storage_problem(self, cloning: CloningConfig) -> str | None:
        """Cloning mirrors need both External_volume and Base_location on Snowflake Iceberg."""
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
                f"{', '.join(missing)} is not set"
            )
        return None

    def alter_table_keyword(self) -> str:
        """Snowflake refuses plain ALTER TABLE on an Iceberg table (SQLSTATE 42601)."""
        return "ALTER ICEBERG TABLE"

    def audit_column_type(self, column: str) -> str:
        """Use TIMESTAMP_NTZ(6): Snowflake Iceberg tables support no tz-aware timestamp.

        TIMESTAMP_TZ is rejected at any scale ("Unsupported data type ... for
        iceberg tables"), verified live. The engine only writes UTC instants,
        so storing them tz-naive at microsecond precision loses nothing.
        """
        if column in {"CREATE_DATE", "UPDATE_DATE"}:
            return "TIMESTAMP_NTZ(6)"
        return super().audit_column_type(column)
