"""PostgreSQL warehouse, native tables."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from etl_craft.config.auth import warehouse_by_key
from etl_craft.core.errors import SqlGuardError
from etl_craft.dialects.engine.postgres import DEFAULT_PORT, psycopg_auth_kwargs
from etl_craft.dialects.warehouse.base import Presented, ReplaceStrategy, WarehouseDialect

if TYPE_CHECKING:
    from etl_craft.config import ConnectionProfile
    from etl_craft.config.targets import WarehouseUrl


class PostgresWarehouse(WarehouseDialect):
    """PostgreSQL, native tables."""

    secure_view = "security_barrier"

    spec = warehouse_by_key("postgres")
    replace_strategy: ReplaceStrategy = "transactional"
    identifier_case = "lower"

    def present(self, profile: ConnectionProfile, secret: str, url: WarehouseUrl) -> Presented:
        """Authenticate through psycopg's own arguments, exactly as the Engine DB does.

        A setting the URL's query already names (sslmode) is left to the URL.
        """
        kwargs = psycopg_auth_kwargs(
            profile.auth_mode,
            user=profile.user,
            secret=secret,
            extra=profile.extra,
            host=str(url.host),
            port=int(url.port or DEFAULT_PORT),
        )
        connect_args = {name: value for name, value in kwargs.items() if name not in url.query}
        return Presented(username=profile.user or None, connect_args=connect_args)

    def timestamp_text(self, value: str, kind: str) -> str:
        """Normalize instants to UTC; naive timestamps already represent UTC."""
        utc = (
            f"({value} AT TIME ZONE 'UTC')"
            if "WITH TIME ZONE" in kind or kind == "TIMESTAMPTZ"
            else value
        )
        return f"TO_CHAR({utc}, 'YYYY-MM-DD\"T\"HH24:MI:SS.US')"

    def date_text(self, value: str) -> str:
        """Use ISO dates even when DateStyle differs."""
        return f"TO_CHAR({value}, 'YYYY-MM-DD')"

    session_sql = "SET TimeZone = 'UTC'"

    def on_connect(self, dbapi_connection: Any, profile: ConnectionProfile, secret: str) -> None:
        """Pin UTC and commit the setting before PostgreSQL's first task transaction."""
        super().on_connect(dbapi_connection, profile, secret)
        dbapi_connection.commit()

    def prepare_update_stage(self, stage: str, keys: tuple[str, ...]) -> tuple[str, ...]:
        """Give the planner merge-key access and current temporary-stage statistics."""
        return (f"CREATE INDEX ON {stage} ({', '.join(keys)})", f"ANALYZE {stage}")

    def replacement_comment(self, conn: Connection, target: str) -> str | None:
        """Preserve comments and refuse rebuilding a partitioned parent as an ordinary table."""
        name = ".".join(target.split(".")[-2:])
        row = conn.execute(
            text(
                "SELECT relkind, obj_description(oid) AS comment FROM pg_class "
                "WHERE oid = CAST(:name AS regclass)"
            ),
            {"name": name},
        ).one()
        if row[0] == "p":
            raise SqlGuardError(f"{target}: CTAS cannot preserve partitioning; use OVERWRITE_TABLE")
        return None if row[1] is None else str(row[1])

    def full_column_types(self, conn: Connection, table: str) -> dict[str, str]:
        """Keep PostgreSQL modifiers, array element types and timestamp precision."""
        name = ".".join(table.split(".")[-2:])
        rows = conn.execute(
            text(
                "SELECT attname AS column_name, format_type(atttypid, atttypmod) AS data_type "
                "FROM pg_attribute WHERE attrelid = CAST(:name AS regclass) "
                "AND attnum > 0 AND NOT attisdropped ORDER BY attnum"
            ),
            {"name": name},
        ).all()
        return {str(row[0]).lower(): str(row[1]) for row in rows}
