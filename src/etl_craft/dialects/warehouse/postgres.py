"""PostgreSQL warehouse, native tables."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from etl_craft.config.auth import warehouse_by_key
from etl_craft.core.errors import SqlGuardError
from etl_craft.core.failures import Failure
from etl_craft.dialects.engine.postgres import DEFAULT_PORT, psycopg_auth_kwargs
from etl_craft.dialects.warehouse.base import Presented, ReplaceStrategy, WarehouseDialect
from etl_craft.dialects.warehouse.metadata import declarable_source, describe_result

if TYPE_CHECKING:
    from etl_craft.config import ConnectionProfile
    from etl_craft.config.targets import WarehouseUrl


class PostgresWarehouse(WarehouseDialect):
    """PostgreSQL, native tables."""

    session_fencing = True

    def session_tag_args(self, tag: str) -> Mapping[str, Any]:
        """Label the session through libpq's application_name, which caps it at 63 bytes."""
        if len(tag.encode()) > 63:
            raise SqlGuardError(f"session tag {tag!r} exceeds PostgreSQL's 63-byte limit")
        return {"application_name": tag}

    def stop_tagged(self, conn: Connection, tag: str) -> None:
        """Terminate every other backend labelled ``tag``, ending its open statement."""
        conn.execute(
            text(
                "SELECT pg_terminate_backend(pid) AS stopped FROM pg_stat_activity "
                "WHERE application_name = :tag AND pid <> pg_backend_pid()"
            ),
            {"tag": tag},
        ).all()

    def running_tagged(self, conn: Connection, tag: str) -> int:
        """Count the backends still labelled ``tag``, idle or not."""
        return int(
            conn.execute(
                text(
                    "SELECT count(*) AS running FROM pg_stat_activity "
                    "WHERE application_name = :tag AND pid <> pg_backend_pid()"
                ),
                {"tag": tag},
            ).scalar_one()
        )

    def classify_failure(self, error: BaseException) -> Failure:
        """PostgreSQL supplies DB-API classes and SQLSTATE, including lock timeouts."""
        return super().classify_failure(error)

    spec = warehouse_by_key("postgres")
    dml_rowcounts = True
    atomic_protection = True
    metadata_types_complete = True
    sqlglot_dialect = "postgres"
    secure_view = "security_barrier"
    replace_strategy: ReplaceStrategy = "transactional"
    identifier_case = "lower"

    def describe_select(self, conn: Connection, sql: str) -> list[tuple[str, str]] | None:
        """Use protocol sizes for scalar types; extensions and nested types need the catalog."""
        if not declarable_source(sql, self.sqlglot_dialect):
            return None
        return describe_result(
            conn, self.event_probe_sql(sql, 0), lambda column: self.result_type(conn, column)
        )

    def result_type(self, conn: Connection, column: object) -> str | None:
        """Resolve the driver's OID registry without an extra catalog query."""
        driver = conn.connection.dbapi_connection
        if driver is None:
            return None
        adapters = driver.adapters
        info = adapters.types.get(getattr(column, "type_code", None))
        if info is None:
            return None
        name = str(info.name)
        if name in {"varchar", "bpchar"}:
            size = getattr(column, "display_size", None)
            return (
                (f"{name}({size})" if size is not None else "VARCHAR")
                if name == "varchar"
                else (f"CHAR({size})" if size is not None else None)
            )
        if name == "numeric":
            precision, scale = getattr(column, "precision", None), getattr(column, "scale", None)
            return (
                f"NUMERIC({precision},{scale})"
                if precision is not None and scale is not None
                else None
            )
        return {
            "int2": "SMALLINT",
            "int4": "INTEGER",
            "int8": "BIGINT",
            "float4": "REAL",
            "float8": "DOUBLE PRECISION",
            "text": "TEXT",
            "bool": "BOOLEAN",
            "bytea": "BYTEA",
            "date": "DATE",
            "uuid": "UUID",
        }.get(name)

    def identity_table_ddl(self, target: str, columns: str, params: Mapping[str, str]) -> str:
        """Declare ROW_ID's identity and key in the original CREATE."""
        return (
            f"CREATE TABLE {target} ({columns}, "
            "ROW_ID BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY)"
        )

    def supports_insert_merge(self, conn: Connection) -> bool:
        """PostgreSQL added MERGE in version 15."""
        return (conn.dialect.server_version_info or (0,)) >= (15,)

    def protection_lock_sql(self, target: str) -> str:
        """Block outside row/schema changes until data and evidence commit."""
        return f"LOCK TABLE {target} IN SHARE ROW EXCLUSIVE MODE"

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

    safe_cast = "pg_input_is_valid (PostgreSQL 16 and later)"

    def reject_checks(self, value: str, kind: str) -> list[tuple[str, str]]:
        """Ask the type's own input function, which refuses an over-long text as well."""
        literal = kind.replace("'", "''")
        return [
            (
                f"NOT pg_input_is_valid(CAST({value} AS TEXT), '{literal}')",
                f"does not cast to {kind}",
            )
        ]

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

    def column_metadata_query(
        self, conn: Connection, table: str, columns: str
    ) -> tuple[str, dict[str, str | None]]:
        """Read complete modifiers, nullability and hash types from PostgreSQL's catalog once."""
        parts = table.split(".")
        name = ".".join(parts[-2:])
        return (
            f"SELECT {columns} FROM (SELECT a.attname AS column_name, "
            "format_type(a.atttypid, a.atttypmod) AS data_type, "
            "NULL AS numeric_precision, NULL AS numeric_scale, "
            "CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable, "
            "a.attnum AS ordinal_position FROM pg_attribute a "
            "WHERE a.attrelid=to_regclass(:name) AND a.attnum>0 AND NOT a.attisdropped "
            "AND (CAST(:catalog AS varchar) IS NULL OR :catalog=current_database())) columns "
            "ORDER BY ordinal_position",
            {"name": name, "catalog": parts[0] if len(parts) == 3 else None},
        )

    def full_column_types(self, conn: Connection, table: str) -> dict[str, str]:
        """Keep PostgreSQL modifiers, array element types and timestamp precision."""
        query, parameters = self.column_metadata_query(conn, table, "column_name, data_type")
        return {str(row[0]).lower(): str(row[1]) for row in conn.execute(text(query), parameters)}
