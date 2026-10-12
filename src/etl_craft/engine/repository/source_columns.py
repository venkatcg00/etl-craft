"""Repository definitions of external and script-landed table columns."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection

from etl_craft.engine.queries import statement


@dataclass(frozen=True)
class SourceColumn:
    """One active source column in its declared order."""

    table_name: str
    column_name: str
    data_type: str
    column_position: int
    is_nullable: str | None = None
    object_kind: str | None = None


def fetch_source_columns(conn: Connection) -> list[SourceColumn]:
    """Read source definitions without connecting to the warehouse."""
    name = "cfg_columns" if conn.dialect.name == "postgresql" else "CFG_COLUMNS"
    if not inspect(conn).has_table(name):
        return [
            SourceColumn(*row)
            for row in conn.execute(
                text(
                    "SELECT TABLE_NAME AS table_name, COLUMN_NAME AS column_name, "
                    "DATA_TYPE AS data_type, "
                    "COLUMN_POSITION AS column_position FROM CFG_SOURCE_COLUMNS "
                    "WHERE ACTIVE_FLAG='Y' ORDER BY lower(TABLE_NAME), COLUMN_POSITION"
                )
            )
        ]
    return [
        SourceColumn(
            ".".join(
                part
                for part in (row.catalog_name, row.schema_name, row.object_name)
                if part is not None
            ),
            row.column_name,
            row.data_type,
            row.column_position,
            row.is_nullable,
            row.object_kind,
        )
        for row in conn.execute(statement(conn, "source_columns"))
    ]
