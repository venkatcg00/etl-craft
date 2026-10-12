"""Repository definitions of external and script-landed table columns."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.engine import Connection

from etl_craft.engine.queries import statement


@dataclass(frozen=True)
class SourceColumn:
    """One active source column in its declared order."""

    table_name: str
    column_name: str
    data_type: str
    column_position: int


def fetch_source_columns(conn: Connection) -> list[SourceColumn]:
    """Read source definitions without connecting to the warehouse."""
    return [SourceColumn(**row._mapping) for row in conn.execute(statement(conn, "source_columns"))]
