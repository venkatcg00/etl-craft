"""Column lineage stored per SQL task, in ``AUD_COLUMN_LINEAGE``."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy.engine import Connection

from etl_craft.engine.queries import statement


@dataclass(frozen=True)
class SqlTaskRef:
    """An active SQL task of an active pipeline."""

    task_id: int
    pipeline_code: str
    task_code: str


def fetch_sql_tasks(conn: Connection) -> list[SqlTaskRef]:
    """Return every active SQL task, by pipeline and task code."""
    rows = conn.execute(statement(conn, "sql_tasks"))
    return [SqlTaskRef(r.task_id, r.pipeline_code, r.task_code) for r in rows]


@dataclass(frozen=True)
class StoredEdge:
    """One target column and one column it is made from; no source for a constant or a count."""

    target_object: str
    target_column: str
    source_object: str | None
    source_column: str | None
    transformation: str


def fetch_task_lineage(conn: Connection, task_id: int, source_sql_hash: str) -> list[StoredEdge]:
    """Return the lineage stored for ``task_id`` from the SELECT hashed ``source_sql_hash``."""
    rows = conn.execute(
        statement(conn, "task_lineage"), {"task_id": task_id, "source_sql_hash": source_sql_hash}
    )
    return [
        StoredEdge(
            r.target_object, r.target_column, r.source_object, r.source_column, r.transformation
        )
        for r in rows
    ]


def store_task_lineage(
    conn: Connection, task_id: int, source_sql_hash: str, edges: Sequence[StoredEdge]
) -> None:
    """Replace the lineage stored for ``task_id``."""
    conn.execute(statement(conn, "delete_task_lineage"), {"task_id": task_id})
    now = datetime.now(UTC)
    if edges:
        conn.execute(
            statement(conn, "insert_lineage_edge"),
            [
                {
                    "task_id": task_id,
                    "source_sql_hash": source_sql_hash,
                    "target_object": e.target_object,
                    "target_column": e.target_column,
                    "source_object": e.source_object,
                    "source_column": e.source_column,
                    "transformation": e.transformation,
                    "now": now,
                }
                for e in edges
            ],
        )
