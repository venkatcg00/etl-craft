"""Ingestion offsets: where each script task left off, in ``AUD_TASK_OFFSET_TRACKER``."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy.engine import Connection

from etl_craft.engine.queries import statement


@dataclass(frozen=True)
class StoredOffset:
    """An offset as stored: its type (``NUMBER``, ``TEXT`` or ``TIMESTAMP``) and its text."""

    offset_type: str
    offset_value: str | None


def fetch_task_offset(conn: Connection, task_id: int) -> StoredOffset | None:
    """Return the offset ``task_id`` stored, or ``None`` before its first successful run."""
    row = conn.execute(statement(conn, "task_offset"), {"task_id": task_id}).one_or_none()
    return None if row is None else StoredOffset(row.offset_type, row.offset_value)


def save_task_offset(conn: Connection, task_id: int, offset: StoredOffset) -> None:
    """Store ``offset`` as where ``task_id`` left off."""
    conn.execute(
        statement(conn, "save_task_offset"),
        {
            "task_id": task_id,
            "offset_type": offset.offset_type,
            "offset_value": offset.offset_value,
            "now": datetime.now(UTC),
        },
    )
