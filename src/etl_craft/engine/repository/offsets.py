"""Committed task positions and bounded SQL ownership in ``AUD_TASK_OFFSET_TRACKER``."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy.engine import Connection

from etl_craft.core.errors import StaleTransitionError
from etl_craft.core.time import as_utc
from etl_craft.engine.queries import statement


@dataclass(frozen=True)
class StoredOffset:
    """A NUMBER, TEXT, TIMESTAMP or PARTITIONS cursor, with any pending timestamp owner."""

    offset_type: str
    offset_value: str | None
    pending_task_run_id: int | None = None
    pending_upper_bound: datetime | None = None


def fetch_task_offset(conn: Connection, task_id: int) -> StoredOffset | None:
    """Return the stored cursor and pending owner, or ``None`` before initialization."""
    row = conn.execute(statement(conn, "task_offset"), {"task_id": task_id}).one_or_none()
    if row is None:
        return None
    return StoredOffset(
        row.offset_type,
        row.offset_value,
        row.pending_task_run_id,
        None if row.pending_upper_bound is None else as_utc(row.pending_upper_bound),
    )


def save_task_offset(conn: Connection, task_id: int, offset: StoredOffset) -> None:
    """Store a committed cursor and clear its completed pending ownership."""
    from etl_craft.engine.timestamp_windows import checked_clock

    now = checked_clock(conn)
    conn.execute(
        statement(conn, "save_task_offset"),
        {
            "task_id": task_id,
            "offset_type": offset.offset_type,
            "offset_value": offset.offset_value,
            "now": now,
        },
    )


def commit_task_offset(
    conn: Connection, task_id: int, task_run_id: int, offset: StoredOffset
) -> None:
    """Advance a pending window only to its frozen high, in its owner's ending transaction."""
    before = fetch_task_offset(conn, task_id)
    try:
        returned = (
            as_utc(offset.offset_value)
            if (
                before is not None
                and before.pending_task_run_id is not None
                and offset.offset_type == before.offset_type
                and offset.offset_value is not None
            )
            else None
        )
    except ValueError as error:
        raise StaleTransitionError(
            f"task_id={task_id}, task_run_id={task_run_id}: invalid timestamp offset "
            f"{offset.offset_value!r}; expected the pending ISO timestamp; "
            "return the admitted high unchanged"
        ) from error
    if (
        before is not None
        and before.pending_task_run_id is not None
        and (
            before.pending_task_run_id != task_run_id
            or offset.offset_type != before.offset_type
            or offset.offset_value is None
            or returned != before.pending_upper_bound
        )
    ):
        raise StaleTransitionError(
            f"task_id={task_id}, task_run_id={task_run_id}: returned offset "
            f"{offset.offset_value!r}, pending owner={before.pending_task_run_id}, "
            f"high={before.pending_upper_bound}; resume the pending owner with its frozen high"
        )
    save_task_offset(conn, task_id, offset)
