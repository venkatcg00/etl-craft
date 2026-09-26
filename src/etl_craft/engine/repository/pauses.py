"""Paused pipelines, in ``AUD_PIPELINE_PAUSES``."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy.engine import Connection

from etl_craft.engine.queries import statement


@dataclass(frozen=True)
class Pause:
    """A pipeline's open pause: when, who, and why."""

    paused_at: datetime
    paused_by: str
    reason: str
    pipeline_code: str = ""

    def describe(self) -> str:
        """Return one line saying since when, by whom and why."""
        return f"paused since {self.paused_at} by {self.paused_by}: {self.reason}"


def fetch_open_pause(conn: Connection, pipeline_id: int) -> Pause | None:
    """Return the open pause of ``pipeline_id``, or ``None`` when it is not paused."""
    row = conn.execute(
        statement(conn, "open_pipeline_pause"), {"pipeline_id": pipeline_id}
    ).one_or_none()
    return None if row is None else Pause(row.paused_at, row.paused_by, row.reason)


def fetch_open_pauses(conn: Connection) -> dict[str, Pause]:
    """Return every paused pipeline's open pause, by pipeline code."""
    return {
        row.pipeline_code: Pause(row.paused_at, row.paused_by, row.reason, row.pipeline_code)
        for row in conn.execute(statement(conn, "open_pipeline_pauses"))
    }


def record_pause(conn: Connection, pipeline_id: int, paused_by: str, reason: str) -> None:
    """Open a pause of ``pipeline_id``."""
    conn.execute(
        statement(conn, "insert_pipeline_pause"),
        {
            "pipeline_id": pipeline_id,
            "now": datetime.now(UTC),
            "paused_by": paused_by,
            "reason": reason,
        },
    )


def close_pause(conn: Connection, pipeline_id: int, resumed_by: str, reason: str) -> bool:
    """Close the open pause of ``pipeline_id``; return whether it had one."""
    result = conn.execute(
        statement(conn, "resume_pipeline_pause"),
        {
            "pipeline_id": pipeline_id,
            "now": datetime.now(UTC),
            "resumed_by": resumed_by,
            "reason": reason,
        },
    )
    return bool(result.rowcount)
