"""Immutable event evidence and file consumption for successful task runs."""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy.engine import Connection

from etl_craft.engine.queries import statement


def consumed_files(conn: Connection, task_id: int) -> set[str]:
    """Return canonical paths already consumed by this event task."""
    return set(
        conn.execute(statement(conn, "event_consumed_files"), {"task_id": task_id}).scalars()
    )


def record_event(
    conn: Connection, task_id: int, task_run_id: int, evidence: dict[str, Any]
) -> None:
    """Store evidence and reserve each file in the successful outcome's transaction."""
    conn.execute(
        statement(conn, "insert_task_event"),
        {
            "task_run_id": task_run_id,
            "evidence": json.dumps(evidence, default=str),
        },
    )
    for file in evidence.get("files", []):
        conn.execute(
            statement(conn, "consume_event_file"),
            {
                "task_id": task_id,
                "task_run_id": task_run_id,
                "file_path": file["path"],
            },
        )


def run_event_files(conn: Connection, pipeline_run_id: int) -> tuple[str, ...]:
    """Return deduplicated paths from successful event tasks under this exact run."""
    paths = (
        file["path"]
        for row in conn.execute(
            statement(conn, "run_event_files"), {"pipeline_run_id": pipeline_run_id}
        )
        for file in json.loads(row.evidence).get("files", [])
    )
    return tuple(dict.fromkeys(paths))
