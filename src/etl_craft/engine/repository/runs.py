"""A pipeline run's tasks and their outcomes."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.engine import Connection

from etl_craft.engine.queries import statement

PENDING = "PENDING"
"""The status reported for a task with no row under the run: it never started."""


@dataclass(frozen=True)
class TaskStatus:
    """One active task's outcome under a pipeline run."""

    task_id: int
    task_code: str
    handler: str
    status: str
    error_message: str | None
    attempt_count: int


def fetch_task_statuses_for_run(
    conn: Connection, pipeline_id: int, pipeline_run_id: int
) -> list[TaskStatus]:
    """Return every active task in ``pipeline_id`` with its status under ``pipeline_run_id``."""
    rows = conn.execute(
        statement(conn, "task_statuses_for_run"),
        {"pipeline_id": pipeline_id, "pipeline_run_id": pipeline_run_id},
    )
    return [
        TaskStatus(
            task_id=row.task_id,
            task_code=row.task_code,
            handler=row.handler,
            status=row.status or PENDING,
            error_message=row.error_message,
            attempt_count=row.attempt_count,
        )
        for row in rows
    ]
