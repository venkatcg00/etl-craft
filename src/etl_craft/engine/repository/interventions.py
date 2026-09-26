"""What operators changed in runs, in ``AUD_RUN_INTERVENTIONS``, and the changes themselves."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy.engine import Connection

from etl_craft.core.enums import InterventionAction, RunStatus
from etl_craft.engine.queries import statement


@dataclass(frozen=True)
class Intervention:
    """One change to a run: what, from what to what, who, when and why.

    ``task_code`` is ``None`` for a change to the run itself; ``to_status`` is ``None`` for a
    task reset to run again.
    """

    intervention_id: int
    pipeline_run_id: int
    task_code: str | None
    action: str
    from_status: str | None
    to_status: str | None
    target_count: int | None
    previous_message: str | None
    reason: str
    requested_by: str
    requested_at: datetime


@dataclass(frozen=True)
class TaskRow:
    """A task's row under a run, and whether an operator marked it."""

    task_run_id: int
    task_id: int
    task_code: str
    status: str
    error_message: str | None
    marked: bool
    has_rule_runs: bool


def record_intervention(
    conn: Connection,
    *,
    pipeline_id: int,
    pipeline_run_id: int,
    action: InterventionAction,
    reason: str,
    requested_by: str,
    task_id: int | None = None,
    from_status: str | None = None,
    to_status: str | None = None,
    target_count: int | None = None,
    previous_message: str | None = None,
) -> None:
    """Record one change an operator made to a run, or to a task under it."""
    conn.execute(
        statement(conn, "insert_intervention"),
        {
            "pipeline_id": pipeline_id,
            "pipeline_run_id": pipeline_run_id,
            "task_id": task_id,
            "action": str(action),
            "from_status": from_status,
            "to_status": to_status,
            "target_count": target_count,
            "previous_message": previous_message,
            "reason": reason,
            "requested_by": requested_by,
            "now": datetime.now(UTC),
        },
    )


def fetch_task_rows(conn: Connection, pipeline_run_id: int) -> list[TaskRow]:
    """Return every task row under ``pipeline_run_id``, by task code."""
    return [
        TaskRow(
            int(row.task_run_id),
            int(row.task_id),
            row.task_code,
            row.status,
            row.error_message,
            bool(row.marked),
            bool(row.has_rule_runs),
        )
        for row in conn.execute(
            statement(conn, "run_task_rows"), {"pipeline_run_id": pipeline_run_id}
        )
    ]


def mark_task_run(
    conn: Connection,
    task_run_id: int,
    *,
    status: str,
    error_message: str,
    target_count: int | None,
) -> None:
    """Set a task row's status as an operator marked it."""
    conn.execute(
        statement(conn, "mark_task_run"),
        {
            "task_run_id": task_run_id,
            "status": status,
            "error_message": error_message,
            "target_count": target_count,
            "sets_count": 1 if status == RunStatus.SUCCESS else 0,
            "now": datetime.now(UTC),
        },
    )


def mark_pipeline_run(conn: Connection, pipeline_run_id: int, status: str) -> None:
    """Set a run's status as an operator marked it, ending it now if it had not ended."""
    conn.execute(
        statement(conn, "mark_pipeline_run"),
        {"pipeline_run_id": pipeline_run_id, "status": status, "now": datetime.now(UTC)},
    )


def cancel_task_run(conn: Connection, task_run_id: int, error_message: str) -> bool:
    """End an ``IN-PROGRESS`` task row ``CANCELLED``; return whether it was still running."""
    result = conn.execute(
        statement(conn, "cancel_task_run"),
        {"task_run_id": task_run_id, "error_message": error_message, "now": datetime.now(UTC)},
    )
    return bool(result.rowcount)


def cancel_pipeline_run(conn: Connection, pipeline_run_id: int) -> bool:
    """End an ``IN-PROGRESS`` run ``CANCELLED``; return whether it was still in progress."""
    result = conn.execute(
        statement(conn, "cancel_pipeline_run"),
        {"pipeline_run_id": pipeline_run_id, "now": datetime.now(UTC)},
    )
    return bool(result.rowcount)


def delete_skipped_task_run(conn: Connection, task_run_id: int) -> None:
    """Remove a ``SKIPPED`` row of a task that never ran, so the resumed run decides again."""
    conn.execute(statement(conn, "delete_task_run"), {"task_run_id": task_run_id})


def fetch_interventions(
    conn: Connection, pipeline_id: int, first_run_id: int
) -> list[Intervention]:
    """Return the changes to the runs of ``pipeline_id`` from ``first_run_id`` on, oldest first."""
    return [
        Intervention(
            int(row.intervention_id),
            int(row.pipeline_run_id),
            row.task_code,
            row.action,
            row.from_status,
            row.to_status,
            None if row.target_count is None else int(row.target_count),
            row.previous_message,
            row.reason,
            row.requested_by,
            row.requested_at,
        )
        for row in conn.execute(
            statement(conn, "run_interventions"),
            {"pipeline_id": pipeline_id, "first_run_id": first_run_id},
        )
    ]
