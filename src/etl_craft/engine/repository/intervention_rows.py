"""What operators changed in runs, in ``AUD_RUN_INTERVENTIONS``, and the changes themselves."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import Boolean
from sqlalchemy.engine import Connection

from etl_craft.core.actor import current_actor
from etl_craft.core.enums import InterventionAction
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
    """A task's row under a run, whether an operator marked it, and who consumed it, if anyone."""

    task_run_id: int
    task_id: int
    task_code: str
    status: str
    error_message: str | None
    marked: bool
    has_rule_runs: bool
    consumed_by: str | None = None


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
            "requested_by_kind": current_actor().kind.value,
            "now": datetime.now(UTC),
        },
    )


def fetch_task_rows(conn: Connection, pipeline_run_id: int) -> list[TaskRow]:
    """Return every task row under ``pipeline_run_id``, by task code."""
    return [
        TaskRow(**row._mapping)
        for row in conn.execute(
            statement(conn, "run_task_rows").columns(marked=Boolean, has_rule_runs=Boolean),
            {"pipeline_run_id": pipeline_run_id},
        )
    ]


def fetch_interventions(
    conn: Connection, pipeline_id: int, first_run_id: int
) -> list[Intervention]:
    """Return the changes to the runs of ``pipeline_id`` from ``first_run_id`` on, oldest first."""
    return [
        Intervention(**row._mapping)
        for row in conn.execute(
            statement(conn, "run_interventions"),
            {"pipeline_id": pipeline_id, "first_run_id": first_run_id},
        )
    ]
