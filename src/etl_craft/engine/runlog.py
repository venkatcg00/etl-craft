"""Read run identities, task summaries, dependency state and SLA results.

Lifecycle writes belong to ``engine.transitions``. Tasks resolve the active pipeline run;
execution attempts keep separate immutable outcomes under each task summary.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime

from sqlalchemy import bindparam
from sqlalchemy.engine import Connection

from etl_craft.core.enums import SlaStatus
from etl_craft.core.errors import RunStateError
from etl_craft.core.graph import TaskRunState
from etl_craft.engine.queries import statement


def fetch_active_pipeline_run_id(conn: Connection, pipeline_id: int) -> int | None:
    """Return the ``IN-PROGRESS`` run of ``pipeline_id``, or ``None``."""
    run_id = conn.execute(
        statement(conn, "active_pipeline_run"), {"pipeline_id": pipeline_id}
    ).scalar_one_or_none()
    return None if run_id is None else int(run_id)


def today() -> date:
    """Return today's date in UTC: a run's ``RUN_DATE`` unless it is given one."""
    return datetime.now(UTC).date()


def latest_run_for_rerun(conn: Connection, pipeline_id: int) -> int:
    """Return the run ``--rerun`` acts on: the run in progress, else the latest one.

    Nothing is changed; the caller reopens the run only once it knows the task will run.
    Raises ``RunStateError`` when the pipeline has no run at all.
    """
    active = fetch_active_pipeline_run_id(conn, pipeline_id)
    if active is not None:
        return active
    latest = conn.execute(
        statement(conn, "latest_pipeline_run"), {"pipeline_id": pipeline_id}
    ).one_or_none()
    if latest is None:
        raise RunStateError(
            f"pipeline_id={pipeline_id} has no run to rerun a task in; run the pipeline first"
        )
    return int(latest.pipeline_run_id)


@dataclass(frozen=True)
class RunKind:
    """The date a run runs as of, and whether it is part of a backfill."""

    run_date: date
    backfill: bool


def fetch_run_kind(conn: Connection, pipeline_run_id: int) -> RunKind:
    """Return the run date and backfill flag of ``pipeline_run_id``, which must exist."""
    row = conn.execute(
        statement(conn, "pipeline_run_kind"), {"pipeline_run_id": pipeline_run_id}
    ).one()
    return RunKind(as_date(row.run_date), row.backfill == "Y")


def as_date(value: object) -> date:
    """Read a ``DATE`` column: a ``date`` from PostgreSQL, ``YYYY-MM-DD`` text from SQLite."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


@dataclass(frozen=True)
class TaskRunBinding:
    """A task's row under a run: its id, its status, and whether this call created it."""

    task_run_id: int
    status: str
    created: bool = False


@dataclass(frozen=True)
class TaskRunResult:
    """A task run's status, error message and attempt count, read back after it ran."""

    status: str
    error_message: str | None
    attempt_count: int


def fetch_task_run_result(conn: Connection, task_run_id: int) -> TaskRunResult:
    """Return the current status of ``task_run_id``; a crashed task is still ``IN-PROGRESS``."""
    row = conn.execute(statement(conn, "task_run_result"), {"task_run_id": task_run_id}).one()
    return TaskRunResult(row.status, row.error_message, row.attempt_count)


def fetch_task_run_status(conn: Connection, task_id: int, pipeline_run_id: int) -> str | None:
    """Return the status of ``task_id`` under ``pipeline_run_id``, or ``None`` if it has no row."""
    row = conn.execute(
        statement(conn, "task_run"), {"task_id": task_id, "pipeline_run_id": pipeline_run_id}
    ).one_or_none()
    return None if row is None else str(row.status)


def fetch_pipeline_run_status(conn: Connection, pipeline_run_id: int) -> str:
    """Return the status of ``pipeline_run_id``, which must exist."""
    return str(
        conn.execute(
            statement(conn, "pipeline_run_status"), {"pipeline_run_id": pipeline_run_id}
        ).scalar_one()
    )


def fetch_run_state(
    conn: Connection, pipeline_run_id: int, task_ids: Sequence[int]
) -> dict[int, TaskRunState]:
    """Return the state of each of ``task_ids`` under the run, as the dependency graph reads it.

    A task with no row is left out: it has not run.
    """
    if not task_ids:
        return {}
    query = statement(conn, "run_state").bindparams(bindparam("task_ids", expanding=True))
    rows = conn.execute(query, {"pipeline_run_id": pipeline_run_id, "task_ids": list(task_ids)})
    return {
        row.task_id: TaskRunState(row.status, row.target_count, row.rows_written) for row in rows
    }


@dataclass(frozen=True)
class SlaResult:
    """How a finished run measured against its pipeline's ``SLA_IN_HOURS``."""

    status: SlaStatus
    sla_hours: float
    elapsed_hours: float

    def describe(self) -> str:
        """Return one line for an outcome message or an alert."""
        return f"SLA of {self.sla_hours:g} h {self.status} (ran {self.elapsed_hours:.2f} h)"


def elapsed_hours(start: datetime, now: datetime) -> float:
    """Return the hours from ``start`` to ``now``, reading a naive ``start`` as UTC."""
    if start.tzinfo is None:
        start = start.replace(tzinfo=UTC)
    return (now - start).total_seconds() / 3600


@dataclass(frozen=True)
class RunEnding:
    """Whether ``finalize_pipeline_run`` ended the run, and its SLA as recorded."""

    ended: bool
    sla: SlaResult | None


@dataclass(frozen=True)
class RunSla:
    """When a run started, and the SLA status recorded for it so far."""

    start_date: datetime
    sla_status: str | None


def fetch_run_sla(conn: Connection, pipeline_run_id: int) -> RunSla:
    """Return when ``pipeline_run_id`` started and its SLA status so far."""
    row = conn.execute(
        statement(conn, "pipeline_run_sla"), {"pipeline_run_id": pipeline_run_id}
    ).one()
    return RunSla(row.start_date, row.sla_status)
