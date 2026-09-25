"""The run log: which pipeline run a task binds to, and each task's row under it.

A task is never given a ``pipeline_run_id``. It resolves the pipeline's one ``IN-PROGRESS`` run
from ``AUD_PIPELINES_RUN_LOG``, where a partial unique index allows only one per pipeline, and
binds to one ``AUD_TASK_RUN_LOG`` row per run. A retry updates that row and counts the attempt;
it never adds a second row.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import bindparam
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError

from etl_craft.core.enums import FINISHED_RUN_STATUSES, Mode, RunStatus, SlaStatus
from etl_craft.core.errors import RunStateError
from etl_craft.core.graph import TaskRunState
from etl_craft.engine.queries import statement


def fetch_active_pipeline_run_id(conn: Connection, pipeline_id: int) -> int | None:
    """Return the ``IN-PROGRESS`` run of ``pipeline_id``, or ``None``."""
    run_id = conn.execute(
        statement(conn, "active_pipeline_run"), {"pipeline_id": pipeline_id}
    ).scalar_one_or_none()
    return None if run_id is None else int(run_id)


def find_or_create_active_run(conn: Connection, pipeline_id: int) -> int:
    """Return the ``IN-PROGRESS`` run of ``pipeline_id``, starting one when there is none.

    When several processes start a run at once, the unique index lets one insert win; the
    others read back its run.
    """
    existing = fetch_active_pipeline_run_id(conn, pipeline_id)
    if existing is not None:
        return existing
    try:
        with conn.begin_nested():
            return int(
                conn.execute(
                    statement(conn, "insert_pipeline_run"), {"pipeline_id": pipeline_id}
                ).scalar_one()
            )
    except IntegrityError:
        winner = fetch_active_pipeline_run_id(conn, pipeline_id)
        if winner is None:
            raise RunStateError(
                f"pipeline_id={pipeline_id}: starting a run hit a unique violation, but no "
                "IN-PROGRESS run exists afterwards"
            ) from None
        return winner


def resolve_run_for_task(
    conn: Connection, pipeline_id: int, *, force: bool = False, mode: Mode = Mode.LOCAL
) -> int:
    """Return the run a single ``run --task_code`` binds to.

    The pipeline's ``IN-PROGRESS`` run when there is one. Otherwise its latest run, but only with
    ``force`` when that run already finished, because binding would rewrite a finished run's
    rows. Raises ``RunStateError`` when there is no run at all, or only a finished one.
    """
    active = fetch_active_pipeline_run_id(conn, pipeline_id)
    if active is not None:
        return active
    latest = conn.execute(
        statement(conn, "latest_pipeline_run"), {"pipeline_id": pipeline_id}
    ).one_or_none()
    if latest is None:
        raise RunStateError(
            f"pipeline_id={pipeline_id} has no run to bind a single task to — start one with "
            "`etl-craft run --pipeline_code <code> --init-only`, or run the whole pipeline"
        )
    if not force and latest.status in FINISHED_RUN_STATUSES:
        remedy = (
            "Start a new run with `etl-craft run --pipeline_code <code> --init-only`, which gives "
            "it a new pipeline_run_id and leaves the finished run as it was"
        )
        if mode == Mode.LOCAL:
            remedy += ", or pass --force to bind to the finished run and rewrite its rows"
        raise RunStateError(
            f"pipeline_id={pipeline_id} has no active run: its latest run "
            f"(pipeline_run_id={latest.pipeline_run_id}) is already {latest.status}. {remedy}."
        )
    conn.execute(
        statement(conn, "touch_pipeline_run"),
        {"pipeline_run_id": latest.pipeline_run_id, "now": datetime.now(UTC)},
    )
    return int(latest.pipeline_run_id)


@dataclass(frozen=True)
class TaskRunBinding:
    """A task's row under a run: its id, its status, and whether this call created it."""

    task_run_id: int
    status: str
    created: bool = False


def find_or_create_task_run(conn: Connection, task_id: int, pipeline_run_id: int) -> TaskRunBinding:
    """Return the row of ``task_id`` under ``pipeline_run_id``, creating it ``IN-PROGRESS``.

    Like ``find_or_create_active_run``, a concurrent insert that loses reads back the winner.
    """
    params = {"task_id": task_id, "pipeline_run_id": pipeline_run_id}
    existing = conn.execute(statement(conn, "task_run"), params).one_or_none()
    if existing is not None:
        return TaskRunBinding(existing.task_run_id, existing.status)
    try:
        with conn.begin_nested():
            task_run_id = conn.execute(statement(conn, "insert_task_run"), params).scalar_one()
        return TaskRunBinding(int(task_run_id), RunStatus.IN_PROGRESS, created=True)
    except IntegrityError:
        winner = conn.execute(statement(conn, "task_run"), params).one_or_none()
        if winner is None:
            raise RunStateError(
                f"task_id={task_id}, pipeline_run_id={pipeline_run_id}: binding hit a unique "
                "violation, but no row exists afterwards"
            ) from None
        return TaskRunBinding(winner.task_run_id, winner.status)


def begin_attempt(conn: Connection, task_run_id: int) -> int:
    """Start another attempt on an existing row and return its number.

    The row goes back to ``IN-PROGRESS`` with a new START_DATE, and the previous attempt's
    counts, message and log are cleared, so the row never mixes two attempts' results.
    """
    return int(
        conn.execute(
            statement(conn, "begin_attempt"),
            {"task_run_id": task_run_id, "now": datetime.now(UTC)},
        ).scalar_one()
    )


def finish_task_run(
    conn: Connection,
    task_run_id: int,
    *,
    status: str,
    source_count: int | None = None,
    target_count: int | None = None,
    insert_count: int | None = None,
    update_count: int | None = None,
    delete_count: int | None = None,
    error_message: str | None = None,
    task_log: str | None = None,
) -> None:
    """Record the current attempt's outcome on its row; each attempt writes only its own counts."""
    conn.execute(
        statement(conn, "finish_task_run"),
        {
            "task_run_id": task_run_id,
            "status": status,
            "now": datetime.now(UTC),
            "source_count": source_count,
            "target_count": target_count,
            "insert_count": insert_count,
            "update_count": update_count,
            "delete_count": delete_count,
            "error_message": error_message,
            "task_log": task_log,
        },
    )


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
    return {row.task_id: TaskRunState(row.status, row.target_count) for row in rows}


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


def finalize_pipeline_run(
    conn: Connection, pipeline_run_id: int, status: str, *, sla_in_hours: float | None = None
) -> SlaResult | None:
    """End ``pipeline_run_id`` with ``status`` and END_DATE now.

    With ``sla_in_hours`` (SLA enforcement on, and the pipeline has one), the run is also marked
    ``MET`` or ``BREACHED``, measured from START_DATE. STATUS is left alone either way: a late
    run did its work.
    """
    now = datetime.now(UTC)
    sla = None
    if sla_in_hours is not None:
        start = conn.execute(
            statement(conn, "pipeline_run_start"), {"pipeline_run_id": pipeline_run_id}
        ).scalar_one()
        hours = elapsed_hours(start, now)
        sla = SlaResult(
            SlaStatus.BREACHED if hours > sla_in_hours else SlaStatus.MET,
            float(sla_in_hours),
            hours,
        )
    conn.execute(
        statement(conn, "finish_pipeline_run"),
        {
            "pipeline_run_id": pipeline_run_id,
            "status": status,
            "now": now,
            "sla_status": None if sla is None else str(sla.status),
        },
    )
    return sla
