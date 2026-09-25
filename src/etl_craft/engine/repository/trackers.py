"""Cross-pipeline dependency trackers: upstream runs, and the run each dependency last consumed."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy.engine import Connection

from etl_craft.engine.queries import statement


@dataclass(frozen=True)
class LatestRun:
    """An upstream's most recent run, in any status, and when it started."""

    run_id: int
    status: str
    start_date: datetime


@dataclass(frozen=True)
class FinishedRun:
    """An upstream's latest finished run: its status, and whether it wrote any rows."""

    run_id: int
    status: str
    has_data: bool


def fetch_latest_pipeline_run(conn: Connection, pipeline_id: int) -> LatestRun | None:
    """Return the most recently started run of ``pipeline_id``, or ``None``."""
    row = conn.execute(
        statement(conn, "latest_pipeline_run"), {"pipeline_id": pipeline_id}
    ).one_or_none()
    return None if row is None else LatestRun(row.pipeline_run_id, row.status, row.start_date)


def fetch_latest_task_run(conn: Connection, task_id: int) -> LatestRun | None:
    """Return the latest row of ``task_id``, or ``None``."""
    row = conn.execute(statement(conn, "latest_task_run"), {"task_id": task_id}).one_or_none()
    return None if row is None else LatestRun(row.task_run_id, row.status, row.start_date)


def fetch_latest_finished_pipeline_run(
    conn: Connection, pipeline_id: int, ended_by: datetime
) -> FinishedRun | None:
    """Return the latest run of ``pipeline_id`` that finished by ``ended_by``, or ``None``.

    A pipeline run has data when any of its tasks reported a positive target count.
    """
    row = conn.execute(
        statement(conn, "latest_finished_pipeline_run"),
        {"pipeline_id": pipeline_id, "ended_by": ended_by},
    ).one_or_none()
    return None if row is None else FinishedRun(row.run_id, row.status, bool(row.has_data))


def fetch_latest_finished_task_run(conn: Connection, task_id: int) -> FinishedRun | None:
    """Return the latest finished row of ``task_id``, or ``None``."""
    row = conn.execute(
        statement(conn, "latest_finished_task_run"), {"task_id": task_id}
    ).one_or_none()
    return None if row is None else FinishedRun(row.run_id, row.status, bool(row.has_data))


def fetch_average_pipeline_seconds(conn: Connection, pipeline_id: int) -> float | None:
    """Return the average length of the finished runs of ``pipeline_id``, or ``None``."""
    seconds = conn.execute(
        statement(conn, "average_pipeline_duration"), {"pipeline_id": pipeline_id}
    ).scalar_one_or_none()
    return None if seconds is None else float(seconds)


def fetch_average_task_seconds(conn: Connection, task_id: int) -> float | None:
    """Return the average length of the finished runs of ``task_id``, or ``None``."""
    seconds = conn.execute(
        statement(conn, "average_task_duration"), {"task_id": task_id}
    ).scalar_one_or_none()
    return None if seconds is None else float(seconds)


def fetch_pipeline_last_consumed(conn: Connection, pipeline_dependency_id: int) -> int | None:
    """Return the upstream run a pipeline dependency last consumed, or ``None``."""
    value = conn.execute(
        statement(conn, "pipeline_dependency_tracker"), {"dependency_id": pipeline_dependency_id}
    ).scalar_one_or_none()
    return None if value is None else int(value)


def fetch_task_last_consumed(conn: Connection, task_dependency_id: int) -> int | None:
    """Return the upstream task run a task dependency last consumed, or ``None``."""
    value = conn.execute(
        statement(conn, "task_dependency_tracker"), {"dependency_id": task_dependency_id}
    ).scalar_one_or_none()
    return None if value is None else int(value)


def record_pipeline_consumed(
    conn: Connection,
    pipeline_dependency_id: int,
    pipeline_id: int,
    depends_on_pipeline_id: int,
    run_id: int,
) -> None:
    """Record that ``pipeline_id`` consumed upstream run ``run_id`` through the dependency."""
    conn.execute(
        statement(conn, "consume_pipeline_dependency"),
        {
            "dependency_id": pipeline_dependency_id,
            "pipeline_id": pipeline_id,
            "depends_on_pipeline_id": depends_on_pipeline_id,
            "run_id": run_id,
            "now": datetime.now(UTC),
        },
    )


def record_task_consumed(
    conn: Connection,
    task_dependency_id: int,
    task_id: int,
    pipeline_id: int,
    depends_on_task_id: int,
    depends_on_pipeline_id: int,
    run_id: int,
) -> None:
    """Record that ``task_id`` consumed upstream task run ``run_id`` through the dependency."""
    conn.execute(
        statement(conn, "consume_task_dependency"),
        {
            "dependency_id": task_dependency_id,
            "task_id": task_id,
            "pipeline_id": pipeline_id,
            "depends_on_task_id": depends_on_task_id,
            "depends_on_pipeline_id": depends_on_pipeline_id,
            "run_id": run_id,
            "now": datetime.now(UTC),
        },
    )
