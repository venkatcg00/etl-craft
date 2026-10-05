"""Cross-pipeline dependency trackers: upstream runs, and the run each dependency last consumed."""

from __future__ import annotations

from dataclasses import dataclass, field
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
    revision: int = 1
    pipeline_run_id: int | None = None


def fetch_latest_pipeline_run(conn: Connection, pipeline_id: int) -> LatestRun | None:
    """Return the most recently started run of ``pipeline_id`` outside backfills, or ``None``."""
    row = conn.execute(
        statement(conn, "latest_scheduled_pipeline_run"), {"pipeline_id": pipeline_id}
    ).one_or_none()
    return None if row is None else LatestRun(row.pipeline_run_id, row.status, row.start_date)


def fetch_latest_task_run(conn: Connection, task_id: int) -> LatestRun | None:
    """Return the latest row of ``task_id`` outside backfill runs, or ``None``."""
    row = conn.execute(
        statement(conn, "latest_scheduled_task_run"), {"task_id": task_id}
    ).one_or_none()
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
    return (
        None
        if row is None
        else FinishedRun(
            row.run_id, row.status, bool(row.has_data), row.revision, row.pipeline_run_id
        )
    )


def fetch_latest_finished_task_run(conn: Connection, task_id: int) -> FinishedRun | None:
    """Return the latest finished row of ``task_id``, or ``None``."""
    row = conn.execute(
        statement(conn, "latest_finished_task_run"), {"task_id": task_id}
    ).one_or_none()
    return (
        None
        if row is None
        else FinishedRun(
            row.run_id, row.status, bool(row.has_data), row.revision, row.pipeline_run_id
        )
    )


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


@dataclass(frozen=True)
class ConsumedRun:
    """The upstream identity and published revision last consumed by an edge."""

    run_id: int
    revision: int


def _last_consumed(conn: Connection, query: str, dependency_id: int) -> ConsumedRun | None:
    row = conn.execute(statement(conn, query), {"dependency_id": dependency_id}).one_or_none()
    return None if row is None else ConsumedRun(row.last_consumed, row.revision)


def fetch_pipeline_last_consumed(
    conn: Connection, pipeline_dependency_id: int
) -> ConsumedRun | None:
    """Return the upstream pipeline identity and revision last consumed."""
    return _last_consumed(conn, "last_consumed_pipeline_run", pipeline_dependency_id)


def fetch_task_last_consumed(conn: Connection, task_dependency_id: int) -> ConsumedRun | None:
    """Return the upstream task identity and its published pipeline revision last consumed."""
    return _last_consumed(conn, "last_consumed_task_run", task_dependency_id)


@dataclass(frozen=True)
class GateDecision:
    """An immutable judgement, including the exact upstream snapshot it inspected."""

    dependency_id: int
    task: bool
    selected: FinishedRun | None
    result: str
    reason: str
    decided_at: datetime = field(default_factory=lambda: datetime.now(UTC))


def record_decisions(
    conn: Connection,
    run_id: int,
    decisions: tuple[GateDecision, ...],
    *,
    attempt_id: int | None = None,
) -> None:
    """Record the admission snapshot in the transaction that creates its run or attempt."""
    for decision in decisions:
        selected = decision.selected
        conn.execute(
            statement(conn, "insert_gate_decision"),
            {
                "run_id": run_id,
                "attempt_id": attempt_id,
                "pipeline_dependency_id": None if decision.task else decision.dependency_id,
                "task_dependency_id": decision.dependency_id if decision.task else None,
                "selected_pipeline_run_id": None if selected is None else selected.pipeline_run_id,
                "selected_task_run_id": selected.run_id
                if selected is not None and decision.task
                else None,
                "revision": None if selected is None else selected.revision,
                "result": decision.result,
                "reason": decision.reason,
                "now": decision.decided_at,
            },
        )


def consume_pipeline_decisions(conn: Connection, run_id: int) -> None:
    """Consume only the successful run's recorded satisfied admission snapshots."""
    conn.execute(
        statement(conn, "consume_pipeline_decisions"), {"run_id": run_id, "now": datetime.now(UTC)}
    )


def consume_task_decisions(
    conn: Connection, task_id: int, run_id: int, *, attempt_id: int | None = None
) -> None:
    """Consume snapshots of successful attempts without rereading upstream history or edges."""
    conn.execute(
        statement(conn, "consume_task_decisions"),
        {"task_id": task_id, "run_id": run_id, "attempt_id": attempt_id, "now": datetime.now(UTC)},
    )
