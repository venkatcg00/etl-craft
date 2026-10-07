"""Read run identities, task summaries, dependency state and SLA results.

Lifecycle writes belong to ``engine.transitions``. Tasks receive a selected pipeline run;
execution attempts keep separate immutable outcomes under each task summary.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime

from sqlalchemy import bindparam
from sqlalchemy.engine import Connection

from etl_craft.core.cron import timezone
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


def today(zone: str = "UTC") -> date:
    """Return today in the supplied project timezone; audit timestamps remain UTC."""
    return datetime.now(timezone(zone)).date()


@dataclass(frozen=True)
class RunSelector:
    """An explicit run id or pipeline-scoped key, or the single non-terminal run."""

    run_id: int | None = None
    run_key: str | None = None

    def __post_init__(self) -> None:
        """Refuse conflicting or empty identities before looking at any run."""
        if self.run_id is not None and self.run_key is not None:
            raise RunStateError("choose --run-id or --run-key, not both")
        if self.run_id is not None and self.run_id < 1:
            raise RunStateError("--run-id must be a positive integer")
        if self.run_key is not None and not self.run_key.strip():
            raise RunStateError("--run-key must not be empty")


ACTIVE_RUN = RunSelector()


@dataclass(frozen=True)
class RunIdentity:
    """A run's identity and logical date, independent of when it started."""

    pipeline_run_id: int
    run_key: str
    trigger_kind: str
    run_date: date
    status: str


def run_candidates(conn: Connection, pipeline_id: int) -> list[RunIdentity]:
    """List runs of this pipeline by id, for selection and actionable refusals."""
    return [
        RunIdentity(
            int(r.pipeline_run_id),
            str(r.run_key),
            str(r.trigger_kind),
            as_date(r.run_date),
            str(r.status),
        )
        for r in conn.execute(
            statement(conn, "pipeline_run_candidates"), {"pipeline_id": pipeline_id}
        )
    ]


def select_run(
    conn: Connection, pipeline_id: int, selector: RunSelector = ACTIVE_RUN
) -> RunIdentity:
    """Select exactly one run; never select an ended run without an explicit identity."""
    candidates = run_candidates(conn, pipeline_id)
    matches = [
        r
        for r in candidates
        if (
            r.pipeline_run_id == selector.run_id
            if selector.run_id is not None
            else r.run_key == selector.run_key
            if selector.run_key is not None
            else r.status in {"QUEUED", "IN-PROGRESS"}
        )
    ]
    if len(matches) == 1:
        return matches[0]
    listed = (
        "\n".join(
            f"id={r.pipeline_run_id} key={r.run_key} kind={r.trigger_kind} "
            f"run_date={r.run_date} status={r.status}"
            for r in candidates
        )
        or "(no runs)"
    )
    raise RunStateError(
        f"pipeline_id={pipeline_id}: run selection matched {len(matches)} runs "
        f"(--run-id={selector.run_id!r}, --run-key={selector.run_key!r}); expected exactly one. "
        "Pass --run-id or --run-key belonging to this pipeline, or start a new run with "
        "--init-only. Candidates:\n" + listed
    )


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
