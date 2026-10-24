"""Run and SLA evidence read for an attempt's email report."""

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy.engine import Connection

from etl_craft.core.definitions import using
from etl_craft.core.enums import SlaStatus
from etl_craft.core.errors import MetadataError
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pipelines import fetch_pipeline_detail, resolve_pipeline_id
from etl_craft.engine.repository.runs import (
    TaskStatus,
    fetch_latest_pipeline_run,
    fetch_task_statuses_for_run,
)
from etl_craft.engine.repository.tasks import FailureWatchMessage, fetch_failure_watch_messages
from etl_craft.engine.runlog import SlaResult, elapsed_hours, fetch_run_sla


@dataclass(frozen=True)
class PipelineDigest:
    """A pipeline's latest run, and its tasks, for the status table."""

    pipeline_code: str
    status: str
    pipeline_run_id: int | None
    start_date: datetime | None
    end_date: datetime | None
    tasks: list[TaskStatus]


def latest_run_of(conn: Connection, pipeline_code: str) -> PipelineDigest:
    """Return a pipeline's latest run; an unknown code is ``UNKNOWN``, one never run ``NEVER_RUN``.

    One wrong code in ``EMAIL_PIPELINES`` shows as its own row rather than losing the report on
    every other pipeline.
    """
    with using(None):
        try:
            pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        except MetadataError:
            return PipelineDigest(pipeline_code, "UNKNOWN", None, None, None, [])
        latest = fetch_latest_pipeline_run(conn, pipeline_id)
        if latest is None:
            return PipelineDigest(pipeline_code, "NEVER_RUN", None, None, None, [])
        tasks = fetch_task_statuses_for_run(conn, pipeline_id, latest.pipeline_run_id)
        return PipelineDigest(
            pipeline_code,
            latest.status,
            latest.pipeline_run_id,
            latest.start_date,
            latest.end_date,
            tasks,
        )


@dataclass(frozen=True)
class AlertState:
    """The task's permitted report, independently of the machine sending SMTP."""

    statuses: tuple[TaskStatus, ...]
    sla: SlaResult | None
    watched: tuple[FailureWatchMessage, ...]
    digests: tuple[PipelineDigest, ...]


def read_alert(
    conn: Connection,
    pipeline_id: int,
    run_id: int,
    task_id: int,
    enforce_sla: bool,
    pipelines: str | None,
) -> AlertState:
    """Read the selected run and the digest requested by its admitted task."""
    sla = None
    if enforce_sla:
        hours = fetch_pipeline_detail(conn, pipeline_id).sla_in_hours
        if hours is not None:
            elapsed = elapsed_hours(fetch_run_sla(conn, run_id).start_date, datetime.now(UTC))
            if elapsed > hours:
                sla = SlaResult(SlaStatus.BREACHED, hours, elapsed)
    codes = []
    if pipelines:
        codes = (
            [
                str(code)
                for code in conn.execute(
                    statement(conn, "active_pipeline_codes"), {"project_id": None}
                ).scalars()
            ]
            if pipelines.strip().upper() == "ALL"
            else [code.strip() for code in pipelines.split("|") if code.strip()]
        )
    return AlertState(
        tuple(fetch_task_statuses_for_run(conn, pipeline_id, run_id)),
        sla,
        tuple(fetch_failure_watch_messages(conn, task_id)),
        tuple(latest_run_of(conn, code) for code in codes),
    )
