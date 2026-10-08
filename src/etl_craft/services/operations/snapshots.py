"""Read canonical execution views without selecting runs by recency."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime

from sqlalchemy.engine import Connection, RowMapping

from etl_craft.core.errors import RunStateError, UsageError
from etl_craft.core.time import as_utc
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pauses import fetch_open_pause
from etl_craft.engine.repository.pipelines import fetch_pipeline_detail
from etl_craft.engine.runlog import as_date
from etl_craft.services.operations.models import AttemptView, PipelineView, RunView, TaskRunView


def timestamp(value: object) -> datetime | None:
    """Normalize nullable Engine DB timestamps to aware UTC instants."""
    return None if value is None else as_utc(value)


def pipeline_view(conn: Connection, pipeline_id: int) -> PipelineView:
    """Read one definition and its pause within the caller's snapshot."""
    detail = fetch_pipeline_detail(conn, pipeline_id)
    pause = fetch_open_pause(conn, pipeline_id)
    if pause is not None:
        pause = replace(
            pause, paused_at=as_utc(pause.paused_at), pipeline_code=detail.pipeline_code
        )
    return PipelineView(
        pipeline_id,
        detail.pipeline_code,
        detail.pipeline_name,
        detail.refresh_type,
        detail.run_schedule,
        detail.sla_in_hours,
        pause,
    )


def run_view(conn: Connection, pipeline_id: int, pipeline_run_id: int) -> RunView:
    """Read the exact run; a foreign pipeline's identity never matches."""
    row = conn.execute(
        statement(conn, "operation_run"),
        {"pipeline_id": pipeline_id, "pipeline_run_id": pipeline_run_id},
    ).one_or_none()
    if row is None:
        raise RunStateError(
            f"pipeline_id={pipeline_id}: no pipeline_run_id={pipeline_run_id}; "
            "select a run belonging to this pipeline"
        )
    return run_document(row._mapping)


def run_document(row: RowMapping) -> RunView:
    """Convert a stored run row into its public document."""
    values = dict(row)
    values["run_date"] = None if values["run_date"] is None else as_date(values["run_date"])
    values["start_date"] = timestamp(values["start_date"])
    values["end_date"] = timestamp(values["end_date"])
    values["backfill"] = values["backfill"] == "Y"
    return RunView(**values)


def task_run_view(
    conn: Connection,
    pipeline_id: int,
    *,
    task_run_id: int | None = None,
    pipeline_run_id: int | None = None,
    task_id: int | None = None,
) -> TaskRunView | None:
    """Read a task run by its id or by an exact task and pipeline-run pair."""
    if task_run_id is None and (pipeline_run_id is None or task_id is None):
        raise UsageError(
            "task run inspection needs task_run_id or both pipeline_run_id and task_id; "
            "supply an exact execution identity"
        )
    row = conn.execute(
        statement(conn, "operation_task_run"),
        {
            "pipeline_id": pipeline_id,
            "task_run_id": task_run_id,
            "pipeline_run_id": pipeline_run_id,
            "task_id": task_id,
        },
    ).one_or_none()
    if row is None:
        return None
    return task_document(conn, row._mapping)


def task_document(conn: Connection, row: RowMapping) -> TaskRunView:
    """Convert a task summary and its attempts into one public document."""
    values = dict(row)
    values["start_date"] = timestamp(values["start_date"])
    values["end_date"] = timestamp(values["end_date"])
    attempts = []
    for attempt in conn.execute(
        statement(conn, "operation_attempts"), {"task_run_id": values["task_run_id"]}
    ):
        captured = dict(attempt._mapping)
        for key in (
            "lease_expires_at",
            "heartbeat_at",
            "queued_at",
            "claimed_at",
            "started_at",
            "ended_at",
        ):
            captured[key] = timestamp(captured[key])
        attempts.append(AttemptView(**captured))
    return TaskRunView(**values, attempts=tuple(attempts))
