"""Exact HTTP identities and bounded history over shared operation documents."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

from sqlalchemy import Boolean

from etl_craft.core.actor import acting_as
from etl_craft.core.errors import ResourceNotFoundError, UsageError
from etl_craft.engine.connection import read_snapshot
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.runlog import RunSelector
from etl_craft.execution.runner import attempt_log_path
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.models import AttemptView, PipelineView, RunView
from etl_craft.services.operations.snapshots import attempt_document, pipeline_view, run_document
from etl_craft.services.operations.status import StatusView, pipeline_status


@dataclass(frozen=True)
class RunsView:
    """A page of runs, newest identity first, with an exclusive identity cursor."""

    SCHEMA: ClassVar[str] = "etl-craft/runs/1"
    runs: tuple[RunView, ...]
    before: int | None


def get_pipeline(ctx: OperationContext, code: str) -> PipelineView:
    """Read one active definition without duplicating pipeline assembly."""
    with acting_as(ctx.actor), read_snapshot(ctx.engine) as conn:
        return pipeline_view(conn, resolve_pipeline_id(conn, code))


def get_runs(
    ctx: OperationContext, code: str, limit: int = 20, before: int | None = None
) -> RunsView:
    """Read a bounded identity page without selecting a run by recency."""
    if not 1 <= limit <= 100 or (before is not None and before < 1):
        raise UsageError("runs needs limit between 1 and 100 and a positive before run id")
    with acting_as(ctx.actor), read_snapshot(ctx.engine) as conn:
        pipeline_id = resolve_pipeline_id(conn, code)
        rows = conn.execute(
            statement(conn, "api_run_history"),
            {"pipeline_id": pipeline_id, "limit": limit + 1, "before": before},
        ).all()
        runs = tuple(run_document(row._mapping) for row in rows[:limit])
    return RunsView(runs, runs[-1].pipeline_run_id if len(rows) > limit else None)


def run_code(ctx: OperationContext, run_id: int) -> str:
    """Resolve only the requested run's pipeline, never an active or recent replacement."""
    with ctx.engine.connect() as conn:
        code = conn.execute(
            statement(conn, "api_run_code"), {"run_id": run_id}
        ).scalar_one_or_none()
    if code is None:
        raise ResourceNotFoundError(f"pipeline_run_id={run_id}: run does not exist")
    return str(code)


def get_run_status(ctx: OperationContext, run_id: int) -> StatusView:
    """Return the shared status document for an exact HTTP run identity."""
    return pipeline_status(ctx, run_code(ctx, run_id), selector=RunSelector(run_id=run_id))


def get_attempt(ctx: OperationContext, attempt_id: int) -> AttemptView:
    """Read one attempt with the same fields and conversions as CLI inspection."""
    with acting_as(ctx.actor), read_snapshot(ctx.engine) as conn:
        row = conn.execute(
            statement(conn, "api_attempt").columns(retryable=Boolean), {"attempt_id": attempt_id}
        ).one_or_none()
    if row is None:
        raise ResourceNotFoundError(f"attempt_id={attempt_id}: attempt does not exist")
    return attempt_document(row._mapping)


def attempt_log(ctx: OperationContext, attempt_id: int, offset: int = 0) -> Iterator[bytes]:
    """Stream a stored attempt's canonical log in bounded chunks from a byte offset."""
    if offset < 0:
        raise UsageError("log offset must be a nonnegative byte offset")
    attempt = get_attempt(ctx, attempt_id)
    if attempt.log_path is None:
        raise ResourceNotFoundError(f"attempt_id={attempt_id}: no log has been recorded")
    with ctx.engine.connect() as conn:
        row = conn.execute(
            statement(conn, "api_attempt_identity"), {"attempt_id": attempt_id}
        ).one()
    expected = attempt_log_path(
        ctx.config,
        row.pipeline_code,
        attempt.pipeline_run_id,
        row.task_code,
        attempt.attempt_number,
    ).resolve()
    path = Path(attempt.log_path).resolve()
    if path != expected or not path.is_relative_to(ctx.config.log_dir.resolve()):
        raise ResourceNotFoundError(
            f"attempt_id={attempt_id}: log is outside its configured attempt path"
        )
    try:
        stream = path.open("rb")
    except OSError as error:
        raise ResourceNotFoundError(f"attempt_id={attempt_id}: log file is unavailable") from error

    def chunks() -> Iterator[bytes]:
        with stream:
            stream.seek(offset)
            while data := stream.read(65536):
                yield data

    return chunks()
