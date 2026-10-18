"""Exact HTTP identities and bounded history over shared operation documents."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

from sqlalchemy import Boolean

from etl_craft.core.actor import acting_as
from etl_craft.core.errors import ResourceNotFoundError, UsageError
from etl_craft.engine.connection import read_snapshot
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.runlog import RunSelector
from etl_craft.execution.attempt_records import attempt_log_path
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.models import AttemptView, PipelineView
from etl_craft.services.operations.permissions import authorize
from etl_craft.services.operations.snapshots import attempt_document, pipeline_view
from etl_craft.services.operations.status import StatusView, pipeline_status


def get_pipeline(ctx: OperationContext, code: str) -> PipelineView:
    """Read one active definition without duplicating pipeline assembly."""
    authorize(ctx, "list")
    with acting_as(ctx.actor), read_snapshot(ctx.engine) as conn:
        return pipeline_view(conn, resolve_pipeline_id(conn, code))


def run_code(ctx: OperationContext, run_id: int) -> str:
    """Resolve only the requested run's pipeline, never an active or recent replacement."""
    authorize(ctx, "history")
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
    authorize(ctx, "attempt")
    with acting_as(ctx.actor), read_snapshot(ctx.engine) as conn:
        row = conn.execute(
            statement(conn, "api_attempt").columns(retryable=Boolean), {"attempt_id": attempt_id}
        ).one_or_none()
    if row is None:
        raise ResourceNotFoundError(f"attempt_id={attempt_id}: attempt does not exist")
    return attempt_document(row._mapping)


def attempt_log(ctx: OperationContext, attempt_id: int, offset: int = 0) -> Iterator[bytes]:
    """Stream a stored attempt's canonical log in bounded chunks from a byte offset."""
    authorize(ctx, "attempt log")
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
