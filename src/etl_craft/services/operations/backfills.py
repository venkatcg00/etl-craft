"""Backfill operations preserving each date's exact run identity."""

from __future__ import annotations

from datetime import date

from etl_craft.engine.connection import read_snapshot
from etl_craft.execution import pipeline as execution
from etl_craft.services.cloning import run_hooks
from etl_craft.services.operations.context import OperationContext, operation
from etl_craft.services.operations.models import BackfillView, OperationResult
from etl_craft.services.operations.requests import RunRequest
from etl_craft.services.operations.results import prepare_execution
from etl_craft.services.operations.snapshots import run_view


def run_backfill(
    ctx: OperationContext,
    pipeline_code: str,
    first: date,
    last: date,
    reason: str,
) -> BackfillView:
    """Run a bounded date range using the existing pause, overlap and stopping rules."""
    with operation(
        ctx,
        "run",
        RunRequest(pipeline_code, backfill=(first, last), reason=reason).arguments(),
    ):
        pipeline_id = prepare_execution(ctx, pipeline_code)
        done = execution.backfill(
            ctx.engine,
            ctx.config,
            pipeline_code,
            first,
            last,
            reason,
            child=ctx.child,
            hooks=run_hooks(ctx.config, ctx.engine),
        )
        with read_snapshot(ctx.engine) as conn:

            def capture(outcome: execution.PipelineOutcome) -> OperationResult:
                view = (
                    None
                    if outcome.pipeline_run_id is None
                    else run_view(conn, pipeline_id, outcome.pipeline_run_id)
                )
                return OperationResult(
                    outcome.status, outcome.message, pipeline_id, pipeline_code, run=view
                )

            results = tuple(capture(outcome) for outcome in done.runs)
            stopped = None
            if done.stopped is not None:
                stopped = (
                    results[-1]
                    if done.runs and done.runs[-1] is done.stopped
                    else capture(done.stopped)
                )
        return BackfillView(
            pipeline_id,
            pipeline_code,
            first,
            last,
            done.status,
            done.message,
            results,
            stopped,
        )
