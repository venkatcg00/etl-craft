"""Run lifecycle operations and execution-request dispatch."""

from __future__ import annotations

from datetime import date

from etl_craft.core.enums import RunStatus
from etl_craft.core.errors import UsageError
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.repository.tasks import resolve_task_id
from etl_craft.engine.runlog import ACTIVE_RUN, RunSelector
from etl_craft.execution import interventions
from etl_craft.execution import pipeline as execution
from etl_craft.execution.reconcile import reconcile
from etl_craft.services.cloning import run_hooks
from etl_craft.services.operations.context import OperationContext, PipelineRef, operation
from etl_craft.services.operations.models import BackfillView, OperationResult, ReconciliationView
from etl_craft.services.operations.requests import RunRequest
from etl_craft.services.operations.results import prepare_execution, result, selected_date


def trigger_run(
    ctx: OperationContext,
    pipeline: PipelineRef,
    *,
    run_date: date | None = None,
    reason: str | None = None,
    force: bool = False,
    selector: RunSelector = ACTIVE_RUN,
) -> OperationResult:
    """Run or resume a pipeline under the caller's actor and return its exact stored state."""
    with operation(
        ctx,
        "run",
        {
            "pipeline_code": pipeline.pipeline_code,
            "reason": reason,
            "force": force,
            "run_date": run_date,
            "run_id": selector.run_id,
            "run_key": selector.run_key,
        },
    ):
        pipeline_id = prepare_execution(ctx, pipeline)
        done = execution.run_pipeline(
            ctx.engine,
            ctx.config,
            pipeline.pipeline_code,
            force=force,
            child=ctx.child,
            hooks=run_hooks(ctx.config, ctx.engine),
            run_date=run_date,
            selector=selector,
        )
        return result(
            ctx,
            pipeline,
            pipeline_id,
            done.status,
            done.message,
            pipeline_run_id=done.pipeline_run_id,
        )


def initialize_run(
    ctx: OperationContext,
    pipeline: PipelineRef,
    *,
    run_date: date | None = None,
    selector: RunSelector = ACTIVE_RUN,
) -> OperationResult:
    """Initialize or resume a run without running its tasks."""
    with operation(
        ctx,
        "run",
        {
            "pipeline_code": pipeline.pipeline_code,
            "init_only": True,
            "run_date": run_date,
            "run_id": selector.run_id,
            "run_key": selector.run_key,
        },
    ):
        pipeline_id = prepare_execution(ctx, pipeline)
        done = execution.init_pipeline_run(
            ctx.engine,
            ctx.config,
            pipeline.pipeline_code,
            hooks=run_hooks(ctx.config, ctx.engine),
            run_date=run_date,
            selector=selector,
        )
        return result(
            ctx,
            pipeline,
            pipeline_id,
            done.status,
            done.message,
            pipeline_run_id=done.pipeline_run_id,
        )


def finalize_run(
    ctx: OperationContext,
    pipeline: PipelineRef,
    *,
    selector: RunSelector = ACTIVE_RUN,
    run_date: date | None = None,
) -> OperationResult:
    """Finalize the selected run from its task summaries."""
    with operation(
        ctx,
        "run",
        {
            "pipeline_code": pipeline.pipeline_code,
            "finalize_only": True,
            "run_date": run_date,
            "run_id": selector.run_id,
            "run_key": selector.run_key,
        },
    ):
        pipeline_id = prepare_execution(ctx, pipeline)
        selector = selected_date(ctx, pipeline_id, selector, run_date)
        done = execution.finalize_active_run(
            ctx.engine,
            ctx.config,
            pipeline.pipeline_code,
            hooks=run_hooks(ctx.config, ctx.engine),
            selector=selector,
        )
        return result(
            ctx,
            pipeline,
            pipeline_id,
            done.status,
            done.message,
            pipeline_run_id=done.pipeline_run_id,
        )


def skip_run(ctx: OperationContext, pipeline: PipelineRef, reason: str) -> OperationResult:
    """Create a skipped run without running any task."""
    with operation(
        ctx, "run", {"pipeline_code": pipeline.pipeline_code, "skip": True, "reason": reason}
    ):
        pipeline_id = prepare_execution(ctx, pipeline)
        done = interventions.skip_run(ctx.engine, ctx.config, pipeline.pipeline_code, reason)
        return result(
            ctx,
            pipeline,
            pipeline_id,
            RunStatus.SKIPPED,
            done.message,
            pipeline_run_id=done.pipeline_run_id,
        )


def mark_run(
    ctx: OperationContext,
    pipeline: PipelineRef,
    status: str,
    reason: str,
    *,
    selector: RunSelector = ACTIVE_RUN,
) -> OperationResult:
    """Mark the selected run; the operation succeeds independently of the marked status."""
    with operation(
        ctx,
        "mark",
        {
            "pipeline_code": pipeline.pipeline_code,
            "status": status,
            "reason": reason,
            "run_id": selector.run_id,
            "run_key": selector.run_key,
        },
    ):
        done = interventions.mark_run(
            ctx.engine,
            ctx.config,
            pipeline.pipeline_code,
            status,
            reason,
            selector=selector,
        )
        with ctx.engine.connect() as conn:
            pipeline_id = resolve_pipeline_id(conn, pipeline.pipeline_code)
        return result(
            ctx,
            pipeline,
            pipeline_id,
            RunStatus.SUCCESS,
            done.message,
            pipeline_run_id=done.pipeline_run_id,
        )


def stand_in_run(
    ctx: OperationContext,
    pipeline: PipelineRef,
    status: str,
    reason: str,
    *,
    task_code: str | None = None,
    rows: int | None = None,
) -> OperationResult:
    """Record a stand-in run and optionally its stand-in task."""
    with operation(
        ctx,
        "mark",
        {
            "pipeline_code": pipeline.pipeline_code,
            "new_run": True,
            "status": status,
            "reason": reason,
            "task_code": task_code,
            "rows": rows,
        },
    ):
        done = interventions.record_stand_in_run(
            ctx.engine,
            ctx.config,
            pipeline.pipeline_code,
            status,
            reason,
            task_code=task_code,
            rows=rows,
        )
        with ctx.engine.connect() as conn:
            pipeline_id = resolve_pipeline_id(conn, pipeline.pipeline_code)
        return result(
            ctx,
            pipeline,
            pipeline_id,
            RunStatus.SUCCESS,
            done.message,
            pipeline_run_id=done.pipeline_run_id,
            task_code=task_code,
        )


def cancel_run(
    ctx: OperationContext,
    pipeline: PipelineRef,
    reason: str,
    *,
    selector: RunSelector = ACTIVE_RUN,
) -> OperationResult:
    """Cancel the exact selected run using the existing process and lease guards."""
    with operation(
        ctx,
        "cancel",
        {
            "pipeline_code": pipeline.pipeline_code,
            "reason": reason,
            "run_id": selector.run_id,
            "run_key": selector.run_key,
        },
    ):
        done = interventions.cancel_run(
            ctx.engine,
            ctx.config,
            pipeline.pipeline_code,
            reason,
            selector=selector,
        )
        with ctx.engine.connect() as conn:
            pipeline_id = resolve_pipeline_id(conn, pipeline.pipeline_code)
        return result(
            ctx,
            pipeline,
            pipeline_id,
            RunStatus.SUCCESS,
            done.message,
            pipeline_run_id=done.pipeline_run_id,
        )


def reconcile_runs(
    ctx: OperationContext,
    pipeline: PipelineRef | None = None,
    task_code: str | None = None,
) -> ReconciliationView:
    """Fence expired owners, optionally within one pipeline or task."""
    if task_code and pipeline is None:
        raise UsageError("--task_code needs --pipeline_code")
    with operation(
        ctx,
        "reconcile",
        {
            "pipeline_code": None if pipeline is None else pipeline.pipeline_code,
            "task_code": task_code,
        },
    ):
        with ctx.engine.connect() as conn:
            pipeline_id = (
                None if pipeline is None else resolve_pipeline_id(conn, pipeline.pipeline_code)
            )
            task_id = (
                resolve_task_id(conn, pipeline_id, task_code)
                if pipeline_id is not None and task_code
                else None
            )
        report = reconcile(ctx.engine, pipeline_id=pipeline_id, task_id=task_id)
        message = (
            f"reconcile: {len(report.lost)} attempt(s) LOST; "
            f"{len(report.released)} run lease(s) released"
        )
        return ReconciliationView(tuple(report.lost), tuple(report.released), message)


def execute_run(ctx: OperationContext, request: RunRequest) -> OperationResult | BackfillView:
    """Dispatch one validated request without CLI argument objects."""
    from etl_craft.services.operations import backfills, tasks

    with operation(ctx, "run", request.arguments()):
        if request.backfill is not None:
            return backfills.run_backfill(
                ctx,
                request.pipeline,
                *request.backfill,
                request.reason or "",
            )
        if request.skip:
            return skip_run(ctx, request.pipeline, request.reason or "")
        if request.rerun:
            assert request.task_code is not None
            return tasks.rerun_task(
                ctx,
                request.pipeline,
                request.task_code,
                request.reason or "",
                with_downstream=request.with_downstream,
                selector=request.selector,
                run_date=request.run_date,
            )
        if request.task_code is not None and request.force:
            return tasks.force_task(
                ctx,
                request.pipeline,
                request.task_code,
                selector=request.selector,
                run_date=request.run_date,
            )
        if request.task_code is not None:
            return tasks.run_task(
                ctx,
                request.pipeline,
                request.task_code,
                selector=request.selector,
                run_date=request.run_date,
                reason=(request.reason or "") if request.ignore_dependencies else None,
            )
        if request.init_only:
            return initialize_run(
                ctx, request.pipeline, selector=request.selector, run_date=request.run_date
            )
        if request.finalize_only:
            return finalize_run(
                ctx, request.pipeline, selector=request.selector, run_date=request.run_date
            )
        return trigger_run(
            ctx,
            request.pipeline,
            force=request.force,
            selector=request.selector,
            run_date=request.run_date,
        )


def mark(
    ctx: OperationContext,
    pipeline: PipelineRef,
    status: str,
    reason: str,
    *,
    task_code: str | None = None,
    rows: int | None = None,
    stale: bool = False,
    new_run: bool = False,
    selector: RunSelector = ACTIVE_RUN,
) -> OperationResult:
    """Mark a task, a run, or a new stand-in run with one request audit."""
    from etl_craft.services.operations import tasks

    if stale and (not task_code or new_run):
        raise UsageError("--stale marks one task: pass --task_code, without --new-run")
    if new_run and (selector.run_id is not None or selector.run_key is not None):
        raise UsageError("--new-run creates a stand-in run; do not pass a run selector")
    with operation(
        ctx,
        "mark",
        {
            "pipeline_code": pipeline.pipeline_code,
            "status": status,
            "reason": reason,
            "task_code": task_code,
            "rows": rows,
            "stale": stale,
            "new_run": new_run,
            "run_id": selector.run_id,
            "run_key": selector.run_key,
        },
    ):
        if new_run:
            return stand_in_run(ctx, pipeline, status, reason, task_code=task_code, rows=rows)
        if task_code is not None:
            return tasks.mark_task(
                ctx, pipeline, task_code, status, reason, rows=rows, stale=stale, selector=selector
            )
        return mark_run(ctx, pipeline, status, reason, selector=selector)
