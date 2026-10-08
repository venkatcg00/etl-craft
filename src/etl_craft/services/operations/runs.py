"""Run lifecycle operations and execution-request dispatch."""

from __future__ import annotations

from datetime import date

from etl_craft.core.enums import RunStatus
from etl_craft.core.errors import UsageError
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.repository.tasks import resolve_task_id
from etl_craft.engine.runlog import ACTIVE_RUN, RunSelector
from etl_craft.execution import interventions, runner
from etl_craft.execution import pipeline as execution
from etl_craft.execution.reconcile import reconcile
from etl_craft.services.cloning import run_hooks
from etl_craft.services.operations.context import OperationContext, operation
from etl_craft.services.operations.models import BackfillView, OperationResult, ReconciliationView
from etl_craft.services.operations.requests import RunRequest
from etl_craft.services.operations.results import prepare_execution, result, selected_date


def trigger_run(
    ctx: OperationContext,
    pipeline_code: str,
    *,
    run_date: date | None = None,
    reason: str | None = None,
    force: bool = False,
    selector: RunSelector = ACTIVE_RUN,
) -> OperationResult:
    """Run or resume a pipeline and return its exact stored state."""
    request = RunRequest(pipeline_code, run_date=run_date, force=force, selector=selector)
    with operation(ctx, "run", {**request.arguments(), "reason": reason}):
        return _execute(ctx, request)


def initialize_run(
    ctx: OperationContext,
    pipeline_code: str,
    *,
    run_date: date | None = None,
    selector: RunSelector = ACTIVE_RUN,
) -> OperationResult:
    """Initialize or resume a run without running its tasks."""
    return _run_request(
        ctx, RunRequest(pipeline_code, init_only=True, run_date=run_date, selector=selector)
    )


def finalize_run(
    ctx: OperationContext,
    pipeline_code: str,
    *,
    selector: RunSelector = ACTIVE_RUN,
    run_date: date | None = None,
) -> OperationResult:
    """Finalize the selected run from its task summaries."""
    return _run_request(
        ctx, RunRequest(pipeline_code, finalize_only=True, run_date=run_date, selector=selector)
    )


def skip_run(ctx: OperationContext, pipeline_code: str, reason: str) -> OperationResult:
    """Create a skipped run without running any task."""
    return _run_request(ctx, RunRequest(pipeline_code, skip=True, reason=reason))


def mark_run(
    ctx: OperationContext,
    pipeline_code: str,
    status: str,
    reason: str,
    *,
    selector: RunSelector = ACTIVE_RUN,
) -> OperationResult:
    """Mark the selected run independently of the marked status."""
    return mark(ctx, pipeline_code, status, reason, selector=selector)


def stand_in_run(
    ctx: OperationContext,
    pipeline_code: str,
    status: str,
    reason: str,
    *,
    task_code: str | None = None,
    rows: int | None = None,
) -> OperationResult:
    """Record a stand-in run and optionally its stand-in task."""
    return mark(ctx, pipeline_code, status, reason, new_run=True, task_code=task_code, rows=rows)


def cancel_run(
    ctx: OperationContext,
    pipeline_code: str,
    reason: str,
    *,
    selector: RunSelector = ACTIVE_RUN,
) -> OperationResult:
    """Cancel the exact selected run using the existing process and lease guards."""
    with operation(
        ctx,
        "cancel",
        {
            "pipeline_code": pipeline_code,
            "reason": reason,
            "run_id": selector.run_id,
            "run_key": selector.run_key,
        },
    ):
        done = interventions.cancel_run(
            ctx.engine, ctx.config, pipeline_code, reason, selector=selector
        )
        return result(
            ctx,
            pipeline_code,
            RunStatus.SUCCESS,
            done.message,
            pipeline_run_id=done.pipeline_run_id,
        )


def reconcile_runs(
    ctx: OperationContext,
    pipeline_code: str | None = None,
    task_code: str | None = None,
) -> ReconciliationView:
    """Fence expired owners, optionally within one pipeline or task."""
    if task_code and pipeline_code is None:
        raise UsageError("--task_code needs --pipeline_code")
    with operation(
        ctx,
        "reconcile",
        {
            "pipeline_code": pipeline_code,
            "task_code": task_code,
        },
    ):
        with ctx.engine.connect() as conn:
            pipeline_id = (
                None if pipeline_code is None else resolve_pipeline_id(conn, pipeline_code)
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
    from etl_craft.services.operations import backfills

    if request.backfill is not None:
        return backfills.run_backfill(
            ctx, request.pipeline_code, *request.backfill, request.reason or ""
        )
    return _run_request(ctx, request)


def _run_request(ctx: OperationContext, request: RunRequest) -> OperationResult:
    """Audit a single-run request before dispatching its execution step."""
    with operation(ctx, "run", request.arguments()):
        return _execute(ctx, request)


def _execute(ctx: OperationContext, request: RunRequest) -> OperationResult:
    pipeline_code = request.pipeline_code
    pipeline_id = prepare_execution(ctx, pipeline_code)
    selector = request.selector
    if request.task_code is not None or request.finalize_only:
        selector = selected_date(ctx, pipeline_id, selector, request.run_date)
    if request.task_code is not None and not (request.force or request.rerun):
        task = runner.run_task(
            ctx.engine,
            ctx.config,
            pipeline_code,
            request.task_code,
            child=ctx.child,
            selector=selector,
            override=runner.Override(request.reason or "") if request.ignore_dependencies else None,
        )
        return result(
            ctx,
            pipeline_code,
            task.status,
            task.message,
            task_run_id=task.task_run_id,
            pipeline_id=pipeline_id,
        )
    if request.skip:
        skipped = interventions.skip_run(
            ctx.engine, ctx.config, pipeline_code, request.reason or ""
        )
        return result(
            ctx,
            pipeline_code,
            RunStatus.SKIPPED,
            skipped.message,
            pipeline_run_id=skipped.pipeline_run_id,
            pipeline_id=pipeline_id,
        )
    hooks = run_hooks(ctx.config, ctx.engine)
    if request.rerun:
        assert request.task_code is not None
        done = execution.rerun_task(
            ctx.engine,
            ctx.config,
            pipeline_code,
            request.task_code,
            request.reason or "",
            with_downstream=request.with_downstream,
            child=ctx.child,
            hooks=hooks,
            selector=selector,
        )
    elif request.task_code is not None:
        done = execution.force_task(
            ctx.engine,
            ctx.config,
            pipeline_code,
            request.task_code,
            child=ctx.child,
            hooks=hooks,
            selector=selector,
        )
    elif request.init_only:
        done = execution.init_pipeline_run(
            ctx.engine,
            ctx.config,
            pipeline_code,
            hooks=hooks,
            run_date=request.run_date,
            selector=selector,
        )
    elif request.finalize_only:
        done = execution.finalize_active_run(
            ctx.engine, ctx.config, pipeline_code, hooks=hooks, selector=selector
        )
    else:
        done = execution.run_pipeline(
            ctx.engine,
            ctx.config,
            pipeline_code,
            force=request.force,
            child=ctx.child,
            hooks=hooks,
            run_date=request.run_date,
            selector=selector,
        )
    return result(
        ctx,
        pipeline_code,
        done.status,
        done.message,
        pipeline_run_id=done.pipeline_run_id,
        task_code=request.task_code,
        pipeline_id=pipeline_id,
    )


def mark(
    ctx: OperationContext,
    pipeline_code: str,
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
    if stale and (not task_code or new_run):
        raise UsageError("--stale marks one task: pass --task_code, without --new-run")
    if new_run and (selector.run_id is not None or selector.run_key is not None):
        raise UsageError("--new-run creates a stand-in run; do not pass a run selector")
    with operation(
        ctx,
        "mark",
        {
            "pipeline_code": pipeline_code,
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
            done = interventions.record_stand_in_run(
                ctx.engine,
                ctx.config,
                pipeline_code,
                status,
                reason,
                task_code=task_code,
                rows=rows,
            )
        elif task_code is not None:
            done = interventions.mark_task(
                ctx.engine,
                ctx.config,
                pipeline_code,
                task_code,
                status,
                reason,
                rows=rows,
                stale=stale,
                selector=selector,
            )
        else:
            done = interventions.mark_run(
                ctx.engine, ctx.config, pipeline_code, status, reason, selector=selector
            )
        return result(
            ctx,
            pipeline_code,
            RunStatus.SUCCESS,
            done.message,
            pipeline_run_id=done.pipeline_run_id,
            task_code=task_code,
        )
