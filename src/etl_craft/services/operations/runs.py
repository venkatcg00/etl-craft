"""Run lifecycle operations and execution-request dispatch."""

from __future__ import annotations

from dataclasses import replace

from etl_craft.core.enums import RunStatus, Transport
from etl_craft.core.errors import UsageError
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.repository.tasks import resolve_task_id
from etl_craft.engine.runlog import RunSelector
from etl_craft.execution import interventions, reruns, runner
from etl_craft.execution import pipeline as execution
from etl_craft.execution.attempt_types import Override
from etl_craft.execution.reconcile import reconcile
from etl_craft.execution.run_types import PipelineOutcome
from etl_craft.services.cloning import run_hooks
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.models import BackfillView, OperationResult, ReconciliationView
from etl_craft.services.operations.requests import MarkRequest, ReconcileInput, RunRequest
from etl_craft.services.operations.results import prepare_execution, result, selected_date


def reconcile_runs(ctx: OperationContext, request: ReconcileInput) -> ReconciliationView:
    """Fence expired owners, optionally within one pipeline or task."""
    pipeline_code, task_code = request.pipeline_code, request.task_code
    with ctx.engine.connect() as conn:
        pipeline_id = None if pipeline_code is None else resolve_pipeline_id(conn, pipeline_code)
        task_id = (
            resolve_task_id(conn, pipeline_id, task_code)
            if pipeline_id is not None and task_code
            else None
        )
    report = reconcile(ctx.engine, pipeline_id=pipeline_id, task_id=task_id)
    from etl_craft.execution import control_flow

    control_flow.reconcile_endings(
        ctx.engine, ctx.config, tuple(report.lost), pipeline_id=pipeline_id, task_id=task_id
    )
    message = (
        f"reconcile: {len(report.lost)} attempt(s) LOST; "
        f"{len(report.released)} run lease(s) released"
    )
    return ReconciliationView(tuple(report.lost), tuple(report.released), message)


HOST_ONLY = ("force", "skip", "ignore_dependencies")
"""Run options that only the controller's host can apply; the API refuses them."""


def execute_run(ctx: OperationContext, request: RunRequest) -> OperationResult | BackfillView:
    """Run, resume, initialize, skip or backfill, as the validated request says.

    Over HTTP a whole-pipeline run is admitted for the controller, which runs it; the options in
    ``HOST_ONLY`` are refused there, except ``ignore_dependencies`` with ``standalone``.
    """
    if ctx.transport == Transport.HTTP:
        request = _over_http(request)
    if request.backfill is not None:
        from etl_craft.services.operations import backfills

        return backfills.run_backfill(ctx, request)
    return _execute(ctx, request)


def _over_http(request: RunRequest) -> RunRequest:
    for option in HOST_ONLY:
        if getattr(request, option) and not (
            option == "ignore_dependencies" and request.standalone
        ):
            raise UsageError(
                f"`{option}` works only on the controller's host; run it there, or start a run, "
                "a backfill or a rerun over the API"
            )
    if request.task_code is None and request.backfill is None:
        return replace(request, init_only=True)
    return request


def _execute(ctx: OperationContext, request: RunRequest) -> OperationResult:
    pipeline_code = request.pipeline_code
    pipeline_id = prepare_execution(ctx, pipeline_code)
    selector = request.selector
    if request.standalone:
        return _manual(ctx, request, pipeline_id)
    if request.task_code is not None:
        selector = selected_date(ctx, pipeline_id, selector, request.run_date)
    if request.task_code is not None and not (request.force or request.rerun):
        task = runner.run_task(
            ctx.engine,
            ctx.config,
            pipeline_code,
            request.task_code,
            child=ctx.child,
            selector=selector,
            override=Override(request.reason or "") if request.ignore_dependencies else None,
        )
        return result(
            ctx,
            pipeline_code,
            task.status,
            task.message,
            task_run_id=task.task_run_id,
            waiting=task.waiting,
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
    done = _run_step(ctx, request, selector)
    return result(
        ctx,
        pipeline_code,
        done.status,
        done.message,
        pipeline_run_id=done.pipeline_run_id,
        task_code=request.task_code,
        pipeline_id=pipeline_id,
    )


def _run_step(ctx: OperationContext, request: RunRequest, selector: RunSelector) -> PipelineOutcome:
    """Execute one selected run lifecycle step."""
    pipeline_code = request.pipeline_code
    hooks = run_hooks(ctx.config, ctx.engine)
    if request.rerun:
        assert request.task_code is not None
        done = reruns.rerun_task(
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
        done = reruns.force_task(
            ctx.engine,
            ctx.config,
            pipeline_code,
            request.task_code,
            child=ctx.child,
            hooks=hooks,
            selector=selector,
        )
    elif request.init_only:
        from etl_craft.services.operations.triggers import initialize

        done = initialize(ctx, request)
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
    return done


def _manual(ctx: OperationContext, request: RunRequest, pipeline_id: int) -> OperationResult:
    """Execute an independently selected manual run through the shared scheduler."""
    pipeline_code = request.pipeline_code
    selector = request.selector
    from etl_craft.execution.manual import run_manual

    done = run_manual(
        ctx.engine,
        ctx.config,
        pipeline_code,
        request.task_code or "",
        selector=selector,
        run_date=request.run_date,
        with_downstream=request.with_downstream,
        ignore_dependencies=request.ignore_dependencies,
        reason=request.reason,
        child=ctx.child,
        hooks=run_hooks(ctx.config, ctx.engine),
    )
    return result(
        ctx,
        pipeline_code,
        done.status,
        done.message,
        pipeline_run_id=done.pipeline_run_id,
        pipeline_id=pipeline_id,
    )


def mark(ctx: OperationContext, request: MarkRequest) -> OperationResult:
    """Mark a task, a run, or a new stand-in run, or reset a task's watermark or offset."""
    pipeline_code, task_code, reason = request.pipeline_code, request.task_code, request.reason
    selector = request.selector
    if request.watermark is not None:
        assert task_code is not None
        done = interventions.reset_watermark(
            ctx.engine,
            ctx.config,
            pipeline_code,
            task_code,
            request.watermark,
            reason,
            selector=selector,
        )
    elif request.offset is not None:
        assert task_code is not None
        done = interventions.reset_offset(
            ctx.engine,
            ctx.config,
            interventions.OffsetReset(pipeline_code, task_code, request.offset, reason),
            selector=selector,
        )
    elif request.new_run:
        assert request.status is not None
        done = interventions.record_stand_in_run(
            ctx.engine,
            ctx.config,
            pipeline_code,
            request.status,
            reason,
            task_code=task_code,
            rows=request.rows,
        )
    elif task_code is not None:
        assert request.status is not None
        done = interventions.mark_task(
            ctx.engine,
            ctx.config,
            pipeline_code,
            task_code,
            request.status,
            reason,
            rows=request.rows,
            stale=request.stale,
            selector=selector,
        )
    else:
        assert request.status is not None
        done = interventions.mark_run(
            ctx.engine, ctx.config, pipeline_code, request.status, reason, selector=selector
        )
    return result(
        ctx,
        pipeline_code,
        RunStatus.SUCCESS,
        done.message,
        pipeline_run_id=done.pipeline_run_id,
        task_code=task_code,
    )
