"""Task execution and intervention operations under exact run selectors."""

from __future__ import annotations

from datetime import date

from etl_craft.core.enums import RunStatus
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.runlog import ACTIVE_RUN, RunSelector
from etl_craft.execution import interventions, runner
from etl_craft.execution import pipeline as execution
from etl_craft.services.cloning import run_hooks
from etl_craft.services.operations.context import OperationContext, PipelineRef, operation
from etl_craft.services.operations.models import OperationResult
from etl_craft.services.operations.results import prepare_execution, result, selected_date


def run_task(
    ctx: OperationContext,
    pipeline: PipelineRef,
    task_code: str,
    *,
    selector: RunSelector = ACTIVE_RUN,
    run_date: date | None = None,
    reason: str | None = None,
) -> OperationResult:
    """Run one task; a reason requests the recorded dependency override."""
    with operation(
        ctx,
        "run",
        {
            "pipeline_code": pipeline.pipeline_code,
            "task_code": task_code,
            "run_id": selector.run_id,
            "run_key": selector.run_key,
            "run_date": run_date,
            "ignore_dependencies": reason is not None,
            "reason": reason,
        },
    ):
        pipeline_id = prepare_execution(ctx, pipeline)
        selector = selected_date(ctx, pipeline_id, selector, run_date)
        done = runner.run_task(
            ctx.engine,
            ctx.config,
            pipeline.pipeline_code,
            task_code,
            child=ctx.child,
            override=None if reason is None else runner.Override(reason),
            selector=selector,
        )
        return result(
            ctx, pipeline, pipeline_id, done.status, done.message, task_run_id=done.task_run_id
        )


def force_task(
    ctx: OperationContext,
    pipeline: PipelineRef,
    task_code: str,
    *,
    selector: RunSelector = ACTIVE_RUN,
    run_date: date | None = None,
) -> OperationResult:
    """Force one task and settle a reopened pipeline as the execution layer requires."""
    with operation(
        ctx,
        "run",
        {
            "pipeline_code": pipeline.pipeline_code,
            "task_code": task_code,
            "force": True,
            "run_id": selector.run_id,
            "run_key": selector.run_key,
            "run_date": run_date,
        },
    ):
        pipeline_id = prepare_execution(ctx, pipeline)
        selector = selected_date(ctx, pipeline_id, selector, run_date)
        done = execution.force_task(
            ctx.engine,
            ctx.config,
            pipeline.pipeline_code,
            task_code,
            child=ctx.child,
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
            task_code=task_code,
        )


def rerun_task(
    ctx: OperationContext,
    pipeline: PipelineRef,
    task_code: str,
    reason: str,
    *,
    with_downstream: bool = False,
    selector: RunSelector = ACTIVE_RUN,
    run_date: date | None = None,
) -> OperationResult:
    """Rerun one task and optionally its downstream tasks, preserving attempt history."""
    with operation(
        ctx,
        "run",
        {
            "pipeline_code": pipeline.pipeline_code,
            "task_code": task_code,
            "rerun": True,
            "reason": reason,
            "with_downstream": with_downstream,
            "run_id": selector.run_id,
            "run_key": selector.run_key,
            "run_date": run_date,
        },
    ):
        pipeline_id = prepare_execution(ctx, pipeline)
        selector = selected_date(ctx, pipeline_id, selector, run_date)
        done = execution.rerun_task(
            ctx.engine,
            ctx.config,
            pipeline.pipeline_code,
            task_code,
            reason,
            with_downstream=with_downstream,
            child=ctx.child,
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
            task_code=task_code,
        )


def mark_task(
    ctx: OperationContext,
    pipeline: PipelineRef,
    task_code: str,
    status: str,
    reason: str,
    *,
    rows: int | None = None,
    stale: bool = False,
    selector: RunSelector = ACTIVE_RUN,
) -> OperationResult:
    """Mark one task, reconciling expired attempts first only when stale is requested."""
    with operation(
        ctx,
        "mark",
        {
            "pipeline_code": pipeline.pipeline_code,
            "task_code": task_code,
            "status": status,
            "reason": reason,
            "rows": rows,
            "stale": stale,
            "run_id": selector.run_id,
            "run_key": selector.run_key,
        },
    ):
        done = interventions.mark_task(
            ctx.engine,
            ctx.config,
            pipeline.pipeline_code,
            task_code,
            status,
            reason,
            rows=rows,
            stale=stale,
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
            task_code=task_code,
        )
