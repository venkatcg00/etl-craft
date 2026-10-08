"""Task execution and intervention operations under exact run selectors."""

from __future__ import annotations

from datetime import date

from etl_craft.engine.runlog import ACTIVE_RUN, RunSelector
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.models import OperationResult
from etl_craft.services.operations.requests import RunRequest
from etl_craft.services.operations.runs import _run_request, mark


def run_task(
    ctx: OperationContext,
    pipeline_code: str,
    task_code: str,
    *,
    selector: RunSelector = ACTIVE_RUN,
    run_date: date | None = None,
    reason: str | None = None,
) -> OperationResult:
    """Run one task; a reason requests the recorded dependency override."""
    return _run_request(
        ctx,
        RunRequest(
            pipeline_code,
            task_code=task_code,
            selector=selector,
            run_date=run_date,
            ignore_dependencies=reason is not None,
            reason=reason,
        ),
    )


def force_task(
    ctx: OperationContext,
    pipeline_code: str,
    task_code: str,
    *,
    selector: RunSelector = ACTIVE_RUN,
    run_date: date | None = None,
) -> OperationResult:
    """Force one task and settle a reopened pipeline as the execution layer requires."""
    return _run_request(
        ctx,
        RunRequest(
            pipeline_code, task_code=task_code, force=True, selector=selector, run_date=run_date
        ),
    )


def rerun_task(
    ctx: OperationContext,
    pipeline_code: str,
    task_code: str,
    reason: str,
    *,
    with_downstream: bool = False,
    selector: RunSelector = ACTIVE_RUN,
    run_date: date | None = None,
) -> OperationResult:
    """Rerun one task and optionally its downstream tasks, preserving attempt history."""
    return _run_request(
        ctx,
        RunRequest(
            pipeline_code,
            task_code=task_code,
            rerun=True,
            reason=reason,
            with_downstream=with_downstream,
            selector=selector,
            run_date=run_date,
        ),
    )


def mark_task(
    ctx: OperationContext,
    pipeline_code: str,
    task_code: str,
    status: str,
    reason: str,
    *,
    rows: int | None = None,
    stale: bool = False,
    selector: RunSelector = ACTIVE_RUN,
) -> OperationResult:
    """Mark one task, reconciling expired attempts first only when stale is requested."""
    return mark(
        ctx,
        pipeline_code,
        status,
        reason,
        task_code=task_code,
        rows=rows,
        stale=stale,
        selector=selector,
    )
