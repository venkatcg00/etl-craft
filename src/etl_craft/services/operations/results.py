"""Build post-operation views from exact execution identities."""

from __future__ import annotations

from datetime import date

from etl_craft.core.enums import RunStatus
from etl_craft.core.errors import RunStateError, UsageError
from etl_craft.engine.connection import read_snapshot
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.repository.tasks import resolve_task_id
from etl_craft.engine.runlog import RunSelector, select_run
from etl_craft.execution.reconcile import reconcile
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.models import OperationResult
from etl_craft.services.operations.snapshots import run_view, task_run_view


def prepare_execution(ctx: OperationContext, pipeline_code: str) -> int:
    """Reconcile expired owners before dispatch, without adopting them."""
    with ctx.engine.connect() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
    reconcile(ctx.engine, pipeline_id=pipeline_id)
    return pipeline_id


def selected_date(
    ctx: OperationContext, pipeline_id: int, selector: RunSelector, run_date: date | None
) -> RunSelector:
    """Pin an existing run before checking its immutable logical date."""
    if run_date is None:
        return selector
    with ctx.engine.connect() as conn:
        selected = select_run(conn, pipeline_id, selector)
    if selected.run_date != run_date:
        raise UsageError(
            f"pipeline_run_id={selected.pipeline_run_id} has run_date={selected.run_date}, "
            f"not {run_date}; a run's date cannot change"
        )
    return RunSelector(run_id=selected.pipeline_run_id)


def result(
    ctx: OperationContext,
    pipeline_code: str,
    status: RunStatus,
    message: str,
    *,
    pipeline_run_id: int | None = None,
    task_run_id: int | None = None,
    task_code: str | None = None,
    pipeline_id: int | None = None,
) -> OperationResult:
    """Read one consistent result; a task's returned id determines its pipeline run."""
    with read_snapshot(ctx.engine) as conn:
        if pipeline_id is None:
            pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        task = None
        if task_run_id is not None:
            task = task_run_view(conn, pipeline_id, task_run_id=task_run_id)
            if task is None:
                raise RunStateError(
                    f"pipeline_id={pipeline_id}: no task_run_id={task_run_id}; "
                    "inspect the exact task run before retrying"
                )
            if pipeline_run_id is not None and pipeline_run_id != task.pipeline_run_id:
                raise RunStateError(
                    f"task_run_id={task_run_id} belongs to pipeline_run_id={task.pipeline_run_id}, "
                    f"not {pipeline_run_id}; use the task's recorded pipeline run"
                )
            pipeline_run_id = task.pipeline_run_id
        elif task_code is not None and pipeline_run_id is not None:
            task = task_run_view(
                conn,
                pipeline_id,
                pipeline_run_id=pipeline_run_id,
                task_id=resolve_task_id(conn, pipeline_id, task_code),
            )
        run = None if pipeline_run_id is None else run_view(conn, pipeline_id, pipeline_run_id)
    return OperationResult(status, message, pipeline_id, pipeline_code, run, task)
