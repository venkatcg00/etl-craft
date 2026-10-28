"""Pipeline pause and resume operations."""

from __future__ import annotations

from etl_craft.core.enums import ControlAction, RunStatus
from etl_craft.engine.connection import read_snapshot
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.execution import interventions
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.models import OperationResult
from etl_craft.services.operations.requests import ControlRequest
from etl_craft.services.operations.snapshots import pipeline_view


def set_pause(ctx: OperationContext, request: ControlRequest) -> OperationResult:
    """Pause or resume using the existing run and process ownership guards."""
    if request.action == ControlAction.PAUSE:
        if request.then is not None:
            from etl_craft.core.errors import UsageError

            raise UsageError("--then applies only to scheduled pipeline resume; omit it for pause")
        message = interventions.pause_pipeline(
            ctx.engine, ctx.config, request.pipeline_code, request.reason
        )
    else:
        from etl_craft.services.operations.pipeline_resume import resume

        message = resume(ctx, request)
    with read_snapshot(ctx.engine) as conn:
        pipeline_id = resolve_pipeline_id(conn, request.pipeline_code)
        view = pipeline_view(conn, pipeline_id)
    return OperationResult(
        RunStatus.SUCCESS, message, pipeline_id, request.pipeline_code, pipeline=view
    )
