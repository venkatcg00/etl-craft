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
    change = (
        interventions.pause_pipeline
        if request.action == ControlAction.PAUSE
        else interventions.resume_pipeline
    )
    message = change(ctx.engine, ctx.config, request.pipeline_code, request.reason)
    with read_snapshot(ctx.engine) as conn:
        pipeline_id = resolve_pipeline_id(conn, request.pipeline_code)
        view = pipeline_view(conn, pipeline_id)
    return OperationResult(
        RunStatus.SUCCESS, message, pipeline_id, request.pipeline_code, pipeline=view
    )
