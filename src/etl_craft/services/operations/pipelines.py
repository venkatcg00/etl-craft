"""Pipeline pause and resume operations."""

from __future__ import annotations

from etl_craft.core.enums import RunStatus
from etl_craft.engine.connection import read_snapshot
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.execution import interventions
from etl_craft.services.operations.context import OperationContext, PipelineRef, operation
from etl_craft.services.operations.models import OperationResult
from etl_craft.services.operations.snapshots import pipeline_view


def pause_pipeline(ctx: OperationContext, pipeline: PipelineRef, reason: str) -> OperationResult:
    """Pause a pipeline; existing run and process ownership remains with execution."""
    with operation(ctx, "pause", {"pipeline_code": pipeline.pipeline_code, "reason": reason}):
        message = interventions.pause_pipeline(
            ctx.engine, ctx.config, pipeline.pipeline_code, reason
        )
        with read_snapshot(ctx.engine) as conn:
            pipeline_id = resolve_pipeline_id(conn, pipeline.pipeline_code)
            view = pipeline_view(conn, pipeline_id)
        return OperationResult(
            RunStatus.SUCCESS, message, pipeline_id, pipeline.pipeline_code, pipeline=view
        )


def resume_pipeline(ctx: OperationContext, pipeline: PipelineRef, reason: str) -> OperationResult:
    """Resume a pipeline without manufacturing a run or task."""
    with operation(ctx, "resume", {"pipeline_code": pipeline.pipeline_code, "reason": reason}):
        message = interventions.resume_pipeline(
            ctx.engine, ctx.config, pipeline.pipeline_code, reason
        )
        with read_snapshot(ctx.engine) as conn:
            pipeline_id = resolve_pipeline_id(conn, pipeline.pipeline_code)
            view = pipeline_view(conn, pipeline_id)
        return OperationResult(
            RunStatus.SUCCESS, message, pipeline_id, pipeline.pipeline_code, pipeline=view
        )
