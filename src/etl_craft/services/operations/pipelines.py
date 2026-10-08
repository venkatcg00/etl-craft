"""Pipeline pause and resume operations."""

from __future__ import annotations

from typing import Literal

from etl_craft.core.enums import RunStatus
from etl_craft.engine.connection import read_snapshot
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.execution import interventions
from etl_craft.services.operations.context import OperationContext, operation
from etl_craft.services.operations.models import OperationResult
from etl_craft.services.operations.snapshots import pipeline_view


def set_pause(
    ctx: OperationContext, pipeline_code: str, reason: str, *, verb: Literal["pause", "resume"]
) -> OperationResult:
    """Pause or resume using the existing run and process ownership guards."""
    action = {"pause": interventions.pause_pipeline, "resume": interventions.resume_pipeline}[verb]
    with operation(ctx, verb, {"pipeline_code": pipeline_code, "reason": reason}):
        message = action(ctx.engine, ctx.config, pipeline_code, reason)
        with read_snapshot(ctx.engine) as conn:
            pipeline_id = resolve_pipeline_id(conn, pipeline_code)
            view = pipeline_view(conn, pipeline_id)
        return OperationResult(
            RunStatus.SUCCESS, message, pipeline_id, pipeline_code, pipeline=view
        )
