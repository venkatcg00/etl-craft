"""A task's stored position: an ingestion script's offset or a SQL task's watermark."""

from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar

from etl_craft.core.enums import Handler, PositionKind, ScriptKind
from etl_craft.engine.connection import read_snapshot
from etl_craft.engine.repository.offsets import fetch_task_offset
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.repository.tasks import (
    fetch_task_execution_detail,
    fetch_task_parameters,
    resolve_task_id,
)
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.requests import PipelineInput, require_text


@dataclass(frozen=True, kw_only=True)
class TaskInput(PipelineInput):
    """One task of a pipeline, without a run."""

    task_code: str

    def __post_init__(self) -> None:
        """Refuse a blank task code as well."""
        super().__post_init__()
        require_text("task_code", self.task_code)


@dataclass(frozen=True)
class TaskOffset:
    """What the task keeps between runs, if anything, and what is stored now."""

    SCHEMA: ClassVar[str] = "etl-craft/task-offset/1"
    pipeline_code: str
    task_code: str
    kind: PositionKind | None
    offset_type: str | None = None
    offset_value: str | None = None
    pending_task_run_id: int | None = None
    pending_upper_bound: datetime | None = None


def position_kind(handler: str, params: dict[str, str]) -> PositionKind | None:
    """Return what the task keeps: an ingestion offset, a SQL watermark, or nothing."""
    if handler == Handler.PYTHON and params.get("SCRIPT_KIND") == ScriptKind.INGESTION:
        return PositionKind.OFFSET
    if handler == Handler.SQL and params.get("WATERMARK_COLUMN"):
        return PositionKind.WATERMARK
    return None


def read_offset(ctx: OperationContext, request: TaskInput) -> TaskOffset:
    """Read the stored position from the same tracker row the task advances."""
    with read_snapshot(ctx.engine) as conn:
        pipeline_id = resolve_pipeline_id(conn, request.pipeline_code)
        task_id = resolve_task_id(conn, pipeline_id, request.task_code)
        handler = fetch_task_execution_detail(conn, task_id).handler
        kind = position_kind(handler, fetch_task_parameters(conn, task_id))
        stored = fetch_task_offset(conn, task_id) if kind is not None else None
    base = TaskOffset(request.pipeline_code, request.task_code, kind)
    if stored is None:
        return base
    return TaskOffset(
        base.pipeline_code,
        base.task_code,
        kind,
        stored.offset_type,
        stored.offset_value,
        stored.pending_task_run_id,
        stored.pending_upper_bound,
    )
