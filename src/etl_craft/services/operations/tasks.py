"""One task's current definition: its handler and parameters."""

from dataclasses import dataclass
from typing import ClassVar

from etl_craft.core.enums import Handler
from etl_craft.core.text import pipe_list
from etl_craft.engine.connection import read_snapshot
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.repository.tasks import (
    fetch_task_execution_detail,
    fetch_task_parameters,
    resolve_task_id,
)
from etl_craft.handlers.sql.spec import MERGES
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.offsets import TaskInput


@dataclass(frozen=True)
class TaskDefinition:
    """The task's handler and its active parameters, as configured now."""

    SCHEMA: ClassVar[str] = "etl-craft/task/1"
    pipeline_code: str
    task_code: str
    handler: str
    parameters: dict[str, str]
    merge_key: tuple[str, ...] = ()
    merge_columns: tuple[str, ...] = ()


def read_task(ctx: OperationContext, request: TaskInput) -> TaskDefinition:
    """Read the definition new runs use, not one admitted by an earlier run."""
    with read_snapshot(ctx.engine) as conn:
        pipeline_id = resolve_pipeline_id(conn, request.pipeline_code)
        task_id = resolve_task_id(conn, pipeline_id, request.task_code)
        handler = fetch_task_execution_detail(conn, task_id).handler
        parameters = fetch_task_parameters(conn, task_id)
    merging = handler == Handler.SQL and parameters.get("SQL_ACTION") in MERGES
    return TaskDefinition(
        request.pipeline_code,
        request.task_code,
        handler,
        parameters,
        pipe_list(parameters.get("MERGE_KEY")) if merging else (),
        pipe_list(parameters.get("MERGE_COMPARE_COLUMNS")) if merging else (),
    )
