"""Building a task's context from the Engine DB, by its task run."""

from __future__ import annotations

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.errors import RunStateError
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.tasks import fetch_task_execution_detail, fetch_task_parameters
from etl_craft.engine.runlog import fetch_run_kind
from etl_craft.handlers.registry import TaskContext


def build_task_context(
    engine: Engine,
    config: ConnectorConfig,
    task_run_id: int,
    *,
    force: bool = False,
    rerun: bool = False,
) -> TaskContext:
    """Return the context of the attempt ``task_run_id`` is running.

    A task process is started with only its ``task_run_id``; everything else is read here, so
    nothing is passed between processes but that id. Raises ``RunStateError`` for an unknown id.
    """
    with engine.connect() as conn:
        row = conn.execute(
            statement(conn, "task_run_context"), {"task_run_id": task_run_id}
        ).one_or_none()
        if row is None:
            raise RunStateError(f"no task run with task_run_id={task_run_id}")
        detail = fetch_task_execution_detail(conn, row.task_id)
        params = fetch_task_parameters(conn, row.task_id)
        kind = fetch_run_kind(conn, row.pipeline_run_id)
    return TaskContext(
        config=config,
        pipeline_id=detail.pipeline_id,
        pipeline_code=detail.pipeline_code,
        task_id=row.task_id,
        task_code=detail.task_code,
        pipeline_run_id=row.pipeline_run_id,
        task_run_id=task_run_id,
        attempt=row.attempt_count,
        handler=detail.handler,
        refresh_type=detail.refresh_type,
        task_params=params,
        force=force,
        rerun=rerun,
        run_date=kind.run_date,
        backfill=kind.backfill,
    )
