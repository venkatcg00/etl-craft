"""Building a task's context from the Engine DB, by its task run."""

from __future__ import annotations

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.definitions import Definition, using
from etl_craft.core.enums import YesNo
from etl_craft.core.errors import RunStateError
from etl_craft.engine import run_scopes
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.events import run_event_data
from etl_craft.engine.repository.tasks import TaskExecutionDetail, fetch_task_parameters
from etl_craft.engine.runlog import as_date
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
        definition = Definition.read(row.metadata_snapshot, row.pipeline_run_id, row.bundle_id)
        with using(definition):
            task = definition.row("CFG_TASKS", "task_id", row.task_id)
            pipeline = definition.row("CFG_PIPELINES", "pipeline_id", row.pipeline_id)
            detail = TaskExecutionDetail(
                str(task["handler"]),
                str(task["task_code"]),
                row.pipeline_id,
                str(pipeline["pipeline_code"]),
                str(pipeline["refresh_type"]),
            )
            params = fetch_task_parameters(conn, row.task_id)
            selected = (
                None
                if row.task_selection is None
                else run_scopes.TaskSelection.deserialize(row.task_selection)
            )
            run_scopes.check_task(
                conn,
                row.pipeline_run_id,
                row.task_id,
                definition=(detail, params),
                selection=(selected, row.exclusive_run),
            )
        event_data = run_event_data(conn, row.pipeline_run_id)
    return TaskContext(
        config=config,
        bundle_id=row.bundle_id,
        metadata_version=row.metadata_version,
        _definition=definition,
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
        run_date=as_date(row.run_date),
        backfill=row.backfill == YesNo.YES,
        event_files=event_data.files,
        event_keys=event_data.keys,
        event_rows=event_data.rows,
        writer=row.writer,
    )
