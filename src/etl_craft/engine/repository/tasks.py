"""Tasks: resolving a code, what running one needs, and its parameters."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.engine import Connection

from etl_craft.core.errors import MetadataError
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pipelines import with_suggestions


def resolve_task_id(conn: Connection, pipeline_id: int, task_code: str) -> int:
    """Return the id of active task ``task_code`` in ``pipeline_id``; ``MetadataError`` if none."""
    task_id = conn.execute(
        statement(conn, "task_id_by_code"), {"pipeline_id": pipeline_id, "task_code": task_code}
    ).scalar_one_or_none()
    if task_id is None:
        known = list(fetch_task_codes(conn, pipeline_id).values())
        raise MetadataError(
            with_suggestions(
                f"no active task with TASK_CODE={task_code!r} in pipeline_id={pipeline_id}",
                task_code,
                known,
            )
        )
    return int(task_id)


def fetch_task_codes(conn: Connection, pipeline_id: int) -> dict[int, str]:
    """Map the id of every active task in ``pipeline_id`` to its code."""
    rows = conn.execute(statement(conn, "task_codes"), {"pipeline_id": pipeline_id})
    return {row.task_id: row.task_code for row in rows}


@dataclass(frozen=True)
class TaskExecutionDetail:
    """What running a task needs beyond its parameters."""

    handler: str
    task_code: str
    pipeline_id: int
    pipeline_code: str
    refresh_type: str


def fetch_task_execution_detail(conn: Connection, task_id: int) -> TaskExecutionDetail:
    """Return what running ``task_id`` needs; the task must exist."""
    row = conn.execute(statement(conn, "task_execution_detail"), {"task_id": task_id}).one()
    return TaskExecutionDetail(
        handler=row.handler,
        task_code=row.task_code,
        pipeline_id=row.pipeline_id,
        pipeline_code=row.pipeline_code,
        refresh_type=row.refresh_type,
    )


def fetch_task_parameters(conn: Connection, task_id: int) -> dict[str, str]:
    """Map each active parameter name of ``task_id`` to its value."""
    rows = conn.execute(statement(conn, "task_parameters"), {"task_id": task_id})
    return {row.parameter_name: row.parameter_value for row in rows}


@dataclass(frozen=True)
class TargetTask:
    """Another active SQL task in the same pipeline with the same ``TARGET_OBJECT``."""

    task_id: int
    task_code: str
    sql_action: str


def fetch_target_tasks(
    conn: Connection, pipeline_id: int, task_id: int, target_object: str
) -> list[TargetTask]:
    """Return the other active SQL tasks in ``pipeline_id`` writing ``target_object``."""
    rows = conn.execute(
        statement(conn, "target_tasks"),
        {"pipeline_id": pipeline_id, "task_id": task_id, "target_object": target_object},
    )
    return [TargetTask(row.task_id, row.task_code, row.sql_action) for row in rows]


@dataclass(frozen=True)
class FailureWatchMessage:
    """A task watched through a FAILURE dependency, with its latest error message."""

    depends_on_task_code: str
    error_message: str | None


def fetch_failure_watch_messages(conn: Connection, task_id: int) -> list[FailureWatchMessage]:
    """Return each task ``task_id`` watches through an active FAILURE dependency.

    The message is from the watched task's latest run under any pipeline run, since it may be in
    another pipeline with no run in common.
    """
    rows = conn.execute(statement(conn, "failure_watch_messages"), {"task_id": task_id})
    return [
        FailureWatchMessage(
            depends_on_task_code=row.depends_on_task_code, error_message=row.error_message
        )
        for row in rows
    ]
