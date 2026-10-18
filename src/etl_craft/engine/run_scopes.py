"""Persisted selections for independent manual task executions."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import date

from sqlalchemy.engine import Connection

from etl_craft.core.actor import Actor
from etl_craft.core.enums import Handler, RunStatus, YesNo
from etl_craft.core.errors import RunRefusedError, RunStateError
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.dependencies import PipelineGraphData
from etl_craft.engine.repository.tasks import (
    TaskExecutionDetail,
    fetch_task_execution_detail,
    fetch_task_parameters,
)


@dataclass(frozen=True)
class TaskSelection:
    """The selected task ids and explicit permission to omit their external dependencies."""

    task_ids: tuple[int, ...]
    ignore_dependencies: bool = False

    def serialize(self) -> str:
        """Store a stable selection for every later supervisor and task invocation."""
        return json.dumps(
            {"task_ids": self.task_ids, "ignore_dependencies": self.ignore_dependencies}
        )

    @classmethod
    def deserialize(cls, value: str) -> TaskSelection:
        """Read a selection written by etl-craft."""
        data = json.loads(value)
        return cls(tuple(data["task_ids"]), data["ignore_dependencies"])

    def exclusive(self, conn: Connection) -> bool:
        """Check handler isolation; email alone has no shared target or checkpoint state."""
        exclusive = False
        for task_id in self.task_ids:
            detail = fetch_task_execution_detail(conn, task_id)
            params = fetch_task_parameters(conn, task_id)
            exclusive |= task_exclusive(detail, params)
        return exclusive


def task_exclusive(detail: TaskExecutionDetail, params: Mapping[str, str]) -> bool:
    """Validate the actual handler inputs and report whether they need exclusive admission."""
    if detail.handler not in {Handler.SQL, Handler.EMAIL_ALERT} or any(
        name.startswith("WATERMARK_") for name in params
    ):
        raise RunRefusedError(
            f"task {detail.task_code}: handler={detail.handler}, "
            f"watermark={params.get('WATERMARK_COLUMN')!r}; independent execution "
            "requires isolated target/checkpoint state. Run this task in an explicit "
            "pipeline run; Python offsets, SQL watermarks, business-rule flags and "
            "event checkpoints do not yet have independent namespaces."
        )
    return detail.handler == Handler.SQL


def fetch_selection(conn: Connection, run_id: int) -> TaskSelection | None:
    """Read the exact immutable selection, or None for a whole-pipeline run."""
    row = conn.execute(statement(conn, "run_task_selection"), {"pipeline_run_id": run_id}).one()
    if row.task_selection is None:
        return None
    return TaskSelection.deserialize(row.task_selection)


def check_task(
    conn: Connection,
    run_id: int,
    task_id: int,
    *,
    detail: TaskExecutionDetail | None = None,
    params: Mapping[str, str] | None = None,
) -> None:
    """Refuse execution outside the selected scope or after an unsafe metadata edit."""
    scope = fetch_selection(conn, run_id)
    if scope is None:
        return
    if task_id not in scope.task_ids:
        raise RunStateError(
            f"pipeline_run_id={run_id}: task_id={task_id} is outside selected tasks "
            f"{scope.task_ids}; select a run containing this task or start a new manual run"
        )
    row = conn.execute(statement(conn, "run_task_selection"), {"pipeline_run_id": run_id}).one()
    exclusive = (
        scope.exclusive(conn)
        if detail is None or params is None
        else task_exclusive(detail, params)
    )
    if exclusive and row.exclusive_run == YesNo.NO:
        raise RunRefusedError(
            f"pipeline_run_id={run_id}: selected task metadata now needs exclusive admission; "
            "cancel this manual run and start a new one with the current definition"
        )


def selected_graph(data: PipelineGraphData, scope: TaskSelection) -> PipelineGraphData:
    """Keep selected edges; outside inputs require a recorded explicit override."""
    ids = set(scope.task_ids)
    available = {task.task_id for task in data.tasks}
    missing = ids - available
    outside = [
        edge
        for edge in data.same_pipeline_edges
        if edge.task_id in ids and edge.depends_on_task_id not in ids
    ]
    cross = ids & data.cross_pipeline_task_ids
    if missing or ((outside or cross) and not scope.ignore_dependencies):
        raise RunRefusedError(
            f"manual selection={scope.task_ids}: inactive/missing tasks={sorted(missing)}, "
            f"dependencies outside selection={outside}: "
            f"cross-pipeline inputs={sorted(cross)}; select a pipeline run with recorded inputs "
            "or explicitly use --ignore-dependencies with --reason for current inputs"
        )
    return PipelineGraphData(
        [
            replace(task, run_condition=None, run_condition_count=None, cross_pipeline_edge_count=0)
            if scope.ignore_dependencies
            else task
            for task in data.tasks
            if task.task_id in ids
        ],
        [
            edge
            for edge in data.same_pipeline_edges
            if edge.task_id in ids and edge.depends_on_task_id in ids
        ],
        frozenset(),
    )


def create_manual_run(
    conn: Connection,
    pipeline_id: int,
    actor: Actor,
    scope: TaskSelection,
    run_key: str | None,
    run_date: date,
) -> int:
    """Persist selection before admission, atomically with the exclusive-slot check."""
    from etl_craft.engine import transitions

    exclusive = scope.exclusive(conn)
    run_id = transitions.create_run(
        conn, pipeline_id, actor, run_key=run_key, run_date=run_date, status=RunStatus.QUEUED
    )
    conn.execute(
        statement(conn, "manual_run_selection"),
        {
            "pipeline_run_id": run_id,
            "selection": scope.serialize(),
            "exclusive": YesNo.of(exclusive),
        },
    )
    transitions.admit_run(conn, run_id, actor)
    return run_id
