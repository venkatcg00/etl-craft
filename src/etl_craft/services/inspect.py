"""What the Engine DB says about pipelines: the list, a pipeline's graph, its steps, its runs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy.engine import Connection

from etl_craft.core.errors import UsageError
from etl_craft.core.graph import build_graph
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.dependencies import (
    fetch_cross_pipeline_task_edges,
    fetch_pipeline_dependency_edges,
    fetch_pipeline_graph,
)
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.repository.tasks import (
    fetch_task_codes,
    fetch_task_parameters,
    resolve_task_id,
)


@dataclass(frozen=True)
class PipelineSummary:
    """An active pipeline."""

    pipeline_code: str
    pipeline_name: str
    refresh_type: str
    run_schedule: str | None
    sla_in_hours: float | None


def list_pipelines(conn: Connection) -> list[PipelineSummary]:
    """Return every active pipeline, by code."""
    return [
        PipelineSummary(
            row.pipeline_code,
            row.pipeline_name,
            row.refresh_type,
            row.run_schedule,
            None if row.sla_in_hours is None else float(row.sla_in_hours),
        )
        for row in conn.execute(statement(conn, "active_pipelines"))
    ]


@dataclass(frozen=True)
class PipelineGraph:
    """A pipeline's order: its task waves, and what it waits for.

    ``waves`` is the guaranteed-safe static order; a task in ``conditional`` has an ``ANY`` or
    ``N`` run condition and may start earlier during a run. ``depends_on`` maps each task to
    ``(upstream, dependency type)`` pairs, with ``PIPELINE.TASK`` for an upstream elsewhere.
    """

    pipeline_code: str
    waves: list[list[str]]
    conditional: list[str]
    depends_on: dict[str, list[tuple[str, str]]]
    pipeline_dependencies: list[tuple[str, str]]


def pipeline_graph(conn: Connection, pipeline_code: str) -> PipelineGraph:
    """Return the graph of ``pipeline_code``: its waves, dependencies and conditional tasks."""
    pipeline_id = resolve_pipeline_id(conn, pipeline_code)
    data = fetch_pipeline_graph(conn, pipeline_id)
    codes = fetch_task_codes(conn, pipeline_id)
    graph = build_graph(data.tasks, data.same_pipeline_edges)
    depends_on: dict[str, list[tuple[str, str]]] = {
        codes[task_id]: [] for task_id in graph.task_ids
    }
    for edge in data.same_pipeline_edges:
        depends_on[codes[edge.task_id]].append(
            (codes[edge.depends_on_task_id], edge.dependency_type)
        )
    for task_id in sorted(data.cross_pipeline_task_ids):
        for cross in fetch_cross_pipeline_task_edges(conn, task_id):
            depends_on[codes[task_id]].append((cross.depends_on_label, cross.dependency_type))
    return PipelineGraph(
        pipeline_code=pipeline_code,
        waves=[[codes[task_id] for task_id in wave] for wave in graph.waves()],
        conditional=sorted(
            codes[task.task_id] for task in data.tasks if (task.run_condition or "ALL") != "ALL"
        ),
        depends_on={code: sorted(edges) for code, edges in sorted(depends_on.items())},
        pipeline_dependencies=[
            (edge.depends_on_pipeline_code, edge.dependency_type)
            for edge in fetch_pipeline_dependency_edges(conn, pipeline_id)
        ],
    )


@dataclass(frozen=True)
class Step:
    """An active task and its active parameters."""

    task_code: str
    task_type: str
    handler: str
    run_condition: str | None
    run_condition_count: int | None
    parameters: dict[str, str]


def pipeline_steps(conn: Connection, pipeline_code: str) -> list[Step]:
    """Return the active tasks of ``pipeline_code`` with their parameters, by task code."""
    pipeline_id = resolve_pipeline_id(conn, pipeline_code)
    rows = conn.execute(statement(conn, "pipeline_steps"), {"pipeline_id": pipeline_id}).all()
    return [
        Step(
            row.task_code,
            row.task_type,
            row.handler,
            row.run_condition,
            row.run_condition_count,
            dict(sorted(fetch_task_parameters(conn, row.task_id).items())),
        )
        for row in rows
    ]


@dataclass(frozen=True)
class RunEntry:
    """One run of a pipeline, or of a task within one; task-only fields are ``None`` otherwise."""

    pipeline_run_id: int
    status: str
    start_date: datetime | None
    end_date: datetime | None
    sla_status: str | None = None
    attempt_count: int | None = None
    source_count: int | None = None
    target_count: int | None = None
    error_message: str | None = None


def run_history(
    conn: Connection, pipeline_code: str, task_code: str | None = None, *, limit: int = 20
) -> list[RunEntry]:
    """Return the latest ``limit`` runs of a pipeline, or of one of its tasks, newest first."""
    if limit < 1:
        raise UsageError(f"--limit must be 1 or more, got {limit}")
    pipeline_id = resolve_pipeline_id(conn, pipeline_code)
    if task_code is None:
        rows = conn.execute(
            statement(conn, "pipeline_run_history"), {"pipeline_id": pipeline_id, "limit": limit}
        )
        return [
            RunEntry(r.pipeline_run_id, r.status, r.start_date, r.end_date, sla_status=r.sla_status)
            for r in rows
        ]
    task_id = resolve_task_id(conn, pipeline_id, task_code)
    rows = conn.execute(statement(conn, "task_run_history"), {"task_id": task_id, "limit": limit})
    return [
        RunEntry(
            r.pipeline_run_id,
            r.status,
            r.start_date,
            r.end_date,
            attempt_count=r.attempt_count,
            source_count=r.source_count,
            target_count=r.target_count,
            error_message=r.error_message,
        )
        for r in rows
    ]
