"""Dependencies: a pipeline's graph, and the edges the cross-pipeline gates check."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass

from sqlalchemy.engine import Connection

from etl_craft.core.graph import TaskEdge, TaskNode
from etl_craft.engine.queries import statement


@dataclass(frozen=True)
class PipelineGraphData:
    """A pipeline's active tasks and same-pipeline edges, as ``build_graph`` takes them.

    Edges on tasks in other pipelines are not part of the graph; each task counts them, and
    ``cross_pipeline_task_ids`` names the tasks that have any.
    """

    tasks: list[TaskNode]
    same_pipeline_edges: list[TaskEdge]
    cross_pipeline_task_ids: frozenset[int]


def fetch_pipeline_graph(conn: Connection, pipeline_id: int) -> PipelineGraphData:
    """Return the tasks and dependencies of ``pipeline_id`` for its dependency graph."""
    edges = conn.execute(
        statement(conn, "pipeline_task_dependencies"), {"pipeline_id": pipeline_id}
    ).all()
    same = [
        TaskEdge(row.task_id, row.depends_on_task_id, row.dependency_type)
        for row in edges
        if row.depends_on_pipeline_id == pipeline_id
    ]
    cross = Counter(row.task_id for row in edges if row.depends_on_pipeline_id != pipeline_id)
    tasks = [
        TaskNode(
            task_id=row.task_id,
            run_condition=row.run_condition,
            run_condition_count=row.run_condition_count,
            cross_pipeline_edge_count=cross.get(row.task_id, 0),
        )
        for row in conn.execute(
            statement(conn, "pipeline_graph_tasks"), {"pipeline_id": pipeline_id}
        )
    ]
    return PipelineGraphData(tasks, same, frozenset(cross))


@dataclass(frozen=True)
class PipelineDependencyEdge:
    """One active dependency of a pipeline on another pipeline."""

    pipeline_dependency_id: int
    depends_on_pipeline_id: int
    dependency_type: str


def fetch_pipeline_dependency_edges(
    conn: Connection, pipeline_id: int
) -> list[PipelineDependencyEdge]:
    """Return the active dependencies of ``pipeline_id`` on other pipelines."""
    rows = conn.execute(statement(conn, "pipeline_dependency_edges"), {"pipeline_id": pipeline_id})
    return [
        PipelineDependencyEdge(
            row.pipeline_dependency_id, row.depends_on_pipeline_id, row.dependency_type
        )
        for row in rows
    ]


@dataclass(frozen=True)
class CrossPipelineTaskEdge:
    """One active dependency of a task on a task in another pipeline."""

    task_dependency_id: int
    pipeline_id: int
    depends_on_pipeline_id: int
    depends_on_task_id: int
    dependency_type: str


def fetch_cross_pipeline_task_edges(conn: Connection, task_id: int) -> list[CrossPipelineTaskEdge]:
    """Return the active dependencies of ``task_id`` on tasks in other pipelines."""
    rows = conn.execute(statement(conn, "task_cross_pipeline_dependencies"), {"task_id": task_id})
    return [
        CrossPipelineTaskEdge(
            row.task_dependency_id,
            row.pipeline_id,
            row.depends_on_pipeline_id,
            row.depends_on_task_id,
            row.dependency_type,
        )
        for row in rows
    ]
