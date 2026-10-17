"""Why a task cannot start yet: pending upstream tasks, unready reasons and backfill gates."""

from __future__ import annotations

from sqlalchemy.engine import Engine

from etl_craft.core.enums import (
    TERMINAL_STATUSES,
    DependencyType,
)
from etl_craft.core.graph import DependencyGraph, RunState, TaskRunState
from etl_craft.engine.repository.dependencies import (
    fetch_cross_pipeline_task_edges,
)
from etl_craft.execution.gates import CrossPipelineCheck

BACKFILL_ASSUMED = frozenset(
    {DependencyType.SUCCESS, DependencyType.HAS_DATA, DependencyType.ALWAYS}
)


def backfill_cross_check(engine: Engine, task_id: int, needed: int) -> CrossPipelineCheck:
    """Judge a task's dependencies on other pipelines in a backfill, which checks none of them.

    A backfill re-runs history as if its upstreams had delivered, so ``SUCCESS``, ``HAS_DATA``
    and ``ALWAYS`` dependencies count as met. A ``FAILURE`` dependency does not: nothing says an
    upstream failed, and a task that runs only on an upstream failure (an alert) must not run.
    Nothing is consumed either way.
    """
    with engine.connect() as conn:
        edges = fetch_cross_pipeline_task_edges(conn, task_id)
    assumed = sum(1 for edge in edges if edge.dependency_type in BACKFILL_ASSUMED)
    unmet = [edge for edge in edges if edge.dependency_type not in BACKFILL_ASSUMED]
    reasons = tuple(
        f"upstream task {edge.depends_on_label} ({edge.dependency_type}) is not evaluated in a "
        "backfill, so it is not met"
        for edge in unmet
    )
    return CrossPipelineCheck(min(needed, assumed), reasons)


def upstream_pending(graph: DependencyGraph, task_id: int, run_state: RunState) -> bool:
    """Whether a same-pipeline upstream of ``task_id`` has not finished yet."""
    return any(
        run_state.get(edge.depends_on_task_id, TaskRunState()).status not in TERMINAL_STATUSES
        for edge in graph.dependencies_of(task_id)
    )


def describe_unready(
    graph: DependencyGraph, task_id: int, pipeline_run_id: int, *, can_never: bool = False
) -> str:
    """Explain why ``task_id`` cannot start in ``pipeline_run_id``."""
    counts = (
        f"(needs {graph.required_edge_count(task_id)} of {graph.total_edge_count(task_id)} "
        "dependencies satisfied)"
    )
    if can_never:
        return f"its dependencies can never be met under pipeline_run_id={pipeline_run_id} {counts}"
    return f"its dependencies are not met yet under pipeline_run_id={pipeline_run_id} {counts}"
