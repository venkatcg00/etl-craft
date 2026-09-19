"""Read-only queries against CFG_ tables needed by `run` and the resolver."""

# Writing CFG_ rows stays outside the CLI entirely, per CLAUDE.md's CLI
# surface section — pipeline/task/dependency registration is manual,
# git-managed migrations. Everything here is SELECT-only.

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.engine import Connection

from etl_craft.resolver import TaskEdge, TaskNode


class CfgError(Exception):
    """Raised when a --pipeline_code/--task_code doesn't resolve to an active CFG_ row."""


def resolve_pipeline_id(conn: Connection, pipeline_code: str) -> int:
    """Resolve an active PIPELINE_CODE to its PIPELINE_ID."""
    pipeline_id = conn.execute(
        text(
            "SELECT PIPELINE_ID FROM CFG_PIPELINES "
            "WHERE PIPELINE_CODE = :pipeline_code AND ACTIVE_FLAG = 'Y'"
        ),
        {"pipeline_code": pipeline_code},
    ).scalar_one_or_none()
    if pipeline_id is None:
        raise CfgError(f"no active pipeline with PIPELINE_CODE={pipeline_code!r}")
    return pipeline_id


def resolve_task_id(conn: Connection, pipeline_id: int, task_code: str) -> int:
    """Resolve an active TASK_CODE (scoped to `pipeline_id`) to its TASK_ID."""
    task_id = conn.execute(
        text(
            "SELECT TASK_ID FROM CFG_TASKS "
            "WHERE PIPELINE_ID = :pipeline_id AND TASK_CODE = :task_code AND ACTIVE_FLAG = 'Y'"
        ),
        {"pipeline_id": pipeline_id, "task_code": task_code},
    ).scalar_one_or_none()
    if task_id is None:
        raise CfgError(
            f"no active task with TASK_CODE={task_code!r} under pipeline_id={pipeline_id}"
        )
    return task_id


def fetch_task_handler(conn: Connection, task_id: int) -> str:
    """Fetch the HANDLER value for `task_id` (assumed to already be a valid, active task)."""
    return conn.execute(
        text("SELECT HANDLER FROM CFG_TASKS WHERE TASK_ID = :task_id"),
        {"task_id": task_id},
    ).scalar_one()


@dataclass(frozen=True)
class PipelineGraphData:
    """The CFG_TASKS/CFG_TASK_DEPENDENCY rows resolver.build_graph needs for one pipeline."""

    tasks: list[TaskNode]
    same_pipeline_edges: list[TaskEdge]
    # Tasks with >=1 cross-pipeline dependency edge (DEPENDS_ON_PIPELINE_ID !=
    # this pipeline's own id) — excluded from same_pipeline_edges since the
    # resolver only handles same-pipeline structure (see resolver.py's own
    # module docstring). Surfaced here, not resolved: cross-pipeline edges
    # are the self-check/poll step's job, not built yet.
    cross_pipeline_task_ids: frozenset[int]


def fetch_pipeline_graph(conn: Connection, pipeline_id: int) -> PipelineGraphData:
    """Fetch active tasks and same-pipeline dependency edges for `pipeline_id`."""
    task_rows = conn.execute(
        text(
            "SELECT TASK_ID AS task_id FROM CFG_TASKS "
            "WHERE PIPELINE_ID = :pipeline_id AND ACTIVE_FLAG = 'Y'"
        ),
        {"pipeline_id": pipeline_id},
    ).all()
    tasks = [TaskNode(task_id=row.task_id) for row in task_rows]

    edge_rows = conn.execute(
        text(
            "SELECT TASK_ID AS task_id, DEPENDS_ON_TASK_ID AS depends_on_task_id, "
            "DEPENDS_ON_PIPELINE_ID AS depends_on_pipeline_id, "
            "DEPENDENCY_TYPE AS dependency_type "
            "FROM CFG_TASK_DEPENDENCY WHERE PIPELINE_ID = :pipeline_id AND ACTIVE_FLAG = 'Y'"
        ),
        {"pipeline_id": pipeline_id},
    ).all()
    same_pipeline_edges = [
        TaskEdge(
            task_id=row.task_id,
            depends_on_task_id=row.depends_on_task_id,
            dependency_type=row.dependency_type,
        )
        for row in edge_rows
        if row.depends_on_pipeline_id == pipeline_id
    ]
    cross_pipeline_task_ids = frozenset(
        row.task_id for row in edge_rows if row.depends_on_pipeline_id != pipeline_id
    )
    return PipelineGraphData(
        tasks=tasks,
        same_pipeline_edges=same_pipeline_edges,
        cross_pipeline_task_ids=cross_pipeline_task_ids,
    )
