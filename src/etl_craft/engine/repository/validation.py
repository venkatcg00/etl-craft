"""What ``validate`` reads: every active task, and every active dependency with its upstream."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.engine import Connection

from etl_craft.engine.queries import statement


@dataclass(frozen=True)
class ActiveTask:
    """An active task of an active pipeline."""

    task_id: int
    pipeline_id: int
    pipeline_code: str
    task_code: str
    handler: str
    refresh_type: str

    @property
    def label(self) -> str:
        """``PIPELINE_CODE.TASK_CODE``."""
        return f"{self.pipeline_code}.{self.task_code}"


def fetch_active_tasks(conn: Connection) -> list[ActiveTask]:
    """Return every active task of an active pipeline, by pipeline and task code."""
    return [
        ActiveTask(
            r.task_id, r.pipeline_id, r.pipeline_code, r.task_code, r.handler, r.refresh_type
        )
        for r in conn.execute(statement(conn, "active_tasks"))
    ]


def fetch_pipeline_parameters(conn: Connection) -> list[tuple[str, object]]:
    """Return every active pipeline's code and ``PIPELINE_PARAMETERS`` as stored."""
    rows = conn.execute(statement(conn, "active_pipeline_parameters"))
    return [(r.pipeline_code, r.pipeline_parameters) for r in rows]


@dataclass(frozen=True)
class DependencyEdge:
    """An active dependency of an active task, with what is known of its upstream task.

    ``written_pipeline_id`` is the row's ``DEPENDS_ON_PIPELINE_ID``; ``depends_on_pipeline_id``
    is the pipeline the upstream task belongs to.
    """

    pipeline_code: str
    task_code: str
    dependency_type: str
    written_pipeline_id: int | None
    depends_on_task_id: int
    depends_on_task_code: str
    depends_on_handler: str
    depends_on_task_active: bool
    depends_on_pipeline_id: int
    depends_on_pipeline_code: str
    depends_on_pipeline_active: bool

    @property
    def label(self) -> str:
        """The dependent task, ``PIPELINE_CODE.TASK_CODE``."""
        return f"{self.pipeline_code}.{self.task_code}"

    @property
    def depends_on_label(self) -> str:
        """The upstream task, ``PIPELINE_CODE.TASK_CODE``."""
        return f"{self.depends_on_pipeline_code}.{self.depends_on_task_code}"


def fetch_task_dependency_edges(conn: Connection) -> list[DependencyEdge]:
    """Return every active dependency of an active task of an active pipeline."""
    return [
        DependencyEdge(
            r.pipeline_code,
            r.task_code,
            r.dependency_type,
            r.written_pipeline_id,
            r.depends_on_task_id,
            r.depends_on_task_code,
            r.depends_on_handler,
            r.depends_on_task_active == "Y",
            r.depends_on_pipeline_id,
            r.depends_on_pipeline_code,
            r.depends_on_pipeline_active == "Y",
        )
        for r in conn.execute(statement(conn, "active_task_dependency_edges"))
    ]


@dataclass(frozen=True)
class PipelineEdge:
    """An active dependency of an active pipeline on another."""

    pipeline_code: str
    depends_on_pipeline_code: str
    dependency_type: str
    depends_on_pipeline_active: bool


def fetch_pipeline_edges(conn: Connection) -> list[PipelineEdge]:
    """Return every active dependency of an active pipeline, upstream active or not."""
    return [
        PipelineEdge(
            r.pipeline_code,
            r.depends_on_pipeline_code,
            r.dependency_type,
            r.depends_on_pipeline_active == "Y",
        )
        for r in conn.execute(statement(conn, "active_pipeline_dependency_edges"))
    ]
