"""Find the project's pipelines, tasks, tables and runs from one search text."""

from dataclasses import dataclass
from typing import ClassVar

from sqlalchemy.engine import Connection

from etl_craft.config.targets import active_catalog
from etl_craft.core.enums import RunStatus, SearchKind
from etl_craft.core.errors import UsageError
from etl_craft.engine.connection import read_snapshot
from etl_craft.engine.queries import statement
from etl_craft.services.catalog.html import table_url
from etl_craft.services.lineage import table_name
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.permissions import PipelineAccess, pipeline_access
from etl_craft.services.operations.requests import Request

MAX_TEXT = 200


@dataclass(frozen=True, kw_only=True)
class SearchInput(Request):
    """Text matched case-insensitively within codes and names, and exactly as a run id or key."""

    q: str
    limit: int = 20

    def __post_init__(self) -> None:
        """Refuse empty, oversized or unbounded searches before database work."""
        if not self.q.strip() or len(self.q) > MAX_TEXT:
            raise UsageError(
                f"q has {len(self.q.strip())} characters; expected 1 to {MAX_TEXT}; "
                "type part of a pipeline, task or table name, or a run id or key"
            )
        if not 1 <= self.limit <= 50:
            raise UsageError(f"limit={self.limit}; expected 1 to 50; choose a smaller limit")


@dataclass(frozen=True)
class SearchHit:
    """One found object: the pipeline and run that locate it, or its catalog page."""

    kind: SearchKind
    label: str
    detail: str
    pipeline_code: str | None = None
    run_id: int | None = None
    catalog_path: str | None = None


@dataclass(frozen=True)
class SearchView:
    """Hits in kind order (runs, pipelines, tasks, tables); ``truncated`` when more matched."""

    SCHEMA: ClassVar[str] = "etl-craft/search/1"
    q: str
    hits: tuple[SearchHit, ...]
    truncated: bool


def search(ctx: OperationContext, request: SearchInput) -> SearchView:
    """Match visible objects only; tables need whole-project view, as the catalog does."""
    text = request.q.strip()
    with read_snapshot(ctx.engine) as conn:
        access = pipeline_access(conn, ctx)
        hits = [
            *_runs(conn, access, text),
            *_pipelines(conn, access, text.lower()),
            *_tasks(conn, access, text.lower()),
        ]
        if all(role is not None for role in access.roles.values()):
            hits.extend(_tables(conn, ctx, text.lower()))
    return SearchView(text, tuple(hits[: request.limit]), len(hits) > request.limit)


def _runs(conn: Connection, access: PipelineAccess, text: str) -> list[SearchHit]:
    hits = []
    if text.isdigit() and len(text) <= 18:
        code = conn.execute(
            statement(conn, "api_run_code"), {"run_id": int(text)}
        ).scalar_one_or_none()
        if code is not None:
            hits.append(SearchHit(SearchKind.RUN, f"Run {text}", "run id", str(code), int(text)))
    hits.extend(
        SearchHit(
            SearchKind.RUN,
            f"Run {row.pipeline_run_id}",
            f"run key · {RunStatus(row.status)}",
            row.pipeline_code,
            row.pipeline_run_id,
        )
        for row in conn.execute(statement(conn, "search_run_key"), {"run_key": text})
    )
    return [hit for hit in hits if access.permits(str(hit.pipeline_code))]


def _pipelines(conn: Connection, access: PipelineAccess, text: str) -> list[SearchHit]:
    return [
        SearchHit(
            SearchKind.PIPELINE, row.pipeline_code, row.pipeline_name or "", row.pipeline_code
        )
        for row in conn.execute(statement(conn, "active_pipelines"))
        if access.permits(row.pipeline_code)
        and (text in row.pipeline_code.lower() or text in (row.pipeline_name or "").lower())
    ]


def _tasks(conn: Connection, access: PipelineAccess, text: str) -> list[SearchHit]:
    return [
        SearchHit(
            SearchKind.TASK,
            row.task_code,
            f"{row.handler} in {row.pipeline_code}",
            row.pipeline_code,
        )
        for row in conn.execute(statement(conn, "active_tasks"))
        if access.permits(row.pipeline_code) and text in row.task_code.lower()
    ]


def _tables(conn: Connection, ctx: OperationContext, text: str) -> list[SearchHit]:
    database = active_catalog(ctx.config) if ctx.config.warehouse is not None else None
    hits = []
    for row in conn.execute(statement(conn, "catalog_tables")):
        name = table_name((row.catalog_name or "", row.schema_name, row.table_name), database)
        if text in name:
            hits.append(
                SearchHit(
                    SearchKind.TABLE, name, str(row.object_kind), catalog_path=table_url(name)
                )
            )
    return hits
