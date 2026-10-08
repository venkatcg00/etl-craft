"""Read-only operation documents for pipeline definitions and execution history."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime

from etl_craft.core.actor import acting_as
from etl_craft.core.errors import UsageError
from etl_craft.core.graph import build_graph
from etl_craft.core.time import as_utc
from etl_craft.engine.connection import read_snapshot
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.audit_reads import fetch_audit_records
from etl_craft.engine.repository.dependencies import (
    fetch_cross_pipeline_task_edges,
    fetch_pipeline_dependency_edges,
    fetch_pipeline_graph,
)
from etl_craft.engine.repository.interventions import fetch_interventions
from etl_craft.engine.repository.pauses import fetch_open_pauses
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.repository.tasks import (
    fetch_task_codes,
    fetch_task_parameters,
    resolve_task_id,
)
from etl_craft.engine.runlog import ACTIVE_RUN, RunSelector, select_run
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.models import (
    ActionView,
    AuditView,
    GraphView,
    HistoryView,
    MetadataChangeView,
    PipelineListView,
    PipelineView,
    RunView,
    StepsView,
    StepView,
    TaskRunView,
)
from etl_craft.services.operations.snapshots import run_document, task_document


def list_pipelines(ctx: OperationContext) -> PipelineListView:
    """List active definitions without recording a mutating action."""
    with acting_as(ctx.actor), read_snapshot(ctx.engine) as conn:
        pauses = fetch_open_pauses(conn)
        views = []
        for row in conn.execute(statement(conn, "active_pipelines")):
            pause = pauses.get(row.pipeline_code)
            if pause is not None:
                pause = replace(pause, paused_at=as_utc(pause.paused_at))
            views.append(
                PipelineView(
                    row.pipeline_id,
                    row.pipeline_code,
                    row.pipeline_name,
                    row.refresh_type,
                    row.run_schedule,
                    None if row.sla_in_hours is None else float(row.sla_in_hours),
                    pause,
                )
            )
        return PipelineListView(tuple(views))


def pipeline_graph(ctx: OperationContext, pipeline_code: str) -> GraphView:
    """Read the static graph with the pipeline's canonical definition id."""
    with acting_as(ctx.actor), read_snapshot(ctx.engine) as conn:
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
        return GraphView(
            pipeline_id,
            pipeline_code,
            tuple(tuple(codes[task_id] for task_id in wave) for wave in graph.waves()),
            tuple(
                sorted(
                    codes[task.task_id]
                    for task in data.tasks
                    if (task.run_condition or "ALL") != "ALL"
                )
            ),
            {code: tuple(sorted(edges)) for code, edges in sorted(depends_on.items())},
            tuple(
                (edge.depends_on_pipeline_code, edge.dependency_type)
                for edge in fetch_pipeline_dependency_edges(conn, pipeline_id)
            ),
        )


def pipeline_steps(
    ctx: OperationContext,
    pipeline_code: str,
    *,
    selector: RunSelector = ACTIVE_RUN,
) -> StepsView:
    """Read configured tasks under the exact selected run, including tasks not yet started."""
    with acting_as(ctx.actor), read_snapshot(ctx.engine) as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        selected = select_run(conn, pipeline_id, selector)
        steps = tuple(
            StepView(
                pipeline_id,
                selected.pipeline_run_id,
                row.task_id,
                row.task_code,
                row.task_run_id,
                row.task_type,
                row.handler,
                row.run_condition,
                row.run_condition_count,
                dict(sorted(fetch_task_parameters(conn, row.task_id).items())),
                row.status,
            )
            for row in conn.execute(
                statement(conn, "pipeline_steps"),
                {"pipeline_id": pipeline_id, "run_id": selected.pipeline_run_id},
            )
        )
        return StepsView(pipeline_id, selected.pipeline_run_id, steps)


def run_history(
    ctx: OperationContext,
    pipeline_code: str,
    task_code: str | None = None,
    *,
    limit: int = 20,
    all_runs: bool = False,
    selector: RunSelector = ACTIVE_RUN,
) -> HistoryView:
    """Read bounded history; selection never falls back to the most recent ended run."""
    if limit < 1:
        raise UsageError(f"--limit must be 1 or more, got {limit}")
    if all_runs and (selector.run_id is not None or selector.run_key is not None):
        raise UsageError("--all lists runs; do not pass a run selector")
    with acting_as(ctx.actor), read_snapshot(ctx.engine) as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        selected = None if all_runs else select_run(conn, pipeline_id, selector)
        parameters = {
            "pipeline_id": pipeline_id,
            "limit": limit,
            "run_id": None if selected is None else selected.pipeline_run_id,
        }
        entries: tuple[RunView | TaskRunView, ...]
        if task_code is None:
            entries = tuple(
                run_document(row._mapping)
                for row in conn.execute(statement(conn, "pipeline_run_history"), parameters)
            )
        else:
            parameters["task_id"] = resolve_task_id(conn, pipeline_id, task_code)
            entries = tuple(
                task_document(conn, row._mapping)
                for row in conn.execute(statement(conn, "task_run_history"), parameters)
            )
        listed = {entry.pipeline_run_id for entry in entries}
        changes = (
            ()
            if not listed
            else tuple(
                replace(change, requested_at=as_utc(change.requested_at))
                for change in fetch_interventions(conn, pipeline_id, min(listed))
                if change.pipeline_run_id in listed
                and (task_code is None or change.task_code in (None, task_code))
            )
        )
        return HistoryView(pipeline_id, pipeline_code, task_code, entries, changes)


def audit(
    ctx: OperationContext,
    pipeline_code: str | None = None,
    *,
    since: datetime | None = None,
) -> AuditView:
    """Read completed requests and immutable metadata captures without making an action."""
    since = None if since is None else as_utc(since)
    with acting_as(ctx.actor), read_snapshot(ctx.engine) as conn:
        pipeline_id = None if pipeline_code is None else resolve_pipeline_id(conn, pipeline_code)
        actions, changes = fetch_audit_records(conn, pipeline_id, since)
        for row in (*actions, *changes):
            row["at"] = as_utc(row["at"])
        return AuditView(
            tuple(ActionView(**row) for row in actions),
            tuple(MetadataChangeView(**row) for row in changes),
        )
