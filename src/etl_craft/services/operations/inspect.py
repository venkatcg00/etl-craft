"""Read-only operation documents for pipeline definitions and execution history."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime

from etl_craft.core.actor import acting_as
from etl_craft.core.errors import RunStateError, UsageError
from etl_craft.engine.connection import read_snapshot
from etl_craft.engine.repository.audit_reads import fetch_audit_records
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.repository.tasks import resolve_task_id
from etl_craft.engine.runlog import ACTIVE_RUN, RunSelector, select_run
from etl_craft.execution.leases import as_utc
from etl_craft.services import inspect as queries
from etl_craft.services.operations.context import OperationContext, PipelineRef
from etl_craft.services.operations.models import (
    ActionView,
    AuditView,
    GraphView,
    HistoryView,
    MetadataChangeView,
    PipelineListView,
    RunView,
    StepsView,
    StepView,
    TaskRunView,
)
from etl_craft.services.operations.snapshots import pipeline_view, run_view, task_run_view


def list_pipelines(ctx: OperationContext) -> PipelineListView:
    """List active definitions without recording a mutating action."""
    with acting_as(ctx.actor), read_snapshot(ctx.engine) as conn:
        return PipelineListView(
            tuple(
                pipeline_view(conn, resolve_pipeline_id(conn, item.pipeline_code))
                for item in queries.list_pipelines(conn)
            )
        )


def pipeline_graph(ctx: OperationContext, pipeline: PipelineRef) -> GraphView:
    """Read the static graph with the pipeline's canonical definition id."""
    with acting_as(ctx.actor), read_snapshot(ctx.engine) as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline.pipeline_code)
        graph = queries.pipeline_graph(conn, pipeline.pipeline_code)
        return GraphView(
            pipeline_id,
            pipeline.pipeline_code,
            tuple(tuple(w) for w in graph.waves),
            tuple(graph.conditional),
            {code: tuple(edges) for code, edges in graph.depends_on.items()},
            tuple(graph.pipeline_dependencies),
        )


def pipeline_steps(
    ctx: OperationContext,
    pipeline: PipelineRef,
    *,
    selector: RunSelector = ACTIVE_RUN,
) -> StepsView:
    """Read configured tasks under the exact selected run, including tasks not yet started."""
    with acting_as(ctx.actor), read_snapshot(ctx.engine) as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline.pipeline_code)
        selected = select_run(conn, pipeline_id, selector)
        steps = []
        for step in queries.pipeline_steps(
            conn,
            pipeline.pipeline_code,
            run_id=selected.pipeline_run_id,
        ):
            task_id = resolve_task_id(conn, pipeline_id, step.task_code)
            task = task_run_view(
                conn,
                pipeline_id,
                pipeline_run_id=selected.pipeline_run_id,
                task_id=task_id,
            )
            steps.append(
                StepView(
                    pipeline_id,
                    selected.pipeline_run_id,
                    task_id,
                    step.task_code,
                    None if task is None else task.task_run_id,
                    step.task_type,
                    step.handler,
                    step.run_condition,
                    step.run_condition_count,
                    dict(step.parameters),
                    step.status,
                )
            )
        return StepsView(pipeline_id, selected.pipeline_run_id, tuple(steps))


def run_history(
    ctx: OperationContext,
    pipeline: PipelineRef,
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
        pipeline_id = resolve_pipeline_id(conn, pipeline.pipeline_code)
        selected = None if all_runs else select_run(conn, pipeline_id, selector)
        summaries = queries.run_history(
            conn,
            pipeline.pipeline_code,
            task_code,
            limit=limit,
            run_id=None if selected is None else selected.pipeline_run_id,
        )
        entries: list[RunView | TaskRunView] = []
        for summary in summaries:
            if task_code is None:
                entries.append(run_view(conn, pipeline_id, summary.pipeline_run_id))
            else:
                captured = task_run_view(conn, pipeline_id, task_run_id=summary.task_run_id)
                if captured is None:
                    raise RunStateError(
                        f"task_run_id={summary.task_run_id} disappeared from history; "
                        "read history again after the current operation finishes"
                    )
                entries.append(captured)
        changes = queries.run_interventions(conn, pipeline.pipeline_code, summaries, task_code)
        return HistoryView(
            pipeline_id,
            pipeline.pipeline_code,
            task_code,
            tuple(entries),
            tuple(replace(change, requested_at=as_utc(change.requested_at)) for change in changes),
        )


def audit(
    ctx: OperationContext,
    pipeline: PipelineRef | None = None,
    *,
    since: datetime | None = None,
) -> AuditView:
    """Read completed requests and immutable metadata captures without making an action."""
    since = None if since is None else as_utc(since)
    with acting_as(ctx.actor), read_snapshot(ctx.engine) as conn:
        pipeline_id = (
            None if pipeline is None else resolve_pipeline_id(conn, pipeline.pipeline_code)
        )
        actions, changes = fetch_audit_records(conn, pipeline_id, since)
        for row in (*actions, *changes):
            row["at"] = as_utc(row["at"])
        return AuditView(
            tuple(ActionView(**row) for row in actions),
            tuple(MetadataChangeView(**row) for row in changes),
        )
