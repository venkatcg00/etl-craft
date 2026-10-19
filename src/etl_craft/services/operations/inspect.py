"""Read-only operation documents for pipeline definitions and execution history."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path

from sqlalchemy import Boolean

from etl_craft.core.enums import RunCondition, RunStatus
from etl_craft.core.errors import ResourceNotFoundError
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
from etl_craft.engine.repository.intervention_rows import fetch_interventions
from etl_craft.engine.repository.pauses import fetch_open_pauses
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.repository.tasks import (
    fetch_task_codes,
    fetch_task_parameters,
    resolve_task_id,
)
from etl_craft.engine.runlog import select_run
from etl_craft.execution.attempt_records import attempt_log_path
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.models import (
    ActionView,
    AttemptView,
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
from etl_craft.services.operations.requests import (
    AttemptId,
    AuditInput,
    HistoryInput,
    LogInput,
    NoInput,
    PipelineInput,
    RunId,
    RunSelection,
)
from etl_craft.services.operations.snapshots import (
    attempt_document,
    pipeline_view,
    run_document,
    run_view,
    task_document,
)


def list_pipelines(ctx: OperationContext, request: NoInput) -> PipelineListView:
    """List the active definitions."""
    with read_snapshot(ctx.engine) as conn:
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


def pipeline_graph(ctx: OperationContext, request: PipelineInput) -> GraphView:
    """Read the static graph with the pipeline's canonical definition id."""
    pipeline_code = request.pipeline_code
    with read_snapshot(ctx.engine) as conn:
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
                    if (task.run_condition or RunCondition.ALL) != RunCondition.ALL
                )
            ),
            {code: tuple(sorted(edges)) for code, edges in sorted(depends_on.items())},
            tuple(
                (edge.depends_on_pipeline_code, edge.dependency_type)
                for edge in fetch_pipeline_dependency_edges(conn, pipeline_id)
            ),
        )


def pipeline_steps(ctx: OperationContext, request: RunSelection) -> StepsView:
    """Read configured tasks under the exact selected run, including tasks not yet started."""
    with read_snapshot(ctx.engine) as conn:
        pipeline_id = resolve_pipeline_id(conn, request.pipeline_code)
        selected = select_run(conn, pipeline_id, request.selector)
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
                None if row.status is None else RunStatus(row.status),
            )
            for row in conn.execute(
                statement(conn, "pipeline_steps"),
                {"pipeline_id": pipeline_id, "run_id": selected.pipeline_run_id},
            )
        )
        return StepsView(pipeline_id, selected.pipeline_run_id, steps)


def run_history(ctx: OperationContext, request: HistoryInput) -> HistoryView:
    """Read bounded history; selection never falls back to the most recent ended run.

    Entries come newest run first; the view's ``before`` is the next page's cursor, or ``None``
    on the last page.
    """
    pipeline_code, task_code, limit = request.pipeline_code, request.task_code, request.limit
    all_runs, before, selector = request.all, request.before, request.selector
    with read_snapshot(ctx.engine) as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        selected = None if all_runs else select_run(conn, pipeline_id, selector)
        parameters = {
            "pipeline_id": pipeline_id,
            "limit": limit + 1,
            "run_id": None if selected is None else selected.pipeline_run_id,
            "before": before,
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
        more, entries = len(entries) > limit, entries[:limit]
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
        return HistoryView(
            pipeline_id,
            pipeline_code,
            task_code,
            entries,
            changes,
            entries[-1].pipeline_run_id if more else None,
        )


def audit(ctx: OperationContext, request: AuditInput) -> AuditView:
    """Read completed requests and immutable metadata captures without making an action."""
    pipeline_code = request.pipeline_code
    since = None if request.since is None else as_utc(request.since)
    with read_snapshot(ctx.engine) as conn:
        pipeline_id = None if pipeline_code is None else resolve_pipeline_id(conn, pipeline_code)
        actions, changes = fetch_audit_records(conn, pipeline_id, since)
        for row in (*actions, *changes):
            row["at"] = as_utc(row["at"])
        return AuditView(
            tuple(ActionView(**row) for row in actions),
            tuple(MetadataChangeView(**row) for row in changes),
        )


def get_pipeline(ctx: OperationContext, request: PipelineInput) -> PipelineView:
    """Read one active definition without duplicating pipeline assembly."""
    with read_snapshot(ctx.engine) as conn:
        return pipeline_view(conn, resolve_pipeline_id(conn, request.pipeline_code))


def get_run(ctx: OperationContext, request: RunId) -> RunView:
    """Read one run by id, never an active or recent replacement."""
    with read_snapshot(ctx.engine) as conn:
        code = conn.execute(
            statement(conn, "api_run_code"), {"run_id": request.run_id}
        ).scalar_one_or_none()
        if code is None:
            raise ResourceNotFoundError(f"pipeline_run_id={request.run_id}: run does not exist")
        return run_view(conn, resolve_pipeline_id(conn, str(code)), request.run_id)


def get_attempt(ctx: OperationContext, request: AttemptId) -> AttemptView:
    """Read one attempt with the same fields and conversions as CLI inspection."""
    with read_snapshot(ctx.engine) as conn:
        row = conn.execute(
            statement(conn, "api_attempt").columns(retryable=Boolean),
            {"attempt_id": request.attempt_id},
        ).one_or_none()
    if row is None:
        raise ResourceNotFoundError(f"attempt_id={request.attempt_id}: attempt does not exist")
    return attempt_document(row._mapping)


def attempt_log(ctx: OperationContext, request: LogInput) -> Iterator[bytes]:
    """Stream a stored attempt's canonical log in bounded chunks from a byte offset."""
    attempt_id = request.attempt_id
    attempt = get_attempt(ctx, request)
    if attempt.log_path is None:
        raise ResourceNotFoundError(f"attempt_id={attempt_id}: no log has been recorded")
    with ctx.engine.connect() as conn:
        row = conn.execute(
            statement(conn, "api_attempt_identity"), {"attempt_id": attempt_id}
        ).one()
    expected = attempt_log_path(
        ctx.config,
        row.pipeline_code,
        attempt.pipeline_run_id,
        row.task_code,
        attempt.attempt_number,
    ).resolve()
    path = Path(attempt.log_path).resolve()
    if path != expected or not path.is_relative_to(ctx.config.log_dir.resolve()):
        raise ResourceNotFoundError(
            f"attempt_id={attempt_id}: log is outside its configured attempt path"
        )
    try:
        stream = path.open("rb")
    except OSError as error:
        raise ResourceNotFoundError(f"attempt_id={attempt_id}: log file is unavailable") from error

    def chunks() -> Iterator[bytes]:
        with stream:
            stream.seek(request.offset)
            while data := stream.read(65536):
                yield data

    return chunks()
