"""Read-only operation documents for pipeline definitions and execution history."""

from __future__ import annotations

from collections.abc import Generator, Iterator
from dataclasses import replace
from datetime import UTC, datetime

from sqlalchemy import Boolean

from etl_craft.core.enums import ApiRole, RunCondition, RunStatus
from etl_craft.core.errors import ConfigurationError, ResourceNotFoundError
from etl_craft.core.file_bytes import open_regular
from etl_craft.core.graph import build_graph
from etl_craft.core.time import as_utc
from etl_craft.engine.connection import read_snapshot
from etl_craft.engine.logs import (
    LIVE_ATTEMPTS,
    LogAttempt,
    log_key,
    log_location,
    read_log_attempt,
    shared_log,
    stored_log,
)
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.audit_reads import fetch_audit_records
from etl_craft.engine.repository.business_rules import fetch_rule_observations
from etl_craft.engine.repository.dependencies import (
    fetch_cross_pipeline_task_edges,
    fetch_pipeline_dependency_edges,
    fetch_pipeline_graph,
)
from etl_craft.engine.repository.intervention_rows import fetch_interventions
from etl_craft.engine.repository.pauses import fetch_open_pauses
from etl_craft.engine.repository.pipelines import read_tags, resolve_pipeline_id, tagged_pipelines
from etl_craft.engine.repository.tasks import (
    fetch_task_codes,
    fetch_task_parameters,
    resolve_task_id,
)
from etl_craft.engine.run_definitions import for_run
from etl_craft.engine.runlog import select_run
from etl_craft.execution.attempt_records import local_attempt_log
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
from etl_craft.services.operations.permissions import pipeline_access, visible_codes
from etl_craft.services.operations.pipeline_summaries import summarize
from etl_craft.services.operations.requests import (
    AttemptId,
    AuditInput,
    HistoryInput,
    LogInput,
    PipelineInput,
    RunId,
    RunSelection,
    TagFilter,
)
from etl_craft.services.operations.snapshots import (
    attempt_document,
    pipeline_view,
    run_document,
    run_view,
    task_document,
)


def list_pipelines(ctx: OperationContext, request: TagFilter) -> PipelineListView:
    """List the active definitions."""
    from etl_craft.engine.standing_pauses import current

    with read_snapshot(ctx.engine) as conn:
        pauses = fetch_open_pauses(conn)
        task_pauses = tuple(p for p in current(conn, None) if p.active(datetime.now(UTC)))
        access = pipeline_access(conn, ctx)
        views = []
        for row in conn.execute(statement(conn, "active_pipelines")):
            tags = read_tags(row.pipeline_parameters, row.pipeline_code)
            if not access.permits(row.pipeline_code) or not set(request.tag) <= set(tags):
                continue
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
                    tags,
                    tuple(p for p in task_pauses if p.pipeline_id == row.pipeline_id),
                )
            )
        return PipelineListView(summarize(conn, views, datetime.now(UTC), _pinned(ctx)))


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
            pipeline_view(conn, pipeline_id).task_pauses,
        )


def pipeline_steps(ctx: OperationContext, request: RunSelection) -> StepsView:
    """Read configured tasks under the exact selected run, including tasks not yet started."""
    with read_snapshot(ctx.engine) as conn:
        pipeline_id = resolve_pipeline_id(conn, request.pipeline_code)
        selected = select_run(conn, pipeline_id, request.selector)
        with for_run(conn, selected.pipeline_run_id):
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
    """Read one pipeline's history, or a bounded page across matching active pipelines."""
    if request.pipeline_code is not None and not request.tag:
        return _pipeline_history(ctx, request)
    with read_snapshot(ctx.engine) as conn:
        pipeline_id = (
            None
            if request.pipeline_code is None
            else resolve_pipeline_id(conn, request.pipeline_code)
        )
        codes = visible_codes(conn, ctx, tagged_pipelines(conn, request.tag))
    if request.pipeline_code is not None:
        if request.tag and request.pipeline_code not in codes:
            return HistoryView(pipeline_id, request.pipeline_code, request.task_code, (), ())
        return _pipeline_history(ctx, request)
    histories = [
        _pipeline_history(ctx, replace(request, pipeline_code=code, tag=(), all=True))
        for code in codes
    ]
    entries = sorted(
        (entry for h in histories for entry in h.entries),
        key=lambda r: r.pipeline_run_id,
        reverse=True,
    )
    more = len(entries) > request.limit or any(h.before is not None for h in histories)
    entries = entries[: request.limit]
    listed = {entry.pipeline_run_id for entry in entries}
    return HistoryView(
        None,
        None,
        None,
        tuple(entries),
        tuple(
            change for h in histories for change in h.changes if change.pipeline_run_id in listed
        ),
        entries[-1].pipeline_run_id if more and entries else None,
    )


def _pipeline_history(ctx: OperationContext, request: HistoryInput) -> HistoryView:
    """Read bounded history; selection never falls back to the most recent ended run.

    Entries come newest run first; the view's ``before`` is the next page's cursor, or ``None``
    on the last page.
    """
    pipeline_code, task_code, limit = request.pipeline_code, request.task_code, request.limit
    all_runs, before, selector = request.all, request.before, request.selector
    assert pipeline_code is not None
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
            rules=tuple(
                r
                for r in fetch_rule_observations(
                    conn, pipeline_id, None if selected is None else selected.pipeline_run_id
                )
                if task_code is None or r.task_code == task_code
            ),
        )


def audit(ctx: OperationContext, request: AuditInput) -> AuditView:
    """Read completed requests and immutable metadata captures without making an action."""
    from etl_craft.engine.repository.pauses import resumes

    pipeline_code = request.pipeline_code
    since = None if request.since is None else as_utc(request.since)
    with read_snapshot(ctx.engine) as conn:
        pipeline_id = None if pipeline_code is None else resolve_pipeline_id(conn, pipeline_code)
        ids = (
            set(visible_codes(conn, ctx, tagged_pipelines(conn, request.tag)).values())
            if request.tag or ctx.role != ApiRole.ADMIN
            else None
        )
        if ids is not None and pipeline_id is not None:
            ids &= {pipeline_id}
        actions, changes = fetch_audit_records(
            conn,
            pipeline_id if ids is None else None,
            since,
            pipeline_ids=ids,
            project_id=ctx.project_id,
        )
        for row in (*actions, *changes):
            row["at"] = as_utc(row["at"])
        return AuditView(
            tuple(ActionView(**row) for row in actions),
            tuple(MetadataChangeView(**row) for row in changes),
            tuple(
                r for r in resumes(conn, pipeline_id, since) if ids is None or r.pipeline_id in ids
            ),
        )


def get_pipeline(ctx: OperationContext, request: PipelineInput) -> PipelineView:
    """Read one active definition without duplicating pipeline assembly."""
    with read_snapshot(ctx.engine) as conn:
        view = pipeline_view(conn, resolve_pipeline_id(conn, request.pipeline_code))
        return summarize(conn, (view,), datetime.now(UTC), _pinned(ctx))[0]


def _pinned(ctx: OperationContext) -> bool:
    """Whether schedules read the refreshed pipeline definitions, as the controller does."""
    return bool(ctx.engine.get_execution_options().get("refresh_required"))


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
        view = attempt_document(row._mapping)
        if row.has_items:
            from etl_craft.engine.items import attempt_summaries

            return replace(
                view, item_progress=str(attempt_summaries(conn, row.task_run_id)[row.attempt_id])
            )
        return view


def attempt_log(ctx: OperationContext, request: LogInput) -> Iterator[bytes]:
    """Stream a stored attempt's canonical log in bounded chunks from a byte offset."""
    attempt_id = request.attempt_id
    with ctx.engine.connect() as conn:
        row = read_log_attempt(conn, attempt_id)
        if row is None:
            raise ResourceNotFoundError(f"attempt_id={attempt_id}: attempt does not exist")
        if row.log_removed_at is not None:
            return iter([f"removed by retention on {row.log_removed_at}\n".encode()])
        shared = conn.execute(
            statement(conn, "agent_chunk_last"), {"attempt_id": attempt_id}
        ).first()
    if row.log_path is None:
        if shared is not None and shared.byte_offset + shared.byte_length > 0:
            return shared_log(ctx.engine, attempt_id, request.offset)
        if shared is None and row.status not in LIVE_ATTEMPTS:
            raise ResourceNotFoundError(
                f"attempt_id={attempt_id}: no stored log or uploaded chunks; expected a recorded "
                "log location; inspect preserved TASK_LOG and restore the original log source"
            )
        raise ResourceNotFoundError(
            f"attempt_id={attempt_id}: log pending upload; expected acknowledged log bytes; "
            "wait for the agent or controller to publish the log"
        )
    if "://" in row.log_path:
        key = log_key(
            row.engine_id, row.project_id, row.pipeline_run_id, row.task_run_id, attempt_id
        )
        expected_location = log_location(ctx.config, key)
        if row.log_path != expected_location:
            raise ResourceNotFoundError(
                f"attempt_id={attempt_id}: LOG_PATH={row.log_path!r}; expected "
                f"{expected_location!r}; restore the original Logs.Store and canonical identity"
            )
        source = stored_log(ctx.config, key, request.offset)
    else:
        source = local_log(ctx, row, request.offset)
    try:
        first = next(source, None)
    except (ConfigurationError, ResourceNotFoundError):
        with ctx.engine.connect() as conn:
            removed = conn.execute(
                statement(conn, "retention_log_removed"), {"attempt_id": attempt_id}
            ).scalar_one_or_none()
        if removed is not None:
            return iter([f"removed by retention on {removed}\n".encode()])
        raise

    def chunks() -> Iterator[bytes]:
        try:
            if first is not None:
                yield first
            yield from source
        finally:
            source.close()

    return chunks()


def local_log(ctx: OperationContext, row: LogAttempt, offset: int) -> Generator[bytes, None, None]:
    """Hold each local path ancestor while reading, refusing symlinks and special files."""
    path = local_attempt_log(ctx.config, row)
    try:
        with open_regular(
            ctx.config.log_dir.absolute(),
            path.relative_to(ctx.config.log_dir.absolute()).as_posix(),
        ) as stream:
            stream.seek(offset)
            while data := stream.read(65536):
                yield data
    except ConfigurationError as error:
        raise ResourceNotFoundError(
            f"attempt_id={row.attempt_id}: log file is unavailable; {error}"
        ) from error
