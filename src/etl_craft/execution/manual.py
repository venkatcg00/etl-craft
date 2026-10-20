"""Independent task selections driven by the existing local run scheduler."""

from __future__ import annotations

from datetime import date
from typing import TypedDict, Unpack

from sqlalchemy.engine import Connection, Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.actor import current_actor
from etl_craft.core.enums import InterventionAction, RunStatus
from etl_craft.core.errors import RunRefusedError, RunStateError, StaleTransitionError
from etl_craft.core.graph import build_graph
from etl_craft.engine import run_scopes, runlog
from etl_craft.engine.repository.dependencies import (
    fetch_pipeline_dependency_edges,
    fetch_pipeline_graph,
)
from etl_craft.engine.repository.intervention_rows import record_intervention
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.repository.tasks import resolve_task_id
from etl_craft.execution.attempt_types import ChildOptions
from etl_craft.execution.interventions import check_override, open_pause
from etl_craft.execution.pipeline import run_pipeline
from etl_craft.execution.run_types import PipelineOutcome, RunHooks


class _ManualOptions(TypedDict, total=False):
    selector: runlog.RunSelector
    run_date: date | None
    with_downstream: bool
    ignore_dependencies: bool
    reason: str | None
    child: ChildOptions | None
    hooks: RunHooks | None


def run_manual(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    task_code: str,
    **options: Unpack[_ManualOptions],
) -> PipelineOutcome:
    """Create or explicitly resume a manual selection without adopting another run."""
    selector = options.get("selector", runlog.ACTIVE_RUN)
    pause = open_pause(engine, pipeline_code, selector=selector)
    if pause is not None:
        raise RunRefusedError(
            f"{pipeline_code}: {pause.describe()}; independent manual dispatch while paused "
            "is not supported yet. Resume this pipeline before starting a manual selection."
        )
    ignore = options.get("ignore_dependencies", False)
    if ignore:
        check_override(config, "`ignore_dependencies`", options.get("reason") or "")
    with engine.begin() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        scope = select_tasks(
            conn,
            pipeline_id,
            task_code,
            with_downstream=options.get("with_downstream", False),
            ignore_dependencies=options.get("ignore_dependencies", False),
        )
        selected = _select_run(conn, pipeline_id, selector, scope, options.get("run_date"))
        if selected is not None:
            run_id = selected.pipeline_run_id
            if selected.status != RunStatus.IN_PROGRESS:
                return PipelineOutcome(
                    RunStatus(selected.status),
                    f"{pipeline_code}: pipeline_run_id={run_id} already {selected.status}; "
                    "the existing result is preserved",
                    run_id,
                )
        else:
            try:
                run_id = run_scopes.create_manual_run(
                    conn,
                    pipeline_id,
                    current_actor(),
                    scope,
                    selector.run_key,
                    options.get("run_date") or runlog.today(config.timezone),
                )
            except StaleTransitionError as error:
                other = runlog.fetch_exclusive_run_id(conn, pipeline_id)
                raise RunStateError(
                    f"{pipeline_code}: selected tasks need exclusive admission but "
                    f"pipeline_run_id={other} is IN-PROGRESS. "
                    "Finish or cancel that exact run first; "
                    "email-only manual selections can overlap pipeline execution."
                ) from error
        if ignore and selected is None:
            record_intervention(
                conn,
                pipeline_id=pipeline_id,
                pipeline_run_id=run_id,
                action=InterventionAction.GATE_BYPASS,
                reason=options.get("reason") or "",
                requested_by=current_actor().name,
            )
    return run_pipeline(
        engine,
        config,
        pipeline_code,
        selector=runlog.RunSelector(run_id=run_id),
        child=options.get("child"),
        hooks=options.get("hooks"),
        run_date=options.get("run_date"),
    )


def select_tasks(
    conn: Connection,
    pipeline_id: int,
    task_code: str,
    *,
    with_downstream: bool = False,
    ignore_dependencies: bool = False,
) -> run_scopes.TaskSelection:
    """Validate the requested graph and handler isolation before creating a run."""
    ignore = ignore_dependencies
    task_id = resolve_task_id(conn, pipeline_id, task_code)
    if fetch_pipeline_dependency_edges(conn, pipeline_id) and not ignore:
        raise RunRefusedError(
            f"pipeline_id={pipeline_id}: pipeline dependencies exist outside the manual selection; "
            "use a pipeline run with recorded inputs, or `ignore_dependencies` with `reason`"
        )
    data = fetch_pipeline_graph(conn, pipeline_id)
    graph = build_graph(data.tasks, data.same_pipeline_edges)
    ids = {task_id}
    if with_downstream:
        ids |= graph.downstream_of(task_id)
    scope = run_scopes.TaskSelection(tuple(sorted(ids)), ignore)
    run_scopes.selected_graph(data, scope)
    scope.exclusive(conn)
    return scope


def _select_run(
    conn: Connection,
    pipeline_id: int,
    selector: runlog.RunSelector,
    scope: run_scopes.TaskSelection,
    run_date: date | None,
) -> runlog.RunIdentity | None:
    """Only an explicit identity may resume the same manual selection."""
    candidates = runlog.run_candidates(conn, pipeline_id)
    if selector.run_id is None and not any(r.run_key == selector.run_key for r in candidates):
        return None
    selected = runlog.select_run(conn, pipeline_id, selector)
    if run_date is not None and selected.run_date != run_date:
        raise RunStateError(
            f"run_id={selected.pipeline_run_id}: run_date={selected.run_date}, "
            f"requested={run_date}; "
            "a run date cannot change. Use its original date or start a new manual run."
        )
    stored = run_scopes.fetch_selection(conn, selected.pipeline_run_id)
    if stored != scope:
        raise RunStateError(
            f"run_id={selected.pipeline_run_id}: selection={stored}; expected manual "
            f"selection={scope}. Use its original selection to resume, or omit the selector "
            "to start a new manual run"
        )
    return selected
