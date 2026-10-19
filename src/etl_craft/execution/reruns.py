"""Rerunning and forcing a task of an ended run, and ending the reopened run."""

from __future__ import annotations

import logging
from dataclasses import replace

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.enums import (
    RunStatus,
)
from etl_craft.core.errors import RunStateError
from etl_craft.core.log import log_context
from etl_craft.engine import runlog
from etl_craft.engine.repository.pipelines import (
    resolve_pipeline_id,
)
from etl_craft.engine.repository.tasks import resolve_task_id
from etl_craft.execution.attempt_types import ChildOptions, Override, TaskOutcome
from etl_craft.execution.interventions import (
    check_override,
    reset_engine_skipped,
)
from etl_craft.execution.run_finish import finalize
from etl_craft.execution.run_start import (
    load_pipeline,
)
from etl_craft.execution.run_types import PipelineOutcome, PipelineRun, RunHooks, default_hooks
from etl_craft.execution.runner import run_task

logger = logging.getLogger(__name__)


def rerun_task(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    task_code: str,
    reason: str,
    *,
    with_downstream: bool = False,
    child: ChildOptions | None = None,
    hooks: RunHooks | None = None,
    selector: runlog.RunSelector = runlog.ACTIVE_RUN,
) -> PipelineOutcome:
    """Run ``task_code`` again under the selected run: ``run --task_code --rerun``.

    With ``with_downstream``, every task after it runs again too. The task
    runs as it stands, without its dependencies checked again. Each task after it runs again,
    in dependency order, when its dependencies are satisfied by then; the others keep their
    rows. A run that had ended is reopened for this and then ended again from its tasks'
    statuses; a run still in progress is left for ``run --pipeline_code`` to finish.
    """
    check_override(config, "--rerun", reason)
    child = child or ChildOptions()
    with engine.connect() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        task_id = resolve_task_id(conn, pipeline_id, task_code)
        selected = runlog.select_run(conn, pipeline_id, selector)
        pipeline_run_id = selected.pipeline_run_id
        before = pipeline_run_id if selected.status == RunStatus.IN_PROGRESS else None
        failed_before = (
            runlog.fetch_task_run_status(conn, task_id, pipeline_run_id) == RunStatus.FAILED
        )
        selector = runlog.RunSelector(run_id=pipeline_run_id)
        detail, graph, task_codes = load_pipeline(conn, pipeline_id, run_id=pipeline_run_id)
    run = PipelineRun(
        pipeline_code, pipeline_id, pipeline_run_id, graph, task_codes, detail.sla_in_hours
    )
    override = Override(reason, rerun=True)
    first = run_task(
        engine, config, pipeline_code, task_code, child=child, override=override, selector=selector
    )
    if first.task_run_id is None:
        raise RunStateError(first.message)
    ran = [first]
    left: list[str] = []
    kept: list[str] = []
    if failed_before and first.status == RunStatus.SUCCESS:
        # The tasks the engine skipped because this task had failed get to run again.
        _, kept = reset_engine_skipped(
            engine, pipeline_id, pipeline_run_id, reason, only=graph.downstream_of(task_id)
        )
    if with_downstream and first.status == RunStatus.SUCCESS:
        later, left = _rerun_downstream(engine, config, run, task_id, child, override)
        ran.extend(later)
    summary = "; ".join(outcome.message for outcome in ran)
    if left:
        summary += f"; not run again, their dependencies are not met: {', '.join(left)}"
    if kept:
        summary += f"; kept SKIPPED: {', '.join(kept)}"
    if before is not None:
        status = (
            RunStatus.FAILED
            if any(o.status == RunStatus.FAILED for o in ran)
            else RunStatus.IN_PROGRESS
        )
        return PipelineOutcome(
            status,
            f"{pipeline_code}: pipeline_run_id={pipeline_run_id} is still IN-PROGRESS: {summary}",
            pipeline_run_id,
        )
    return _end_reopened(
        engine, run, hooks or default_hooks(config, engine), f"ran again: {summary}"
    )


def _rerun_downstream(
    engine: Engine,
    config: ConnectorConfig,
    run: PipelineRun,
    task_id: int,
    child: ChildOptions,
    override: Override,
) -> tuple[list[TaskOutcome], list[str]]:
    """Rerun downstream tasks in dependency order, retaining unmet tasks unchanged."""
    ran: list[TaskOutcome] = []
    left: list[str] = []
    downstream = run.graph.downstream_of(task_id)
    for wave in run.graph.waves():
        for later in (t for t in wave if t in downstream):
            with engine.connect() as conn:
                run_state = runlog.fetch_run_state(
                    conn, run.pipeline_run_id, list(run.graph.task_ids)
                )
            if run.graph.satisfied_edge_count(later, run_state) < run.graph.required_edge_count(
                later
            ):
                left.append(run.task_codes[later])
                continue
            ran.append(
                run_task(
                    engine,
                    config,
                    run.pipeline_code,
                    run.task_codes[later],
                    child=child,
                    override=override,
                    selector=runlog.RunSelector(run_id=run.pipeline_run_id),
                )
            )
    return ran, left


def force_task(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    task_code: str,
    *,
    child: ChildOptions | None = None,
    hooks: RunHooks | None = None,
    selector: runlog.RunSelector = runlog.ACTIVE_RUN,
) -> PipelineOutcome:
    """Run ``task_code`` past its checks: ``run --task_code --force``.

    Under the pipeline's active run the task just runs. A run that had ended is reopened for
    it and then ended again from its tasks' statuses, so a forced task that fails leaves the
    run ``FAILED``.
    """
    with engine.connect() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        selected = runlog.select_run(conn, pipeline_id, selector)
    selector = runlog.RunSelector(run_id=selected.pipeline_run_id)
    outcome = run_task(
        engine, config, pipeline_code, task_code, force=True, child=child, selector=selector
    )
    with engine.connect() as conn:
        run_id = selected.pipeline_run_id
        if outcome.reopened is None:
            return PipelineOutcome(outcome.status, outcome.message, run_id)
        detail, graph, task_codes = load_pipeline(conn, pipeline_id, run_id=run_id)
    return _end_reopened(
        engine,
        PipelineRun(pipeline_code, pipeline_id, run_id, graph, task_codes, detail.sla_in_hours),
        hooks or default_hooks(config, engine),
        f"forced: {outcome.message}",
    )


def _end_reopened(
    engine: Engine, run: PipelineRun, hooks: RunHooks, summary: str
) -> PipelineOutcome:
    """End again a run reopened to run some of its tasks, or keep it open for the rest.

    A task with no row under the run has yet to run, so the run stays ``IN-PROGRESS`` for
    ``run --pipeline_code`` to resume it.
    """
    with engine.connect() as conn:
        rows = runlog.fetch_run_state(conn, run.pipeline_run_id, list(run.graph.task_ids))
    not_run = sorted(run.task_codes[t] for t in run.graph.task_ids if t not in rows)
    if not_run:
        message = (
            f"{run.pipeline_code}: pipeline_run_id={run.pipeline_run_id} "
            f"stays IN-PROGRESS ({summary}); "
            f"{len(not_run)} task(s) still need to run: {', '.join(not_run)}. "
            f"`etl-craft run --pipeline_code {run.pipeline_code}` runs them"
        )
        logger.warning("%s", message)
        return PipelineOutcome(RunStatus.IN_PROGRESS, message, run.pipeline_run_id)
    with log_context(pipeline=run.pipeline_code, pipeline_run_id=run.pipeline_run_id):
        ended = finalize(engine, run, hooks)
    return replace(ended, message=f"{ended.message} ({summary})")
