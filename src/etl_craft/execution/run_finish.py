"""Finalizing a pipeline run: its outcome, SLA, orchestrated record and unsatisfiable tasks."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
from etl_craft.core.enums import (
    SETTLED_STATUSES,
    Handler,
    InterventionAction,
    Mode,
    RunStatus,
    SlaStatus,
)
from etl_craft.core.errors import RunStateError
from etl_craft.core.graph import DependencyGraph, TaskRunState
from etl_craft.core.log import log_context
from etl_craft.engine import runlog, transitions
from etl_craft.engine.repository.interventions import fetch_interventions, fetch_task_rows
from etl_craft.engine.repository.pipelines import (
    resolve_pipeline_id,
)
from etl_craft.engine.repository.runs import fetch_task_statuses_for_run
from etl_craft.execution import leases
from etl_craft.execution.run_start import (
    load_pipeline,
)
from etl_craft.execution.run_types import (
    PipelineOutcome,
    PipelineRun,
    RunHooks,
    call_hook,
    default_hooks,
)
from etl_craft.execution.sla import SlaLapse, SlaWatch

NOT_RUN_BY_ORCHESTRATOR = "not run by the orchestrator"
"""Why ``--finalize-only`` records a task ``SKIPPED`` in remote mode."""


logger = logging.getLogger(__name__)


def finalize_active_run(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    *,
    hooks: RunHooks | None = None,
    selector: runlog.RunSelector = runlog.ACTIVE_RUN,
) -> PipelineOutcome:
    """End the active run of ``pipeline_code`` from its tasks' statuses, for an orchestrator.

    In remote mode this records the orchestrator's decisions (see ``_record_orchestrated``);
    in local mode, tasks that can never run are recorded ``SKIPPED`` as in a local run. Raises
    ``RunStateError`` when the pipeline has no active run.
    """
    with engine.connect() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        selected = runlog.select_run(conn, pipeline_id, selector)
        pipeline_run_id = selected.pipeline_run_id
        if selected.status != RunStatus.IN_PROGRESS:
            raise RunStateError(
                f"{pipeline_code}: run_id={pipeline_run_id} is {selected.status}; "
                "expected IN-PROGRESS to finalize"
            )
        detail, graph, task_codes = load_pipeline(conn, pipeline_id)
    with log_context(pipeline=pipeline_code, pipeline_run_id=pipeline_run_id):
        return finalize(
            engine,
            PipelineRun(
                pipeline_code, pipeline_id, pipeline_run_id, graph, task_codes, detail.sla_in_hours
            ),
            hooks or default_hooks(config, engine),
            orchestrated=config.mode == Mode.REMOTE,
        )


@acting_as(SYSTEM_ACTOR)
def settle_unsatisfiable(
    engine: Engine,
    graph: DependencyGraph,
    pipeline_run_id: int,
    task_codes: dict[int, str],
    *,
    failures_final: bool = False,
) -> list[int]:
    """Record ``SKIPPED`` for every task not yet run whose dependencies can never be met.

    Returns the tasks recorded.
    """
    with engine.connect() as conn:
        run_state = runlog.fetch_run_state(conn, pipeline_run_id, list(graph.task_ids))
    skipped: list[int] = []
    for task_id in graph.unsatisfiable(run_state, failures_final=failures_final):
        reason = f"its dependencies can never be met under pipeline_run_id={pipeline_run_id}"
        with engine.begin() as conn:
            binding = transitions.find_or_create_task_run(conn, task_id, pipeline_run_id)
            if not binding.created:
                continue
            transitions.finish_task_run(
                conn, binding.task_run_id, status=RunStatus.SKIPPED, error_message=reason
            )
        skipped.append(task_id)
        logger.info("%s: SKIPPED — %s", task_codes[task_id], reason)
    return skipped


@acting_as(SYSTEM_ACTOR)
def _record_orchestrated(
    engine: Engine, graph: DependencyGraph, pipeline_run_id: int, task_codes: dict[int, str]
) -> list[int]:
    """Record what the orchestrator decided about the tasks it left unfinished; remote mode.

    A task with no row under the run was never run: its trigger rule was not met, or the DAG
    run was stopped. It is recorded ``SKIPPED``. A task still ``IN-PROGRESS`` lost its
    ``run --task_code`` process (the orchestrator waits for every task before finalizing), so it
    is recorded ``FAILED``. Returns the tasks recorded ``SKIPPED``.
    """
    with engine.connect() as conn:
        run_state = runlog.fetch_run_state(conn, pipeline_run_id, list(graph.task_ids))
    not_run: list[int] = []
    for task_id in sorted(graph.task_ids, key=lambda t: task_codes[t]):
        state = run_state.get(task_id)
        if state is not None and state.status != RunStatus.IN_PROGRESS:
            continue
        with engine.begin() as conn:
            binding = transitions.find_or_create_task_run(conn, task_id, pipeline_run_id)
            if binding.created:
                transitions.finish_task_run(
                    conn,
                    binding.task_run_id,
                    status=RunStatus.SKIPPED,
                    error_message=NOT_RUN_BY_ORCHESTRATOR,
                )
                not_run.append(task_id)
                logger.info("%s: SKIPPED — %s", task_codes[task_id], NOT_RUN_BY_ORCHESTRATOR)
                continue
            if binding.status != RunStatus.IN_PROGRESS:
                continue
            reason = (
                "still IN-PROGRESS when the orchestrator finalized the run: its run --task_code "
                "process ended without recording an outcome"
            )
            transitions.finish_task_run(
                conn, binding.task_run_id, status=RunStatus.FAILED, error_message=reason
            )
        logger.error("%s: FAILED — %s", task_codes[task_id], reason)
    return not_run


@dataclass(frozen=True)
class _Judgement:
    status: RunStatus
    data_tasks: set[int]
    alert_failures: list[str]
    unsettled: dict[int, str]
    not_run: list[int]


@acting_as(SYSTEM_ACTOR)
def finalize(
    engine: Engine,
    run: PipelineRun,
    hooks: RunHooks,
    never_ready: Sequence[int] = (),
    after_failure: Sequence[int] = (),
    *,
    orchestrated: bool = False,
) -> PipelineOutcome:
    """End the run from its tasks' statuses, record its SLA, and consume its upstream runs.

    ``orchestrated`` (remote mode) records the orchestrator's decisions instead of settling the
    tasks that can never run, and consumes nothing. A run whose tasks all ended ``SKIPPED``, or
    whose tasks the orchestrator ran none of (a sensor on an upstream failed, say), ends
    ``SKIPPED``, which a downstream ``SUCCESS`` dependency does not accept.
    Alerts do not affect a data pipeline's outcome; a pipeline of only alerts judges its alerts.
    """
    not_run: list[int] = []
    if orchestrated:
        not_run = _record_orchestrated(engine, run.graph, run.pipeline_run_id, run.task_codes)
    else:
        settle_unsatisfiable(engine, run.graph, run.pipeline_run_id, run.task_codes)
        running = _running_elsewhere(engine, run.pipeline_code, run.pipeline_run_id)
        if running is not None:
            return running
    judgement = _judge_run(engine, run, not_run, orchestrated)
    status = judgement.status
    with engine.begin() as conn:
        breached_before = runlog.fetch_run_sla(conn, run.pipeline_run_id).sla_status
        backfill_run = runlog.fetch_run_kind(conn, run.pipeline_run_id).backfill
        ending = transitions.finalize_pipeline_run(
            conn,
            run.pipeline_run_id,
            status,
            sla_in_hours=run.sla_hours,
            owner=leases.run_owner(run.pipeline_run_id),
            consume=not orchestrated and not backfill_run,
        )
        current = runlog.fetch_pipeline_run_status(conn, run.pipeline_run_id)
    if not ending.ended:
        message = (
            f"{run.pipeline_code}: pipeline_run_id={run.pipeline_run_id} is {current}: "
            "it was ended "
            f"elsewhere (an operator's cancel, say) while this process was ending it {status}; "
            f"nothing more was recorded. `etl-craft history --pipeline_code "
            f"{run.pipeline_code}` shows "
            "who ended it"
        )
        logger.warning("%s", message)
        return PipelineOutcome(RunStatus(current), message, run.pipeline_run_id)
    sla = ending.sla
    outcome = _outcome(engine, run, judgement, sla, never_ready, after_failure)
    if sla is not None and sla.status == SlaStatus.BREACHED and breached_before != sla.status:
        lapse = SlaLapse(run.pipeline_code, run.pipeline_run_id, sla.sla_hours, sla.elapsed_hours)
        call_hook("on_sla_lapse", hooks.on_sla_lapse, lapse)
    call_hook("on_finalized", hooks.on_finalized, outcome)
    return outcome


def _judge_run(
    engine: Engine, run: PipelineRun, not_run: list[int], orchestrated: bool
) -> _Judgement:
    """Judge data tasks, using alert outcomes only for an alert-only pipeline."""
    with engine.connect() as conn:
        run_state = runlog.fetch_run_state(conn, run.pipeline_run_id, list(run.graph.task_ids))
        data_tasks = {
            task.task_id
            for task in fetch_task_statuses_for_run(conn, run.pipeline_id, run.pipeline_run_id)
            if task.handler != Handler.EMAIL_ALERT
        } & set(run.graph.task_ids)
    judged_tasks = data_tasks or set(run.graph.task_ids)
    alert_failures = sorted(
        run.task_codes[task_id]
        for task_id in set(run.graph.task_ids) - data_tasks
        if run_state.get(task_id, TaskRunState()).status == RunStatus.FAILED
    )
    unsettled = {
        task_id: run_state.get(task_id, TaskRunState()).status or "never started"
        for task_id in judged_tasks
        if run_state.get(task_id, TaskRunState()).status not in SETTLED_STATUSES
    }
    status = RunStatus.FAILED if unsettled else RunStatus.SUCCESS
    if orchestrated and not unsettled and not_run and judged_tasks <= set(not_run):
        status = RunStatus.SKIPPED
    if (
        not unsettled
        and judged_tasks
        and all(run_state[task_id].status == RunStatus.SKIPPED for task_id in judged_tasks)
    ):
        status = RunStatus.SKIPPED
    return _Judgement(status, data_tasks, alert_failures, unsettled, not_run)


def _outcome(
    engine: Engine,
    run: PipelineRun,
    judgement: _Judgement,
    sla: runlog.SlaResult | None,
    never_ready: Sequence[int],
    after_failure: Sequence[int],
) -> PipelineOutcome:
    """Describe the committed ending, its unsuccessful tasks, bypasses and SLA."""
    status = judgement.status
    data_tasks, alert_failures = judgement.data_tasks, judgement.alert_failures
    unsettled, not_run = judgement.unsettled, judgement.not_run
    message = f"{run.pipeline_code}: pipeline_run_id={run.pipeline_run_id} {status}"
    if alert_failures and data_tasks:
        message += f"; alert task(s) failed: {', '.join(alert_failures)} (see their attempt logs)"
        logger.error("%s: alert task(s) failed: %s", run.pipeline_code, ", ".join(alert_failures))
    if unsettled:
        listed = ", ".join(f"{run.task_codes[t]} ({state})" for t, state in unsettled.items())
        message += f" — {len(unsettled)} task(s) did not succeed: {listed}"
        if never_ready:
            message += (
                f"; {len(never_ready)} of them could not start because their dependencies "
                "were not met"
            )
        if after_failure:
            skipped = ", ".join(run.task_codes[t] for t in after_failure)
            message += f"; skipped because of the failure: {skipped}"
    if not_run:
        message += f"; not run by the orchestrator: {', '.join(run.task_codes[t] for t in not_run)}"
    with engine.connect() as conn:
        bypasses = [
            change
            for change in fetch_interventions(conn, run.pipeline_id, run.pipeline_run_id)
            if change.pipeline_run_id == run.pipeline_run_id
            and change.action == InterventionAction.GATE_BYPASS
        ]
    if bypasses:
        where = sorted({change.task_code or run.pipeline_code for change in bypasses})
        message += f"; dependency gates bypassed for {', '.join(where)} (see history)"
    if sla is not None and sla.status == SlaStatus.BREACHED:
        message += f"; {sla.describe()}"
    outcome = PipelineOutcome(status, message, run.pipeline_run_id, sla)
    logger.log(logging.INFO if status == RunStatus.SUCCESS else logging.ERROR, "%s", message)
    return outcome


def _running_elsewhere(
    engine: Engine, pipeline_code: str, pipeline_run_id: int
) -> PipelineOutcome | None:
    """Return the outcome that leaves the run open when a task of it is still running.

    By the time a local run is finalized, every task this process started has ended, so a task
    still ``IN-PROGRESS`` belongs to another process, or to one that died without recording an
    outcome. Either way the run must not be ended under it.
    """
    with engine.connect() as conn:
        running = [
            row
            for row in fetch_task_rows(conn, pipeline_run_id)
            if row.status == RunStatus.IN_PROGRESS
        ]
    if not running:
        return None
    listed = ", ".join(f"{row.task_code} (task_run_id={row.task_run_id})" for row in running)
    first = running[0].task_code
    message = (
        f"{pipeline_code}: pipeline_run_id={pipeline_run_id} stays IN-PROGRESS: {listed} "
        f"{'is' if len(running) == 1 else 'are'} still IN-PROGRESS in another process. When it "
        f"ends, `etl-craft run --pipeline_code {pipeline_code}` finishes the run. If that process "
        f"is gone, record the task's outcome with `etl-craft mark --pipeline_code {pipeline_code} "
        f"--task_code {first} --status FAILED --stale --reason ...`"
    )
    logger.warning("%s", message)
    return PipelineOutcome(RunStatus.IN_PROGRESS, message, pipeline_run_id)


def sla_watch(
    engine: Engine, pipeline_code: str, run_id: int, hours: float | None, hooks: RunHooks
) -> SlaWatch:
    """Watch run ``run_id`` against ``hours`` of SLA, reporting a lapse to ``hooks``."""
    return SlaWatch(
        engine,
        pipeline_code,
        run_id,
        hours,
        lambda lapse: call_hook("on_sla_lapse", hooks.on_sla_lapse, lapse),
    )
