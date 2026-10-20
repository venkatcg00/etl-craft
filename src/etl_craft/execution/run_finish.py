"""Finalizing a pipeline run: its outcome, SLA and unsatisfiable tasks."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass

from sqlalchemy.engine import Engine

from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
from etl_craft.core.enums import (
    SETTLED_STATUSES,
    Handler,
    InterventionAction,
    RunStatus,
    SlaStatus,
)
from etl_craft.core.graph import DependencyGraph, TaskRunState
from etl_craft.engine import controls, runlog, transitions
from etl_craft.engine.repository.intervention_rows import fetch_interventions, fetch_task_rows
from etl_craft.engine.repository.runs import fetch_task_statuses_for_run
from etl_craft.execution import leases
from etl_craft.execution.run_types import (
    PipelineOutcome,
    PipelineRun,
    RunHooks,
    call_hook,
)
from etl_craft.execution.sla import SlaLapse, SlaWatch

logger = logging.getLogger(__name__)


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
        fences = controls.blocked(conn, pipeline_run_id)
    if None in fences:
        return []
    run_state.update(
        {t: TaskRunState(status=RunStatus.IN_PROGRESS) for t in fences if t is not None}
    )
    skipped: list[int] = []
    for task_id in graph.unsatisfiable(run_state, failures_final=failures_final):
        reason = f"its dependencies can never be met under pipeline_run_id={pipeline_run_id}"
        with engine.begin() as conn:
            controls.lock_run(conn, pipeline_run_id)
            if controls.blocked(conn, pipeline_run_id) != fences:
                return skipped
            binding = transitions.find_or_create_task_run(conn, task_id, pipeline_run_id)
            if not binding.created:
                continue
            transitions.finish_task_run(
                conn, binding.task_run_id, status=RunStatus.SKIPPED, error_message=reason
            )
        skipped.append(task_id)
        logger.info("%s: SKIPPED — %s", task_codes[task_id], reason)
    return skipped


@dataclass(frozen=True)
class _Judgement:
    status: RunStatus
    data_tasks: set[int]
    alert_failures: list[str]
    unsettled: dict[int, str]


@acting_as(SYSTEM_ACTOR)
def finalize(
    engine: Engine,
    run: PipelineRun,
    hooks: RunHooks,
    never_ready: Sequence[int] = (),
    after_failure: Sequence[int] = (),
) -> PipelineOutcome:
    """End the run from its tasks' statuses, record its SLA, and consume its upstream runs.

    A run whose data tasks all ended ``SKIPPED`` ends ``SKIPPED``, which a downstream
    ``SUCCESS`` dependency does not accept.
    Alerts do not affect a data pipeline's outcome; a pipeline of only alerts judges its alerts.
    """
    settle_unsatisfiable(engine, run.graph, run.pipeline_run_id, run.task_codes)
    running = _running_elsewhere(engine, run.pipeline_code, run.pipeline_run_id)
    if running is not None:
        return running
    judgement = _judge_run(engine, run)
    status = judgement.status
    with engine.begin() as conn:
        controls.lock_run(conn, run.pipeline_run_id)
        if controls.blocked(conn, run.pipeline_run_id):
            return PipelineOutcome(
                RunStatus(runlog.fetch_pipeline_run_status(conn, run.pipeline_run_id)),
                f"pipeline_run_id={run.pipeline_run_id}: dispatch is fenced; inspect the control "
                "request and release the exact scope before finalizing",
                run.pipeline_run_id,
            )
        breached_before = runlog.fetch_run_sla(conn, run.pipeline_run_id).sla_status
        backfill_run = runlog.fetch_run_kind(conn, run.pipeline_run_id).backfill
        ending = transitions.finalize_pipeline_run(
            conn,
            run.pipeline_run_id,
            status,
            sla_in_hours=run.sla_hours,
            owner=leases.run_owner(run.pipeline_run_id),
            consume=not backfill_run,
        )
        current = runlog.fetch_pipeline_run_status(conn, run.pipeline_run_id)
    if not ending.ended:
        message = (
            f"{run.pipeline_code}: pipeline_run_id={run.pipeline_run_id} is {current}: "
            "it was ended "
            f"elsewhere (an operator's cancel, say) while this process was ending it {status}; "
            f"nothing more was recorded. `etl-craft history --pipeline-code "
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


def _judge_run(engine: Engine, run: PipelineRun) -> _Judgement:
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
    if (
        not unsettled
        and judged_tasks
        and all(run_state[task_id].status == RunStatus.SKIPPED for task_id in judged_tasks)
    ):
        status = RunStatus.SKIPPED
    return _Judgement(status, data_tasks, alert_failures, unsettled)


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
    unsettled = judgement.unsettled
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
        f"ends, `etl-craft run --pipeline-code {pipeline_code}` finishes the run. If that process "
        f"is gone, record the task's outcome with `etl-craft mark --pipeline-code {pipeline_code} "
        f"--task-code {first} --status FAILED --stale --reason ...`"
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
