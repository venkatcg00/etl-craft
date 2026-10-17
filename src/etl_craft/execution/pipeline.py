"""``run --pipeline_code``: run a whole pipeline; ``--init-only`` and ``--finalize-only``.

In local mode the engine runs every active task under one run, starting ready tasks after each
completion, at most ``Orchestration.Max_parallel_tasks`` at once. Each task goes through
``run_task`` in a process of its own. Gates wait cooperatively without a worker;
a task whose dependencies can never be met is recorded ``SKIPPED``. Execution continues until
every task is settled or none can start. Data tasks determine the run's outcome; failed alerts
are logged and recorded on their own tasks. A pipeline containing only alerts uses those
tasks' outcomes.

A new run starts only when the pipeline's dependencies on other pipelines are satisfied (see
``gates``); otherwise the run is recorded ``SKIPPED``. An ``IN-PROGRESS`` run is resumed instead,
without that check, and its finished tasks are not run again. Before either, the connections the
run uses are tested (see ``connections``).

In remote mode the orchestrator is the only source of truth for scheduling (see ``remote``). It
runs each task with ``run --task_code``, between ``run --init-only``, which refuses rules the
orchestrator does not support, tests connections and starts the run, and ``run
--finalize-only``, which records its decisions and ends the run: a task it never ran is
``SKIPPED``, and the run is ``FAILED`` when a data task failed, ``SKIPPED`` when it ran none.
No gate is checked and no upstream run is consumed; the orchestrator's DAGs hold those rules.

An operator may cancel a local run (``etl-craft cancel``): its running tasks are stopped, no
other task starts, and the run ends ``CANCELLED``.

Every run of a pipeline with ``SLA_IN_HOURS`` is marked ``MET`` or ``BREACHED``. While a local run
is going, a watcher marks it ``BREACHED`` as soon as the SLA passes; otherwise the finalize step
does. ``RunHooks.on_sla_lapse`` is called once per run, the first time the breach is seen.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Generator
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import date, timedelta
from typing import TypedDict, TypeVar, Unpack

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.enums import (
    Mode,
    RunStatus,
)
from etl_craft.core.errors import RunRefusedError, RunStateError, UsageError
from etl_craft.core.log import log_context
from etl_craft.engine import runlog
from etl_craft.engine.repository.dependencies import PipelineGraphData
from etl_craft.engine.repository.pipelines import (
    resolve_pipeline_id,
)
from etl_craft.engine.repository.tasks import fetch_task_codes
from etl_craft.execution import leases
from etl_craft.execution.attempt_records import run_cancelled
from etl_craft.execution.attempt_types import ChildOptions
from etl_craft.execution.gates import (
    Clock,
)
from etl_craft.execution.interventions import (
    check_override,
    open_pause,
)
from etl_craft.execution.pools import Pool
from etl_craft.execution.run_finish import finalize, settle_unsatisfiable, sla_watch
from etl_craft.execution.run_start import (
    StartOptions,
    cancelled_run,
    load_pipeline,
    paused_outcome,
    prepare,
    skipped_run,
    start_steps,
)
from etl_craft.execution.run_types import (
    PipelineOutcome,
    PipelineRun,
    RunHooks,
    default_hooks,
    drive,
)
from etl_craft.execution.scheduler import Scheduler

logger = logging.getLogger(__name__)

T = TypeVar("T")


class _PipelineOptions(TypedDict, total=False):
    force: bool
    clock: Clock | None
    child: ChildOptions | None
    hooks: RunHooks | None
    run_date: date | None
    backfill: str | None
    selector: runlog.RunSelector
    stop_dispatch: threading.Event | None
    graph_data: PipelineGraphData | None


def run_pipeline(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    **kwargs: Unpack[_PipelineOptions],
) -> PipelineOutcome:
    """Run the cooperative local scheduler in the foreground until it settles."""
    return drive(
        pipeline_steps(engine, config, pipeline_code, **kwargs), kwargs.get("clock") or Clock()
    )


def pipeline_steps(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    *,
    force: bool = False,
    clock: Clock | None = None,
    child: ChildOptions | None = None,
    hooks: RunHooks | None = None,
    run_date: date | None = None,
    backfill: str | None = None,
    selector: runlog.RunSelector = runlog.ACTIVE_RUN,
    stop_dispatch: threading.Event | None = None,
    graph_data: PipelineGraphData | None = None,
    pool: Pool | None = None,
) -> Generator[float, None, PipelineOutcome]:
    """Advance ready tasks of ``pipeline_code`` cooperatively; local mode only.

    ``force`` skips the pipeline's gate and runs every task after its upstreams finish, whatever its
    status or dependencies. ``run_date`` is the date a new run runs as of (today unless given),
    and ``backfill``, the reason for one, makes it a backfill run (see ``backfill``). When
    interrupted, the running task processes are stopped and recorded ``FAILED``, and the run
    stays ``IN-PROGRESS`` so the next run resumes it.
    """
    if config.mode == Mode.REMOTE:
        raise RunRefusedError(
            "run --pipeline_code without --task_code runs the whole pipeline, which only local "
            "mode does; in remote mode the orchestrator runs each task, between run --init-only "
            "and run --finalize-only"
        )
    paused = open_pause(engine, pipeline_code)
    if paused is not None:
        return paused_outcome(pipeline_code, paused)
    clock = clock or Clock()
    hooks = hooks or default_hooks(config, engine)
    pipeline_id, detail = prepare(engine, config, pipeline_code)
    pipeline_run_id, skip_reason = yield from start_steps(
        engine,
        config,
        pipeline_code,
        pipeline_id,
        clock,
        StartOptions(
            check_gate=not force and backfill is None,
            run_date=run_date,
            backfill=backfill,
            selector=selector,
        ),
    )
    if run_cancelled(engine, pipeline_run_id):
        with engine.connect() as conn:
            task_codes = fetch_task_codes(conn, pipeline_id)
        return cancelled_run(engine, pipeline_code, pipeline_run_id, task_codes, hooks)
    with (
        leases.supervise_run(
            engine, pipeline_run_id, cancel=None if child is None else child.cancel
        )
        if skip_reason is None
        else nullcontext(),
        log_context(pipeline=pipeline_code, pipeline_run_id=pipeline_run_id),
    ):
        if skip_reason is not None:
            return skipped_run(pipeline_code, pipeline_run_id, skip_reason, hooks)
        with engine.connect() as conn:
            detail, graph, task_codes = load_pipeline(
                conn, pipeline_id, detail=detail, graph_data=graph_data
            )
        run = PipelineRun(
            pipeline_code, pipeline_id, pipeline_run_id, graph, task_codes, detail.sla_in_hours
        )
        options = _ScheduleOptions(force, clock, child, hooks, stop_dispatch, pool)
        return (yield from _run_schedule(engine, config, run, options))


@dataclass(frozen=True)
class _ScheduleOptions:
    """The controls shared by a run's scheduler and its completion checks."""

    force: bool
    clock: Clock
    child: ChildOptions | None
    hooks: RunHooks
    stop_dispatch: threading.Event | None
    pool: Pool | None


def _run_schedule(
    engine: Engine, config: ConnectorConfig, run: PipelineRun, options: _ScheduleOptions
) -> Generator[float, None, PipelineOutcome]:
    """Drive admitted tasks, then finalize only when the supervisor is allowed to finish."""
    scheduler = Scheduler(
        engine,
        config,
        run.pipeline_code,
        run.pipeline_id,
        run.pipeline_run_id,
        run.task_codes,
        run.graph,
        force=options.force,
        clock=options.clock,
        pool=options.pool,
        child=options.child or ChildOptions(),
        paused=lambda: (
            (options.stop_dispatch is not None and options.stop_dispatch.is_set())
            or open_pause(engine, run.pipeline_code) is not None
        ),
        settle=lambda final: settle_unsatisfiable(
            engine, run.graph, run.pipeline_run_id, run.task_codes, failures_final=final
        ),
    )
    yield from _drive_scheduler(engine, run, scheduler, options)
    stopped = _stopped_run(engine, run, scheduler, options)
    if stopped is not None:
        return stopped
    return finalize(engine, run, options.hooks, scheduler.never_ready, scheduler.after_failure)


def _drive_scheduler(
    engine: Engine, run: PipelineRun, scheduler: Scheduler, options: _ScheduleOptions
) -> Generator[float, None, None]:
    """Yield scheduler deadlines and close all jobs on completion or interruption."""
    try:
        with sla_watch(
            engine, run.pipeline_code, run.pipeline_run_id, run.sla_hours, options.hooks
        ):
            while not scheduler.step_riding_outages():
                delay = (
                    0.1
                    if scheduler.jobs or scheduler.next_check_at is None
                    else max(0.0, (scheduler.next_check_at - options.clock.now()).total_seconds())
                )
                if any(event.next_check_at for event in scheduler.event_due.values()):
                    delay = min(1.0, delay)
                yield delay
    except BaseException:
        scheduler.close(interrupted=True)
        raise
    else:
        scheduler.close()


def _stopped_run(
    engine: Engine, run: PipelineRun, scheduler: Scheduler, options: _ScheduleOptions
) -> PipelineOutcome | None:
    """Report cancellation, stopped dispatch or a pause without finalizing the run."""
    if run_cancelled(engine, run.pipeline_run_id):
        return cancelled_run(
            engine, run.pipeline_code, run.pipeline_run_id, run.task_codes, options.hooks
        )
    if scheduler.cancel.is_set() or (
        options.stop_dispatch is not None and options.stop_dispatch.is_set()
    ):
        return PipelineOutcome(
            RunStatus.IN_PROGRESS,
            f"pipeline_run_id={run.pipeline_run_id}: supervisor stopped admitting tasks; "
            "resume this run",
            run.pipeline_run_id,
        )
    paused = open_pause(engine, run.pipeline_code)
    if paused is not None:
        message = (
            f"{run.pipeline_code}: pipeline_run_id={run.pipeline_run_id} stays IN-PROGRESS, "
            f"{paused.describe()}; the tasks that were running finished, and "
            "`etl-craft run` goes on with the rest once it is resumed"
        )
        logger.warning("%s", message)
        return PipelineOutcome(RunStatus.IN_PROGRESS, message, run.pipeline_run_id)
    return None


MAX_BACKFILL_DAYS = 366
"""The most dates one backfill runs; a longer one is split, so a typo cannot start thousands."""


@dataclass(frozen=True)
class BackfillOutcome:
    """How a backfill went: each run it made, and the one that stopped it, if any."""

    runs: list[PipelineOutcome]
    stopped: PipelineOutcome | None
    message: str

    @property
    def status(self) -> RunStatus:
        """``SUCCESS`` when every date ran to the end, else the status of the run that stopped."""
        return RunStatus.SUCCESS if self.stopped is None else self.stopped.status


def backfill(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    first: date,
    last: date,
    reason: str,
    *,
    clock: Clock | None = None,
    child: ChildOptions | None = None,
    hooks: RunHooks | None = None,
) -> BackfillOutcome:
    """Run ``pipeline_code`` once for each date from ``first`` to ``last``, oldest first.

    Each is a run of its own, as of its date (SQL's ``$$run_date``, a script's
    ``task.run_date``), marked a backfill run: it checks no dependency on other pipelines,
    consumes no upstream run, and its scripts get no offset and store none, so the scheduled
    runs go on as before. The backfill stops at the first run that does not end ``SUCCESS`` or
    ``SKIPPED``; a later backfill from that date takes up again. Local mode only, with a reason,
    at most ``MAX_BACKFILL_DAYS`` dates, and not while the pipeline has a run in progress.
    """
    check_override(config, "--backfill", reason)
    if last < first:
        raise UsageError(
            f"--backfill runs from the first date to the last: {first.isoformat()} is after "
            f"{last.isoformat()}"
        )
    days = (last - first).days + 1
    if days > MAX_BACKFILL_DAYS:
        raise UsageError(
            f"--backfill {first.isoformat()}:{last.isoformat()} is {days} dates; one backfill "
            f"runs at most {MAX_BACKFILL_DAYS}, so split it"
        )
    runs: list[PipelineOutcome] = []
    for offset in range(days):
        day = first + timedelta(days=offset)
        try:
            # Checked before every date: another run may have started since the last one.
            with engine.connect() as conn:
                pipeline_id = resolve_pipeline_id(conn, pipeline_code)
                active = runlog.fetch_active_pipeline_run_id(conn, pipeline_id)
            if active is not None:
                raise RunStateError(
                    f"{pipeline_code} has a run in progress (pipeline_run_id={active}); finish "
                    "or cancel it before a backfill"
                )
            logger.info("%s: backfill run for %s (%d of %d)", pipeline_code, day, offset + 1, days)
            outcome = run_pipeline(
                engine,
                config,
                pipeline_code,
                clock=clock,
                child=child,
                hooks=hooks,
                run_date=day,
                backfill=reason,
            )
        except RunStateError as error:
            if not runs:
                raise
            message = (
                f"{pipeline_code}: backfill stopped at {day.isoformat()} after {len(runs)} of "
                f"{days} run(s): {error}. `etl-craft run --pipeline_code {pipeline_code} "
                f"--backfill {day.isoformat()}:{last.isoformat()} --reason ...` takes up from there"
            )
            logger.error("%s", message)
            stopped = PipelineOutcome(RunStatus.SKIPPED, message, None)
            return BackfillOutcome(runs, stopped, message)
        runs.append(outcome)
        if outcome.pipeline_run_id is None:
            message = f"{pipeline_code}: backfill stopped at {day.isoformat()}: {outcome.message}"
            logger.warning("%s", message)
            return BackfillOutcome(runs, outcome, message)
        if outcome.status not in (RunStatus.SUCCESS, RunStatus.SKIPPED):
            message = (
                f"{pipeline_code}: backfill stopped at {day.isoformat()} after {len(runs)} of "
                f"{days} run(s): {outcome.message}. `etl-craft run --pipeline_code "
                f"{pipeline_code} --backfill {day.isoformat()}:{last.isoformat()}` takes up "
                "from there once it is fixed"
            )
            if outcome.status == RunStatus.IN_PROGRESS:
                message = (
                    f"{pipeline_code}: backfill stopped at {day.isoformat()} after {len(runs)} "
                    f"of {days} run(s): {outcome.message}"
                )
            logger.error("%s", message)
            return BackfillOutcome(runs, outcome, message)
    message = (
        f"{pipeline_code}: backfill of {days} date(s) from {first.isoformat()} to "
        f"{last.isoformat()} done: " + ", ".join(f"{o.pipeline_run_id} {o.status}" for o in runs)
    )
    logger.info("%s", message)
    return BackfillOutcome(runs, None, message)
