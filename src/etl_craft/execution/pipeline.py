"""``run --pipeline_code``: run a whole pipeline; ``--init-only`` and ``--finalize-only``.

In local mode the engine runs every active task of the pipeline under one run, in dependency
waves. Each wave is the tasks that are ready, at most ``Orchestration.Max_parallel_tasks`` at
once, and each task goes through ``run_task`` in a process of its own. Waves repeat until every
task is settled or none can start; a task whose dependencies can never be met is recorded
``SKIPPED``. The run then ends ``SUCCESS`` when every task is ``SUCCESS`` or ``SKIPPED``, and
``FAILED`` otherwise.

A new run starts only when the pipeline's dependencies on other pipelines are satisfied (see
``gates``); otherwise the run is recorded ``SKIPPED``. An ``IN-PROGRESS`` run is resumed instead,
without that check, and its finished tasks are not run again. Before either, the connections the
run uses are tested (see ``connections``).

In remote mode an orchestrator runs each task with ``run --task_code``, between ``run
--init-only``, which tests connections, checks the gate and starts the run, and ``run
--finalize-only``, which ends it.

Every run of a pipeline with ``SLA_IN_HOURS`` is marked ``MET`` or ``BREACHED``. While a local run
is going, a watcher marks it ``BREACHED`` as soon as the SLA passes; otherwise the finalize step
does. ``RunHooks.on_sla_lapse`` is called once per run, the first time the breach is seen.
"""

from __future__ import annotations

import contextvars
import logging
import threading
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from types import TracebackType
from typing import TypeVar

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.enums import SETTLED_STATUSES, Mode, RunStatus, SlaStatus
from etl_craft.core.errors import EtlCraftError, RunRefusedError, RunStateError
from etl_craft.core.graph import DependencyGraph, TaskRunState, build_graph
from etl_craft.core.log import log_context
from etl_craft.engine import runlog
from etl_craft.engine.repository.dependencies import fetch_pipeline_graph
from etl_craft.engine.repository.pipelines import (
    PipelineDetail,
    fetch_pipeline_detail,
    fetch_pipeline_handlers,
    resolve_pipeline_id,
)
from etl_craft.engine.repository.tasks import fetch_task_codes
from etl_craft.execution.connections import check_run_connections
from etl_craft.execution.gates import (
    Clock,
    TrackedGate,
    check_pipeline_dependencies,
    consume_pipeline_dependencies,
)
from etl_craft.execution.runner import ChildOptions, TaskOutcome, run_task
from etl_craft.handlers.mail import send_sla_lapse_email

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass(frozen=True)
class PipelineOutcome:
    """How a pipeline run, or one step of it, ended.

    ``status`` is ``IN-PROGRESS`` after ``--init-only`` started the run.
    """

    status: RunStatus
    message: str
    pipeline_run_id: int
    sla: runlog.SlaResult | None = None


@dataclass(frozen=True)
class SlaLapse:
    """A run that has passed its pipeline's SLA."""

    pipeline_code: str
    pipeline_run_id: int
    sla_hours: float
    elapsed_hours: float


@dataclass(frozen=True)
class RunHooks:
    """What other layers do at points of a run.

    ``on_sla_lapse`` is called once per run, when it is first seen past its SLA.
    ``on_finalized`` is called after the run has ended. A hook that raises is logged and does
    not change how the run ended. A run given no hooks uses ``default_hooks``.
    """

    on_sla_lapse: Callable[[SlaLapse], None] | None = None
    on_finalized: Callable[[PipelineOutcome], None] | None = None


def default_hooks(config: ConnectorConfig, engine: Engine) -> RunHooks:
    """Return the hooks a run gets unless given others: the SLA email, with ``Enforce_sla`` on.

    The email goes to the pipeline's ``EMAIL_RECIPIENTS``, else the DAG default recipients,
    through the Email settings.
    """
    if not config.limits.enforce_sla:
        return RunHooks()

    def email_the_lapse(lapse: SlaLapse) -> None:
        with engine.connect() as conn:
            pipeline_id = resolve_pipeline_id(conn, lapse.pipeline_code)
            recipients = fetch_pipeline_detail(conn, pipeline_id).email_recipients
        send_sla_lapse_email(
            config,
            recipients,
            pipeline_code=lapse.pipeline_code,
            pipeline_run_id=lapse.pipeline_run_id,
            sla_hours=lapse.sla_hours,
            elapsed_hours=lapse.elapsed_hours,
        )

    return RunHooks(on_sla_lapse=email_the_lapse)


def run_pipeline(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    *,
    force: bool = False,
    clock: Clock | None = None,
    child: ChildOptions | None = None,
    hooks: RunHooks | None = None,
) -> PipelineOutcome:
    """Run every active task of ``pipeline_code`` in dependency waves; local mode only.

    ``force`` skips the pipeline's gate and runs every task in its static wave, whatever its
    status or dependencies. When interrupted, the running task processes are stopped and
    recorded ``FAILED``, and the run stays ``IN-PROGRESS`` so the next run resumes it.
    """
    if config.mode == Mode.REMOTE:
        raise RunRefusedError(
            "run --pipeline_code without --task_code runs the whole pipeline, which only local "
            "mode does; in remote mode the orchestrator runs each task, between run --init-only "
            "and run --finalize-only"
        )
    clock = clock or Clock()
    hooks = hooks or default_hooks(config, engine)
    pipeline_id, detail = _prepare(engine, config, pipeline_code)
    pipeline_run_id, skip_reason = _start_run(
        engine, pipeline_code, pipeline_id, clock, check_gate=not force
    )
    with log_context(pipeline=pipeline_code, pipeline_run_id=pipeline_run_id):
        if skip_reason is not None:
            return _skipped_run(pipeline_code, pipeline_run_id, skip_reason, hooks)
        with engine.connect() as conn:
            graph_data = fetch_pipeline_graph(conn, pipeline_id)
            task_codes = fetch_task_codes(conn, pipeline_id)
        graph = build_graph(graph_data.tasks, graph_data.same_pipeline_edges)
        waves = _Waves(
            engine,
            config,
            pipeline_code,
            task_codes,
            force=force,
            gate=TrackedGate(clock),
            child=child or ChildOptions(),
        )
        with _SlaWatch(engine, pipeline_code, pipeline_run_id, detail.sla_in_hours, hooks):
            if force:
                for wave in graph.waves():
                    waves.run(wave)
                never_ready: list[int] = []
                after_failure: list[int] = []
            else:
                never_ready, after_failure = _run_until_settled(
                    engine, graph, pipeline_run_id, task_codes, waves
                )
        return _finalize(
            engine,
            pipeline_code,
            pipeline_id,
            pipeline_run_id,
            graph,
            task_codes,
            detail.sla_in_hours,
            hooks,
            never_ready,
            after_failure,
        )


def init_pipeline_run(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    *,
    clock: Clock | None = None,
    hooks: RunHooks | None = None,
) -> PipelineOutcome:
    """Start or resume the run of ``pipeline_code``, for an orchestrator's first step.

    Tests the run's connections and, for a new run, checks the pipeline's gate. The outcome is
    ``IN-PROGRESS``, or ``SKIPPED`` when the gate was not satisfied.
    """
    pipeline_id, _ = _prepare(engine, config, pipeline_code)
    pipeline_run_id, skip_reason = _start_run(
        engine, pipeline_code, pipeline_id, clock or Clock(), check_gate=True
    )
    if skip_reason is not None:
        return _skipped_run(
            pipeline_code, pipeline_run_id, skip_reason, hooks or default_hooks(config, engine)
        )
    return PipelineOutcome(
        RunStatus.IN_PROGRESS,
        f"{pipeline_code}: pipeline_run_id={pipeline_run_id} IN-PROGRESS",
        pipeline_run_id,
    )


def finalize_active_run(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    *,
    hooks: RunHooks | None = None,
) -> PipelineOutcome:
    """End the active run of ``pipeline_code`` from its tasks' statuses, for an orchestrator.

    Raises ``RunStateError`` when the pipeline has no active run.
    """
    with engine.connect() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        pipeline_run_id = runlog.fetch_active_pipeline_run_id(conn, pipeline_id)
        if pipeline_run_id is None:
            raise RunStateError(
                f"{pipeline_code} has no active run to finalize; --finalize-only ends the run "
                "that --init-only started, after its tasks"
            )
        detail = fetch_pipeline_detail(conn, pipeline_id)
        graph_data = fetch_pipeline_graph(conn, pipeline_id)
        task_codes = fetch_task_codes(conn, pipeline_id)
    graph = build_graph(graph_data.tasks, graph_data.same_pipeline_edges)
    with log_context(pipeline=pipeline_code, pipeline_run_id=pipeline_run_id):
        return _finalize(
            engine,
            pipeline_code,
            pipeline_id,
            pipeline_run_id,
            graph,
            task_codes,
            detail.sla_in_hours,
            hooks or default_hooks(config, engine),
        )


def _prepare(
    engine: Engine, config: ConnectorConfig, pipeline_code: str
) -> tuple[int, PipelineDetail]:
    """Resolve the pipeline and test the connections its run uses."""
    with engine.connect() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        detail = fetch_pipeline_detail(conn, pipeline_id)
        handlers = fetch_pipeline_handlers(conn, pipeline_id)
    sends_sla_email = config.limits.enforce_sla and detail.sla_in_hours is not None
    check_run_connections(engine, config, pipeline_code, handlers, sends_sla_email=sends_sla_email)
    return pipeline_id, detail


def _start_run(
    engine: Engine, pipeline_code: str, pipeline_id: int, clock: Clock, *, check_gate: bool
) -> tuple[int, str | None]:
    """Return the run to use and, when the gate refused a new one, why it was ``SKIPPED``."""
    with engine.connect() as conn:
        existing = runlog.fetch_active_pipeline_run_id(conn, pipeline_id)
    if existing is not None:
        logger.info("%s: resuming pipeline_run_id=%d", pipeline_code, existing)
        return existing, None
    reason = None
    if check_gate:
        gate = check_pipeline_dependencies(engine, pipeline_id, clock)
        if not gate.satisfied:
            reason = "; ".join(gate.reasons)
    with engine.begin() as conn:
        pipeline_run_id = runlog.find_or_create_active_run(conn, pipeline_id)
        if reason is not None:
            runlog.finalize_pipeline_run(conn, pipeline_run_id, RunStatus.SKIPPED)
    logger.info("%s: started pipeline_run_id=%d", pipeline_code, pipeline_run_id)
    return pipeline_run_id, reason


def _skipped_run(
    pipeline_code: str, pipeline_run_id: int, reason: str, hooks: RunHooks
) -> PipelineOutcome:
    outcome = PipelineOutcome(
        RunStatus.SKIPPED,
        f"{pipeline_code}: pipeline_run_id={pipeline_run_id} SKIPPED — {reason}",
        pipeline_run_id,
    )
    logger.warning("%s", outcome.message)
    _call_hook("on_finalized", hooks.on_finalized, outcome)
    return outcome


class _Waves:
    """Runs one wave of tasks at a time, each through ``run_task``, in parallel."""

    def __init__(
        self,
        engine: Engine,
        config: ConnectorConfig,
        pipeline_code: str,
        task_codes: dict[int, str],
        *,
        force: bool,
        gate: TrackedGate,
        child: ChildOptions,
    ) -> None:
        self.engine = engine
        self.config = config
        self.pipeline_code = pipeline_code
        self.task_codes = task_codes
        self.force = force
        self.gate = gate
        self.cancel = threading.Event()
        self.child = replace(child, cancel=self.cancel)
        self.count = 0

    def run(self, task_ids: Sequence[int]) -> None:
        """Run ``task_ids``, at most ``Max_parallel_tasks`` at once, and wait for all of them.

        When waiting is interrupted, the running task processes are stopped first.
        """
        if not task_ids:
            return
        self.count += 1
        codes = [self.task_codes[task_id] for task_id in task_ids]
        logger.info("%s: wave %d: %s", self.pipeline_code, self.count, ", ".join(codes))
        workers = min(max(self.config.limits.max_parallel_tasks, 1), len(codes))
        with ThreadPoolExecutor(workers, thread_name_prefix="etl-craft-task") as pool:
            futures = [
                pool.submit(contextvars.copy_context().run, self._run_one, code) for code in codes
            ]
            try:
                for future in as_completed(futures):
                    future.result()
            except BaseException:
                self.cancel.set()
                logger.warning(
                    "%s: interrupted; stopping the running tasks. The run stays IN-PROGRESS, "
                    "so the next run resumes it",
                    self.pipeline_code,
                )
                raise

    def _run_one(self, task_code: str) -> TaskOutcome | None:
        if self.cancel.is_set():
            return None
        try:
            return run_task(
                self.engine,
                self.config,
                self.pipeline_code,
                task_code,
                force=self.force,
                gate=self.gate,
                child=self.child,
            )
        except EtlCraftError as error:
            logger.error("%s: could not run: %s", task_code, error)
            return None


def _run_until_settled(
    engine: Engine,
    graph: DependencyGraph,
    pipeline_run_id: int,
    task_codes: dict[int, str],
    waves: _Waves,
) -> tuple[list[int], list[int]]:
    """Run waves until every task is settled or none can start.

    When nothing more can start, no failed task will be retried in this run, which is about to
    end: the tasks that could only have run after a failure's retry are recorded ``SKIPPED``,
    and those that wait for them with ``ALWAYS`` or ``FAILURE`` (an alert) run in turn.
    Returns the tasks never started, and those skipped because of a failure.
    """
    task_ids = list(graph.task_ids)
    attempted: set[int] = set()
    failures_final = False
    after_failure: list[int] = []
    while True:
        skipped = _settle_unsatisfiable(
            engine, graph, pipeline_run_id, task_codes, failures_final=failures_final
        )
        if failures_final:
            after_failure.extend(skipped)
        with engine.connect() as conn:
            run_state = runlog.fetch_run_state(conn, pipeline_run_id, task_ids)
        pending = [
            task_id
            for task_id in task_ids
            if run_state.get(task_id, TaskRunState()).status not in SETTLED_STATUSES
            and task_id not in attempted
        ]
        if not pending:
            return [], after_failure
        ready = [task_id for task_id in graph.ready(run_state) if task_id not in attempted]
        if not ready:
            if failures_final and not skipped:
                return pending, after_failure
            failures_final = True
            continue
        attempted.update(ready)
        waves.run(ready)


def _settle_unsatisfiable(
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
            binding = runlog.find_or_create_task_run(conn, task_id, pipeline_run_id)
            if not binding.created:
                continue
            runlog.finish_task_run(
                conn, binding.task_run_id, status=RunStatus.SKIPPED, error_message=reason
            )
        skipped.append(task_id)
        logger.info("%s: SKIPPED — %s", task_codes[task_id], reason)
    return skipped


def _finalize(
    engine: Engine,
    pipeline_code: str,
    pipeline_id: int,
    pipeline_run_id: int,
    graph: DependencyGraph,
    task_codes: dict[int, str],
    sla_hours: float | None,
    hooks: RunHooks,
    never_ready: Sequence[int] = (),
    after_failure: Sequence[int] = (),
) -> PipelineOutcome:
    """End the run from its tasks' statuses, record its SLA, and consume its upstream runs."""
    _settle_unsatisfiable(engine, graph, pipeline_run_id, task_codes)
    with engine.connect() as conn:
        run_state = runlog.fetch_run_state(conn, pipeline_run_id, list(graph.task_ids))
    unsettled = {
        task_id: run_state.get(task_id, TaskRunState()).status or "never started"
        for task_id in graph.task_ids
        if run_state.get(task_id, TaskRunState()).status not in SETTLED_STATUSES
    }
    status = RunStatus.FAILED if unsettled else RunStatus.SUCCESS
    with engine.begin() as conn:
        breached_before = runlog.fetch_run_sla(conn, pipeline_run_id).sla_status
        sla = runlog.finalize_pipeline_run(conn, pipeline_run_id, status, sla_in_hours=sla_hours)
    if status == RunStatus.SUCCESS:
        consume_pipeline_dependencies(engine, pipeline_id, pipeline_run_id)

    message = f"{pipeline_code}: pipeline_run_id={pipeline_run_id} {status}"
    if unsettled:
        listed = ", ".join(f"{task_codes[t]} ({state})" for t, state in unsettled.items())
        message += f" — {len(unsettled)} task(s) did not succeed: {listed}"
        if never_ready:
            message += (
                f"; {len(never_ready)} of them could not start because their dependencies "
                "were not met"
            )
        if after_failure:
            skipped = ", ".join(task_codes[t] for t in after_failure)
            message += f"; skipped because of the failure: {skipped}"
    if sla is not None and sla.status == SlaStatus.BREACHED:
        message += f"; {sla.describe()}"
    outcome = PipelineOutcome(status, message, pipeline_run_id, sla)
    logger.log(logging.INFO if status == RunStatus.SUCCESS else logging.ERROR, "%s", message)

    if sla is not None and sla.status == SlaStatus.BREACHED and breached_before != sla.status:
        lapse = SlaLapse(pipeline_code, pipeline_run_id, sla.sla_hours, sla.elapsed_hours)
        _call_hook("on_sla_lapse", hooks.on_sla_lapse, lapse)
    _call_hook("on_finalized", hooks.on_finalized, outcome)
    return outcome


class _SlaWatch:
    """Marks a running run ``BREACHED`` the moment its SLA passes, from a background thread."""

    def __init__(
        self,
        engine: Engine,
        pipeline_code: str,
        pipeline_run_id: int,
        sla_hours: float | None,
        hooks: RunHooks,
    ) -> None:
        self.engine = engine
        self.pipeline_code = pipeline_code
        self.pipeline_run_id = pipeline_run_id
        self.sla_hours = sla_hours
        self.hooks = hooks
        self.stop = threading.Event()
        self.thread: threading.Thread | None = None

    def __enter__(self) -> _SlaWatch:
        hours = self.sla_hours
        if hours is None:
            return self
        with self.engine.connect() as conn:
            start = runlog.fetch_run_sla(conn, self.pipeline_run_id).start_date
        if start.tzinfo is None:
            start = start.replace(tzinfo=UTC)
        self.thread = threading.Thread(
            target=contextvars.copy_context().run,
            args=(self._watch, start, hours),
            name="etl-craft-sla-watch",
            daemon=True,
        )
        self.thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.stop.set()
        if self.thread is not None:
            self.thread.join()

    def _watch(self, start: datetime, hours: float) -> None:
        deadline = start + timedelta(hours=hours)
        wait = (deadline - datetime.now(UTC)).total_seconds()
        if self.stop.wait(timeout=max(wait, 0.0)):
            return
        try:
            with self.engine.begin() as conn:
                marked = runlog.mark_sla_breached(conn, self.pipeline_run_id)
        except Exception:
            logger.exception("%s: could not mark the SLA breached", self.pipeline_code)
            return
        if not marked:
            return
        elapsed = runlog.elapsed_hours(start, datetime.now(UTC))
        logger.warning(
            "%s: pipeline_run_id=%d is still running past its SLA of %g h",
            self.pipeline_code,
            self.pipeline_run_id,
            hours,
        )
        lapse = SlaLapse(self.pipeline_code, self.pipeline_run_id, hours, elapsed)
        _call_hook("on_sla_lapse", self.hooks.on_sla_lapse, lapse)


def _call_hook(name: str, hook: Callable[[T], None] | None, value: T) -> None:
    if hook is None:
        return
    try:
        hook(value)
    except Exception:
        logger.exception("the %s hook failed; the run's outcome is unchanged", name)
