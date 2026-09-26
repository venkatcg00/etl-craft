"""``run --task_code``: run one task of a pipeline, in its own process.

The task resolves its pipeline's active run itself; nothing hands it a ``pipeline_run_id``. In
local mode it runs only when it may: not again once ``SUCCESS`` or ``SKIPPED`` under the run, not
while it is already ``IN-PROGRESS``, and only when enough of its dependencies are satisfied. A
task whose dependencies can never be satisfied under the run is recorded ``SKIPPED``.

In remote mode the orchestrator decides, so the task runs whenever it is told to, with none of
those checks (see ``remote``). Run again after it succeeded, it is a new attempt that skips
nothing; run after the run ended, it reopens the run, which ``--finalize-only`` ends again.

The task itself runs in a freshly started interpreter, supervised with its time limit. Its
output goes to the attempt's log file, whose tail is kept in ``TASK_LOG``. When that process
ends without recording an outcome (it crashed, was killed, or ran out of time), the task is
recorded ``FAILED`` with the reason. While it runs, the run is watched: once an operator
cancels it (``etl-craft cancel``), the task process is stopped and the task is ``CANCELLED``.
"""

from __future__ import annotations

import logging
import sys
import threading
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.enums import (
    SETTLED_STATUSES,
    TERMINAL_STATUSES,
    InterventionAction,
    Mode,
    RunStatus,
)
from etl_craft.core.errors import ConfigurationError, RunRefusedError, UsageError
from etl_craft.core.graph import DependencyGraph, RunState, TaskRunState, build_graph
from etl_craft.engine import runlog
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.dependencies import fetch_pipeline_graph
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.repository.tasks import fetch_task_parameters, resolve_task_id
from etl_craft.execution.gates import (
    CrossPipelineCheck,
    CrossPipelineGate,
    TrackedGate,
)
from etl_craft.execution.interventions import (
    check_override,
    open_pause,
    record_change,
    record_gate_bypass,
)
from etl_craft.execution.limits import task_timeout_seconds
from etl_craft.execution.supervisor import (
    KILL_GRACE_SECONDS,
    ChildResult,
    ChildSpec,
    run_child,
)

logger = logging.getLogger(__name__)

CHILD_MODULE = "etl_craft.execution.child"
"""The module a task attempt runs as, with ``python -m``."""

CANCEL_POLL_SECONDS = 2.0
"""How often a running task looks whether an operator cancelled its run."""


@dataclass(frozen=True)
class TaskOutcome:
    """How ``run --task_code`` ended: ``SUCCESS``, ``FAILED`` or ``SKIPPED``, and why."""

    status: RunStatus
    message: str
    task_run_id: int | None = None


@dataclass(frozen=True)
class Override:
    """An operator's override of the checks before a task runs, with the reason for it.

    ``rerun`` runs the task again although it already ended ``SUCCESS`` or ``SKIPPED``, as a
    new attempt that skips nothing, reopening the run if it had ended (``--rerun``). Otherwise
    the task runs without its dependencies being checked (``--ignore-dependencies``). Neither
    checks the task's dependencies or consumes an upstream run, and each is recorded in
    ``AUD_RUN_INTERVENTIONS``.
    """

    reason: str
    rerun: bool = False

    @property
    def action(self) -> InterventionAction:
        """How the override is recorded."""
        return InterventionAction.RERUN if self.rerun else InterventionAction.IGNORE_DEPENDENCIES


@dataclass(frozen=True)
class ChildOptions:
    """How the task process is started: its module, log settings and kill grace period.

    Setting ``cancel`` stops a running task process; the pipeline runner sets it when the run is
    interrupted. ``cancel_poll_seconds`` is how often a running task looks whether an operator
    cancelled its run.
    """

    module: str = CHILD_MODULE
    log_level: str = "INFO"
    log_format: str = "text"
    kill_grace_seconds: float = KILL_GRACE_SECONDS
    cancel: threading.Event | None = None
    cancel_poll_seconds: float = CANCEL_POLL_SECONDS


def run_task(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    task_code: str,
    *,
    force: bool = False,
    gate: CrossPipelineGate | None = None,
    child: ChildOptions | None = None,
    override: Override | None = None,
) -> TaskOutcome:
    """Run one task under its pipeline's active run and return how it ended.

    ``force`` runs the task even when it already succeeded or its dependencies are not met, and
    rebinds a finished run; it is local mode's override, refused in remote mode, where the task
    always runs as the orchestrator says. ``override`` is an operator's recorded override (see
    ``Override``), also local mode's. Raises ``MetadataError`` for an unknown code and
    ``RunStateError`` when there is no run to bind to.
    """
    if override is not None:
        option = "--rerun" if override.rerun else "--ignore-dependencies"
        check_override(config, option, override.reason)
        if force:
            raise UsageError(f"{option} and --force are different overrides: choose one")
    if config.mode == Mode.LOCAL:
        paused = open_pause(engine, pipeline_code)
        if paused is not None:
            return _skipped(
                f"{task_code}: {pipeline_code} is {paused.describe()}; nothing started or "
                f"recorded. `etl-craft resume --pipeline_code {pipeline_code}` lets it run again"
            )
    if override is not None:
        return _run_overridden(engine, config, pipeline_code, task_code, override, child)
    if force and config.mode == Mode.REMOTE:
        raise RunRefusedError(
            "--force is only allowed in local mode; in remote mode run --task_code already runs "
            "the task whenever the orchestrator says, so there is nothing to override"
        )
    if config.mode == Mode.REMOTE:
        return _run_for_orchestrator(engine, config, pipeline_code, task_code, child)
    gate = gate or TrackedGate(policy=config.dependency_gates)
    with engine.connect() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        task_id = resolve_task_id(conn, pipeline_id, task_code)
    with engine.begin() as conn:
        pipeline_run_id = runlog.resolve_run_for_task(
            conn, pipeline_id, force=force, mode=config.mode
        )
    consumed: dict[int, int] | None = None
    if not force:
        blocked, cross = _preflight(engine, gate, pipeline_id, task_id, task_code, pipeline_run_id)
        if blocked is not None:
            logger.info(blocked.message)
            return blocked
        consumed = cross.consumed
        if cross.bypassed:
            record_gate_bypass(
                engine,
                pipeline_id,
                pipeline_run_id,
                config.dependency_gates,
                cross.bypassed,
                task_id=task_id,
            )
    outcome = _run_attempt(
        engine, config, task_id, task_code, pipeline_code, pipeline_run_id, force, child
    )
    if consumed and outcome.status == RunStatus.SUCCESS:
        gate.consume(engine, task_id, consumed)
    return outcome


def _run_overridden(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    task_code: str,
    override: Override,
    child: ChildOptions | None,
) -> TaskOutcome:
    """Run the task past the checks the override names, and record it."""
    with engine.begin() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        task_id = resolve_task_id(conn, pipeline_id, task_code)
        if override.rerun:
            pipeline_run_id, reopened = runlog.resolve_run_for_orchestrator(conn, pipeline_id)
        else:
            pipeline_run_id = runlog.resolve_run_for_task(conn, pipeline_id, mode=config.mode)
            reopened = None
        status = runlog.fetch_task_run_status(conn, task_id, pipeline_run_id)
    if reopened is not None:
        record_change(
            engine,
            pipeline_id=pipeline_id,
            pipeline_run_id=pipeline_run_id,
            action=InterventionAction.REOPEN,
            from_status=reopened,
            to_status=RunStatus.IN_PROGRESS,
            reason=override.reason,
        )
    if status == RunStatus.IN_PROGRESS:
        return _skipped(
            f"{task_code}: already IN-PROGRESS under pipeline_run_id={pipeline_run_id}; not "
            "starting it twice"
        )
    if status in SETTLED_STATUSES and not override.rerun:
        return _skipped(
            f"{task_code}: already {status} under pipeline_run_id={pipeline_run_id}; pass "
            "--rerun to run it again"
        )
    logger.warning(
        "%s: %s under pipeline_run_id=%d, without checking its dependencies: %s",
        task_code,
        "running again" if override.rerun else "running",
        pipeline_run_id,
        override.reason,
    )
    outcome = _run_attempt(
        engine,
        config,
        task_id,
        task_code,
        pipeline_code,
        pipeline_run_id,
        False,
        child,
        rerun=status in SETTLED_STATUSES,
    )
    record_change(
        engine,
        pipeline_id=pipeline_id,
        pipeline_run_id=pipeline_run_id,
        task_id=task_id,
        action=override.action,
        from_status=status,
        to_status=outcome.status,
        reason=override.reason,
    )
    return outcome


def _run_for_orchestrator(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    task_code: str,
    child: ChildOptions | None,
) -> TaskOutcome:
    """Run the task as the orchestrator said, under the run it resolves; remote mode."""
    with engine.begin() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        task_id = resolve_task_id(conn, pipeline_id, task_code)
        pipeline_run_id, reopened = runlog.resolve_run_for_orchestrator(conn, pipeline_id)
        status = runlog.fetch_task_run_status(conn, task_id, pipeline_run_id)
    if reopened is not None:
        logger.warning(
            "%s: pipeline_run_id=%d had already ended %s; the orchestrator ran %s again, so the "
            "run is IN-PROGRESS again until --finalize-only ends it",
            pipeline_code,
            pipeline_run_id,
            reopened,
            task_code,
        )
    if status == RunStatus.IN_PROGRESS:
        logger.warning(
            "%s: already IN-PROGRESS under pipeline_run_id=%d; the orchestrator started it again, "
            "so this is a new attempt",
            task_code,
            pipeline_run_id,
        )
    rerun = status in SETTLED_STATUSES
    if rerun:
        logger.info(
            "%s: already %s under pipeline_run_id=%d; the orchestrator runs it again",
            task_code,
            status,
            pipeline_run_id,
        )
    return _run_attempt(
        engine,
        config,
        task_id,
        task_code,
        pipeline_code,
        pipeline_run_id,
        False,
        child,
        rerun=rerun,
    )


NO_CROSS = CrossPipelineCheck(0)
"""What a preflight that stopped before the cross-pipeline gate reports from it."""


def _preflight(
    engine: Engine,
    gate: CrossPipelineGate,
    pipeline_id: int,
    task_id: int,
    task_code: str,
    pipeline_run_id: int,
) -> tuple[TaskOutcome | None, CrossPipelineCheck]:
    """Return the outcome that stops the task from running now, if any, and the gate's check.

    The check says what the task consumes once it succeeds, and what ``Dependency_gates``
    bypassed.
    """
    with engine.connect() as conn:
        status = runlog.fetch_task_run_status(conn, task_id, pipeline_run_id)
        run_status = runlog.fetch_pipeline_run_status(conn, pipeline_run_id)
        backfill = runlog.fetch_run_kind(conn, pipeline_run_id).backfill
        graph_data = fetch_pipeline_graph(conn, pipeline_id)
        run_state = runlog.fetch_run_state(
            conn, pipeline_run_id, [task.task_id for task in graph_data.tasks]
        )
    if status in SETTLED_STATUSES:
        return _skipped(
            f"{task_code}: already {status} under pipeline_run_id={pipeline_run_id}"
        ), NO_CROSS
    if status == RunStatus.IN_PROGRESS:
        return _skipped(
            f"{task_code}: already IN-PROGRESS under pipeline_run_id={pipeline_run_id}; not "
            "starting it twice"
        ), NO_CROSS
    if run_status == RunStatus.SKIPPED:
        reason = f"pipeline_run_id={pipeline_run_id} is itself SKIPPED"
        return _record_skipped(engine, task_id, pipeline_run_id, task_code, reason), NO_CROSS

    graph = build_graph(graph_data.tasks, graph_data.same_pipeline_edges)
    still_needed = graph.required_edge_count(task_id) - graph.satisfied_edge_count(
        task_id, run_state, 0
    )
    cross = CrossPipelineCheck(0)
    if still_needed > 0 and task_id in graph_data.cross_pipeline_task_ids:
        if backfill:
            # A backfill checks no dependency on another pipeline, and consumes nothing.
            node = next(t for t in graph_data.tasks if t.task_id == task_id)
            cross = CrossPipelineCheck(min(still_needed, node.cross_pipeline_edge_count))
        else:
            cross = gate.check(engine, task_id, still_needed)
        still_needed -= cross.satisfied_count
    if still_needed <= 0:
        return None, cross

    unready = _describe_unready(graph, task_id, pipeline_run_id)
    if cross.reasons:
        reasons = "; ".join(cross.reasons)
        if cross.definitive and not _upstream_pending(graph, task_id, run_state):
            return _record_skipped(engine, task_id, pipeline_run_id, task_code, reasons), NO_CROSS
        return _skipped(f"{task_code}: {reasons}, and {unready}; nothing recorded"), NO_CROSS
    if task_id in graph.unsatisfiable(run_state):
        never = _describe_unready(graph, task_id, pipeline_run_id, can_never=True)
        return _record_skipped(engine, task_id, pipeline_run_id, task_code, never), NO_CROSS
    waiting = f"{task_code}: {unready}; nothing recorded, run it again once they are"
    return _skipped(waiting), NO_CROSS


def _skipped(message: str) -> TaskOutcome:
    return TaskOutcome(RunStatus.SKIPPED, message)


def _upstream_pending(graph: DependencyGraph, task_id: int, run_state: RunState) -> bool:
    """Whether a same-pipeline upstream of ``task_id`` has not finished yet."""
    return any(
        run_state.get(edge.depends_on_task_id, TaskRunState()).status not in TERMINAL_STATUSES
        for edge in graph.dependencies_of(task_id)
    )


def _describe_unready(
    graph: DependencyGraph, task_id: int, pipeline_run_id: int, *, can_never: bool = False
) -> str:
    counts = (
        f"(needs {graph.required_edge_count(task_id)} of {graph.total_edge_count(task_id)} "
        "dependencies satisfied)"
    )
    if can_never:
        return f"its dependencies can never be met under pipeline_run_id={pipeline_run_id} {counts}"
    return f"its dependencies are not met yet under pipeline_run_id={pipeline_run_id} {counts}"


def _record_skipped(
    engine: Engine, task_id: int, pipeline_run_id: int, task_code: str, reason: str
) -> TaskOutcome:
    with engine.begin() as conn:
        binding = runlog.find_or_create_task_run(conn, task_id, pipeline_run_id)
        runlog.finish_task_run(
            conn, binding.task_run_id, status=RunStatus.SKIPPED, error_message=reason
        )
    return TaskOutcome(RunStatus.SKIPPED, f"{task_code}: SKIPPED — {reason}", binding.task_run_id)


def attempt_log_path(
    config: ConnectorConfig, pipeline_code: str, pipeline_run_id: int, task_code: str, attempt: int
) -> Path:
    """Return the log file of one task attempt, under ``Orchestration.Log_dir``."""
    return (
        config.log_dir
        / pipeline_code
        / f"run-{pipeline_run_id}"
        / f"{task_code}.attempt-{attempt}.log"
    )


def _run_attempt(
    engine: Engine,
    config: ConnectorConfig,
    task_id: int,
    task_code: str,
    pipeline_code: str,
    pipeline_run_id: int,
    force: bool,
    child: ChildOptions | None,
    *,
    rerun: bool = False,
) -> TaskOutcome:
    """Bind and start an attempt, run it in its own process, and record how it ended."""
    child = child or ChildOptions()
    if config.config_path is None:
        raise ConfigurationError("a task process needs the craft-connector.yml it was loaded from")
    with engine.begin() as conn:
        binding = runlog.find_or_create_task_run(conn, task_id, pipeline_run_id)
        attempt = 1 if binding.created else runlog.begin_attempt(conn, binding.task_run_id)
        params = fetch_task_parameters(conn, task_id)
    timeout = task_timeout_seconds(params, config)
    log_path = attempt_log_path(config, pipeline_code, pipeline_run_id, task_code, attempt)
    argv = [
        "-m",
        child.module,
        "--config",
        str(config.config_path),
        "--task-run-id",
        str(binding.task_run_id),
        "--log-level",
        child.log_level,
        "--log-format",
        child.log_format,
    ]
    if force:
        argv.append("--force")
    if rerun:
        argv.append("--rerun")
    logger.info(
        "starting %s attempt %d (task_run_id=%d), time limit %s, log %s",
        task_code,
        attempt,
        binding.task_run_id,
        f"{timeout}s" if timeout else "none",
        log_path,
    )
    with _CancelWatch(engine, pipeline_run_id, child) as stop:
        result = run_child(
            ChildSpec(argv=(sys.executable, *argv), timeout_seconds=timeout, log_path=log_path),
            kill_grace_seconds=child.kill_grace_seconds,
            cancel=stop,
        )
    return _record_attempt(
        engine, binding.task_run_id, pipeline_run_id, task_code, result, log_path
    )


class _CancelWatch:
    """Sets the event that stops a task process when the run is interrupted or cancelled.

    A thread looks every ``cancel_poll_seconds`` whether an operator cancelled the run, and
    passes on the pipeline runner's own ``cancel`` event at once.
    """

    def __init__(self, engine: Engine, pipeline_run_id: int, child: ChildOptions) -> None:
        self.engine = engine
        self.pipeline_run_id = pipeline_run_id
        self.child = child
        self.stop = threading.Event()
        self.done = threading.Event()
        self.thread = threading.Thread(
            target=self._watch, name="etl-craft-cancel-watch", daemon=True
        )

    def __enter__(self) -> threading.Event:
        self.thread.start()
        return self.stop

    def __exit__(self, *exc: object) -> None:
        self.done.set()
        self.thread.join()

    def _watch(self) -> None:
        interrupt = self.child.cancel
        poll = max(self.child.cancel_poll_seconds, 0.05)
        waited = 0.0
        while not self.done.is_set():
            if interrupt is not None and interrupt.is_set():
                self.stop.set()
                return
            if waited >= poll:
                waited = 0.0
                if run_cancelled(self.engine, self.pipeline_run_id):
                    logger.warning(
                        "pipeline_run_id=%d was cancelled; stopping the task", self.pipeline_run_id
                    )
                    self.stop.set()
                    return
            self.done.wait(0.05)
            waited += 0.05


def run_cancelled(engine: Engine, pipeline_run_id: int) -> bool:
    """Whether an operator cancelled ``pipeline_run_id``; a failed look counts as no."""
    try:
        with engine.connect() as conn:
            status = runlog.fetch_pipeline_run_status(conn, pipeline_run_id)
    except Exception:
        logger.warning("could not read the status of pipeline_run_id=%d", pipeline_run_id)
        return False
    return status == RunStatus.CANCELLED


def _record_attempt(
    engine: Engine,
    task_run_id: int,
    pipeline_run_id: int,
    task_code: str,
    result: ChildResult,
    log_path: Path,
) -> TaskOutcome:
    """Record the attempt's outcome, failing it if the task process ended without one.

    A task stopped because its run was cancelled is ``CANCELLED``.
    """
    with engine.begin() as conn:
        recorded = runlog.fetch_task_run_result(conn, task_run_id)
        status = RunStatus(recorded.status)
        message = recorded.error_message
        values = conn.execute(
            statement(conn, "task_run_log"), {"task_run_id": task_run_id}
        ).scalar_one_or_none()
        cancelled = runlog.fetch_pipeline_run_status(conn, pipeline_run_id) == RunStatus.CANCELLED
        if status == RunStatus.IN_PROGRESS and cancelled:
            status = RunStatus.CANCELLED
            message = f"the run was cancelled, so the task process {result.describe()}"
            runlog.finish_task_run(conn, task_run_id, status=status, error_message=message)
        elif status == RunStatus.IN_PROGRESS:
            status = RunStatus.FAILED
            message = f"the task process {result.describe()} before recording an outcome"
            if result.timed_out:
                message += (
                    " (set the task's TASK_TIMEOUT_SECONDS, or Orchestration.Task_timeout_seconds,"
                    " to change its time limit)"
                )
            runlog.finish_task_run(conn, task_run_id, status=status, error_message=message)
            logger.error("%s: %s; see %s", task_code, message, log_path)
        conn.execute(
            statement(conn, "set_task_log"),
            {"task_run_id": task_run_id, "task_log": _task_log(values, result.output_tail)},
        )
    if status == RunStatus.SUCCESS:
        logger.info("%s: SUCCESS", task_code)
        return TaskOutcome(status, f"{task_code}: SUCCESS", task_run_id)
    logger.error("%s: %s — %s (log: %s)", task_code, status, message, log_path)
    return TaskOutcome(status, f"{task_code}: {status} — {message}", task_run_id)


def _task_log(values: str | None, output_tail: str) -> str | None:
    """Combine the task's reported values with the tail of its output."""
    parts = [part for part in (values, output_tail.strip()) if part]
    return "\n\n".join(parts) or None
