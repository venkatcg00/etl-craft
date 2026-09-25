"""``run --task_code``: run one task of a pipeline, in its own process.

The task resolves its pipeline's active run itself; nothing hands it a ``pipeline_run_id``. It
runs only when it may: not again once ``SUCCESS`` or ``SKIPPED`` under the run, not while it is
already ``IN-PROGRESS``, and only when enough of its dependencies are satisfied. A task whose
dependencies can never be satisfied under the run is recorded ``SKIPPED``.

The task itself runs in a freshly started interpreter, supervised with its time limit. Its
output goes to the attempt's log file, whose tail is kept in ``TASK_LOG``. When that process
ends without recording an outcome (it crashed, was killed, or ran out of time), the task is
recorded ``FAILED`` with the reason.
"""

from __future__ import annotations

import logging
import sys
import threading
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.enums import SETTLED_STATUSES, TERMINAL_STATUSES, Mode, RunStatus
from etl_craft.core.errors import ConfigurationError, RunRefusedError
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


@dataclass(frozen=True)
class TaskOutcome:
    """How ``run --task_code`` ended: ``SUCCESS``, ``FAILED`` or ``SKIPPED``, and why."""

    status: RunStatus
    message: str
    task_run_id: int | None = None


@dataclass(frozen=True)
class ChildOptions:
    """How the task process is started: its module, log settings and kill grace period.

    Setting ``cancel`` stops a running task process; the pipeline runner sets it when the run is
    interrupted.
    """

    module: str = CHILD_MODULE
    log_level: str = "INFO"
    log_format: str = "text"
    kill_grace_seconds: float = KILL_GRACE_SECONDS
    cancel: threading.Event | None = None


def run_task(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    task_code: str,
    *,
    force: bool = False,
    gate: CrossPipelineGate | None = None,
    child: ChildOptions | None = None,
) -> TaskOutcome:
    """Run one task under its pipeline's active run and return how it ended.

    ``force`` runs the task even when it already succeeded or its dependencies are not met, and
    rebinds a finished run; it is refused in remote mode, where an orchestrator owns the runs.
    Raises ``MetadataError`` for an unknown code and ``RunStateError`` when there is no run to
    bind to.
    """
    if force and config.mode == Mode.REMOTE:
        raise RunRefusedError(
            "--force is only allowed in local mode; in remote mode the orchestrator owns the runs"
        )
    gate = gate or TrackedGate()
    with engine.connect() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        task_id = resolve_task_id(conn, pipeline_id, task_code)
    with engine.begin() as conn:
        pipeline_run_id = runlog.resolve_run_for_task(
            conn, pipeline_id, force=force, mode=config.mode
        )
    consumed: dict[int, int] | None = None
    if not force:
        blocked, consumed = _preflight(
            engine, gate, pipeline_id, task_id, task_code, pipeline_run_id
        )
        if blocked is not None:
            logger.info(blocked.message)
            return blocked
    outcome = _run_attempt(
        engine, config, task_id, task_code, pipeline_code, pipeline_run_id, force, child
    )
    if consumed and outcome.status == RunStatus.SUCCESS:
        gate.consume(engine, task_id, consumed)
    return outcome


def _preflight(
    engine: Engine,
    gate: CrossPipelineGate,
    pipeline_id: int,
    task_id: int,
    task_code: str,
    pipeline_run_id: int,
) -> tuple[TaskOutcome | None, dict[int, int]]:
    """Return the outcome that stops the task from running now, if any, and what it consumed."""
    with engine.connect() as conn:
        status = runlog.fetch_task_run_status(conn, task_id, pipeline_run_id)
        run_status = runlog.fetch_pipeline_run_status(conn, pipeline_run_id)
        graph_data = fetch_pipeline_graph(conn, pipeline_id)
        run_state = runlog.fetch_run_state(
            conn, pipeline_run_id, [task.task_id for task in graph_data.tasks]
        )
    if status in SETTLED_STATUSES:
        return _skipped(
            f"{task_code}: already {status} under pipeline_run_id={pipeline_run_id}"
        ), {}
    if status == RunStatus.IN_PROGRESS:
        return _skipped(
            f"{task_code}: already IN-PROGRESS under pipeline_run_id={pipeline_run_id}; not "
            "starting it twice"
        ), {}
    if run_status == RunStatus.SKIPPED:
        reason = f"pipeline_run_id={pipeline_run_id} is itself SKIPPED"
        return _record_skipped(engine, task_id, pipeline_run_id, task_code, reason), {}

    graph = build_graph(graph_data.tasks, graph_data.same_pipeline_edges)
    still_needed = graph.required_edge_count(task_id) - graph.satisfied_edge_count(
        task_id, run_state, 0
    )
    cross = CrossPipelineCheck(0)
    if still_needed > 0 and task_id in graph_data.cross_pipeline_task_ids:
        cross = gate.check(engine, task_id, still_needed)
        still_needed -= cross.satisfied_count
    if still_needed <= 0:
        return None, cross.consumed

    unready = _describe_unready(graph, task_id, pipeline_run_id)
    if cross.reasons:
        reasons = "; ".join(cross.reasons)
        if cross.definitive and not _upstream_pending(graph, task_id, run_state):
            return _record_skipped(engine, task_id, pipeline_run_id, task_code, reasons), {}
        return _skipped(f"{task_code}: {reasons}, and {unready}; nothing recorded"), {}
    if task_id in graph.unsatisfiable(run_state):
        never = _describe_unready(graph, task_id, pipeline_run_id, can_never=True)
        return _record_skipped(engine, task_id, pipeline_run_id, task_code, never), {}
    return _skipped(f"{task_code}: {unready}; nothing recorded, run it again once they are"), {}


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
    logger.info(
        "starting %s attempt %d (task_run_id=%d), time limit %s, log %s",
        task_code,
        attempt,
        binding.task_run_id,
        f"{timeout}s" if timeout else "none",
        log_path,
    )
    result = run_child(
        ChildSpec(argv=(sys.executable, *argv), timeout_seconds=timeout, log_path=log_path),
        kill_grace_seconds=child.kill_grace_seconds,
        cancel=child.cancel,
    )
    return _record_attempt(engine, binding.task_run_id, task_code, result, log_path)


def _record_attempt(
    engine: Engine, task_run_id: int, task_code: str, result: ChildResult, log_path: Path
) -> TaskOutcome:
    """Record the attempt's outcome, failing it if the task process ended without one."""
    with engine.begin() as conn:
        recorded = runlog.fetch_task_run_result(conn, task_run_id)
        status = RunStatus(recorded.status)
        message = recorded.error_message
        values = conn.execute(
            statement(conn, "task_run_log"), {"task_run_id": task_run_id}
        ).scalar_one_or_none()
        if status == RunStatus.IN_PROGRESS:
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
