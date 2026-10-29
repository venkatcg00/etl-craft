"""Attempt bookkeeping: log paths, the cancellation watch, recorded outcomes and starts."""

from __future__ import annotations

import contextvars
import logging
import socket
import threading
from contextlib import suppress
from pathlib import Path

from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.config import ConnectorConfig
from etl_craft.core.actor import SYSTEM_ACTOR, acting_as, current_actor
from etl_craft.core.enums import (
    AttemptStatus,
    ControlAction,
    RunStatus,
)
from etl_craft.core.errors import ResourceNotFoundError
from etl_craft.core.text import byte_tail, is_metadata_code
from etl_craft.engine import attempt_cancellation, attempt_starts, controls, runlog, transitions
from etl_craft.engine.logs import LogAttempt
from etl_craft.engine.metrics import lifecycle
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pipelines import current_project_id
from etl_craft.execution import leases
from etl_craft.execution.attempt_types import ChildOptions, TaskOutcome
from etl_craft.execution.processes import (
    ChildResult,
)

logger = logging.getLogger(__name__)
STOP_ACTIONS = frozenset({ControlAction.STOP, ControlAction.ABORT})


def attempt_log_path(
    config: ConnectorConfig,
    pipeline_code: str,
    pipeline_run_id: int,
    task_code: str,
    attempt: int,
    *,
    project_id: int | None = None,
) -> Path:
    """Return the log file of one task attempt, under ``Orchestration.Log_dir``."""
    return (
        config.log_dir
        / f"project-{project_id if project_id is not None else current_project_id()}"
        / pipeline_code
        / f"run-{pipeline_run_id}"
        / f"{task_code}.attempt-{attempt}.log"
    )


def local_attempt_log(config: ConnectorConfig, row: LogAttempt) -> Path:
    """Validate recorded local identities independently of mutable pipeline/task codes."""
    root = config.log_dir.resolve()
    path = None if row.log_path is None else Path(row.log_path)
    relative = None
    if path is not None and ".." not in path.parts:
        with suppress(ValueError):
            relative = path.absolute().relative_to(config.log_dir.absolute())
    parts = () if relative is None else relative.parts
    scoped = len(parts) == 4 and parts[0] == f"project-{row.project_id}"
    identity = parts[1:] if scoped else parts
    suffix = f".attempt-{row.attempt_number}.log"
    shape = (
        len(identity) == 3
        and is_metadata_code(identity[0])
        and identity[1] == f"run-{row.pipeline_run_id}"
        and identity[2].endswith(suffix)
        and is_metadata_code(identity[2][: -len(suffix)])
    )
    boundary = root / f"project-{row.project_id}" if scoped else root
    contained = False
    if path is not None and shape:
        with suppress(OSError, RuntimeError, ValueError):
            resolved = path.resolve().relative_to(boundary).parts
            contained = (
                len(resolved) == 3
                and is_metadata_code(resolved[0])
                and resolved[1] == identity[1]
                and resolved[2] == identity[2]
            )
    if path is None or not shape or not contained:
        raise ResourceNotFoundError(
            f"attempt_id={row.attempt_id}: LOG_PATH={row.log_path!r}; expected "
            f"{root}/project-{row.project_id}/<pipeline>/run-{row.pipeline_run_id}/"
            f"<task>{suffix} (or the recorded legacy layout without project directory), "
            "with valid codes, no traversal and no symlink escaping its root; "
            "restore the recorded canonical attempt path inside Orchestration.Log_dir"
        )
    return path.absolute()


class CancelWatch:
    """Sets the event that stops a task process when the run is interrupted or cancelled.

    A thread looks every ``cancel_poll_seconds`` whether an operator cancelled the run, and
    passes on the pipeline runner's own ``cancel`` event at once.
    """

    def __init__(
        self, engine: Engine, pipeline_run_id: int, child: ChildOptions, task_id: int | None = None
    ) -> None:
        """Watch ``pipeline_run_id`` at ``child``'s poll interval."""
        self.engine = engine
        self.pipeline_run_id = pipeline_run_id
        self.child = child
        self.task_id = task_id
        self.stop = threading.Event()
        self.done = threading.Event()
        self.thread = threading.Thread(
            target=contextvars.copy_context().run,
            args=(self._watch,),
            name="etl-craft-cancel-watch",
            daemon=True,
        )

    def __enter__(self) -> threading.Event:
        """Start watching; return the event set when the run is cancelled."""
        self.thread.start()
        return self.stop

    def __exit__(self, *exc: object) -> None:
        """Stop watching."""
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
                from etl_craft.execution import control_flow

                try:
                    control_flow.advance(self.engine, self.pipeline_run_id, observed=True)
                    aborted = self.task_id is not None and control_flow.abort_requested(
                        self.engine, self.pipeline_run_id, self.task_id
                    )
                except SQLAlchemyError:
                    logger.exception(
                        "could not inspect controls for run_id=%d", self.pipeline_run_id
                    )
                    continue
                if aborted or run_cancelled(self.engine, self.pipeline_run_id):
                    logger.warning(
                        "stop requested for pipeline_run_id=%d, task_id=%s; "
                        "stopping this task process",
                        self.pipeline_run_id,
                        self.task_id,
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


def record_attempt(
    engine: Engine,
    task_run_id: int,
    pipeline_run_id: int,
    task_code: str,
    result: ChildResult,
    log_path: Path,
    *,
    attempt_id: int,
    owner: str,
) -> TaskOutcome:
    """Record the attempt's outcome, failing it if the task process ended without one.

    A task stopped because its run was cancelled is ``CANCELLED``.
    """
    with engine.begin() as conn:
        transitions.assert_attempt(conn, attempt_id, task_run_id, owner)
        handoff = _handoff_outcome(conn, attempt_id, task_run_id, task_code)
        if handoff is not None:
            return handoff
        recorded = runlog.fetch_task_run_result(conn, task_run_id)
        status = RunStatus(recorded.status)
        message = recorded.error_message
        values = conn.execute(
            statement(conn, "task_run_log"), {"task_run_id": task_run_id}
        ).scalar_one_or_none()
        row = conn.execute(
            statement(conn, "control_task_context"), {"task_run_id": task_run_id}
        ).one()
        fences = controls.blocked(conn, pipeline_run_id)
        aborted = any(fences.get(key) in STOP_ACTIONS for key in (None, row.task_id))
        cancelled = (
            aborted
            or runlog.fetch_pipeline_run_status(conn, pipeline_run_id) == RunStatus.CANCELLED
        )
        if status == RunStatus.IN_PROGRESS and cancelled:
            status = RunStatus.CANCELLED
            message = f"an abort was requested, so the task process {result.describe()}"
            if result.queries_ended:
                attempt_cancellation.acknowledge_cancellation(conn, attempt_id, owner)
            transitions.cancel_attempt(conn, attempt_id, current_actor(), error_message=message)
        elif status == RunStatus.IN_PROGRESS:
            status = RunStatus.FAILED
            message = f"the task process {result.describe()} before recording an outcome"
            if result.timed_out:
                message += (
                    " (set the task's TASK_TIMEOUT_SECONDS, or Orchestration.Task_timeout_seconds,"
                    " to change its time limit)"
                )
            if result.timed_out:
                transitions.time_out_attempt(
                    conn,
                    attempt_id,
                    current_actor(),
                    owner=owner,
                    error_message=message,
                    task_log=result.output_tail,
                )
            else:
                transitions.finish_attempt(
                    conn,
                    attempt_id,
                    AttemptStatus.FAILED,
                    current_actor(),
                    owner=owner,
                    error_message=message,
                    task_log=result.output_tail,
                    exit_code=result.returncode,
                )
            logger.error("%s: %s; see %s", task_code, message, log_path)
        controls.observe_exit(conn, attempt_id, queries_ended=result.queries_ended)
        transitions.set_task_log(
            conn, task_run_id, attempt_id, owner, _task_log(values, result.output_tail)
        )
        lifecycle(conn, "attempt", attempt_id)
    if status == RunStatus.SUCCESS:
        logger.info("%s: SUCCESS", task_code)
        return TaskOutcome(status, f"{task_code}: SUCCESS", task_run_id)
    logger.error("%s: %s — %s (log: %s)", task_code, status, message, log_path)
    return TaskOutcome(status, f"{task_code}: {status} — {message}", task_run_id)


def _task_log(values: str | None, output_tail: str) -> str | None:
    """Combine the task's reported values with the tail of its output."""
    parts = [part for part in (values, output_tail.strip()) if part]
    return "\n\n".join(parts) or None


def attempt_started(engine: Engine, attempt_id: int, owner: str, pid: int) -> None:
    """Record the process started by this attempt's owner."""
    with engine.begin() as conn:
        attempt_starts.ensure_attempt_started(
            conn,
            attempt_id,
            current_actor(),
            owner=owner,
            process=attempt_starts.StartedProcess(
                socket.gethostname(), pid, leases.process_start(pid)
            ),
            completed=True,
        )


def _handoff_outcome(
    conn: Connection, attempt_id: int, task_run_id: int, task_code: str
) -> TaskOutcome | None:
    """Observe a handed-off process without overwriting the controller's task outcome."""
    attempt = conn.execute(
        statement(conn, "trigger_attempt"),
        {"attempt_id": attempt_id, "task_run_id": task_run_id},
    ).one()
    if attempt.status == AttemptStatus.HANDED_OFF:
        lifecycle(conn, "attempt", attempt_id)
        controls.observe_exit(conn, attempt_id, queries_ended=True)
        recorded = runlog.fetch_task_run_result(conn, task_run_id)
        return TaskOutcome(
            RunStatus(recorded.status),
            f"{task_code}: controller owns child wait",
            task_run_id,
            waiting=recorded.status == RunStatus.IN_PROGRESS,
        )
    return None


def refuse_attempt(conn: Connection, attempt_id: int, reason: str, tail_bytes: int) -> None:
    """End only a claimed attempt whose process has not been dispatched."""
    with acting_as(SYSTEM_ACTOR):
        transitions.cancel_attempt(
            conn,
            attempt_id,
            SYSTEM_ACTOR,
            error_message=reason,
            task_log=byte_tail(reason, tail_bytes),
        )
    logger.error("attempt_id=%s: %s", attempt_id, reason)
