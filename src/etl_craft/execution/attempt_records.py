"""Attempt bookkeeping: log paths, the cancellation watch, recorded outcomes and starts."""

from __future__ import annotations

import contextvars
import logging
import socket
import threading
from pathlib import Path

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.actor import current_actor
from etl_craft.core.enums import (
    AttemptStatus,
    RunStatus,
)
from etl_craft.engine import runlog, transitions
from etl_craft.engine.queries import statement
from etl_craft.execution import leases
from etl_craft.execution.attempt_types import ChildOptions, TaskOutcome
from etl_craft.execution.supervisor import (
    ChildResult,
)

logger = logging.getLogger(__name__)


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


class CancelWatch:
    """Sets the event that stops a task process when the run is interrupted or cancelled.

    A thread looks every ``cancel_poll_seconds`` whether an operator cancelled the run, and
    passes on the pipeline runner's own ``cancel`` event at once.
    """

    def __init__(self, engine: Engine, pipeline_run_id: int, child: ChildOptions) -> None:
        """Watch ``pipeline_run_id`` at ``child``'s poll interval."""
        self.engine = engine
        self.pipeline_run_id = pipeline_run_id
        self.child = child
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
        transitions.set_task_log(
            conn, task_run_id, attempt_id, owner, _task_log(values, result.output_tail)
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


def attempt_started(engine: Engine, attempt_id: int, owner: str, pid: int) -> None:
    """Record the process started by this attempt's owner."""
    with engine.begin() as conn:
        transitions.ensure_attempt_started(
            conn,
            attempt_id,
            current_actor(),
            owner=owner,
            host=socket.gethostname(),
            pid=pid,
            completed=True,
            process_start=leases.process_start(pid),
        )
