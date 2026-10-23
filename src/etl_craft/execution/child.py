"""The process one task attempt runs in: ``python -m etl_craft.execution.child``.

``run --task-code`` starts it with the config file and the ``task_run_id`` of the attempt, and
everything it writes (its own log records and the handler's output) is captured into the
attempt's log file. It runs the task's handler and records the outcome on the task's row. If it
dies before recording anything, the process that started it records the task ``FAILED``.

As a process (``run_as_process``) it turns SIGTERM into ``KeyboardInterrupt``, so what the
handler wrote is flushed before it is stopped, and it exits as soon as the outcome is recorded,
without waiting for threads a script left running.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import socket
import sys
import threading
from collections.abc import Callable, Sequence
from contextlib import suppress
from contextvars import ContextVar
from dataclasses import replace
from pathlib import Path
from types import FrameType
from typing import NoReturn

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig, load_config
from etl_craft.core import log
from etl_craft.core.actor import SYSTEM_ACTOR, acting_as, current_actor
from etl_craft.core.enums import AttemptStatus, ControlAcknowledgement, Handler, RunStatus
from etl_craft.core.errors import EtlCraftError, ExitCode
from etl_craft.core.faults import fault_point
from etl_craft.core.ownership import attempt_holder
from etl_craft.engine import attempt_starts, controls, transitions
from etl_craft.engine.connection import engine_db
from etl_craft.engine.resource_locks import attempt_scope, holder_tag
from etl_craft.engine.transitions import finish_task_run
from etl_craft.execution.classify import classify_failure
from etl_craft.execution.context import build_task_context
from etl_craft.execution.control_flow import process_local_queries
from etl_craft.execution.leases import process_start
from etl_craft.handlers.registry import TaskContext, format_task_log, resolve_handler
from etl_craft.warehouse.fencing import cancellation_scope, stop_current_queries

logger = logging.getLogger(__name__)

_identity: ContextVar[tuple[int, str] | None] = ContextVar("attempt_identity", default=None)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the task process's arguments."""
    parser = argparse.ArgumentParser(prog="etl_craft.execution.child")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--task-run-id", type=int, required=True)
    parser.add_argument("--attempt-id", type=int)
    parser.add_argument("--owner-id")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--rerun", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--log-format", default="text")
    args = parser.parse_args(argv)
    if (args.attempt_id is None) != (args.owner_id is None):
        parser.error("--attempt-id and --owner-id must be supplied together")
    return args


def main(
    argv: Sequence[str] | None = None,
    *,
    trigger: Callable[[Engine, TaskContext], int] | None = None,
) -> int:
    """Run one task attempt; return 0 on success, else the exit status of what failed."""
    args = parse_args(argv)
    log.configure(args.log_level, args.log_format)
    config = load_config(args.config)
    engine = engine_db(config, supervised=True)
    attempt_id = args.attempt_id or os.environ.get("ETL_CRAFT_ATTEMPT_ID")
    owner = args.owner_id or os.environ.get("ETL_CRAFT_ATTEMPT_OWNER")
    identity = (int(attempt_id), owner) if attempt_id is not None and owner else None
    token = _identity.set(identity)
    try:
        with acting_as(SYSTEM_ACTOR):
            return _run_bound(engine, config, args, trigger=trigger)
    finally:
        _identity.reset(token)
        engine.dispose()


def _run_bound(
    engine: Engine,
    config: ConnectorConfig,
    args: argparse.Namespace,
    *,
    trigger: Callable[[Engine, TaskContext], int] | None = None,
) -> int:
    with log.log_context(task_run_id=args.task_run_id):
        identity = _identity.get()
        if identity is not None:
            try:
                with engine.begin() as conn:
                    row = transitions.assert_attempt(
                        conn, identity[0], args.task_run_id, identity[1]
                    )
                    started = process_start(os.getpid())
                    if row.status != AttemptStatus.CLAIMED:
                        # The supervisor acknowledged this process's start first.
                        attempt_starts.assert_started(
                            conn, row, owner=identity[1], pid=os.getpid(), process_start=started
                        )
                    else:
                        attempt_starts.ensure_attempt_started(
                            conn,
                            identity[0],
                            current_actor(),
                            owner=identity[1],
                            process=attempt_starts.StartedProcess(
                                socket.gethostname(), os.getpid(), started
                            ),
                        )
            except EtlCraftError as error:
                logger.error("could not start the attempt: %s", error)
                return error.exit_code
        try:
            context = build_task_context(
                engine, config, args.task_run_id, force=args.force, rerun=args.rerun
            )
        except EtlCraftError as error:
            logger.error("could not start the task: %s", error)
            _record_failure(engine, args.task_run_id, error, config)
            return error.exit_code
        with log.log_context(
            pipeline=context.pipeline_code,
            task=context.task_code,
            pipeline_run_id=context.pipeline_run_id,
            attempt=context.attempt,
        ):
            try:
                return run_handler(engine, context, trigger=trigger)
            except EtlCraftError as error:
                logger.error("attempt outcome refused: %s", error)
                return error.exit_code


def run_handler(
    engine: Engine,
    context: TaskContext,
    *,
    trigger: Callable[[Engine, TaskContext], int] | None = None,
) -> int:
    """Run ``context``'s handler and record its outcome on the task's row."""
    identity = _identity.get()
    if identity is not None:
        context = replace(context, attempt_id=identity[0], owner=identity[1])
    logger.info("running the %s handler", context.handler)
    try:
        if context.handler == Handler.RUN_PIPELINE and trigger is not None:
            return trigger(engine, context)
        if identity is None:
            result = resolve_handler(context.handler)(context, engine)
        else:
            with (
                attempt_scope(*identity),
                cancellation_scope(context.config, holder_tag(attempt_holder(*identity))),
            ):
                result = resolve_handler(context.handler)(context, engine)
    except EtlCraftError as error:
        logger.error("task failed: %s", error)
        if context.handler != Handler.SQL_SCRIPT:
            logger.debug("traceback", exc_info=True)
        _record_failure(
            engine,
            context.task_run_id,
            error,
            context.config,
            script=context.handler == Handler.PYTHON,
        )
        return error.exit_code
    except Exception as error:
        logger.exception("task failed with an unexpected %s", type(error).__name__)
        _record_failure(
            engine,
            context.task_run_id,
            error,
            context.config,
            script=context.handler == Handler.PYTHON,
        )
        return ExitCode.UNEXPECTED
    with engine.begin() as conn:
        finish_task_run(
            conn,
            context.task_run_id,
            status=RunStatus.SUCCESS,
            counts=result,
            offset=result.offset,
            task_log=format_task_log(result),
            attempt_id=None if identity is None else identity[0],
            owner=None if identity is None else identity[1],
        )
        if identity is not None and (
            context.handler != Handler.PYTHON or process_local_queries(context.config)
        ):
            controls.acknowledge(conn, identity[0], ControlAcknowledgement.QUERIES)
    fault_point("child.after_outcome")
    logger.info(
        "task succeeded: source %s, target %s, inserted %s, updated %s, deleted %s, rejected %s",
        result.source_count,
        result.target_count,
        result.insert_count,
        result.update_count,
        result.delete_count,
        result.reject_count,
    )
    return ExitCode.SUCCESS


def _record_failure(
    engine: Engine,
    task_run_id: int,
    error: BaseException,
    config: ConnectorConfig,
    *,
    script: bool = False,
) -> None:
    identity = _identity.get()
    failure = classify_failure(error, config, script=script)
    with engine.begin() as conn:
        if identity is not None:
            transitions.record_attempt_failure(conn, identity[0], failure)
        finish_task_run(
            conn,
            task_run_id,
            status=RunStatus.FAILED,
            error_message=str(error)
            if isinstance(error, EtlCraftError)
            else f"{type(error).__name__}: {error}",
            attempt_id=None if identity is None else identity[0],
            owner=None if identity is None else identity[1],
        )
        if identity is not None:
            logger.error(
                "%s: %s %s; recorded RETRY_ON policy determines retry eligibility",
                failure.kind,
                failure.source,
                failure.code or "",
            )


def run_as_process(
    argv: Sequence[str] | None = None,
    *,
    trigger: Callable[[Engine, TaskContext], int] | None = None,
) -> NoReturn:
    """Run ``main`` as the task process and exit with its status as soon as it returns."""
    signal.signal(signal.SIGTERM, _interrupt)
    exit_now(main(argv, trigger=trigger))


def exit_now(code: int) -> NoReturn:
    """Flush the output and logs, and exit with ``code`` without waiting for other threads.

    The outcome is recorded by then, so a thread a script left running (a client's pool, say)
    only held the task's slot. It is named in the log, then ends with the process.
    """
    lingering = [
        thread.name
        for thread in threading.enumerate()
        if thread is not threading.main_thread() and thread.is_alive() and not thread.daemon
    ]
    if lingering:
        logger.warning(
            "exiting with %d thread(s) still running, which end with the task process: %s",
            len(lingering),
            ", ".join(lingering),
        )
    _flush()
    logging.shutdown()
    os._exit(code)


def _interrupt(signum: int, frame: FrameType | None) -> None:
    stop_current_queries()
    _flush()
    raise KeyboardInterrupt(f"stopped by {signal.Signals(signum).name}")


def _flush() -> None:
    for stream in (sys.stdout, sys.stderr):
        with suppress(OSError, ValueError):
            stream.flush()


if __name__ == "__main__":  # pragma: no cover - run as a separate process
    run_as_process()
