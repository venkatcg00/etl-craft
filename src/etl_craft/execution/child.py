"""The process one task attempt runs in: ``python -m etl_craft.execution.child``.

``run --task_code`` starts it with the config file and the ``task_run_id`` of the attempt, and
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
import sys
import threading
from collections.abc import Sequence
from contextlib import suppress
from pathlib import Path
from types import FrameType
from typing import NoReturn

from sqlalchemy.engine import Engine

from etl_craft.config import load_config
from etl_craft.core import log
from etl_craft.core.enums import RunStatus
from etl_craft.core.errors import EtlCraftError, ExitCode
from etl_craft.core.faults import fault_point
from etl_craft.engine.connection import engine_db
from etl_craft.engine.runlog import finish_task_run
from etl_craft.execution.context import build_task_context
from etl_craft.handlers.registry import TaskContext, format_task_log, resolve_handler

logger = logging.getLogger(__name__)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the task process's arguments."""
    parser = argparse.ArgumentParser(prog="etl_craft.execution.child")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--task-run-id", type=int, required=True)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--rerun", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--log-format", default="text")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run one task attempt; return 0 on success, else the exit status of what failed."""
    args = parse_args(argv)
    log.configure(args.log_level, args.log_format)
    config = load_config(args.config)
    engine = engine_db(config)
    try:
        with log.log_context(task_run_id=args.task_run_id):
            try:
                context = build_task_context(
                    engine, config, args.task_run_id, force=args.force, rerun=args.rerun
                )
            except EtlCraftError as error:
                logger.error("could not start the task: %s", error)
                _record_failure(engine, args.task_run_id, str(error))
                return error.exit_code
            with log.log_context(
                pipeline=context.pipeline_code,
                task=context.task_code,
                pipeline_run_id=context.pipeline_run_id,
                attempt=context.attempt,
            ):
                return run_handler(engine, context)
    finally:
        engine.dispose()


def run_handler(engine: Engine, context: TaskContext) -> int:
    """Run ``context``'s handler and record its outcome on the task's row."""
    logger.info("running the %s handler", context.handler)
    try:
        result = resolve_handler(context.handler)(context, engine)
    except EtlCraftError as error:
        logger.error("task failed: %s", error)
        logger.debug("traceback", exc_info=True)
        _record_failure(engine, context.task_run_id, str(error))
        return error.exit_code
    except Exception as error:
        logger.exception("task failed with an unexpected %s", type(error).__name__)
        _record_failure(engine, context.task_run_id, f"{type(error).__name__}: {error}")
        return ExitCode.UNEXPECTED
    with engine.begin() as conn:
        finish_task_run(
            conn,
            context.task_run_id,
            status=RunStatus.SUCCESS,
            source_count=result.source_count,
            target_count=result.target_count,
            insert_count=result.insert_count,
            update_count=result.update_count,
            delete_count=result.delete_count,
            rows_written=result.rows_written,
            task_log=format_task_log(result),
        )
    fault_point("child.after_outcome")
    logger.info(
        "task succeeded: source %s, target %s, inserted %s, updated %s, deleted %s",
        result.source_count,
        result.target_count,
        result.insert_count,
        result.update_count,
        result.delete_count,
    )
    return ExitCode.SUCCESS


def _record_failure(engine: Engine, task_run_id: int, message: str) -> None:
    with engine.begin() as conn:
        finish_task_run(conn, task_run_id, status=RunStatus.FAILED, error_message=message)


def run_as_process(argv: Sequence[str] | None = None) -> NoReturn:
    """Run ``main`` as the task process and exit with its status as soon as it returns."""
    signal.signal(signal.SIGTERM, _interrupt)
    exit_now(main(argv))


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
    _flush()
    raise KeyboardInterrupt(f"stopped by {signal.Signals(signum).name}")


def _flush() -> None:
    for stream in (sys.stdout, sys.stderr):
        with suppress(OSError, ValueError):
            stream.flush()


if __name__ == "__main__":  # pragma: no cover - run as a separate process
    run_as_process()
