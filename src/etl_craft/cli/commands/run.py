"""``etl-craft run``: run a pipeline, one task of it, or its first or last step."""

from __future__ import annotations

import argparse
import signal
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from types import FrameType

from etl_craft.cli.commands import Command
from etl_craft.cli.commands.common import (
    command_context,
    configure_output,
    configure_run_selector,
)
from etl_craft.cli.output import Output
from etl_craft.core.enums import RunStatus
from etl_craft.core.errors import ExitCode
from etl_craft.engine.runlog import RunSelector
from etl_craft.services.operations import PipelineRef, runs
from etl_craft.services.operations.requests import RunRequest


def _configure(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    configure_run_selector(parser)
    parser.add_argument("--pipeline_code", required=True, help="the pipeline to run")
    step = parser.add_mutually_exclusive_group()
    step.add_argument("--task_code", help="run only this task, under the selected pipeline run")
    step.add_argument(
        "--init-only",
        action="store_true",
        help="test connections, check the pipeline's dependencies and start its run; an "
        "orchestrator's first step",
    )
    step.add_argument(
        "--finalize-only",
        action="store_true",
        help="end the selected pipeline run from its tasks' statuses; an orchestrator's last step",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="run even if tasks already succeeded or their dependencies are not met; local "
        "mode only",
    )
    parser.add_argument(
        "--ignore-dependencies",
        action="store_true",
        help="with --task_code: run the task without checking its dependencies; local mode "
        "only, with --reason, recorded",
    )
    parser.add_argument(
        "--rerun",
        action="store_true",
        help="with --task_code: run the task again although it already succeeded, reopening "
        "its run if it ended; local mode only, with --reason, recorded",
    )
    parser.add_argument(
        "--with-downstream",
        action="store_true",
        help="with --rerun: run every task after it again too, in dependency order",
    )
    parser.add_argument(
        "--skip",
        action="store_true",
        help="record a run SKIPPED on purpose, running nothing; local mode only, with --reason",
    )
    parser.add_argument(
        "--run-date",
        type=_date,
        metavar="YYYY-MM-DD",
        help="the date a new run runs as of (SQL's $$run_date); today unless given",
    )
    parser.add_argument(
        "--backfill",
        type=_date_range,
        metavar="FROM:TO",
        help="run the pipeline once for each date from FROM to TO (YYYY-MM-DD), as backfill "
        "runs; local mode only, with --reason",
    )
    parser.add_argument(
        "--reason", help="why, for --ignore-dependencies, --rerun, --skip or --backfill"
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    request = RunRequest(
        PipelineRef(args.pipeline_code),
        selector=RunSelector(args.run_id, args.run_key),
        task_code=args.task_code,
        init_only=args.init_only,
        finalize_only=args.finalize_only,
        force=args.force,
        ignore_dependencies=args.ignore_dependencies,
        rerun=args.rerun,
        with_downstream=args.with_downstream,
        skip=args.skip,
        run_date=args.run_date,
        backfill=args.backfill,
        reason=args.reason,
    )
    with command_context(args) as ctx, _terminate_as_interrupt():
        done = runs.execute_run(ctx, request)
    if args.output_format == "json":
        out.document(done)
    else:
        out.line(done.message)
    return (
        ExitCode.FAILURE
        if done.status in (RunStatus.FAILED, RunStatus.CANCELLED)
        else ExitCode.SUCCESS
    )


@contextmanager
def _terminate_as_interrupt() -> Iterator[None]:
    """Treat SIGTERM and SIGHUP like Ctrl-C, on every kind of run.

    The task processes the command started are then stopped and their attempts recorded. A
    SIGKILL cannot be caught: its task processes are left running, and their rows
    ``IN-PROGRESS`` until their leases expire and reconciliation releases them.
    """
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    caught = [signal.SIGTERM] + ([signal.SIGHUP] if hasattr(signal, "SIGHUP") else [])
    previous = {sig: signal.signal(sig, _raise_interrupt) for sig in caught}
    try:
        yield
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def _date(text: str) -> date:
    try:
        return date.fromisoformat(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{text!r} is not a date: write YYYY-MM-DD") from None


def _date_range(text: str) -> tuple[date, date]:
    first, separator, last = text.partition(":")
    if not separator:
        raise argparse.ArgumentTypeError(f"{text!r} is not FROM:TO, such as 2026-09-01:2026-09-07")
    return _date(first), _date(last)


def _raise_interrupt(signum: int, frame: FrameType | None) -> None:
    raise KeyboardInterrupt(signal.Signals(signum).name)


COMMAND = Command(
    name="run",
    help="Run a pipeline in dependency waves, one task of it, or its first or last step.",
    configure=_configure,
    run=_run,
)
