"""``etl-craft run``: run a pipeline, one task of it, or initialize its run."""

from __future__ import annotations

import argparse
import signal
from datetime import date

from etl_craft.cli.commands import control_requests
from etl_craft.cli.commands.common import (
    Command,
    call,
    configure_output,
    configure_run_selector,
    request_from,
)
from etl_craft.cli.output import Output
from etl_craft.core import interrupts
from etl_craft.core.enums import ActionName, RunStatus
from etl_craft.core.errors import ExitCode, UsageError
from etl_craft.services.operations.models import BackfillView, OperationResult
from etl_craft.services.operations.requests import RunRequest


def _configure(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    configure_run_selector(parser)
    parser.add_argument("--pipeline-code", help="The pipeline to run.")
    control_requests.configure(parser)
    step = parser.add_mutually_exclusive_group()
    step.add_argument("--task-code", help="Run only this task, under the selected pipeline run.")
    step.add_argument(
        "--init-only",
        action="store_true",
        help="Test connections, check the pipeline's dependencies and start its run; the "
        "controller picks up the run.",
    )
    parser.add_argument(
        "--standalone",
        action="store_true",
        help="With --task-code: create or explicitly resume an independent manual run.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Run even if tasks already succeeded or their dependencies are not met; on the "
        "controller's host only.",
    )
    parser.add_argument(
        "--ignore-dependencies",
        action="store_true",
        help="With --task-code: run the task without checking its dependencies; "
        "with --reason, recorded.",
    )
    parser.add_argument(
        "--rerun",
        action="store_true",
        help="With --task-code: run the task again although it already succeeded, reopening "
        "its run if it ended; with --reason, recorded.",
    )
    parser.add_argument(
        "--with-downstream",
        action="store_true",
        help="With --rerun or --standalone: include downstream tasks in dependency order.",
    )
    parser.add_argument(
        "--skip",
        action="store_true",
        help="Record a run SKIPPED on purpose, running nothing; with --reason.",
    )
    parser.add_argument(
        "--run-date",
        type=_date,
        metavar="YYYY-MM-DD",
        help="The date a new run runs as of (SQL's $$run_date); today unless given.",
    )
    parser.add_argument(
        "--backfill",
        type=_date_range,
        metavar="FROM:TO",
        help="Run the pipeline once for each date from FROM to TO (YYYY-MM-DD), as backfill "
        "runs; with --reason.",
    )
    parser.add_argument(
        "--reason", help="Why an override, rerun, skip or backfill is needed; recorded."
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    if control_requests.selected(args):
        return control_requests.execute(args, out)
    if args.pipeline_code is None:
        raise UsageError("run needs `pipeline_code` or `controls_file`")
    if args.request_key is not None:
        raise UsageError("`request_key` goes with `recover`, `restart` or `allow_paused`")
    first, last = args.backfill or (None, None)
    request = request_from(args, RunRequest, backfill_from=first, backfill_to=last)
    # Ctrl-C, SIGTERM and SIGHUP stop the run at its next safe point: its task processes are
    # stopped and their attempts recorded. SIGKILL cannot be caught: its task processes are left
    # running, their rows IN-PROGRESS until their leases expire and reconciliation releases them.
    stopping = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
    with interrupts.deferred(*stopping):
        done = call(args, ActionName.RUN, request, role="run")
    assert isinstance(done, (OperationResult, BackfillView))
    out.result(done, args.output_format)
    started_only = args.init_only or (args.api and not (args.task_code or args.backfill))
    return _exit_code(done, started_only=bool(started_only))


def _exit_code(done: OperationResult | BackfillView, *, started_only: bool) -> int:
    """Exit with the run's outcome; a run only started is a success while it is in progress."""
    if done.status in (RunStatus.FAILED, RunStatus.CANCELLED):
        return ExitCode.FAILURE
    if isinstance(done, BackfillView):
        return ExitCode.INCOMPLETE if done.stopped is not None else ExitCode.SUCCESS
    if done.waiting:
        return ExitCode.WAITING
    if done.status == RunStatus.IN_PROGRESS and not started_only:
        return ExitCode.INCOMPLETE
    return ExitCode.SUCCESS


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


COMMAND = Command(
    name="run",
    help="Run a pipeline in dependency waves, one task of it, or initialize its run.",
    configure=_configure,
    run=_run,
    actions=(ActionName.RUN, ActionName.CONTROL, ActionName.CONTROL_BULK),
)
