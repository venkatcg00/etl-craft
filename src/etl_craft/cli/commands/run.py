"""``etl-craft run``: run a pipeline, one task of it, or initialize its run."""

from __future__ import annotations

import argparse
import signal
from datetime import date

from etl_craft.cli.commands import control_requests
from etl_craft.cli.commands.common import (
    Command,
    command_context,
    configure_output,
    configure_run_selector,
)
from etl_craft.cli.output import Output
from etl_craft.cli.remote import ServerClient, api_path, selector_query
from etl_craft.core import interrupts
from etl_craft.core.enums import RunStatus
from etl_craft.core.errors import ExitCode, UsageError
from etl_craft.engine.runlog import RunSelector
from etl_craft.services.operations import runs
from etl_craft.services.operations.models import BackfillView, OperationResult
from etl_craft.services.operations.requests import RunRequest


def _configure(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    configure_run_selector(parser)
    parser.add_argument("--pipeline_code", help="the pipeline to run")
    control_requests.configure(parser)
    step = parser.add_mutually_exclusive_group()
    step.add_argument("--task_code", help="run only this task, under the selected pipeline run")
    step.add_argument(
        "--init-only",
        action="store_true",
        help="test connections, check the pipeline's dependencies and start its run; the "
        "controller picks up the run",
    )
    parser.add_argument(
        "--standalone",
        action="store_true",
        help="with --task_code: create or explicitly resume an independent manual run",
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
        help="with --task_code: run the task without checking its dependencies; "
        "with --reason, recorded",
    )
    parser.add_argument(
        "--rerun",
        action="store_true",
        help="with --task_code: run the task again although it already succeeded, reopening "
        "its run if it ended; with --reason, recorded",
    )
    parser.add_argument(
        "--with-downstream",
        action="store_true",
        help="with --rerun or --standalone: include downstream tasks in dependency order",
    )
    parser.add_argument(
        "--skip",
        action="store_true",
        help="record a run SKIPPED on purpose, running nothing; with --reason",
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
        "runs; with --reason",
    )
    parser.add_argument(
        "--reason", help="remote action context, or why a local intervention is needed"
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    if control_requests.selected(args):
        return control_requests.execute(args, out)
    _ordinary(args)
    # Ctrl-C, SIGTERM and SIGHUP stop the run at its next safe point: its task processes are
    # stopped and their attempts recorded. SIGKILL cannot be caught: its task processes are left
    # running, their rows IN-PROGRESS until their leases expire and reconciliation releases them.
    stopping = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
    with command_context(args, role="run") as ctx, interrupts.deferred(*stopping):
        request = _request(args, args.reason)
        done = runs.execute_run(ctx, request)
    out.result(done, args.output_format)
    return _exit_code(done, started_only=args.init_only)


def _request(args: argparse.Namespace, reason: str | None) -> RunRequest:
    """Parse execution flags once for local and server-backed task requests."""
    return RunRequest(
        args.pipeline_code,
        selector=RunSelector(args.run_id, args.run_key),
        task_code=args.task_code,
        standalone=args.standalone,
        init_only=args.init_only,
        force=args.force,
        ignore_dependencies=args.ignore_dependencies,
        rerun=args.rerun,
        with_downstream=args.with_downstream,
        skip=args.skip,
        run_date=args.run_date,
        backfill=args.backfill,
        reason=reason,
    )


def _remote(args: argparse.Namespace, out: Output, server: ServerClient) -> int:
    if control_requests.selected(args):
        return control_requests.execute(args, out, server)
    _ordinary(args)
    if args.standalone:
        _request(args, args.reason)
    local_only = [
        flag
        for flag, given in (
            ("--init-only", args.init_only),
            ("--force", args.force),
            ("--ignore-dependencies", args.ignore_dependencies and not args.standalone),
            ("--skip", args.skip),
        )
        if given
    ]
    if local_only:
        raise UsageError(
            f"run {', '.join(local_only)}: works on the server's host only; against a server, "
            "`run` starts a run, a backfill (--backfill) or a rerun (--rerun --task_code)"
        )
    done: OperationResult | BackfillView
    if args.backfill is not None:
        first, last = args.backfill
        done = server.call(
            "POST",
            api_path("pipelines", args.pipeline_code, "backfills"),
            BackfillView,
            body={"first": first.isoformat(), "last": last.isoformat(), "reason": args.reason},
        )
    elif args.standalone or (args.task_code is not None and not args.rerun):
        done = server.call(
            "POST",
            api_path("pipelines", args.pipeline_code, "tasks", args.task_code, "run"),
            OperationResult,
            body={
                "standalone": args.standalone,
                "reason": args.reason,
                "with_downstream": args.with_downstream,
                "ignore_dependencies": args.ignore_dependencies,
                "run_date": None if args.run_date is None else args.run_date.isoformat(),
                **selector_query(args),
            },
        )
    elif args.rerun:
        done = server.call(
            "POST",
            api_path("pipelines", args.pipeline_code, "tasks", args.task_code, "rerun"),
            OperationResult,
            body={
                "reason": args.reason,
                "with_downstream": args.with_downstream,
                **selector_query(args),
            },
        )
    else:
        if args.run_id is not None or args.run_key is not None:
            raise UsageError("run against a server starts a new run; do not pass a run selector")
        done = server.call(
            "POST",
            api_path("pipelines", args.pipeline_code, "runs"),
            OperationResult,
            body={
                "run_date": None if args.run_date is None else args.run_date.isoformat(),
                "reason": args.reason,
            },
        )
    out.result(done, args.output_format)
    return _exit_code(done, started_only=args.backfill is None and args.task_code is None)


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
    remote=_remote,
)


def _ordinary(args: argparse.Namespace) -> None:
    """Keep ordinary execution validation independent of preparatory controls."""
    if args.pipeline_code is None:
        raise UsageError("run needs --pipeline_code or --controls-file")
    if args.request_key is not None:
        raise UsageError("--request-key goes with --recover, --restart or --allow-paused")
