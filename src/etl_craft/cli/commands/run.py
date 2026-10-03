"""``etl-craft run``: run a pipeline, one task of it, or its first or last step."""

from __future__ import annotations

import argparse
import signal
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from types import FrameType

from sqlalchemy.engine import Engine

from etl_craft.cli.commands import Command
from etl_craft.cli.commands.common import connect_engine_db, load_command_config
from etl_craft.cli.output import Output
from etl_craft.config import ConnectorConfig
from etl_craft.core.enums import RunStatus
from etl_craft.core.errors import ExitCode, UsageError
from etl_craft.execution.interventions import skip_run
from etl_craft.execution.pipeline import (
    backfill,
    finalize_active_run,
    force_task,
    init_pipeline_run,
    rerun_task,
    run_pipeline,
)
from etl_craft.execution.runner import ChildOptions, Override, run_task
from etl_craft.services.cloning import run_hooks


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pipeline_code", required=True, help="the pipeline to run")
    step = parser.add_mutually_exclusive_group()
    step.add_argument("--task_code", help="run only this task, under the pipeline's active run")
    step.add_argument(
        "--init-only",
        action="store_true",
        help="test connections, check the pipeline's dependencies and start its run; an "
        "orchestrator's first step",
    )
    step.add_argument(
        "--finalize-only",
        action="store_true",
        help="end the pipeline's active run from its tasks' statuses; an orchestrator's last step",
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
    if args.force and (args.init_only or args.finalize_only):
        raise UsageError("--force runs tasks; it does not apply to --init-only or --finalize-only")
    if (args.ignore_dependencies or args.rerun) and not args.task_code:
        raise UsageError("--ignore-dependencies and --rerun apply to one task: pass --task_code")
    if args.ignore_dependencies and args.rerun:
        raise UsageError("--rerun already runs the task without checking its dependencies")
    if args.force and (args.ignore_dependencies or args.rerun):
        option = "--rerun" if args.rerun else "--ignore-dependencies"
        raise UsageError(f"{option} and --force are different overrides: choose one")
    if args.with_downstream and not args.rerun:
        raise UsageError("--with-downstream goes with --rerun")
    if args.skip and (args.task_code or args.init_only or args.finalize_only or args.force):
        raise UsageError("--skip records a whole run SKIPPED; it takes only --reason")
    if args.reason and not (args.ignore_dependencies or args.rerun or args.skip or args.backfill):
        raise UsageError("--reason goes with --ignore-dependencies, --rerun, --skip or --backfill")
    if args.backfill and (
        args.task_code or args.init_only or args.finalize_only or args.force or args.skip
    ):
        raise UsageError("--backfill runs the whole pipeline once per date; it takes only --reason")
    if args.run_date and (args.task_code or args.finalize_only or args.skip or args.backfill):
        raise UsageError("--run-date starts a run: it goes with a whole run or --init-only")
    config = load_command_config(args)
    engine = connect_engine_db(config)
    child = ChildOptions(log_level=args.log_level, log_format=args.log_format)
    try:
        with _terminate_as_interrupt():
            status, message = _dispatch(args, config, engine, child)
    finally:
        engine.dispose()
    out.line(message)
    if status in (RunStatus.FAILED, RunStatus.CANCELLED):
        return ExitCode.FAILURE
    return ExitCode.SUCCESS


def _dispatch(
    args: argparse.Namespace, config: ConnectorConfig, engine: Engine, child: ChildOptions
) -> tuple[RunStatus, str]:
    """Do what ``args`` asks and return how it ended."""
    if args.backfill:
        first, last = args.backfill
        done = backfill(
            engine,
            config,
            args.pipeline_code,
            first,
            last,
            args.reason or "",
            child=child,
            hooks=run_hooks(config, engine),
        )
        return done.status, done.message
    elif args.skip:
        skipped = skip_run(engine, config, args.pipeline_code, args.reason or "")
        return RunStatus.SKIPPED, skipped.message
    elif args.rerun:
        rerun = rerun_task(
            engine,
            config,
            args.pipeline_code,
            args.task_code,
            args.reason or "",
            with_downstream=args.with_downstream,
            child=child,
            hooks=run_hooks(config, engine),
        )
        return rerun.status, rerun.message
    elif args.task_code and args.force:
        forced = force_task(
            engine,
            config,
            args.pipeline_code,
            args.task_code,
            child=child,
            hooks=run_hooks(config, engine),
        )
        return forced.status, forced.message
    elif args.task_code:
        override = Override(args.reason or "") if args.ignore_dependencies else None
        outcome = run_task(
            engine,
            config,
            args.pipeline_code,
            args.task_code,
            force=args.force,
            child=child,
            override=override,
        )
        return outcome.status, outcome.message
    elif args.init_only:
        started = init_pipeline_run(
            engine,
            config,
            args.pipeline_code,
            hooks=run_hooks(config, engine),
            run_date=args.run_date,
        )
        return started.status, started.message
    elif args.finalize_only:
        ended = finalize_active_run(
            engine, config, args.pipeline_code, hooks=run_hooks(config, engine)
        )
        return ended.status, ended.message
    else:
        ran = run_pipeline(
            engine,
            config,
            args.pipeline_code,
            force=args.force,
            child=child,
            hooks=run_hooks(config, engine),
            run_date=args.run_date,
        )
        return ran.status, ran.message


@contextmanager
def _terminate_as_interrupt() -> Iterator[None]:
    """Treat SIGTERM and SIGHUP like Ctrl-C, on every kind of run.

    The task processes the command started are then stopped and their attempts recorded. A
    SIGKILL cannot be caught: its task processes are left running, and their rows
    ``IN-PROGRESS`` until ``mark --stale`` releases them.
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
