"""``etl-craft run``: run a pipeline, one task of it, or its first or last step."""

from __future__ import annotations

import argparse
import signal
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from types import FrameType

from etl_craft.cli.commands import Command
from etl_craft.cli.commands.common import connect_engine_db, load_command_config
from etl_craft.cli.output import Output
from etl_craft.core.enums import RunStatus
from etl_craft.core.errors import ExitCode, UsageError
from etl_craft.execution.pipeline import (
    finalize_active_run,
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
    parser.add_argument("--reason", help="why, for --ignore-dependencies or --rerun")


def _run(args: argparse.Namespace, out: Output) -> int:
    if args.force and (args.init_only or args.finalize_only):
        raise UsageError("--force runs tasks; it does not apply to --init-only or --finalize-only")
    if (args.ignore_dependencies or args.rerun) and not args.task_code:
        raise UsageError("--ignore-dependencies and --rerun apply to one task: pass --task_code")
    if args.ignore_dependencies and args.rerun:
        raise UsageError("--rerun already runs the task without checking its dependencies")
    if args.with_downstream and not args.rerun:
        raise UsageError("--with-downstream goes with --rerun")
    if args.reason and not (args.ignore_dependencies or args.rerun):
        raise UsageError("--reason goes with --ignore-dependencies or --rerun")
    config = load_command_config(args)
    engine = connect_engine_db(config)
    child = ChildOptions(log_level=args.log_level, log_format=args.log_format)
    try:
        if args.rerun:
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
            status, message = rerun.status, rerun.message
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
            status, message = outcome.status, outcome.message
        elif args.init_only:
            started = init_pipeline_run(
                engine, config, args.pipeline_code, hooks=run_hooks(config, engine)
            )
            status, message = started.status, started.message
        elif args.finalize_only:
            ended = finalize_active_run(
                engine, config, args.pipeline_code, hooks=run_hooks(config, engine)
            )
            status, message = ended.status, ended.message
        else:
            with _terminate_as_interrupt():
                ran = run_pipeline(
                    engine,
                    config,
                    args.pipeline_code,
                    force=args.force,
                    child=child,
                    hooks=run_hooks(config, engine),
                )
            status, message = ran.status, ran.message
    finally:
        engine.dispose()
    out.line(message)
    if status in (RunStatus.FAILED, RunStatus.CANCELLED):
        return ExitCode.FAILURE
    return ExitCode.SUCCESS


@contextmanager
def _terminate_as_interrupt() -> Iterator[None]:
    """Treat SIGTERM like Ctrl-C while a whole pipeline runs, so its tasks are stopped too."""
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    previous = signal.signal(signal.SIGTERM, _raise_interrupt)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous)


def _raise_interrupt(signum: int, frame: FrameType | None) -> None:
    raise KeyboardInterrupt


COMMAND = Command(
    name="run",
    help="Run a pipeline in dependency waves, one task of it, or its first or last step.",
    configure=_configure,
    run=_run,
)
