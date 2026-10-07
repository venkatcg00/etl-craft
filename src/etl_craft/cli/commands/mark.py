"""``etl-craft mark``: set a task or a run's status by hand, with a reason; local mode."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands import Command
from etl_craft.cli.commands.common import (
    command_context,
    configure_output,
    configure_run_selector,
)
from etl_craft.cli.output import Output
from etl_craft.core.enums import MARKABLE_STATUSES
from etl_craft.core.errors import ExitCode, UsageError
from etl_craft.engine.runlog import RunSelector
from etl_craft.services.operations import PipelineRef, runs


def _configure(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    configure_run_selector(parser)
    parser.add_argument("--pipeline_code", required=True, help="the pipeline whose run to mark")
    parser.add_argument("--task_code", help="mark this task of the run; without it, the run itself")
    parser.add_argument(
        "--status", required=True, choices=[str(s) for s in MARKABLE_STATUSES], help="the status"
    )
    parser.add_argument("--reason", required=True, help="why; recorded with the change")
    parser.add_argument(
        "--rows",
        type=int,
        help="with SUCCESS, the row count a HAS_DATA dependency on the task reads",
    )
    parser.add_argument(
        "--stale",
        action="store_true",
        help="with --task_code: reconcile expired attempts before marking; live leases "
        "still refuse the mark",
    )
    parser.add_argument(
        "--new-run",
        action="store_true",
        help="record a finished stand-in run instead, so downstream gates pass where this "
        "pipeline cannot run",
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    if args.stale and (not args.task_code or args.new_run):
        raise UsageError("--stale marks one task: pass --task_code, without --new-run")
    if args.new_run and (args.run_id is not None or args.run_key is not None):
        raise UsageError("--new-run creates a stand-in run; do not pass a run selector")
    with command_context(args) as ctx:
        done = runs.mark(
            ctx,
            PipelineRef(args.pipeline_code),
            args.status,
            args.reason,
            task_code=args.task_code,
            rows=args.rows,
            stale=args.stale,
            new_run=args.new_run,
            selector=RunSelector(args.run_id, args.run_key),
        )
    if args.output_format == "json":
        out.document(done)
    else:
        out.line(done.message)
    return ExitCode.SUCCESS


COMMAND = Command(
    name="mark",
    help="Mark a task or a run SUCCESS, FAILED or SKIPPED, or record a stand-in run; local mode.",
    configure=_configure,
    run=_run,
)
