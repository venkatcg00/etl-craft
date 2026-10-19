"""``etl-craft mark``: set a status or reset a SQL watermark, with a reason."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands.common import (
    Command,
    command_context,
    configure_output,
    configure_run_selector,
)
from etl_craft.cli.output import Output
from etl_craft.cli.remote import ServerClient, api_path, selector_query
from etl_craft.core.enums import MARKABLE_STATUSES
from etl_craft.core.errors import ExitCode
from etl_craft.engine.runlog import RunSelector
from etl_craft.services.operations import runs
from etl_craft.services.operations.models import OperationResult


def _configure(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    configure_run_selector(parser)
    parser.add_argument("--pipeline_code", required=True, help="the pipeline whose run to mark")
    parser.add_argument("--task_code", help="mark this task of the run; without it, the run itself")
    change = parser.add_mutually_exclusive_group(required=True)
    change.add_argument("--status", choices=[str(s) for s in MARKABLE_STATUSES], help="the status")
    change.add_argument("--watermark", help="reset a SQL task watermark to this typed value")
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
    with command_context(args) as ctx:
        done = runs.mark(
            ctx,
            args.pipeline_code,
            args.status,
            args.reason,
            task_code=args.task_code,
            rows=args.rows,
            watermark=args.watermark,
            stale=args.stale,
            new_run=args.new_run,
            selector=RunSelector(args.run_id, args.run_key),
        )
    out.result(done, args.output_format)
    return ExitCode.SUCCESS


def _remote(args: argparse.Namespace, out: Output, server: ServerClient) -> int:
    done = server.call(
        "POST",
        api_path("pipelines", args.pipeline_code, "mark"),
        OperationResult,
        body={
            "reason": args.reason,
            "task_code": args.task_code,
            "status": args.status,
            "watermark": args.watermark,
            "rows": args.rows,
            "stale": args.stale,
            "new_run": args.new_run,
            **selector_query(args),
        },
    )
    out.result(done, args.output_format)
    return ExitCode.SUCCESS


COMMAND = Command(
    name="mark",
    help="Mark a status, record a stand-in run or reset a SQL watermark.",
    configure=_configure,
    run=_run,
    remote=_remote,
)
