"""``etl-craft mark``: set a status or reset a SQL watermark, with a reason."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands.common import (
    Command,
    call,
    configure_output,
    configure_run_selector,
    request_from,
)
from etl_craft.cli.output import Output
from etl_craft.core.enums import MARKABLE_STATUSES, ActionName
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations.requests import MarkRequest


def _configure(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    configure_run_selector(parser)
    parser.add_argument("--pipeline-code", required=True, help="The pipeline whose run to mark.")
    parser.add_argument(
        "--task-code", help="Mark this task of the run; without it, the run itself."
    )
    change = parser.add_mutually_exclusive_group(required=True)
    change.add_argument("--status", choices=[str(s) for s in MARKABLE_STATUSES], help="The status.")
    change.add_argument("--watermark", help="Reset a SQL task watermark to this typed value.")
    change.add_argument(
        "--offset",
        help="Reset a Python task's offset: a PARTITIONS JSON object (null removes a "
        "partition), or a value of the stored type.",
    )
    parser.add_argument("--reason", default="", help="Why; recorded with the change.")
    parser.add_argument(
        "--rows",
        type=int,
        help="With SUCCESS, the row count a HAS_DATA dependency on the task reads.",
    )
    parser.add_argument(
        "--stale",
        action="store_true",
        help="With --task-code: reconcile expired attempts before marking; live leases "
        "still refuse the mark.",
    )
    parser.add_argument(
        "--new-run",
        action="store_true",
        help="Record a finished stand-in run instead, so downstream gates pass where this "
        "pipeline cannot run.",
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    done = call(args, ActionName.MARK, request_from(args, MarkRequest))
    out.result(done, args.output_format)
    return ExitCode.SUCCESS


COMMAND = Command(
    name="mark",
    help="Mark a status, record a stand-in run, or reset a SQL watermark or a script offset.",
    configure=_configure,
    run=_run,
    actions=(ActionName.MARK,),
)
