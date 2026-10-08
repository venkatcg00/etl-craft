"""``etl-craft reconcile``: fence expired attempts and release expired idle supervisors."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands.common import Command, command_context, configure_output
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode, UsageError
from etl_craft.services.operations import runs


def _configure(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    parser.add_argument("--pipeline_code", help="limit reconciliation to this pipeline")
    parser.add_argument("--task_code", help="limit reconciliation to this pipeline's task")


def _run(args: argparse.Namespace, out: Output) -> int:
    if args.task_code and not args.pipeline_code:
        raise UsageError("--task_code needs --pipeline_code")
    with command_context(args) as ctx:
        report = runs.reconcile_runs(
            ctx,
            args.pipeline_code,
            args.task_code,
        )
    if args.output_format == "json":
        out.document(report)
    else:
        out.line(report.message)
    return ExitCode.SUCCESS


COMMAND = Command(
    name="reconcile",
    help="Reconcile expired attempt and run leases.",
    configure=_configure,
    run=_run,
)
