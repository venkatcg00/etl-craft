"""``etl-craft reconcile``: fence expired attempts and release expired idle supervisors."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands.common import Command, call, configure_output, request_from
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations.models import ReconciliationView
from etl_craft.services.operations.requests import ReconcileInput


def _configure(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    parser.add_argument("--pipeline-code", help="Limit reconciliation to this pipeline.")
    parser.add_argument("--task-code", help="Limit reconciliation to this pipeline's task.")


def _run(args: argparse.Namespace, out: Output) -> int:
    report = call(args, ActionName.RECONCILE, request_from(args, ReconcileInput))
    assert isinstance(report, ReconciliationView)
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
    actions=(ActionName.RECONCILE,),
)
