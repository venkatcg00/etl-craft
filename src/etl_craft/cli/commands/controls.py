"""Scoped run controls: ``stop``, ``abort``, ``hold`` and ``release`` share one implementation."""

from __future__ import annotations

import argparse
from functools import partial

from etl_craft.cli.commands.common import (
    Command,
    call,
    configure_output,
    configure_run_selector,
    request_from,
)
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName, ControlAction
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations.requests import ControlRequest

HELP = {
    ControlAction.STOP: (
        "Stop the selected run or task: stop its processes and queries; nothing new starts.",
        "the run or task to stop",
    ),
    ControlAction.ABORT: (
        "Abort the selected run or task: stop its task processes now; commits are kept.",
        "the run or task to abort",
    ),
    ControlAction.HOLD: (
        "Hold dispatch in the selected run or task; running attempts finish.",
        "the run or task to hold",
    ),
    ControlAction.RELEASE: (
        "Release a hold on exactly the run or task that was held.",
        "the run or task to release",
    ),
}


def _configure(parser: argparse.ArgumentParser, *, action: ControlAction) -> None:
    configure_output(parser)
    configure_run_selector(parser)
    parser.add_argument(
        "--pipeline-code", required=True, help=f"The pipeline of {HELP[action][1]}."
    )
    parser.add_argument("--task-code", help="Only this task under the selected run.")
    parser.add_argument("--request-key", help="Idempotency key for this exact action and scope.")
    parser.add_argument("--reason", required=True, help="Why; recorded with the control.")


def _run(args: argparse.Namespace, out: Output, *, action: ControlAction) -> int:
    done = call(args, ActionName.CONTROL, request_from(args, ControlRequest, action=action))
    out.result(done, args.output_format)
    return ExitCode.SUCCESS


def command(action: ControlAction) -> Command:
    """Register one control verb; its name is the action in lower case."""
    return Command(
        action.lower(),
        HELP[action][0],
        run=partial(_run, action=action),
        configure=partial(_configure, action=action),
        actions=(ActionName.CONTROL,),
    )
