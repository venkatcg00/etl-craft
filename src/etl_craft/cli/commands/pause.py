"""Pause and resume a pipeline definition: its schedule and new runs."""

from __future__ import annotations

import argparse
from functools import partial

from etl_craft.cli.commands.common import Command, call, configure_output, request_from
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName, ControlAction
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations.requests import ControlRequest

HELP = {
    ControlAction.PAUSE: "Pause a pipeline definition: no scheduled or new runs until resumed.",
    ControlAction.RESUME: "Resume a paused pipeline definition.",
}


def _configure(parser: argparse.ArgumentParser, *, action: ControlAction) -> None:
    configure_output(parser)
    verb = action.lower()
    parser.add_argument("--pipeline-code", required=True, help=f"The pipeline to {verb}.")
    parser.add_argument("--reason", required=True, help=f"Why; recorded with the {verb}.")


def _run(args: argparse.Namespace, out: Output, *, action: ControlAction) -> int:
    done = call(args, ActionName.CONTROL, request_from(args, ControlRequest, action=action))
    out.result(done, args.output_format)
    return ExitCode.SUCCESS


def command(action: ControlAction) -> Command:
    """Register either verb with its help and shared implementation."""
    return Command(
        action.lower(),
        HELP[action],
        run=partial(_run, action=action),
        configure=partial(_configure, action=action),
        actions=(ActionName.CONTROL,),
    )
