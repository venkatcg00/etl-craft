"""``etl-craft control-request``: one control request and its acknowledgements."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands.common import Command, call, configure_output, request_from
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations.requests import ControlId


def _configure(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    parser.add_argument("--control-id", type=int, required=True, help="The control request.")


def _run(args: argparse.Namespace, out: Output) -> int:
    out.result(
        call(args, ActionName.CONTROL_REQUEST, request_from(args, ControlId)), args.output_format
    )
    return ExitCode.SUCCESS


COMMAND = Command(
    "control-request",
    "Show one control request and its acknowledgements.",
    run=_run,
    configure=_configure,
    actions=(ActionName.CONTROL_REQUEST,),
)
