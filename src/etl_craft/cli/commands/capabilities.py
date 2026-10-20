"""``etl-craft capabilities``: the controls available for a run or task, and why others are not."""

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
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations.capabilities import CapabilitiesView
from etl_craft.services.operations.requests import CapabilitiesInput


def _configure(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    configure_run_selector(parser)
    parser.add_argument("--pipeline-code", required=True, help="The pipeline whose run to inspect.")
    parser.add_argument("--task-code", help="Inspect one task of the run.")


def _run(args: argparse.Namespace, out: Output) -> int:
    done = call(args, ActionName.CAPABILITIES, request_from(args, CapabilitiesInput))
    assert isinstance(done, CapabilitiesView)
    out.result(
        done,
        args.output_format,
        text=lambda: out.rows(
            (a.action, "available" if a.available else "refused", a.reason) for a in done.actions
        ),
    )
    return ExitCode.SUCCESS


COMMAND = Command(
    "capabilities",
    "Show the controls available for the selected run or task, and why others are refused.",
    run=_run,
    configure=_configure,
    actions=(ActionName.CAPABILITIES,),
)
