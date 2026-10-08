"""``etl-craft cancel``: stop a pipeline's run in progress and end it CANCELLED; local mode."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands.common import (
    Command,
    command_context,
    configure_output,
    configure_run_selector,
)
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode
from etl_craft.engine.runlog import RunSelector
from etl_craft.services.operations import runs


def _configure(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    configure_run_selector(parser)
    parser.add_argument("--pipeline_code", required=True, help="the pipeline whose run to cancel")
    parser.add_argument("--reason", required=True, help="why; recorded with the change")


def _run(args: argparse.Namespace, out: Output) -> int:
    with command_context(args) as ctx:
        done = runs.cancel_run(
            ctx,
            args.pipeline_code,
            args.reason,
            selector=RunSelector(args.run_id, args.run_key),
        )
    out.result(done, args.output_format)
    return ExitCode.SUCCESS


COMMAND = Command(
    name="cancel",
    help="Stop a pipeline's run in progress and end it CANCELLED; local mode.",
    configure=_configure,
    run=_run,
)
