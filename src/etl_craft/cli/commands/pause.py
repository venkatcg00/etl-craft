"""``etl-craft pause``: stop a pipeline from running until it is resumed; local mode."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands import Command
from etl_craft.cli.commands.common import command_context, configure_output
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations import PipelineRef, pipelines


def _configure(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    parser.add_argument("--pipeline_code", required=True, help="the pipeline to pause")
    parser.add_argument("--reason", required=True, help="why; recorded with the pause")


def _run(args: argparse.Namespace, out: Output) -> int:
    with command_context(args) as ctx:
        done = pipelines.pause_pipeline(
            ctx,
            PipelineRef(args.pipeline_code),
            args.reason,
        )
    if args.output_format == "json":
        out.document(done)
    else:
        out.line(done.message)
    return ExitCode.SUCCESS


COMMAND = Command(
    name="pause",
    help="Stop a pipeline from running until it is resumed; local mode.",
    configure=_configure,
    run=_run,
)
