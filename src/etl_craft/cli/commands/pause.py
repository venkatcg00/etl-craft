"""Pause and resume CLI commands sharing the same options and output."""

from __future__ import annotations

import argparse
from functools import partial
from typing import Literal

from etl_craft.cli.commands.common import Command, command_context, configure_output
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations import pipelines


def _configure(parser: argparse.ArgumentParser, *, verb: str) -> None:
    configure_output(parser)
    parser.add_argument("--pipeline_code", required=True, help=f"the pipeline to {verb}")
    parser.add_argument("--reason", required=True, help=f"why; recorded with the {verb}")


def _run(args: argparse.Namespace, out: Output, *, verb: Literal["pause", "resume"]) -> int:
    with command_context(args) as ctx:
        done = pipelines.set_pause(ctx, args.pipeline_code, args.reason, verb=verb)
    out.result(done, args.output_format)
    return ExitCode.SUCCESS


def command(verb: Literal["pause", "resume"]) -> Command:
    """Register either verb with its existing help and shared implementation."""
    help_text = {
        "pause": "Stop a pipeline from running until it is resumed; local mode.",
        "resume": "Let a paused pipeline run again; local mode.",
    }[verb]
    return Command(verb, help_text, partial(_configure, verb=verb), partial(_run, verb=verb))
