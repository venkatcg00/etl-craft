"""``etl-craft resume``: let a paused pipeline run again; local mode."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands import Command
from etl_craft.cli.commands.common import connect_engine_db, load_command_config
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode
from etl_craft.execution.interventions import resume_pipeline


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pipeline_code", required=True, help="the pipeline to resume")
    parser.add_argument("--reason", required=True, help="why; recorded with the resume")


def _run(args: argparse.Namespace, out: Output) -> int:
    config = load_command_config(args)
    engine = connect_engine_db(config)
    try:
        out.line(resume_pipeline(engine, config, args.pipeline_code, args.reason))
    finally:
        engine.dispose()
    return ExitCode.SUCCESS


COMMAND = Command(
    name="resume",
    help="Let a paused pipeline run again; local mode.",
    configure=_configure,
    run=_run,
)
