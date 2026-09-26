"""``etl-craft cancel``: stop a pipeline's run in progress and end it CANCELLED; local mode."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands import Command
from etl_craft.cli.commands.common import connect_engine_db, load_command_config
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode
from etl_craft.execution.interventions import cancel_run


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pipeline_code", required=True, help="the pipeline whose run to cancel")
    parser.add_argument("--reason", required=True, help="why; recorded with the change")


def _run(args: argparse.Namespace, out: Output) -> int:
    config = load_command_config(args)
    engine = connect_engine_db(config)
    try:
        done = cancel_run(engine, config, args.pipeline_code, args.reason)
    finally:
        engine.dispose()
    out.line(done.message)
    return ExitCode.SUCCESS


COMMAND = Command(
    name="cancel",
    help="Stop a pipeline's run in progress and end it CANCELLED; local mode.",
    configure=_configure,
    run=_run,
)
