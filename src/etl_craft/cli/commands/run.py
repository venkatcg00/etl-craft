"""``etl-craft run``: run one task of a pipeline."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands import Command
from etl_craft.cli.commands.common import connect_engine_db, load_command_config
from etl_craft.cli.output import Output
from etl_craft.core.enums import RunStatus
from etl_craft.core.errors import ExitCode
from etl_craft.execution.runner import ChildOptions, run_task


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pipeline_code", required=True, help="the pipeline the task is in")
    parser.add_argument("--task_code", required=True, help="the task to run")
    parser.add_argument(
        "--force",
        action="store_true",
        help="run even if the task already succeeded or its dependencies are not met; local "
        "mode only",
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    config = load_command_config(args)
    engine = connect_engine_db(config)
    try:
        outcome = run_task(
            engine,
            config,
            args.pipeline_code,
            args.task_code,
            force=args.force,
            child=ChildOptions(log_level=args.log_level, log_format=args.log_format),
        )
    finally:
        engine.dispose()
    out.line(outcome.message)
    if outcome.status in {RunStatus.SUCCESS, RunStatus.SKIPPED}:
        return ExitCode.SUCCESS
    return ExitCode.FAILURE


COMMAND = Command(
    name="run",
    help="Run one task of a pipeline under the pipeline's active run.",
    configure=_configure,
    run=_run,
)
