"""``etl-craft steps``: a pipeline's active tasks and their parameters."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands import Command
from etl_craft.cli.commands.common import connect_engine_db, load_command_config
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode
from etl_craft.services.inspect import pipeline_steps


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pipeline_code", required=True, help="the pipeline to show")


def _run(args: argparse.Namespace, out: Output) -> int:
    engine = connect_engine_db(load_command_config(args))
    try:
        with engine.connect() as conn:
            steps = pipeline_steps(conn, args.pipeline_code)
    finally:
        engine.dispose()
    if not steps:
        out.empty("no active tasks")
        return ExitCode.SUCCESS
    out.rows([("TASK_CODE", "HANDLER", "TASK_TYPE", "RUN_CONDITION", "PARAMETERS")])
    out.rows(
        (
            s.task_code,
            s.handler,
            s.task_type,
            f"N={s.run_condition_count}" if s.run_condition == "N" else s.run_condition or "ALL",
            ", ".join(f"{name}={value}" for name, value in s.parameters.items()),
        )
        for s in steps
    )
    return ExitCode.SUCCESS


COMMAND = Command(
    name="steps",
    help="List a pipeline's active tasks and their parameters.",
    configure=_configure,
    run=_run,
)
