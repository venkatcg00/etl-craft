"""``etl-craft mark``: set a task or a run's status by hand, with a reason; local mode."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands import Command
from etl_craft.cli.commands.common import connect_engine_db, load_command_config
from etl_craft.cli.output import Output
from etl_craft.core.enums import MARKABLE_STATUSES
from etl_craft.core.errors import ExitCode
from etl_craft.execution.interventions import mark_run, mark_task, record_stand_in_run


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pipeline_code", required=True, help="the pipeline whose run to mark")
    parser.add_argument("--task_code", help="mark this task of the run; without it, the run itself")
    parser.add_argument(
        "--status", required=True, choices=[str(s) for s in MARKABLE_STATUSES], help="the status"
    )
    parser.add_argument("--reason", required=True, help="why; recorded with the change")
    parser.add_argument(
        "--rows",
        type=int,
        help="with SUCCESS, the row count a HAS_DATA dependency on the task reads",
    )
    parser.add_argument(
        "--new-run",
        action="store_true",
        help="record a finished stand-in run instead, so downstream gates pass where this "
        "pipeline cannot run",
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    config = load_command_config(args)
    engine = connect_engine_db(config)
    try:
        if args.new_run:
            done = record_stand_in_run(
                engine,
                config,
                args.pipeline_code,
                args.status,
                args.reason,
                task_code=args.task_code,
                rows=args.rows,
            )
        elif args.task_code:
            done = mark_task(
                engine,
                config,
                args.pipeline_code,
                args.task_code,
                args.status,
                args.reason,
                rows=args.rows,
            )
        else:
            done = mark_run(engine, config, args.pipeline_code, args.status, args.reason)
    finally:
        engine.dispose()
    out.line(done.message)
    return ExitCode.SUCCESS


COMMAND = Command(
    name="mark",
    help="Mark a task or a run SUCCESS, FAILED or SKIPPED, or record a stand-in run; local mode.",
    configure=_configure,
    run=_run,
)
