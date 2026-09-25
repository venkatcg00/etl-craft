"""``etl-craft history``: a pipeline's recent runs, or one of its tasks'."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands import Command
from etl_craft.cli.commands.common import connect_engine_db, load_command_config
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode
from etl_craft.services.inspect import run_history


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pipeline_code", required=True, help="the pipeline to show")
    parser.add_argument("--task_code", help="show this task's runs instead of the pipeline's")
    parser.add_argument("--limit", type=int, default=20, help="how many runs, newest first")


def _run(args: argparse.Namespace, out: Output) -> int:
    engine = connect_engine_db(load_command_config(args))
    try:
        with engine.connect() as conn:
            entries = run_history(conn, args.pipeline_code, args.task_code, limit=args.limit)
    finally:
        engine.dispose()
    if not entries:
        out.empty("no runs yet")
        return ExitCode.SUCCESS
    if args.task_code is None:
        out.rows([("PIPELINE_RUN_ID", "STATUS", "START_DATE", "END_DATE", "SLA_STATUS")])
        out.rows(
            (e.pipeline_run_id, e.status, e.start_date, e.end_date, e.sla_status) for e in entries
        )
        return ExitCode.SUCCESS
    out.rows(
        [
            (
                "PIPELINE_RUN_ID",
                "STATUS",
                "ATTEMPTS",
                "SOURCE_COUNT",
                "TARGET_COUNT",
                "START_DATE",
                "END_DATE",
                "ERROR_MESSAGE",
            )
        ]
    )
    out.rows(
        (
            e.pipeline_run_id,
            e.status,
            e.attempt_count,
            e.source_count,
            e.target_count,
            e.start_date,
            e.end_date,
            e.error_message,
        )
        for e in entries
    )
    return ExitCode.SUCCESS


COMMAND = Command(
    name="history",
    help="Show a pipeline's recent runs, or one task's.",
    configure=_configure,
    run=_run,
)
