"""``etl-craft history``: a pipeline's recent runs, or one of its tasks', and interventions."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands import Command
from etl_craft.cli.commands.common import connect_engine_db, load_command_config
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode
from etl_craft.engine.repository.interventions import Intervention
from etl_craft.services.inspect import run_history, run_interventions


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pipeline_code", required=True, help="the pipeline to show")
    parser.add_argument("--task_code", help="show this task's runs instead of the pipeline's")
    parser.add_argument("--limit", type=int, default=20, help="how many runs, newest first")


def _run(args: argparse.Namespace, out: Output) -> int:
    engine = connect_engine_db(load_command_config(args))
    try:
        with engine.connect() as conn:
            entries = run_history(conn, args.pipeline_code, args.task_code, limit=args.limit)
            changes = run_interventions(conn, args.pipeline_code, entries, args.task_code)
    finally:
        engine.dispose()
    if not entries:
        out.empty("no runs yet")
        return ExitCode.SUCCESS
    if args.task_code is None:
        out.rows(
            [("PIPELINE_RUN_ID", "STATUS", "RUN_DATE", "START_DATE", "END_DATE", "SLA_STATUS")]
        )
        out.rows(
            (
                e.pipeline_run_id,
                e.status,
                f"{e.run_date} (backfill)" if e.backfill else e.run_date,
                e.start_date,
                e.end_date,
                e.sla_status,
            )
            for e in entries
        )
        _interventions(out, changes)
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
    _interventions(out, changes)
    return ExitCode.SUCCESS


def _interventions(out: Output, changes: list[Intervention]) -> None:
    """List what operators changed in the runs shown, if anything."""
    if not changes:
        return
    out.line()
    out.line("Interventions:")
    out.rows([("PIPELINE_RUN_ID", "TASK", "ACTION", "FROM", "TO", "BY", "AT", "REASON")])
    out.rows(
        (
            c.pipeline_run_id,
            c.task_code or "(the run)",
            c.action,
            c.from_status or "-",
            c.to_status or "(reset)",
            c.requested_by,
            c.requested_at,
            c.reason,
        )
        for c in changes
    )


COMMAND = Command(
    name="history",
    help="Show a pipeline's recent runs, or one task's.",
    configure=_configure,
    run=_run,
)
