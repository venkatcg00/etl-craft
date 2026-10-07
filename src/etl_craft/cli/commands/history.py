"""``etl-craft history``: a pipeline's recent runs, or one of its tasks', and interventions."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from etl_craft.cli.commands import Command
from etl_craft.cli.commands.common import (
    command_context,
    configure_output,
    configure_run_selector,
)
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode, UsageError
from etl_craft.engine.repository.interventions import Intervention
from etl_craft.engine.runlog import RunSelector
from etl_craft.services.operations import PipelineRef, inspect
from etl_craft.services.operations.models import RunView, TaskRunView


def _configure(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    configure_run_selector(parser)
    parser.add_argument("--pipeline_code", required=True, help="the pipeline to show")
    parser.add_argument("--task_code", help="show this task's runs instead of the pipeline's")
    parser.add_argument("--all", action="store_true", help="list runs instead of selecting one")
    parser.add_argument("--limit", type=int, default=20, help="how many runs, newest first")


def _run(args: argparse.Namespace, out: Output) -> int:
    if args.limit < 1:
        raise UsageError(f"--limit must be 1 or more, got {args.limit}")
    if args.all and (args.run_id is not None or args.run_key is not None):
        raise UsageError("--all lists runs; do not pass a run selector")
    with command_context(args) as ctx:
        done = inspect.run_history(
            ctx,
            PipelineRef(args.pipeline_code),
            args.task_code,
            limit=args.limit,
            all_runs=args.all,
            selector=RunSelector(args.run_id, args.run_key),
        )
    if args.output_format == "json":
        out.document(done)
        return ExitCode.SUCCESS
    entries, changes = done.entries, done.changes
    if not entries:
        out.empty("no runs yet")
        return ExitCode.SUCCESS
    if args.task_code is None:
        out.rows(
            [
                (
                    "PIPELINE_RUN_ID",
                    "STATUS",
                    "RUN_DATE",
                    "START_DATE",
                    "END_DATE",
                    "SLA_STATUS",
                    "STARTED_BY",
                    "STARTED_BY_KIND",
                    "ENDED_BY",
                    "ENDED_BY_KIND",
                )
            ]
        )
        out.rows(
            (
                e.pipeline_run_id,
                e.status,
                f"{e.run_date} (backfill)" if e.backfill else e.run_date,
                e.start_date,
                e.end_date,
                e.sla_status,
                e.started_by,
                e.started_by_kind,
                e.ended_by,
                e.ended_by_kind,
            )
            for e in entries
            if isinstance(e, RunView)
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
        if isinstance(e, TaskRunView)
    )
    _interventions(out, changes)
    return ExitCode.SUCCESS


def _interventions(out: Output, changes: Sequence[Intervention]) -> None:
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
