"""``etl-craft history``: a pipeline's recent runs, or one of its tasks', and interventions."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from etl_craft.cli.commands.common import (
    Command,
    call,
    configure_output,
    configure_run_selector,
    configure_tags,
    request_from,
)
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName, FailureClass
from etl_craft.core.errors import ExitCode
from etl_craft.engine.repository.intervention_rows import Intervention
from etl_craft.services.operations.models import HistoryView, RunView, TaskRunView
from etl_craft.services.operations.requests import HistoryInput


def _configure(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    configure_tags(parser)
    configure_run_selector(parser)
    parser.add_argument("--pipeline-code", help="The pipeline to show.")
    parser.add_argument("--task-code", help="Show this task's runs instead of the pipeline's.")
    parser.add_argument("--all", action="store_true", help="List runs instead of selecting one.")
    parser.add_argument("--limit", type=int, default=20, help="How many runs, newest first.")
    parser.add_argument("--pool", help="With --all: only runs with an attempt in this pool.")
    parser.add_argument(
        "--agent", type=int, help="With --all: only runs with an attempt on this agent id."
    )
    parser.add_argument(
        "--failure-class",
        type=FailureClass,
        choices=list(FailureClass),
        help="With --all: only runs with an attempt that failed with this class.",
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    done = call(args, ActionName.HISTORY, request_from(args, HistoryInput))
    assert isinstance(done, HistoryView)
    return _render(args, out, done)


def _render(args: argparse.Namespace, out: Output, done: HistoryView) -> int:
    def render_text() -> None:
        entries, changes = done.entries, done.changes
        if not entries:
            out.empty("no runs yet")
            return
        if args.task_code is None:
            out.rows(
                [
                    (
                        "PIPELINE_CODE",
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
                    e.pipeline_code,
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
            return
        out.rows(
            [
                (
                    "PIPELINE_RUN_ID",
                    "STATUS",
                    "ATTEMPTS",
                    "SOURCE_COUNT",
                    "TARGET_COUNT",
                    "REJECT_COUNT",
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
                e.reject_count,
                e.start_date,
                e.end_date,
                e.error_message,
            )
            for e in entries
            if isinstance(e, TaskRunView)
        )
        _interventions(out, changes)
        return

    out.result(done, args.output_format, text=render_text)
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
    actions=(ActionName.HISTORY,),
)
