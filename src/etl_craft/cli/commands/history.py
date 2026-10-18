"""``etl-craft history``: a pipeline's recent runs, or one of its tasks', and interventions."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from etl_craft.cli.commands.common import (
    Command,
    command_context,
    configure_output,
    configure_run_selector,
)
from etl_craft.cli.output import Output
from etl_craft.cli.remote import ServerClient, api_path, selector_query
from etl_craft.core.errors import ExitCode
from etl_craft.engine.repository.interventions import Intervention
from etl_craft.engine.runlog import RunSelector
from etl_craft.services.operations import inspect
from etl_craft.services.operations.models import HistoryQuery, HistoryView, RunView, TaskRunView


def _configure(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    configure_run_selector(parser)
    parser.add_argument("--pipeline_code", required=True, help="the pipeline to show")
    parser.add_argument("--task_code", help="show this task's runs instead of the pipeline's")
    parser.add_argument("--all", action="store_true", help="list runs instead of selecting one")
    parser.add_argument("--limit", type=int, default=20, help="how many runs, newest first")


def _run(args: argparse.Namespace, out: Output) -> int:
    with command_context(args) as ctx:
        done = inspect.run_history(
            ctx,
            args.pipeline_code,
            HistoryQuery(
                args.task_code,
                args.limit,
                args.all,
                selector=RunSelector(args.run_id, args.run_key),
            ),
        )
    return _render(args, out, done)


def _remote(args: argparse.Namespace, out: Output, server: ServerClient) -> int:
    done = server.call(
        "GET",
        api_path("pipelines", args.pipeline_code, "runs"),
        HistoryView,
        query={
            "task_code": args.task_code,
            "all": str(args.all).lower(),
            "limit": args.limit,
            **selector_query(args),
        },
    )
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
    remote=_remote,
)
