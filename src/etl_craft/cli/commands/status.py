"""``etl-craft status``: the selected run and all its configured tasks."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands.common import (
    Command,
    call,
    configure_output,
    configure_run_selector,
    request_from,
)
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName
from etl_craft.services.operations.requests import RunSelection
from etl_craft.services.operations.status import StatusView


def _configure(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    configure_run_selector(parser)
    parser.add_argument("--pipeline-code", required=True, help="The pipeline whose run to show.")


def _run(args: argparse.Namespace, out: Output) -> int:
    done = call(args, ActionName.STATUS, request_from(args, RunSelection))
    assert isinstance(done, StatusView)
    return _render(args, out, done)


def _render(args: argparse.Namespace, out: Output, done: StatusView) -> int:
    def render() -> None:
        run = done.run
        out.line(
            f"{run.pipeline_code}: pipeline_run_id={run.pipeline_run_id} key={run.run_key} "
            f"kind={run.trigger_kind} run_date={run.run_date} status={run.status}"
        )
        out.line(
            f"start={run.start_date} duration_seconds={done.duration_seconds} SLA={run.sla_status} "
            f"sla_hours={done.pipeline.sla_in_hours}"
        )
        if done.pipeline.paused is not None:
            out.line(done.pipeline.paused.describe())
        headers = ("TASK", "STATUS", "ATTEMPTS", "ROWS_WRITTEN", "REJECTED", "DURATION_SECONDS")
        out.rows([(*headers, "ERROR")])
        out.rows(
            (
                t.task_code,
                t.status or "NOT RUN",
                t.attempts,
                t.rows_written,
                t.reject_count,
                t.duration_seconds,
                t.error,
            )
            for t in done.tasks
        )
        out.line("blocked by failures: " + (", ".join(done.blocked_by_failures) or "(none)"))

    out.result(done, args.output_format, text=render)
    return 0


COMMAND = Command(
    "status",
    "Show the selected run and every configured task.",
    run=_run,
    configure=_configure,
    actions=(ActionName.STATUS,),
)
