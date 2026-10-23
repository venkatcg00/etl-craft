"""``etl-craft status``: the selected run and all its configured tasks."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands.common import (
    Command,
    call,
    configure_output,
    configure_run_selector,
    configure_tags,
    request_from,
)
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName
from etl_craft.services.operations.requests import RunFilter
from etl_craft.services.operations.status import StatusListView, StatusView


def _configure(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    configure_tags(parser)
    configure_run_selector(parser)
    parser.add_argument("--pipeline-code", help="The pipeline whose run to show.")


def _run(args: argparse.Namespace, out: Output) -> int:
    done = call(args, ActionName.STATUS, request_from(args, RunFilter))
    assert isinstance(done, (StatusView, StatusListView))
    return _render(args, out, done)


def _render(args: argparse.Namespace, out: Output, done: StatusView | StatusListView) -> int:
    def render() -> None:
        if isinstance(done, StatusListView):
            if not done.runs:
                out.empty("no matching non-terminal runs")
            for run in done.runs:
                _text_status(out, run)
        else:
            _text_status(out, done)
        out.agents(done.agents)

    out.result(done, args.output_format, text=render)
    return 0


def _text_status(out: Output, done: StatusView) -> None:
    run = done.run
    out.line(
        f"{run.pipeline_code}: pipeline_run_id={run.pipeline_run_id} key={run.run_key} "
        f"kind={run.trigger_kind} run_date={run.run_date} status={run.status}"
    )
    out.line(
        f"start={run.start_date} duration_seconds={done.duration_seconds} SLA={run.sla_status} "
        f"sla_hours={done.pipeline.sla_in_hours}"
    )
    if run.parent_run_id is not None:
        out.line(
            f"parent pipeline_run_id={run.parent_run_id}; "
            f"trigger task_run_id={run.triggered_by_task_run_id}"
        )
    if run.child_run_ids:
        out.line(f"triggered child pipeline_run_ids={run.child_run_ids}")
    for task in done.tasks:
        if task.explanation.state == "waiting on child":
            out.line(f"{task.task_code}: {task.explanation.next_action}")
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


COMMAND = Command(
    "status",
    "Show the selected run and every configured task.",
    run=_run,
    configure=_configure,
    actions=(ActionName.STATUS,),
)
