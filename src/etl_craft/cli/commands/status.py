"""``etl-craft status``: the selected run and all its configured tasks."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands.common import (
    Command,
    command_context,
    configure_output,
    configure_run_selector,
)
from etl_craft.cli.output import Output
from etl_craft.cli.remote import ServerClient, api_path, selector_query
from etl_craft.core.errors import UsageError
from etl_craft.engine.controls import ControlView
from etl_craft.engine.runlog import RunSelector
from etl_craft.services.operations.controls import get_control
from etl_craft.services.operations.status import StatusView, pipeline_status


def _configure(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    configure_run_selector(parser)
    parser.add_argument("--pipeline_code")
    parser.add_argument("--request-id", type=int, help="inspect durable control acknowledgements")


def _run(args: argparse.Namespace, out: Output) -> int:
    _validate(args)
    if args.request_id is not None:
        with command_context(args) as ctx:
            done_control = get_control(ctx, args.request_id)
        out.result(done_control, args.output_format)
        return 0
    with command_context(args) as ctx:
        done = pipeline_status(
            ctx, args.pipeline_code, selector=RunSelector(args.run_id, args.run_key)
        )
    return _render(args, out, done)


def _remote(args: argparse.Namespace, out: Output, server: ServerClient) -> int:
    _validate(args)
    if args.request_id is not None:
        done_control = server.call(
            "GET", api_path("control-requests", str(args.request_id)), ControlView
        )
        out.result(done_control, args.output_format)
        return 0
    done = server.call(
        "GET",
        api_path("pipelines", args.pipeline_code, "status"),
        StatusView,
        query=selector_query(args),
    )
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
    remote=_remote,
)


def _validate(args: argparse.Namespace) -> None:
    if args.request_id is not None:
        if args.request_id < 1 or any(
            value is not None for value in (args.pipeline_code, args.run_id, args.run_key)
        ):
            raise UsageError(
                "status --request-id needs a positive id without pipeline/run selectors"
            )
    elif args.pipeline_code is None:
        raise UsageError("status needs --pipeline_code or --request-id")
