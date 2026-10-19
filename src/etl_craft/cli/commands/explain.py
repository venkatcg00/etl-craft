"""``etl-craft explain``: why a task is in its current state."""

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
from etl_craft.engine.runlog import RunSelector
from etl_craft.services.operations.status import Explanation, explain_task


def _configure(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    configure_run_selector(parser)
    parser.add_argument("--pipeline_code", required=True)
    parser.add_argument("--task_code", required=True)


def _run(args: argparse.Namespace, out: Output) -> int:
    with command_context(args) as ctx:
        done = explain_task(
            ctx, args.pipeline_code, args.task_code, selector=RunSelector(args.run_id, args.run_key)
        )
    return _render(args, out, done)


def _remote(args: argparse.Namespace, out: Output, server: ServerClient) -> int:
    done = server.call(
        "GET",
        api_path("pipelines", args.pipeline_code, "tasks", args.task_code, "explain"),
        Explanation,
        query=selector_query(args),
    )
    return _render(args, out, done)


def _render(args: argparse.Namespace, out: Output, done: Explanation) -> int:
    def render() -> None:
        out.line(f"{done.pipeline.pipeline_code}.{done.task_code}: {done.state}")
        out.line(
            f"pipeline_run_id={done.run.pipeline_run_id} status={done.run.status} "
            f"task_status={done.task.status if done.task else 'NOT RUN'} "
            f"attempts={done.task.attempt_count if done.task else 0}"
            + (
                f" rejected={done.task.reject_count}"
                if done.task and done.task.reject_count
                else ""
            )
        )
        if done.task is not None and done.task.watermark_from is not None:
            out.line(f"watermark={done.task.watermark_from} -> {done.task.watermark_to}")
        if done.task is not None:
            for attempt in done.task.attempts:
                if attempt.retry_reason is not None:
                    out.line(f"attempt {attempt.attempt_number}: {attempt.retry_reason}")
        if done.pipeline.paused is not None:
            out.line(done.pipeline.paused.describe())
        out.line(f"run_condition={done.run_condition} required_count={done.required_count}")
        for dependency in (*done.pipeline_dependencies, *done.dependencies):
            out.line(
                f"{dependency.upstream}: {dependency.dependency_type} "
                f"status={dependency.status or 'NOT RUN'} met={dependency.met}; {dependency.reason}"
            )
        for wait in done.gate_waits:
            out.line(
                f"gate task_id={wait.task_id} next_check_at={wait.next_check_at} "
                f"wait_until={wait.wait_until or 'indefinite'} looks={wait.looks}"
            )
        if done.retry_at is not None:
            out.line(f"retry_at={done.retry_at.isoformat()}")
        out.line(done.next_action)

    out.result(done, args.output_format, text=render)
    return 0


COMMAND = Command(
    "explain",
    "Explain the selected task's state and what would let it run.",
    run=_run,
    configure=_configure,
    remote=_remote,
)
