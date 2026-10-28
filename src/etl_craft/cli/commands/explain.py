"""``etl-craft explain``: why a task is in its current state."""

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
from etl_craft.services.operations.requests import TaskSelection
from etl_craft.services.operations.status import Explanation


def _configure(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    configure_run_selector(parser)
    parser.add_argument("--pipeline-code", required=True)
    parser.add_argument("--task-code", required=True)


def _run(args: argparse.Namespace, out: Output) -> int:
    done = call(args, ActionName.EXPLAIN, request_from(args, TaskSelection))
    assert isinstance(done, Explanation)
    return _render(args, out, done)


def _render(args: argparse.Namespace, out: Output, done: Explanation) -> int:
    def render() -> None:
        for resume in done.pipeline.resumes:
            out.line(f"resumed by {resume.actor} at {resume.resumed_at}: {resume.reason}")
        if done.standing_pause is not None:
            out.line(done.standing_pause.describe())
        elif done.task is not None and done.task.standing_pause is not None:
            out.line(done.task.standing_pause.describe())
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
        out.line(
            f"metadata_version={done.run.metadata_version} bundle_id={done.run.bundle_id} "
            f"newer_metadata_available={done.run.newer_metadata_available}"
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
        if done.dependents:
            out.line("Cross-project dependents: " + ", ".join(done.dependents))
        out.line(done.next_action)

    out.result(done, args.output_format, text=render)
    return 0


COMMAND = Command(
    "explain",
    "Explain the selected task's state and what would let it run.",
    run=_run,
    configure=_configure,
    actions=(ActionName.EXPLAIN,),
)
