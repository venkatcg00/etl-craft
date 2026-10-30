"""Pause and resume pipeline definitions or standing task controls."""

from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import datetime
from functools import partial

from etl_craft.cli.commands.common import (
    Command,
    call,
    configure_output,
    configure_tags,
    request_from,
)
from etl_craft.cli.commands.control_requests import render_bulk
from etl_craft.cli.output import Output
from etl_craft.cli.prompts import can_prompt
from etl_craft.core.enums import (
    MARKABLE_STATUSES,
    ActionName,
    ControlAction,
    ResumeChoice,
    enum_value,
)
from etl_craft.core.errors import ExitCode, UsageError
from etl_craft.engine.controls import ControlView
from etl_craft.services.operations.controls import BulkControlView, BulkOutcome
from etl_craft.services.operations.models import OperationResult, PipelineListView
from etl_craft.services.operations.requests import BulkControlRequest, ControlRequest, TagFilter

HELP = {
    ControlAction.PAUSE: "Pause a pipeline or task across runs; optionally set a task outcome.",
    ControlAction.RESUME: "Resume a pipeline or task; scheduled pipelines need --then.",
}


def _configure(parser: argparse.ArgumentParser, *, action: ControlAction) -> None:
    configure_output(parser)
    verb = action.lower()
    parser.add_argument("--pipeline-code", help=f"The pipeline to {verb}.")
    parser.add_argument("--task-code", help="Standing pause of this task across runs.")
    parser.add_argument(
        "--then", choices=tuple(ResumeChoice), help="Scheduled pipeline resume choice."
    )
    if action == ControlAction.PAUSE:
        parser.add_argument(
            "--outcome",
            choices=MARKABLE_STATUSES,
            help="Supply this outcome instead of running the task; omit to hold.",
        )
        parser.add_argument(
            "--expires-at",
            type=datetime.fromisoformat,
            metavar="TIMESTAMP",
            help="Future ISO-8601 timestamp with an offset; omit for no expiry.",
        )
        parser.add_argument(
            "--replace",
            action="store_true",
            help="Atomically replace an existing task pause; no unpaused window.",
        )
    configure_tags(parser)
    parser.add_argument(
        "--yes", action="store_true", help="Confirm the printed pipeline selection."
    )
    parser.add_argument("--reason", default="", help=f"Why; recorded with the {verb}.")


def _run(args: argparse.Namespace, out: Output, *, action: ControlAction) -> int:
    if args.tag and args.pipeline_code is not None:
        raise UsageError("choose --pipeline-code or --tag, not both")
    if args.tag or (args.project and args.pipeline_code is None):
        selected = call(args, ActionName.PIPELINES, request_from(args, TagFilter))
        assert isinstance(selected, PipelineListView)
        codes = tuple(p.pipeline_code for p in selected.pipelines)
        preview = Output(stdout=out.stderr) if args.output_format == "json" else out
        preview.line("Selected pipelines: " + (", ".join(codes) or "(none)"))
        if args.tag and not args.yes:
            raise UsageError(
                "tag-selected changes need --yes; "
                "review the printed pipelines and repeat with --yes"
            )
        requests = tuple(
            request_from(args, ControlRequest, pipeline_code=code, action=action) for code in codes
        )
        outcomes: list[BulkOutcome] = []
        for start in range(0, len(requests), 100):
            batch = call(
                args,
                ActionName.CONTROL_BULK,
                BulkControlRequest(requests=requests[start : start + 100]),
            )
            assert isinstance(batch, BulkControlView)
            for request, outcome in zip(requests[start : start + 100], batch.outcomes, strict=True):
                if (
                    outcome.error
                    and "scheduled resume requires" in outcome.error
                    and request.then is None
                    and can_prompt()
                ):
                    outcome = replace(outcome, control=_single(args, out, request), error=None)
                outcomes.append(outcome)
        failures = sum(outcome.error is not None for outcome in outcomes)
        batch_result = BulkControlView(
            tuple(outcomes), f"{len(outcomes) - failures} accepted; {failures} refused"
        )
        out.result(batch_result, args.output_format, text=lambda: render_bulk(out, batch_result))
        return ExitCode.FAILURE if any(o.error for o in batch_result.outcomes) else ExitCode.SUCCESS
    if args.pipeline_code is None:
        raise UsageError("give --pipeline-code or --tag to choose pipelines")
    done = _single(args, out, request_from(args, ControlRequest, action=action))
    out.result(done, args.output_format)
    return ExitCode.SUCCESS


def _single(
    args: argparse.Namespace, out: Output, request: ControlRequest
) -> ControlView | OperationResult:
    """Ask only after the shared operation has refused and supplied the missed-tick plan."""
    try:
        done = call(args, ActionName.CONTROL, request)
    except UsageError as error:
        if not (
            request.action == ControlAction.RESUME
            and request.then is None
            and request.task_code is None
            and can_prompt()
            and "scheduled resume requires" in str(error)
        ):
            raise
        preview = Output(stdout=out.stderr) if args.output_format == "json" else out
        preview.line(str(error))
        preview.line("Resume then (run-now/catch-up/wait):")
        answer = input().strip()
        done = call(
            args,
            ActionName.CONTROL,
            replace(
                request,
                then=enum_value(
                    ResumeChoice,
                    answer,
                    UsageError(
                        f"--then={answer!r}; expected run-now, catch-up or wait; choose one"
                    ),
                ),
            ),
        )
    assert isinstance(done, (ControlView, OperationResult))
    return done


def command(action: ControlAction) -> Command:
    """Register either verb with its help and shared implementation."""
    return Command(
        action.lower(),
        HELP[action],
        run=partial(_run, action=action),
        configure=partial(_configure, action=action),
        actions=(ActionName.CONTROL, ActionName.PIPELINES, ActionName.CONTROL_BULK),
    )
