"""Pause and resume a pipeline definition: its schedule and new runs."""

from __future__ import annotations

import argparse
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
from etl_craft.core.enums import ActionName, ControlAction
from etl_craft.core.errors import ExitCode, UsageError
from etl_craft.services.operations.controls import BulkControlView, BulkOutcome
from etl_craft.services.operations.models import PipelineListView
from etl_craft.services.operations.requests import BulkControlRequest, ControlRequest, TagFilter

HELP = {
    ControlAction.PAUSE: "Pause a pipeline definition: no scheduled or new runs until resumed.",
    ControlAction.RESUME: "Resume a paused pipeline definition.",
}


def _configure(parser: argparse.ArgumentParser, *, action: ControlAction) -> None:
    configure_output(parser)
    verb = action.lower()
    parser.add_argument("--pipeline-code", help=f"The pipeline to {verb}.")
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
            ControlRequest(pipeline_code=code, action=action, reason=args.reason) for code in codes
        )
        outcomes: list[BulkOutcome] = []
        for start in range(0, len(requests), 100):
            batch = call(
                args,
                ActionName.CONTROL_BULK,
                BulkControlRequest(requests=requests[start : start + 100]),
            )
            assert isinstance(batch, BulkControlView)
            outcomes.extend(batch.outcomes)
        failures = sum(outcome.error is not None for outcome in outcomes)
        batch_result = BulkControlView(
            tuple(outcomes), f"{len(outcomes) - failures} accepted; {failures} refused"
        )
        out.result(batch_result, args.output_format, text=lambda: render_bulk(out, batch_result))
        return ExitCode.FAILURE if any(o.error for o in batch_result.outcomes) else ExitCode.SUCCESS
    if args.pipeline_code is None:
        raise UsageError("give --pipeline-code or --tag to choose pipelines")
    done = call(args, ActionName.CONTROL, request_from(args, ControlRequest, action=action))
    out.result(done, args.output_format)
    return ExitCode.SUCCESS


def command(action: ControlAction) -> Command:
    """Register either verb with its help and shared implementation."""
    return Command(
        action.lower(),
        HELP[action],
        run=partial(_run, action=action),
        configure=partial(_configure, action=action),
        actions=(ActionName.CONTROL, ActionName.PIPELINES, ActionName.CONTROL_BULK),
    )
