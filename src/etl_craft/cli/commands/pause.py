"""Pause and resume CLI commands sharing the same options and output."""

from __future__ import annotations

import argparse
from functools import partial
from typing import Literal

from etl_craft.cli.commands.common import (
    Command,
    command_context,
    configure_output,
    configure_run_selector,
)
from etl_craft.cli.output import Output
from etl_craft.cli.remote import ServerClient, api_path, selector_query
from etl_craft.core.enums import ControlAction
from etl_craft.core.errors import ExitCode
from etl_craft.engine.controls import ControlView
from etl_craft.engine.runlog import RunSelector
from etl_craft.services.operations import pipelines
from etl_craft.services.operations.controls import ControlRequest, request_control
from etl_craft.services.operations.models import OperationResult


def _configure(parser: argparse.ArgumentParser, *, verb: str) -> None:
    configure_output(parser)
    configure_run_selector(parser)
    parser.add_argument("--task_code", help="hold/release this task under the selected run")
    parser.add_argument("--request-key", help="idempotency key for a scoped request")
    parser.add_argument("--pipeline_code", required=True, help=f"the pipeline to {verb}")
    parser.add_argument("--reason", required=True, help=f"why; recorded with the {verb}")


def _run(args: argparse.Namespace, out: Output, *, verb: Literal["pause", "resume"]) -> int:
    done: ControlView | OperationResult
    with command_context(args) as ctx:
        if _scoped(args):
            done = request_control(
                ctx,
                ControlRequest(
                    args.pipeline_code,
                    ControlAction.HOLD if verb == "pause" else ControlAction.RELEASE,
                    args.reason,
                    RunSelector(args.run_id, args.run_key),
                    args.task_code,
                    args.request_key,
                ),
            )
        else:
            done = pipelines.set_pause(ctx, args.pipeline_code, args.reason, verb=verb)
    out.result(done, args.output_format)
    return ExitCode.SUCCESS


def _remote(
    args: argparse.Namespace, out: Output, server: ServerClient, *, verb: Literal["pause", "resume"]
) -> int:
    done: ControlView | OperationResult
    if _scoped(args):
        done = server.call(
            "POST",
            api_path("pipelines", args.pipeline_code, "controls"),
            ControlView,
            body={
                "reason": args.reason,
                **selector_query(args),
                "task_code": args.task_code,
                "request_key": args.request_key,
                "action": ControlAction.HOLD if verb == "pause" else ControlAction.RELEASE,
            },
        )
    else:
        done = server.call(
            "POST",
            api_path("pipelines", args.pipeline_code, verb),
            OperationResult,
            body={"reason": args.reason},
        )
    out.result(done, args.output_format)
    return ExitCode.SUCCESS


def command(verb: Literal["pause", "resume"]) -> Command:
    """Register either verb with its existing help and shared implementation."""
    help_text = {
        "pause": "Stop a pipeline from running until it is resumed; local mode.",
        "resume": "Let a paused pipeline run again; local mode.",
    }[verb]
    return Command(
        verb,
        help_text,
        run=partial(_run, verb=verb),
        configure=partial(_configure, verb=verb),
        remote=partial(_remote, verb=verb),
    )


def _scoped(args: argparse.Namespace) -> bool:
    return any(
        value is not None for value in (args.run_id, args.run_key, args.task_code, args.request_key)
    )
