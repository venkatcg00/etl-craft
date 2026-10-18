"""Scoped run controls: ``stop``, ``abort``, ``hold`` and ``release`` share one implementation."""

from __future__ import annotations

import argparse
from functools import partial

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
from etl_craft.services.operations.controls import ControlRequest, request_control

HELP = {
    ControlAction.STOP: (
        "Stop the selected run or task gracefully: running attempts finish, nothing new starts.",
        "the run or task to stop",
    ),
    ControlAction.ABORT: (
        "Abort the selected run or task: stop its task processes now; commits are kept.",
        "the run or task to abort",
    ),
    ControlAction.HOLD: (
        "Hold dispatch in the selected run or task; running attempts finish.",
        "the run or task to hold",
    ),
    ControlAction.RELEASE: (
        "Release a hold on exactly the run or task that was held.",
        "the run or task to release",
    ),
}


def _configure(parser: argparse.ArgumentParser, *, action: ControlAction) -> None:
    configure_output(parser)
    configure_run_selector(parser)
    parser.add_argument("--pipeline_code", required=True, help=f"the pipeline of {HELP[action][1]}")
    parser.add_argument("--task_code", help="only this task under the selected run")
    parser.add_argument("--request-key", help="idempotency key for this exact action and scope")
    parser.add_argument("--reason", required=True, help="why; recorded with the control")


def _request(args: argparse.Namespace, action: ControlAction) -> ControlRequest:
    return ControlRequest(
        args.pipeline_code,
        action,
        args.reason,
        RunSelector(args.run_id, args.run_key),
        args.task_code,
        args.request_key,
    )


def _run(args: argparse.Namespace, out: Output, *, action: ControlAction) -> int:
    with command_context(args) as ctx:
        done = request_control(ctx, _request(args, action))
    out.result(done, args.output_format)
    return ExitCode.SUCCESS


def _remote(
    args: argparse.Namespace, out: Output, server: ServerClient, *, action: ControlAction
) -> int:
    done = server.call(
        "POST",
        api_path("pipelines", args.pipeline_code, "controls"),
        ControlView,
        body={
            "action": action,
            "reason": args.reason,
            **selector_query(args),
            "task_code": args.task_code,
            "request_key": args.request_key,
        },
    )
    out.result(done, args.output_format)
    return ExitCode.SUCCESS


def command(action: ControlAction) -> Command:
    """Register one control verb; its name is the action in lower case."""
    return Command(
        action.lower(),
        HELP[action][0],
        run=partial(_run, action=action),
        configure=partial(_configure, action=action),
        remote=partial(_remote, action=action),
    )
