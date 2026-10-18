"""Request a durable scoped abort or graceful stop with ``etl-craft cancel``."""

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
from etl_craft.core.enums import ControlAction
from etl_craft.core.errors import ExitCode
from etl_craft.engine.controls import ControlView
from etl_craft.engine.runlog import RunSelector
from etl_craft.services.operations import runs
from etl_craft.services.operations.controls import ControlRequest, request_control


def _configure(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    configure_run_selector(parser)
    parser.add_argument("--pipeline_code", required=True, help="the pipeline whose run to cancel")
    parser.add_argument("--task_code", help="stop only this task under the selected run")
    parser.add_argument(
        "--stop", action="store_true", help="drain running work instead of aborting it"
    )
    parser.add_argument("--request-key", help="idempotency key for this exact action and scope")
    parser.add_argument("--reason", required=True, help="why; recorded with the change")


def _run(args: argparse.Namespace, out: Output) -> int:
    with command_context(args) as ctx:
        if not args.stop and args.task_code is None and args.request_key is None:
            done = runs.cancel_run(
                ctx,
                args.pipeline_code,
                args.reason,
                selector=RunSelector(args.run_id, args.run_key),
            )
        else:
            done = request_control(
                ctx,
                ControlRequest(
                    args.pipeline_code,
                    ControlAction.STOP if args.stop else ControlAction.ABORT,
                    args.reason,
                    RunSelector(args.run_id, args.run_key),
                    args.task_code,
                    args.request_key,
                ),
            )
    out.result(done, args.output_format)
    return ExitCode.SUCCESS


def _remote(args: argparse.Namespace, out: Output, server: ServerClient) -> int:
    if not args.stop and args.task_code is None and args.request_key is None:
        done = server.call(
            "POST",
            api_path("pipelines", args.pipeline_code, "cancel"),
            ControlView,
            body={"reason": args.reason, **selector_query(args)},
        )
    else:
        done = server.call(
            "POST",
            api_path("pipelines", args.pipeline_code, "controls"),
            ControlView,
            body={
                "reason": args.reason,
                **selector_query(args),
                "task_code": args.task_code,
                "request_key": args.request_key,
                "action": ControlAction.STOP if args.stop else ControlAction.ABORT,
            },
        )
    out.result(done, args.output_format)
    return ExitCode.SUCCESS


COMMAND = Command(
    name="cancel",
    help="Request a scoped abort, or drain with --stop; inspect durable acknowledgements.",
    configure=_configure,
    run=_run,
    remote=_remote,
)
