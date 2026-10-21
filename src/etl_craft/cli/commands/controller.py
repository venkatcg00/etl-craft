"""Run the local controller until interrupted."""

from __future__ import annotations

import argparse
from importlib.util import find_spec

from etl_craft.cli.commands.common import Command, command_context, on_host
from etl_craft.cli.lifecycle import shutdown_event
from etl_craft.cli.output import Output
from etl_craft.controller.health import serve_health
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.requests import NoInput


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--no-ui", action="store_true", help="Serve only /health, without the API.")


def _run(args: argparse.Namespace, out: Output) -> int:
    on_host(args, ActionName.CONTROLLER)
    with shutdown_event() as stop, command_context(args, role="controller") as ctx:
        invoke(ctx, ActionName.CONTROLLER, NoInput())
        if args.no_ui or find_spec("fastapi") is None or find_spec("uvicorn") is None:
            serve_health(ctx, stop)
        else:
            from etl_craft.api.serve import serve_http

            serve_http(ctx, stop)
    return ExitCode.SUCCESS


COMMAND = Command(
    "controller",
    "Schedule and supervise pipeline runs until stopped.",
    run=_run,
    configure=_configure,
    actions=(ActionName.CONTROLLER,),
)
