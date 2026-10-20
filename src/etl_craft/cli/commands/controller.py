"""Run the local controller until interrupted."""

from __future__ import annotations

import argparse
import signal
import threading
from importlib.util import find_spec
from types import FrameType

from etl_craft.cli.commands.common import Command, command_context, on_host
from etl_craft.cli.output import Output
from etl_craft.controller.supervisor import serve
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations.actions import admitted
from etl_craft.services.operations.requests import NoInput


def _configure(parser: argparse.ArgumentParser) -> None:
    pass


def _run(args: argparse.Namespace, out: Output) -> int:
    on_host(args, ActionName.CONTROLLER)
    stop = threading.Event()

    def stopping(signum: int, frame: FrameType | None) -> None:
        stop.set()

    previous = {
        number: signal.signal(number, stopping) for number in (signal.SIGTERM, signal.SIGINT)
    }
    try:
        with (
            command_context(args, role="controller") as ctx,
            admitted(ctx, ActionName.CONTROLLER, NoInput()),
        ):
            if find_spec("fastapi") is None or find_spec("uvicorn") is None:
                serve(ctx, stop)
            else:
                from etl_craft.api.serve import serve_http

                serve_http(ctx, stop)
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)
    return ExitCode.SUCCESS


COMMAND = Command(
    "controller",
    "Schedule and supervise pipeline runs until stopped.",
    run=_run,
    configure=_configure,
    actions=(ActionName.CONTROLLER,),
)
