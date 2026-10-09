"""Run the local overseer until interrupted."""

from __future__ import annotations

import argparse
import signal
import threading
from types import FrameType

from etl_craft.cli.commands.common import Command, command_context
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode
from etl_craft.overseer.server import serve


def _configure(parser: argparse.ArgumentParser) -> None:
    pass


def _run(args: argparse.Namespace, out: Output) -> int:
    stop = threading.Event()

    def stopping(signum: int, frame: FrameType | None) -> None:
        stop.set()

    previous = {
        number: signal.signal(number, stopping) for number in (signal.SIGTERM, signal.SIGINT)
    }
    try:
        with command_context(args) as ctx:
            serve(ctx, stop)
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)
    return ExitCode.SUCCESS


COMMAND = Command("server", "supervise active local pipeline runs", run=_run, configure=_configure)
