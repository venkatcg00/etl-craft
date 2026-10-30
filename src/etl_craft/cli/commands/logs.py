"""``etl-craft logs``: one attempt's log, from this project or a server."""

from __future__ import annotations

import argparse
from collections.abc import Iterable

from etl_craft.cli.commands.common import Command, call, request_from
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations.requests import LogInput


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--attempt-id", type=int, required=True, help="The attempt whose log.")
    parser.add_argument(
        "--offset", type=int, default=0, help="Start this many bytes into the log (default: 0)."
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    chunks = call(args, ActionName.LOGS, request_from(args, LogInput))
    assert isinstance(chunks, Iterable)
    out.chunks(chunks)
    return ExitCode.SUCCESS


COMMAND = Command(
    "logs", "Print one attempt's log.", run=_run, configure=_configure, actions=(ActionName.LOGS,)
)
