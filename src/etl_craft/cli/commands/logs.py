"""``etl-craft logs``: one attempt's log, from this project or a server."""

from __future__ import annotations

import argparse
import codecs
from collections.abc import Iterable

from etl_craft.cli.commands.common import Command, command_context
from etl_craft.cli.output import Output
from etl_craft.cli.remote import ServerClient, api_path
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations import api_reads


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--attempt-id", type=int, required=True, help="the attempt whose log")
    parser.add_argument(
        "--offset", type=int, default=0, help="start this many bytes into the log (default: 0)"
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    with command_context(args) as ctx:
        _write(out, api_reads.attempt_log(ctx, args.attempt_id, args.offset))
    return ExitCode.SUCCESS


def _remote(args: argparse.Namespace, out: Output, server: ServerClient) -> int:
    path = api_path("attempts", str(args.attempt_id), "log")
    _write(out, server.stream(path, {"offset": args.offset}))
    return ExitCode.SUCCESS


def _write(out: Output, chunks: Iterable[bytes]) -> None:
    """Decode across chunk boundaries, so a character split between two chunks stays whole."""
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    for chunk in chunks:
        out.stdout.write(decoder.decode(chunk))
    out.stdout.write(decoder.decode(b"", final=True))


COMMAND = Command(
    "logs", "Print one attempt's log.", run=_run, configure=_configure, remote=_remote
)
