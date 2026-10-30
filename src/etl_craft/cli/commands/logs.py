"""``etl-craft logs``: one attempt's log, or a pipeline run's log, from this project or a server."""

from __future__ import annotations

import argparse
from collections.abc import Iterable

from etl_craft.cli.commands.common import Command, call, configure_run_selector, request_from
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import ExitCode, UsageError
from etl_craft.services.operations.requests import LogInput, RunSelection


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--attempt-id", type=int, help="The attempt whose log.")
    parser.add_argument(
        "--offset", type=int, default=0, help="Start this many bytes into the log (default: 0)."
    )
    parser.add_argument(
        "--pipeline-code", help="The pipeline whose run log, rendered from the audit tables."
    )
    configure_run_selector(parser)


def _run(args: argparse.Namespace, out: Output) -> int:
    if (args.attempt_id is None) == (args.pipeline_code is None):
        raise UsageError(
            "logs: give --attempt-id for one attempt's log, or --pipeline-code (with --run-id "
            "or --run-key, or the single active run) for the pipeline run log"
        )
    if args.attempt_id is not None:
        chunks = call(args, ActionName.LOGS, request_from(args, LogInput))
    else:
        chunks = call(args, ActionName.RUN_LOG, request_from(args, RunSelection))
    assert isinstance(chunks, Iterable)
    out.chunks(chunks)
    return ExitCode.SUCCESS


COMMAND = Command(
    "logs",
    "Print one attempt's log, or a pipeline run's log.",
    run=_run,
    configure=_configure,
    actions=(ActionName.LOGS, ActionName.RUN_LOG),
)
