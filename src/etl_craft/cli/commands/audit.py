"""Read command requests and metadata changes without changing the Engine DB."""

from __future__ import annotations

import argparse
import json
from datetime import datetime

from etl_craft.cli.commands.common import Command, command_context, configure_output
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations import inspect


def _configure(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    parser.add_argument("--pipeline_code", help="filter requests and related metadata rows")
    parser.add_argument(
        "--since", type=datetime.fromisoformat, help="changes since an ISO date or timestamp"
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    with command_context(args) as ctx:
        done = inspect.audit(
            ctx,
            args.pipeline_code,
            since=args.since,
        )

    def render_text() -> None:
        out.rows([("AT", "ACTOR", "KIND", "COMMAND", "OUTCOME", "ARGUMENTS")])
        out.rows(
            (row.at, row.actor, row.kind, row.command, row.outcome, json.dumps(row.arguments))
            for row in done.actions
        )
        out.rows(
            [("AT", "ACTOR", "KIND", "TABLE", "ROW", "OPERATION", "BEFORE", "AFTER", "MIGRATION")]
        )
        out.rows(
            (
                row.at,
                row.actor,
                row.kind,
                row.table_name,
                json.dumps(row.row_key),
                row.operation,
                None if row.before_json is None else json.dumps(row.before_json),
                None if row.after_json is None else json.dumps(row.after_json),
                row.migration,
            )
            for row in done.changes
        )
        return

    out.result(done, args.output_format, text=render_text)
    return ExitCode.SUCCESS


COMMAND = Command(
    "audit",
    "Show command requests and metadata changes with their actors.",
    configure=_configure,
    run=_run,
)
