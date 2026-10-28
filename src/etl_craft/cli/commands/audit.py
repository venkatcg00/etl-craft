"""Read command requests and metadata changes without changing the Engine DB."""

from __future__ import annotations

import argparse
import json
from datetime import datetime

from etl_craft.cli.commands.common import (
    Command,
    call,
    configure_output,
    configure_tags,
    request_from,
)
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations.models import AuditView
from etl_craft.services.operations.requests import AuditInput


def _configure(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    configure_tags(parser)
    parser.add_argument("--pipeline-code", help="Filter requests and related metadata rows.")
    parser.add_argument(
        "--since", type=datetime.fromisoformat, help="Changes since an ISO date or timestamp."
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    done = call(args, ActionName.AUDIT, request_from(args, AuditInput))
    assert isinstance(done, AuditView)

    def render_text() -> None:
        for resume in done.resumes:
            out.line(
                f"{resume.pipeline_code}: resumed by {resume.actor} at {resume.resumed_at}: "
                f"{resume.reason}"
            )
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
    actions=(ActionName.AUDIT,),
)
