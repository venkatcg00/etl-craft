"""``etl-craft list``: the active pipelines."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands.common import Command, command_context, configure_output
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations import inspect


def _run(args: argparse.Namespace, out: Output) -> int:
    with command_context(args) as ctx:
        done = inspect.list_pipelines(ctx)

    def render_text() -> None:
        pipelines = done.pipelines
        if not pipelines:
            out.empty("no active pipelines")
            return
        header = ("PIPELINE_CODE", "PIPELINE_NAME", "REFRESH_TYPE", "RUN_SCHEDULE", "SLA_IN_HOURS")
        out.rows([(*header, "PAUSED")])
        out.rows(
            (
                p.pipeline_code,
                p.pipeline_name,
                p.refresh_type,
                p.run_schedule,
                p.sla_in_hours,
                p.paused.describe() if p.paused else "",
            )
            for p in pipelines
        )
        return

    out.result(done, args.output_format, text=render_text)
    return ExitCode.SUCCESS


COMMAND = Command(
    name="list",
    help="List the active pipelines.",
    configure=configure_output,
    run=_run,
)
