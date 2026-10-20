"""``etl-craft pipelines``: the active pipelines."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands.common import Command, call, configure_output
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations.models import PipelineListView
from etl_craft.services.operations.requests import NoInput


def _run(args: argparse.Namespace, out: Output) -> int:
    done = call(args, ActionName.PIPELINES, NoInput())
    assert isinstance(done, PipelineListView)

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
    name="pipelines",
    help="List the active pipelines.",
    configure=configure_output,
    run=_run,
    actions=(ActionName.PIPELINES,),
)
