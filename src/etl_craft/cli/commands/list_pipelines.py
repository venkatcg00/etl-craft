"""``etl-craft list``: the active pipelines."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands import Command
from etl_craft.cli.commands.common import connect_engine_db, load_command_config
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode
from etl_craft.services.inspect import list_pipelines


def _run(args: argparse.Namespace, out: Output) -> int:
    engine = connect_engine_db(load_command_config(args))
    try:
        with engine.connect() as conn:
            pipelines = list_pipelines(conn)
    finally:
        engine.dispose()
    if not pipelines:
        out.empty("no active pipelines")
        return ExitCode.SUCCESS
    out.rows([("PIPELINE_CODE", "PIPELINE_NAME", "REFRESH_TYPE", "RUN_SCHEDULE", "SLA_IN_HOURS")])
    out.rows(
        (p.pipeline_code, p.pipeline_name, p.refresh_type, p.run_schedule, p.sla_in_hours)
        for p in pipelines
    )
    return ExitCode.SUCCESS


COMMAND = Command(
    name="list",
    help="List the active pipelines.",
    configure=lambda parser: None,
    run=_run,
)
