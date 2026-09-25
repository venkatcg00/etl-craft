"""``etl-craft validate``: check every active pipeline's metadata and report every problem."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands import Command
from etl_craft.cli.commands.common import connect_engine_db, load_command_config
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode
from etl_craft.services.doctor import Status
from etl_craft.services.validate import validate


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pipeline_code", help="check only this pipeline")


def _run(args: argparse.Namespace, out: Output) -> int:
    config = load_command_config(args)
    engine = connect_engine_db(config)
    try:
        report = validate(engine, config, args.pipeline_code)
    finally:
        engine.dispose()
    for finding in report.findings:
        out.line(f"[{finding.status.value:<4}] {finding.where}: {finding.message}")
    failed = sum(1 for f in report.findings if f.status is Status.FAIL)
    out.line(
        f"checked {report.pipelines} pipeline(s) and {report.tasks} task(s): {failed} failed, "
        f"{len(report.findings) - failed} warning(s)"
    )
    return ExitCode.FAILURE if report.failed else ExitCode.SUCCESS


COMMAND = Command(
    name="validate",
    help="Check every active pipeline's tasks, parameters and dependencies; exit 1 on a problem.",
    configure=_configure,
    run=_run,
)
