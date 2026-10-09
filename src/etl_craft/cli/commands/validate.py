"""``etl-craft validate``: check every active pipeline's metadata and report every problem."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands.common import Command, command_context, configure_output
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode
from etl_craft.services.doctor import Status
from etl_craft.services.operations.diagnostics import validate_metadata


def _configure(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    parser.add_argument("--pipeline_code", help="check only this pipeline")


def _run(args: argparse.Namespace, out: Output) -> int:
    with command_context(args) as ctx:
        done = validate_metadata(ctx, args.pipeline_code)

    def render() -> None:
        for finding in done.findings:
            out.line(f"[{finding.status.value:<4}] {finding.where}: {finding.message}")
        failed = sum(1 for f in done.findings if f.status is Status.FAIL)
        out.line(
            f"checked {done.pipelines} pipeline(s) and {done.tasks} task(s): {failed} failed, "
            f"{len(done.findings) - failed} warning(s)"
        )

    out.result(done, args.output_format, text=render)
    return ExitCode.FAILURE if done.failed else ExitCode.SUCCESS


COMMAND = Command(
    name="validate",
    help="Check every active pipeline's tasks, parameters and dependencies; exit 1 on a problem.",
    configure=_configure,
    run=_run,
)
