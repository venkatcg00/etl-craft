"""Rename pipeline or task metadata without replacing its execution identity."""

import argparse

from etl_craft.cli.commands.common import Command, call, configure_output, request_from
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations.renames import RenameInput, RenameResult


def _configure(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    parser.add_argument("--pipeline", dest="pipeline_code", required=True)
    parser.add_argument("--task", dest="task_code")
    parser.add_argument("--to", required=True, help="The new unused code.")
    parser.add_argument("--reason", default="", help="Why; required in STRICT projects.")


def _run(args: argparse.Namespace, out: Output) -> int:
    result = call(args, ActionName.RENAME, request_from(args, RenameInput))
    assert isinstance(result, RenameResult)

    def render() -> None:
        out.line(f"Renamed {result.old_code} to {result.new_code}; identity retained.")
        out.line(result.message)
        for path in result.config_files:
            out.line(f"Update old-code references in {path}")

    out.result(result, args.output_format, text=render)
    return ExitCode.SUCCESS


COMMAND = Command(
    name="rename",
    help="Rename a pipeline or task, preserving identity and history.",
    configure=_configure,
    run=_run,
    actions=(ActionName.RENAME,),
)
