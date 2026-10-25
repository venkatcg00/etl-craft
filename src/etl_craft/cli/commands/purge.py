"""Project history retention through the shared authorized operation."""

import argparse

from etl_craft.cli.commands.common import Command, call, configure_output, request_from
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations.retention import PurgeInput, PurgeView


def configure(parser: argparse.ArgumentParser) -> None:
    """Expose the item's read-only eligibility snapshot."""
    configure_output(parser)
    parser.add_argument("--dry-run", action="store_true", help="Report currently removable rows.")


def run(args: argparse.Namespace, out: Output) -> int:
    """Render the same result returned by the API."""
    result = call(args, ActionName.PURGE, request_from(args, PurgeInput))
    assert isinstance(result, PurgeView)
    out.result(
        result,
        args.output_format,
        text=lambda: (
            out.rows(result.counts.items())
            if result.counts
            else out.line("No retention periods configured; history is kept forever.")
        ),
    )
    return ExitCode.SUCCESS


COMMAND = Command(
    "purge",
    "Purge expired history for the selected project.",
    configure=configure,
    run=run,
    actions=(ActionName.PURGE,),
)
