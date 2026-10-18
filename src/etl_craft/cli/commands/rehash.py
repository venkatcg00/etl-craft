"""``etl-craft rehash``: upgrade every row's stored change hash in one target."""

import argparse

from etl_craft.cli.commands.common import Command, command_context
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations import maintenance


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--target", required=True, help="schema.table (or database.schema.table)")
    parser.add_argument(
        "--dry-run", action="store_true", help="validate and show the UPDATE without writing"
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    with command_context(args) as ctx:
        result = maintenance.rehash_target(ctx, args.target, dry_run=args.dry_run)
    if result.dry_run:
        out.line(f"{result.target}: would rehash {result.rows} row(s) with hash version 2")
        out.line(result.sql)
    else:
        out.line(f"{result.target}: rehashed {result.rows} row(s) with hash version 2")
    return ExitCode.SUCCESS


COMMAND = Command(
    name="rehash",
    help="Upgrade an existing merge target to hash version 2.",
    configure=_configure,
    run=_run,
)
