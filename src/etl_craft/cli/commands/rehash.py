"""``etl-craft rehash``: upgrade every row's stored change hash in one target."""

import argparse

from etl_craft.cli.commands.common import Command, call, request_from
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations.maintenance import RehashInput, RehashResult


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--target", required=True, help="The target, as schema.table or database.schema.table."
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Validate and show the UPDATE without writing."
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    name = ActionName.REHASH_PLAN if args.dry_run else ActionName.REHASH
    result = call(args, name, request_from(args, RehashInput))
    assert isinstance(result, RehashResult)
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
    actions=(ActionName.REHASH, ActionName.REHASH_PLAN),
)
