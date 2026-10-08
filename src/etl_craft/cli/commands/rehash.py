"""``etl-craft rehash``: upgrade every row's stored change hash in one target."""

import argparse

from etl_craft.cli.commands.common import Command, connect_engine_db, load_command_config
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode
from etl_craft.services.rehash import rehash


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--target", required=True, help="schema.table (or database.schema.table)")
    parser.add_argument(
        "--dry-run", action="store_true", help="validate and show the UPDATE without writing"
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    config = load_command_config(args)
    engine = connect_engine_db(config)
    try:
        result = rehash(engine, config, args.target, dry_run=args.dry_run)
    finally:
        engine.dispose()
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
