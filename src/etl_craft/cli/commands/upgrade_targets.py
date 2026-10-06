"""``upgrade-targets``: add execution identities without changing historical rows."""

import argparse

from etl_craft.cli.commands import Command
from etl_craft.cli.commands.common import connect_engine_db, load_command_config
from etl_craft.cli.output import Output
from etl_craft.core.enums import SqlAction
from etl_craft.core.errors import ExitCode
from etl_craft.services.upgrade_targets import upgrade_targets


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--action",
        choices=[str(kind) for kind in SqlAction if kind != SqlAction.DROP_TABLE],
        help="filter by SQL action; defaults to configured SQL and ingestion targets",
    )
    parser.add_argument(
        "--target",
        help="one schema.table (or database.schema.table); defaults to all selected targets",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="validate targets and show additions without writing warehouse rows",
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    config = load_command_config(args)
    engine = connect_engine_db(config)
    try:
        results = upgrade_targets(
            engine, config, action=args.action, target=args.target, dry_run=args.dry_run
        )
    finally:
        engine.dispose()
    if not results:
        out.line("No active configured targets to upgrade")
    for result in results:
        if not result.changed:
            out.line(f"{result.target}: identity columns already exist")
        elif result.dry_run:
            columns = ", ".join(f"{name} BIGINT" for name in result.columns)
            out.line(f"{result.target}: would add nullable {columns}")
            assert result.sql is not None
            out.line(result.sql)
        else:
            columns = ", ".join(f"{name} BIGINT" for name in result.columns)
            out.line(f"{result.target}: added nullable {columns}; historical rows retain NULL")
    return ExitCode.SUCCESS


COMMAND = Command(
    "upgrade-targets",
    "Add pipeline and task-run identities to existing targets.",
    _configure,
    _run,
)
