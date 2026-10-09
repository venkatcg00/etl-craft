"""``etl-craft setup``: check the configuration, then create or migrate the Engine DB."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands.common import Command, load_command_config
from etl_craft.cli.commands.doctor import report
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode
from etl_craft.engine.privileges import grants_sql
from etl_craft.services.setup import setup


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--print-grants",
        action="store_true",
        help="print PostgreSQL role grants without executing them",
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    config = load_command_config(args)
    if args.print_grants:
        if not config.engine.jdbc_url.startswith("jdbc:postgresql:"):
            out.line("SQLite uses file permissions; restrict the database file to its owner.")
        else:
            out.line(grants_sql(config.engine.schema or "public"))
        return ExitCode.SUCCESS
    result = setup(config)
    if report(out, result.checks):
        out.line("setup: nothing changed; fix the failed check(s) and run setup again")
        return ExitCode.FAILURE
    if result.created_tables:
        out.line("setup: created the Engine DB tables")
    for version in result.applied_migrations:
        out.line(f"setup: applied {version}")
    if not result.created_tables and not result.applied_migrations:
        out.line("setup: the Engine DB is already up to date")
    return ExitCode.SUCCESS


COMMAND = Command(
    name="setup",
    help="Check the configuration, then create or bring up to date the Engine DB tables.",
    configure=_configure,
    run=_run,
)
