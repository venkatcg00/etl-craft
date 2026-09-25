"""``etl-craft setup``: check the configuration, then create or migrate the Engine DB."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands import Command
from etl_craft.cli.commands.common import load_command_config
from etl_craft.cli.commands.doctor import report
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode
from etl_craft.services.setup import setup


def _configure(parser: argparse.ArgumentParser) -> None:
    del parser


def _run(args: argparse.Namespace, out: Output) -> int:
    result = setup(load_command_config(args))
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
