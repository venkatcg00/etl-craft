"""``etl-craft migrate``: apply pending migrations to an existing Engine DB."""

from __future__ import annotations

import argparse
from pathlib import Path

from etl_craft.cli.commands.common import Command, command_context, provisioning
from etl_craft.cli.output import Output
from etl_craft.services.operations import maintenance


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--migrations-dir",
        type=Path,
        metavar="PATH",
        help="the project's own *.sql migrations (default: $ETL_CRAFT_MIGRATIONS_DIR, then "
        "migrations/ in the project directory if it exists)",
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    with command_context(args) as ctx:
        applied = maintenance.migrate(ctx, args.migrations_dir, provisioning=provisioning())
    if not applied:
        out.line("migrate: already up to date")
    for version in applied:
        out.line(f"applied {version}")
    return 0


COMMAND = Command(
    name="migrate",
    help="Apply the pending packaged and project migrations to the Engine DB.",
    configure=_configure,
    run=_run,
)
