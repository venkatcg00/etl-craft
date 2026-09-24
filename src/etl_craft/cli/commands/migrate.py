"""``etl-craft migrate``: apply pending migrations to an existing Engine DB."""

from __future__ import annotations

import argparse
from pathlib import Path

from etl_craft.cli.commands import Command
from etl_craft.cli.commands.common import connect_engine_db, load_command_config
from etl_craft.cli.output import Output
from etl_craft.engine.migrations import apply_pending_migrations


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--migrations-dir",
        type=Path,
        metavar="PATH",
        help="the project's own *.sql migrations (default: $ETL_CRAFT_MIGRATIONS_DIR, then "
        "./sql/migrations if it exists)",
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    engine = connect_engine_db(load_command_config(args))
    try:
        applied = apply_pending_migrations(engine, args.migrations_dir)
    finally:
        engine.dispose()
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
