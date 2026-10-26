"""``etl-craft migrate``: apply pending migrations to an existing Engine DB."""

from __future__ import annotations

import argparse
from pathlib import Path

from etl_craft.cli.commands.common import Command, call, request_from
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName
from etl_craft.services.operations.maintenance import MigrateInput


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--migrations-dir",
        type=Path,
        metavar="PATH",
        help="The project's own *.sql migrations (default: $ETL_CRAFT_MIGRATIONS_DIR, then "
        "migrations/ in the project directory if it exists).",
    )
    parser.add_argument(
        "--project-config",
        dest="project_configs",
        type=Path,
        action="append",
        default=[],
        metavar="PATH",
        help="Connector config for another project sharing this Engine DB; "
        "repeat for each project's warehouse dialect (paths are on the executing host).",
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    applied = call(
        args,
        ActionName.MIGRATE,
        request_from(args, MigrateInput, project_configs=tuple(args.project_configs)),
    )
    assert isinstance(applied, list)
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
    actions=(ActionName.MIGRATE,),
)
