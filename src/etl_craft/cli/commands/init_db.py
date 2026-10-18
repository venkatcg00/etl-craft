"""``etl-craft init-db``: create the Engine DB schema in an empty database."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands.common import Command, command_context, provisioning
from etl_craft.cli.output import Output
from etl_craft.services.operations import maintenance


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--force",
        action="store_true",
        help="apply the schema even though Engine DB tables already exist",
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    with command_context(args) as ctx:
        result = maintenance.initialize(ctx, force=args.force, provisioning=provisioning())
    out.line(f"init-db: applied {result.statements} statement(s) from the packaged schema")
    if result.recorded_migrations:
        out.line(
            f"init-db: recorded {len(result.recorded_migrations)} packaged migration(s) as "
            "applied (the schema includes them)"
        )
    return 0


COMMAND = Command(
    name="init-db",
    help="Create the Engine DB schema in an empty database.",
    configure=_configure,
    run=_run,
)
