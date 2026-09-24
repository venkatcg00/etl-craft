"""``etl-craft init-db``: create the Engine DB schema in an empty database."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands import Command
from etl_craft.cli.commands.common import connect_engine_db, load_command_config
from etl_craft.cli.output import Output
from etl_craft.engine.schema import init_db


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--force",
        action="store_true",
        help="apply the schema even though Engine DB tables already exist",
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    engine = connect_engine_db(load_command_config(args))
    try:
        result = init_db(engine, force=args.force)
    finally:
        engine.dispose()
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
