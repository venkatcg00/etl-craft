"""``etl-craft init-db``: create the Engine DB schema in an empty database."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands.common import Command, call, request_from
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName
from etl_craft.engine.schema import InitResult
from etl_craft.services.operations.maintenance import InitInput


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--force",
        action="store_true",
        help="Apply the schema even though Engine DB tables already exist.",
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    result = call(args, ActionName.INIT_DB, request_from(args, InitInput))
    assert isinstance(result, InitResult)
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
    actions=(ActionName.INIT_DB,),
)
