"""``etl-craft clone``: copy the Engine DB tables into the warehouse now, as a run's end does."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands.common import Command, call
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName
from etl_craft.services.operations.maintenance import Clones
from etl_craft.services.operations.requests import NoInput


def _run(args: argparse.Namespace, out: Output) -> int:
    done = call(args, ActionName.CLONE, NoInput())
    assert isinstance(done, Clones)
    for table in done.tables:
        change = "\tcreated" if table.created else ""
        if table.added:
            change = f"\tadded {', '.join(table.added)}"
        out.line(f"{table.table}\t{table.mirror}\t{table.rows}{change}")
    return 0


COMMAND = Command(
    name="clone",
    help="Copy the Engine DB tables Cloning.Scope names into the warehouse schema now.",
    run=_run,
    actions=(ActionName.CLONE,),
)
