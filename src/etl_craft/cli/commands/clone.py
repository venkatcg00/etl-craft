"""``etl-craft clone``: copy the Engine DB tables into the warehouse now, as a run's end does."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands.common import Command, command_context
from etl_craft.cli.output import Output
from etl_craft.services.operations import maintenance


def _run(args: argparse.Namespace, out: Output) -> int:
    with command_context(args) as ctx:
        tables = maintenance.clone_tables(ctx)
    for table in tables:
        change = "\tcreated" if table.created else ""
        if table.added:
            change = f"\tadded {', '.join(table.added)}"
        out.line(f"{table.table}\t{table.mirror}\t{table.rows}{change}")
    return 0


COMMAND = Command(
    name="clone",
    help="Copy the Engine DB tables Cloning.Scope names into the warehouse schema now.",
    run=_run,
)
