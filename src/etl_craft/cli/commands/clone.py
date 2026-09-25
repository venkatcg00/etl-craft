"""``etl-craft clone``: copy the Engine DB tables into the warehouse now, as a run's end does."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands import Command
from etl_craft.cli.commands.common import connect_engine_db, load_command_config
from etl_craft.cli.output import Output
from etl_craft.core.errors import ConfigurationError
from etl_craft.services.cloning import clone


def _configure(parser: argparse.ArgumentParser) -> None:
    del parser


def _run(args: argparse.Namespace, out: Output) -> int:
    config = load_command_config(args)
    if not config.cloning.enabled:
        raise ConfigurationError(
            "Cloning is off in craft-connector.yml: set Cloning.Enabled: true and a Scope of cfg, "
            "aud or all"
        )
    engine = connect_engine_db(config)
    try:
        tables = clone(engine, config)
    finally:
        engine.dispose()
    for table in tables:
        change = "\tcreated" if table.created else ""
        if table.added:
            change = f"\tadded {', '.join(table.added)}"
        out.line(f"{table.table}\t{table.mirror}\t{table.rows}{change}")
    return 0


COMMAND = Command(
    name="clone",
    help="Copy the Engine DB tables Cloning.Scope names into the warehouse schema now.",
    configure=_configure,
    run=_run,
)
