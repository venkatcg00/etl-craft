"""The ``etl-craft`` commands.

Each command module defines one ``Command`` and is listed in ``COMMANDS``, which sets the order
they appear in ``etl-craft --help``.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable
from dataclasses import dataclass

from etl_craft.cli.output import Output


@dataclass(frozen=True)
class Command:
    """One subcommand: its name, its one-line help, its options and what it does.

    ``configure`` adds the command's own options to its parser. ``run`` receives the parsed
    arguments, including the global ``config``, ``log_level`` and ``log_format``, and returns
    the exit code. It reports failures by raising an ``EtlCraftError``, which the command line
    turns into an ``error:`` line and that error's exit code.
    """

    name: str
    help: str
    configure: Callable[[argparse.ArgumentParser], None]
    run: Callable[[argparse.Namespace, Output], int]


def _commands() -> tuple[Command, ...]:
    from etl_craft.cli.commands import (
        docs_version,
        generate_yml,
        graph,
        history,
        init_db,
        lineage,
        list_pipelines,
        migrate,
        run,
        steps,
    )

    return (
        run.COMMAND,
        list_pipelines.COMMAND,
        graph.COMMAND,
        steps.COMMAND,
        history.COMMAND,
        lineage.COMMAND,
        docs_version.COMMAND,
        generate_yml.COMMAND,
        init_db.COMMAND,
        migrate.COMMAND,
    )


COMMANDS: tuple[Command, ...] = _commands()
