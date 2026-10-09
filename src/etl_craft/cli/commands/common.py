"""What every command that reads craft-connector.yml does first."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass

from sqlalchemy.engine import Engine

from etl_craft.cli.output import Output
from etl_craft.config import ConnectorConfig, load_config, resolve_config_path
from etl_craft.core.actor import current_actor
from etl_craft.engine.connection import check_reachable, engine_db
from etl_craft.execution.runner import ChildOptions
from etl_craft.services.operations import OperationContext

logger = logging.getLogger(__name__)


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
    run: Callable[[argparse.Namespace, Output], int]
    configure: Callable[[argparse.ArgumentParser], None] | None = None


def load_command_config(args: argparse.Namespace) -> ConnectorConfig:
    """Find and load craft-connector.yml: ``--config``, ``$ETL_CRAFT_CONFIG``, or a search."""
    path = resolve_config_path(args.config)
    logger.debug("reading %s", path)
    return load_config(path)


def connect_engine_db(config: ConnectorConfig) -> Engine:
    """Build the Engine DB engine and check it, and its schema, can be used."""
    engine = engine_db(config)
    try:
        check_reachable(engine, config.engine.schema)
    except BaseException:
        engine.dispose()
        raise
    return engine


def configure_run_selector(parser: argparse.ArgumentParser) -> None:
    """Add mutually exclusive identities for a pipeline's run."""
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--run-id", type=int, help="the exact pipeline run id")
    selection.add_argument("--run-key", help="the exact run key within this pipeline")


def configure_output(parser: argparse.ArgumentParser) -> None:
    """Offer one schema-versioned operation document instead of text output."""
    parser.add_argument(
        "--format",
        dest="output_format",
        choices=("text", "json"),
        default="text",
        help="result format (default: text)",
    )


@contextmanager
def command_context(args: argparse.Namespace) -> Iterator[OperationContext]:
    """Load command resources and dispose them after the service operation."""
    config = load_command_config(args)
    engine = connect_engine_db(config)
    try:
        yield OperationContext(
            engine,
            config,
            current_actor(),
            ChildOptions(log_level=args.log_level, log_format=args.log_format),
        )
    finally:
        engine.dispose()
