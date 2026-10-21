"""The ``etl-craft`` command line."""

from __future__ import annotations

import argparse
import logging
import os
import re
from collections.abc import Sequence
from pathlib import Path

from etl_craft import __version__
from etl_craft.cli.authentication import access_context
from etl_craft.cli.commands import COMMANDS, Command
from etl_craft.cli.output import Output
from etl_craft.core import log
from etl_craft.core.errors import EtlCraftError, ExitCode

logger = logging.getLogger(__name__)

DEFAULT_LOG_LEVEL = "INFO"


def _global_options(*, defaults: bool) -> argparse.ArgumentParser:
    """Build the options every command accepts, before or after the command name.

    The copy attached to each subcommand has no defaults, so a value given before the command
    name is not overwritten by the subcommand's parser.
    """
    parser = argparse.ArgumentParser(add_help=False)
    group = parser.add_argument_group("global options")

    def default(value: object) -> object:
        return value if defaults else argparse.SUPPRESS

    group.add_argument(
        "--config",
        type=Path,
        metavar="PATH",
        help="The craft-connector.yml to read.",
        default=default(None),
    )
    group.add_argument(
        "--api",
        metavar="URL",
        help="Operate this etl-craft API instead of the local project (default: "
        "$ETL_CRAFT_API); https:// except to localhost.",
        default=default(None),
    )
    group.add_argument(
        "--log-level",
        type=str.upper,
        choices=log.LEVELS,
        help=f"Lowest level of log record written to stderr (default: {DEFAULT_LOG_LEVEL}).",
        default=default(DEFAULT_LOG_LEVEL),
    )
    group.add_argument(
        "--log-format",
        type=str.lower,
        choices=[member.value for member in log.LogFormat],
        help="Log records as text lines or JSON objects (default: text).",
        default=default(log.LogFormat.TEXT.value),
    )
    return parser


def build_parser(commands: Sequence[Command] = COMMANDS) -> argparse.ArgumentParser:
    """Build the top-level argument parser with one subcommand per entry in ``commands``."""
    parser = argparse.ArgumentParser(
        prog="etl-craft",
        description="Metadata-driven ETL orchestration engine.",
        parents=[_global_options(defaults=True)],
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"etl-craft {__version__}",
        help="Print the version and exit.",
    )
    if commands:
        subparsers = parser.add_subparsers(dest="command", metavar="COMMAND")
        shared = _global_options(defaults=False)
        for command in commands:
            subparser = subparsers.add_parser(
                command.name, help=command.help, description=command.help, parents=[shared]
            )
            if command.configure is not None:
                command.configure(subparser)
            subparser.set_defaults(handler=command.run)
    return parser


def main(argv: Sequence[str] | None = None, commands: Sequence[Command] = COMMANDS) -> int:
    """Run the command line and return its exit code.

    Returns 2 with the usage when no command is given. An ``EtlCraftError`` is written as an
    ``error:`` line and returns its own exit status; any other exception is a bug, logged with its
    traceback, and returns ``ExitCode.UNEXPECTED``.
    """
    parser = build_parser(commands)
    args = parser.parse_args(argv)
    out = Output()
    if getattr(args, "handler", None) is None:
        parser.print_usage(out.stdout)
        return ExitCode.USAGE
    log.configure(args.log_level, args.log_format)
    args.api = args.api or os.environ.get("ETL_CRAFT_API")
    try:
        if args.api:
            return int(args.handler(args, out))
        with access_context(args):
            return int(args.handler(args, out))
    except EtlCraftError as error:
        out.error(_as_flags(str(error), parser))
        return error.exit_code
    except Exception as error:
        logger.exception("unexpected error in etl-craft %s", args.command)
        out.error(f"unexpected {type(error).__name__}: {error}")
        return ExitCode.UNEXPECTED


def _options(parser: argparse.ArgumentParser) -> dict[str, str]:
    """Map every option's destination to its flag, through every subcommand."""
    found: dict[str, str] = {}
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            for sub in action.choices.values():
                found.update(_options(sub))
        elif action.option_strings:
            found.setdefault(action.dest, max(action.option_strings, key=len))
    return found


def _as_flags(message: str, parser: argparse.ArgumentParser) -> str:
    """Show each request field a message names in backticks as the option that sets it."""
    options = _options(parser)
    return re.sub(
        r"`([a-z_]+)`",
        lambda m: f"`{options[m.group(1)]}`" if m.group(1) in options else m.group(0),
        message,
    )
