"""Run ``etl-craft`` command lines typed in the web console, as the signed-in person.

The console is the CLI with the server's caller: the same parser, the same commands and the
same text, calling each action as that person (``prompts.CONSOLE``). It lists only commands
whose every action has an API route, edits no metadata files and needs no sign-in, and that
the person's account role or pipeline grants can run; the action check still decides each call.
Nothing reads or writes a file on the server and nothing prompts.
"""

from __future__ import annotations

import argparse
import io
import shlex
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import NoReturn

from etl_craft.api.console import Console, ConsoleResult
from etl_craft.cli import as_flags, build_parser
from etl_craft.cli.commands import COMMANDS, Command
from etl_craft.cli.output import Output
from etl_craft.cli.prompts import CONSOLE
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.core.errors import EtlCraftError, ExitCode, UsageError
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.actions import ACTIONS, has_role
from etl_craft.services.operations.permissions import pipeline_access

KEPT_IN_FILES = frozenset(
    {
        ActionName.CONFIG_PLAN,
        ActionName.CONFIG_EXPORT,
        ActionName.CONFIG_APPLY,
        ActionName.RENAME,
    }
)
"""Metadata stays in the project's config files and CI, never typed into the console."""

SIGN_IN = frozenset(
    {
        ActionName.LOGIN,
        ActionName.LOGIN_METHODS,
        ActionName.LOGIN_SSO,
        ActionName.LOGIN_SSO_POLL,
        ActionName.REFRESH,
        ActionName.LOGOUT,
    }
)
"""The console runs inside the browser session; sign-in commands belong to a terminal."""

GLOBAL_REFUSED = ("--api", "--config")


class ConsoleParser(argparse.ArgumentParser):
    """The CLI's parser, raising its usage errors instead of printing them and exiting."""

    def error(self, message: str) -> NoReturn:
        """Raise the message for the console to print with the usage exit code."""
        raise UsageError(f"{self.prog}: {message}")


def console() -> Console:
    """Return the runner the web server calls."""
    return Console(run_line, listed, line_actions)


def _reachable(command: Command) -> bool:
    return bool(command.actions) and all(
        ACTIONS[name].path is not None
        and name not in KEPT_IN_FILES | SIGN_IN
        and ACTIONS[name].role != ApiRole.AGENT
        for name in command.actions
    )


def _best_role(caller: OperationContext) -> ApiRole | None:
    """Return the highest role the caller holds: the account's, or one a pipeline grant gives."""
    with caller.engine.connect() as conn:
        granted = [role for role in pipeline_access(conn, caller).roles.values() if role]
    roles = [role for role in (caller.role, *granted) if role is not None]
    return max(roles, key=tuple(ApiRole).index, default=None)


def allowed(caller: OperationContext) -> tuple[Command, ...]:
    """Return the console commands at least one of whose actions the caller may run."""
    role = _best_role(caller)
    return tuple(
        command
        for command in COMMANDS
        if _reachable(command)
        and any(has_role(role, ACTIONS[name].role) for name in command.actions)
    )


def listed(caller: OperationContext) -> Mapping[str, tuple[str, ...]]:
    """Map each command the caller may type to its flags, for completion and the command list."""
    parser = build_parser(allowed(caller), ConsoleParser)
    return {
        name: tuple(
            sorted(
                flag
                for action in sub._actions
                for flag in action.option_strings
                if flag.startswith("--") and flag not in (*GLOBAL_REFUSED, "--help")
            )
        )
        for name, sub in _subparsers(parser).items()
    }


def line_actions(line: str) -> tuple[ActionName, ...]:
    """Return the actions a line's command may call, for refusal records; empty if unknown."""
    try:
        words = _words(line)
    except UsageError:
        return ()
    command = next((c for c in COMMANDS if words and c.name == words[0]), None)
    return () if command is None else tuple(command.actions)


def run_line(caller: OperationContext, line: str) -> ConsoleResult:
    """Parse ``line`` with the CLI's parser and run it as ``caller``.

    An etl-craft error is printed as the CLI prints it, with its exit code.
    """
    out, err = io.StringIO(), io.StringIO()
    output = Output(out, err)
    parser = build_parser(allowed(caller), ConsoleParser)
    try:
        words = _words(line)
        if not words:
            raise UsageError("type an etl-craft command, for example `pipelines` or `status`")
        if words[-1] in ("-h", "--help") or words == ["help"]:
            return ConsoleResult(ExitCode.SUCCESS, _help(parser, words), "")
        args = _parse(parser, words)
        _refuse_files(parser, args, caller)
        token = CONSOLE.set(caller)
        try:
            code = int(args.handler(args, output))
        finally:
            CONSOLE.reset(token)
    except EtlCraftError as error:
        output.error(as_flags(str(error), parser))
        code = error.exit_code
    return ConsoleResult(code, out.getvalue(), err.getvalue(), line_actions(line))


def _words(line: str) -> list[str]:
    try:
        words = shlex.split(line)
    except ValueError as error:
        raise UsageError(f"cannot read the line: {error}; close the quote") from error
    return words[1:] if words[:1] == ["etl-craft"] else words


def _subparsers(parser: argparse.ArgumentParser) -> dict[str, argparse.ArgumentParser]:
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return dict(action.choices)
    return {}


def _help(parser: argparse.ArgumentParser, words: Sequence[str]) -> str:
    sub = _subparsers(parser).get(words[0]) if words and words[0] not in ("help", "-h") else None
    return (sub or parser).format_help()


def _parse(parser: argparse.ArgumentParser, words: Sequence[str]) -> argparse.Namespace:
    """Parse as the CLI does; an unknown command names what the console runs instead."""
    if words[0] not in _subparsers(parser):
        raise UsageError(
            f"`{words[0]}` is not a command you can run in the console; type `help` for the list. "
            "Host commands (controller, migrate, ui), config files and sign-in use the CLI"
        )
    return parser.parse_args(list(words))


def _every(parser: argparse.ArgumentParser) -> list[argparse.ArgumentParser]:
    """Return the parser and every nested subcommand parser."""
    found = [parser]
    for sub in _subparsers(parser).values():
        found += _every(sub)
    return found


def _refuse_files(
    parser: argparse.ArgumentParser, args: argparse.Namespace, caller: OperationContext
) -> None:
    """Refuse server files, other servers and other projects; the console runs here, as you."""
    for flag in GLOBAL_REFUSED:
        if getattr(args, flag.removeprefix("--"), None) is not None:
            raise UsageError(f"`{flag}` is not used in the console: it runs on this server")
    if args.project is not None and args.project != caller.project:
        raise UsageError(
            f"`--project {args.project}`: this console runs in project {caller.project!r}; "
            "switch projects in the top bar"
        )
    sub = _subparsers(parser)[args.command]
    for action in (a for each in _every(sub) for a in each._actions):
        if action.type is Path and getattr(args, action.dest, None) is not None:
            raise UsageError(
                f"`{max(action.option_strings, key=len)}` reads or writes a file on the server; "
                "run this command with the etl-craft CLI on your own machine"
            )
