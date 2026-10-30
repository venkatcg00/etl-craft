"""What the web console needs from a command-line runner, without importing the CLI.

The CLI builds the runner (it owns the parser and the commands); the server only calls it.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from etl_craft.core.enums import ActionName
from etl_craft.services.operations.context import OperationContext


@dataclass(frozen=True)
class ConsoleResult:
    """What a command line printed and its exit code, as a terminal would show them."""

    exit_code: int
    output: str
    error: str
    actions: tuple[ActionName, ...] = ()


@dataclass(frozen=True)
class Console:
    """Run one command line as a caller, and list the commands that caller may type."""

    run: Callable[[OperationContext, str], ConsoleResult]
    commands: Callable[[OperationContext], Mapping[str, tuple[str, ...]]]
    actions: Callable[[str], tuple[ActionName, ...]] = field(default=lambda line: ())
