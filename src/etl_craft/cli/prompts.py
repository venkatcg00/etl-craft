"""Interactive credentials never enter argument namespaces or command audit payloads."""

from __future__ import annotations

import getpass
import sys
from contextvars import ContextVar
from typing import TYPE_CHECKING

from etl_craft.core.errors import UsageError

if TYPE_CHECKING:
    from etl_craft.services.operations import OperationContext

CONSOLE: ContextVar[OperationContext | None] = ContextVar("etl_craft_console", default=None)
"""The signed-in caller while the web console runs a command line; ``None`` in a terminal."""


def can_prompt() -> bool:
    """Whether a person at this process's terminal can answer a question; never in the console."""
    return CONSOLE.get() is None and sys.stdin.isatty()


def interactive() -> None:
    """Provisioning and password entry require the local person's terminal."""
    if CONSOLE.get() is not None:
        raise UsageError(
            "this command asks for a password, which the web console never reads; "
            "run it with the etl-craft CLI in your own terminal"
        )
    if not sys.stdin.isatty():
        raise UsageError(
            "interactive terminal required; sign in locally or use ETL_CRAFT_TOKEN for CI"
        )


def password(*, confirm: bool = False) -> str:
    """Read a password without echoing it, and confirm new passwords."""
    interactive()
    value = getpass.getpass("Password: ")
    if confirm and value != getpass.getpass("Confirm password: "):
        raise UsageError("password confirmation differs; enter the same password twice")
    return value
