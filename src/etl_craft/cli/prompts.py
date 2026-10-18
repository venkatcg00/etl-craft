"""Interactive credentials never enter argument namespaces or command audit payloads."""

import getpass
import sys

from etl_craft.core.errors import UsageError


def interactive() -> None:
    """Provisioning and password entry require the local person's terminal."""
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
