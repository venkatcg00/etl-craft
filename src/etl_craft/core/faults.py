"""Named failure injection for development and process-lifecycle tests."""

from __future__ import annotations

import os

from etl_craft.core.errors import InjectedFaultError


def fault_point(name: str) -> None:
    """Raise at the selected point, or exit immediately for ``name:kill``; otherwise do nothing."""
    selected = os.environ.get("ETL_CRAFT_FAULT")
    if selected == name:
        raise InjectedFaultError(f"injected fault at {name}")
    if selected == f"{name}:kill":
        os._exit(137)
