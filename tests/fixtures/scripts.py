"""Import the modules under scripts/ so tests can call them directly."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import ModuleType

SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"


def load(name: str) -> ModuleType:
    """Import ``scripts/<name>.py``; its sibling imports resolve as they do when run directly."""
    if str(SCRIPTS_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPTS_DIR))
    return importlib.import_module(name)
