"""Require every collected test to belong to a release suite (or to the service harness).

The release gate reads evidence per suite, so a test without a suite marker would run in no
suite and never count towards a release. Collection stops with a usage error that lists the
unmarked tests.
"""

from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

SUITES_FILE = Path(__file__).resolve().parents[2] / "release" / "required-suites.toml"
EXTRA_MARKERS = frozenset({"harness"})


def suite_markers(path: Path = SUITES_FILE) -> frozenset[str]:
    """Return the markers that place a test in a suite."""
    with path.open("rb") as handle:
        suites = tomllib.load(handle)["suites"]
    return frozenset(str(spec["marker"]) for spec in suites.values()) | EXTRA_MARKERS


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    markers = suite_markers()
    unmarked = [
        item.nodeid
        for item in items
        if not any(mark.name in markers for mark in item.iter_markers())
    ]
    if unmarked:
        raise pytest.UsageError(
            "every test needs a suite marker from release/required-suites.toml (or harness); "
            "unmarked: " + ", ".join(unmarked)
        )
