"""Check that every stabilization defect and obvious-failure row maps to collected tests."""

from __future__ import annotations

import io
import json
import os
import re
import sys
import tempfile
import tomllib
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any

import pytest

from collect_suite import Collection

ROOT = Path(__file__).resolve().parents[1]


def assigned_defects(roadmap: str) -> set[str]:
    """Return defect ids assigned wholly or partly to the stabilization workstreams."""
    rows = re.findall(r"^\| (B\d+) \|[^\n]*\| ([^|]+) \|$", roadmap, re.MULTILINE)
    return {defect for defect, fixes in rows if "S2." in fixes}


def problems(expected: set[str], manifest: dict[str, Any], nodes: list[str]) -> list[str]:
    """Refuse missing defects, empty mappings and test names absent from collection."""
    defects = manifest.get("defects", {})
    if not isinstance(defects, dict):
        return ["regression manifest needs a defects table"]
    failures = []
    for defect in sorted(expected - defects.keys()):
        failures.append(f"{defect}: no regression mapping")
    for defect in sorted(defects.keys() - expected):
        failures.append(f"{defect}: not assigned to the stabilization release")
    for defect, references in defects.items():
        if not isinstance(references, list) or not references:
            failures.append(f"{defect}: needs at least one named test")
            continue
        for reference in references:
            if not isinstance(reference, str) or "::test_" not in reference:
                failures.append(f"{defect}: invalid test reference {reference!r}")
            elif not any(node == reference or node.startswith(reference + "[") for node in nodes):
                failures.append(f"{defect}: test not collected: {reference}")
    return failures


def catalog_problems(catalog: object, nodes: list[str]) -> list[str]:
    """Each obvious-failure row names collected tests, or "open: <item>" for its owner."""
    if not isinstance(catalog, dict) or not catalog:
        return ["regression manifest needs an obvious_failures table"]
    failures = []
    for row, entry in catalog.items():
        if isinstance(entry, str):
            if not re.fullmatch(r"open: S\d+\.[A-Z][\w.]*", entry):
                failures.append(f"{row}: expected tests or 'open: <item>', found {entry!r}")
            continue
        failures += problems({row}, {"defects": {row: entry}}, nodes)
    return failures


def main() -> int:
    """Collect the complete suite and verify the roadmap's defect coverage."""
    manifest = tomllib.loads((ROOT / "release" / "regressions.toml").read_text("utf-8"))
    expected = assigned_defects((ROOT / "docs/development/road-to-1.0.0.md").read_text("utf-8"))
    os.environ.pop("PYTEST_ADDOPTS", None)
    output = io.StringIO()
    with tempfile.TemporaryDirectory() as directory:
        collected = Path(directory) / "nodes.json"
        with redirect_stdout(output), redirect_stderr(output):
            code = pytest.main(
                ["--collect-only", "-q", "-o", "addopts=", "--strict-markers", "--strict-config"],
                plugins=[Collection(collected)],
            )
        if code != 0:
            print(output.getvalue(), file=sys.stderr)
            return 1
        nodes = json.loads(collected.read_text("utf-8"))
        failures = problems(expected, manifest, nodes)
        failures += catalog_problems(manifest.get("obvious_failures"), nodes)
    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    catalog = manifest["obvious_failures"]
    still_open = sorted(row for row, entry in catalog.items() if isinstance(entry, str))
    print(f"{len(expected)} stabilization defects have named, collected regression tests")
    print(f"{len(catalog) - len(still_open)} of {len(catalog)} obvious failures have tests; open:")
    print("\n".join(f"  {row}: {catalog[row][6:]}" for row in still_open))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
