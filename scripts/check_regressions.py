"""Check that every stabilization defect maps to named, collected regression tests."""

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
        failures = problems(expected, manifest, json.loads(collected.read_text("utf-8")))
    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    print(f"{len(expected)} stabilization defects have named, collected regression tests")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
