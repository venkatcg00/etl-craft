"""Collect the complete test ids of a release suite without executing its tests."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import pytest

from suites import load_suites


class Collection:
    """Write the selected node ids after every collection filter has run."""

    def __init__(self, output: Path) -> None:
        """Store the output path for this collection."""
        self.output = output

    def pytest_collection_finish(self, session: pytest.Session) -> None:
        """Persist the final selected node ids, without reading terminal output."""
        self.output.write_text(
            json.dumps(sorted(item.nodeid for item in session.items)), encoding="utf-8"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite")
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    suite = load_suites(Path.cwd() / "release" / "required-suites.toml")[args.suite]
    os.environ.pop("PYTEST_ADDOPTS", None)
    return int(
        pytest.main(
            [
                "--collect-only",
                "-q",
                "-m",
                suite.marker,
                "-o",
                "addopts=",
                "--strict-markers",
                "--strict-config",
                *suite.selection(sys.platform),
            ],
            plugins=[Collection(args.output)],
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
