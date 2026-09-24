"""Run one release suite and record its evidence.

Usage: ``python scripts/run_suite.py SUITE [--wheel PATH] [--version V] [--evidence-dir DIR]
[-- PYTEST_ARGS ...]``

The suite's marker (from release/required-suites.toml) selects the tests. The evidence file
is written to ``release/evidence/<version>/<suite>.json`` unless ``--evidence-dir`` says
otherwise. Suites that test the built wheel need ``--wheel``; its path is passed to the tests
as ``ETL_CRAFT_TEST_WHEEL``.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

from suites import REPO_ROOT, Suite, evidence_dir, load_suites, project_version


def pytest_command(
    suite: Suite, evidence: Path, wheel: Path | None, extra: Sequence[str]
) -> list[str]:
    """Build the pytest command line that runs ``suite`` and writes ``evidence``."""
    command = [
        sys.executable,
        "-m",
        "pytest",
        "-m",
        suite.marker,
        "-p",
        "no:cacheprovider",
        f"--evidence={evidence}",
        f"--evidence-suite={suite.name}",
    ]
    if wheel is not None:
        command.append(f"--evidence-wheel={wheel}")
    command.extend(extra)
    return command


def parse_args(argv: Sequence[str]) -> tuple[argparse.Namespace, list[str]]:
    """Parse the command line; everything after ``--`` is returned for pytest."""
    arguments = list(argv)
    pytest_args: list[str] = []
    if "--" in arguments:
        split = arguments.index("--")
        arguments, pytest_args = arguments[:split], arguments[split + 1 :]
    parser = argparse.ArgumentParser(description="Run one release suite and record evidence.")
    parser.add_argument("suite", help="a suite name from release/required-suites.toml")
    parser.add_argument("--wheel", type=Path, help="the built wheel the suite tests")
    parser.add_argument("--version", help="the release version (default: pyproject.toml)")
    parser.add_argument("--evidence-dir", type=Path, help="where to write the evidence file")
    return parser.parse_args(arguments), pytest_args


def main(argv: Sequence[str] | None = None) -> int:
    """Run the suite and return pytest's exit code (2 for a usage error)."""
    args, pytest_args = parse_args(sys.argv[1:] if argv is None else argv)
    suites = load_suites()
    suite = suites.get(args.suite)
    if suite is None:
        print(f"unknown suite {args.suite!r}; known: {', '.join(suites)}", file=sys.stderr)
        return 2
    if suite.wheel and args.wheel is None:
        print(f"suite {suite.name} tests the built wheel: pass --wheel PATH", file=sys.stderr)
        return 2
    wheel = args.wheel.resolve() if args.wheel is not None else None
    if wheel is not None and not wheel.is_file():
        print(f"wheel not found: {wheel}", file=sys.stderr)
        return 2

    version = args.version or project_version()
    directory = args.evidence_dir or evidence_dir(version)
    directory.mkdir(parents=True, exist_ok=True)
    evidence = (directory / f"{suite.name}.json").resolve()

    env = dict(os.environ)
    if wheel is not None:
        env["ETL_CRAFT_TEST_WHEEL"] = str(wheel)
    command = pytest_command(suite, evidence, wheel, pytest_args)
    status = subprocess.run(command, cwd=REPO_ROOT, env=env, check=False).returncode
    print(f"evidence for {suite.name}: {evidence}")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
