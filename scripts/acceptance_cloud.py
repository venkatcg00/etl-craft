"""Run the cloud acceptance suites against Databricks and Snowflake, from a local session.

Usage: ``python scripts/acceptance_cloud.py [--env-file PATH] [--wheel PATH] [SUITE ...]``

The credentials are read from ``--env-file`` (default ``.env.acceptance``, gitignored; see
``.env.acceptance.example``) with etl-craft's own parser, so values holding ``;`` or ``#`` stay
whole. The wheel is built unless ``--wheel`` names one. Each suite (default: every suite marked
``where = "local"``) runs through ``run_suite.py`` and writes its evidence, test ids and
outcomes only, never a value from the file. A suite whose credentials are missing fails rather
than skipping.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

from etl_craft.core.text import parse_env_file
from suites import REPO_ROOT, load_suites


def build_wheel(out: Path) -> Path:
    """Build the wheel into ``out`` and return it."""
    out.mkdir(parents=True, exist_ok=True)
    for old in out.glob("*.whl"):
        old.unlink()
    subprocess.run(["uv", "build", "--wheel", "--out-dir", str(out)], cwd=REPO_ROOT, check=True)
    (wheel,) = out.glob("*.whl")
    return wheel


def main(argv: Sequence[str] | None = None) -> int:
    """Run the suites; return 0 only when every one passed."""
    parser = argparse.ArgumentParser(description="Run the cloud acceptance suites locally.")
    parser.add_argument("--env-file", type=Path, default=REPO_ROOT / ".env.acceptance")
    parser.add_argument("--wheel", type=Path, help="test this wheel instead of building one")
    parser.add_argument("suites", nargs="*", help="suites to run (default: the local ones)")
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)

    if not args.env_file.is_file():
        example = REPO_ROOT / ".env.acceptance.example"
        print(
            f"{args.env_file} not found: copy {example.name} to it and fill in the values, or "
            "pass --env-file",
            file=sys.stderr,
        )
        return 2
    values = parse_env_file(args.env_file.read_text(encoding="utf-8"))
    local = [name for name, suite in load_suites().items() if suite.where == "local"]
    chosen = args.suites or local
    unknown = sorted(set(chosen) - set(local))
    if unknown:
        known = ", ".join(local)
        print(f"not a local suite: {', '.join(unknown)}; local: {known}", file=sys.stderr)
        return 2
    wheel = args.wheel.resolve() if args.wheel else build_wheel(REPO_ROOT / "dist" / "acceptance")

    env = {**os.environ, **values, "ETL_CRAFT_REQUIRE_SERVICES": "1"}
    failed = []
    for name in chosen:
        command = [sys.executable, str(REPO_ROOT / "scripts" / "run_suite.py"), name]
        command += ["--wheel", str(wheel)]
        if subprocess.run(command, cwd=REPO_ROOT, env=env, check=False).returncode != 0:
            failed.append(name)
    if failed:
        print(f"failed: {', '.join(failed)}", file=sys.stderr)
        return 1
    print(f"passed: {', '.join(chosen)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
