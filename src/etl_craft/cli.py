"""The `etl-craft` command-line entry point.

Per CLAUDE.md's CLI surface, `run` is the one execution primitive — this
module currently wires up only that verb (`list`, `configure`,
`set-execution-mode`, `graph`, `validate`, `generate-yml` are all still
unbuilt). Within `run`, only the `--task_code`-given (single-task) path
works; `--task_code` omitted (the local-orchestrator wave-spawning path)
is refused with a clear "not implemented yet" message rather than silently
doing the wrong thing.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from etl_craft.cfg import CfgError
from etl_craft.config import ConfigError, load_config
from etl_craft.db import build_engine
from etl_craft.runlog import RunLogError
from etl_craft.runner import DependenciesNotMetError, ForceNotAllowedError, run_task

# Every exception run_task can raise for reasons short of a bug: bad
# --pipeline_code/--task_code, --force under Mode=orchestrator, unmet
# dependencies, or a pipeline with no logged run at all to bind to. Caught
# uniformly here as a clean one-line error rather than a raw traceback.
RUN_TASK_ERRORS = (CfgError, RunLogError, ForceNotAllowedError, DependenciesNotMetError)


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level `etl-craft` argument parser."""
    parser = argparse.ArgumentParser(prog="etl-craft")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run a pipeline or a single task")
    run_parser.add_argument("--pipeline_code", required=True)
    run_parser.add_argument("--task_code")
    run_parser.add_argument("--force", action="store_true")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Parse `argv` (default: sys.argv[1:]) and dispatch to the matching command."""
    args = build_parser().parse_args(argv)
    if args.command == "run":
        return _run_command(args)
    return 2  # argparse's `required=True` on the subparsers already rejects anything else


def _run_command(args: argparse.Namespace) -> int:
    try:
        config = load_config()
        engine = build_engine(config)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if args.task_code is None:
        print(
            "error: running a whole pipeline (--task_code omitted) is not implemented yet — "
            "pass --task_code to run a single task",
            file=sys.stderr,
        )
        return 2

    try:
        outcome = run_task(engine, config, args.pipeline_code, args.task_code, force=args.force)
    except RUN_TASK_ERRORS as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(outcome.message)
    return 0 if outcome.status in ("SUCCESS", "SKIPPED") else 1
