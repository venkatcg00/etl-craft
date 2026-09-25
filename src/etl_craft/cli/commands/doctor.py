"""``etl-craft doctor``: check a configuration end to end and report every problem."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from etl_craft.cli.commands import Command
from etl_craft.cli.commands.common import load_command_config
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode
from etl_craft.services.doctor import Check, Status, run_checks


def _configure(parser: argparse.ArgumentParser) -> None:
    del parser


def report(out: Output, checks: Sequence[Check]) -> bool:
    """Write one line per check and a summary; return whether any check failed."""
    for check in checks:
        out.line(f"[{check.status.value:<4}] {check.name}: {check.detail}")
    counts = {status: sum(1 for c in checks if c.status is status) for status in Status}
    out.line(
        f"{counts[Status.OK]} ok, {counts[Status.WARN]} warning(s), {counts[Status.FAIL]} failed"
    )
    return counts[Status.FAIL] > 0


def _run(args: argparse.Namespace, out: Output) -> int:
    failed = report(out, run_checks(load_command_config(args)))
    return ExitCode.FAILURE if failed else ExitCode.SUCCESS


COMMAND = Command(
    name="doctor",
    help="Check the configuration, secrets, connections and Engine DB; exit 1 if any check fails.",
    configure=_configure,
    run=_run,
)
