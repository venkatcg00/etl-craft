"""``etl-craft doctor``: check a configuration end to end and report every problem."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from etl_craft.cli.commands.common import Command, configure_output, load_command_config
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode
from etl_craft.services.doctor import Check, Status
from etl_craft.services.operations.diagnostics import check_configuration


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
    done = check_configuration(load_command_config(args))

    def render() -> None:
        report(out, done.checks)

    out.result(done, args.output_format, text=render)
    return ExitCode.FAILURE if done.failed else ExitCode.SUCCESS


COMMAND = Command(
    name="doctor",
    help="Check the configuration, secrets, connections and Engine DB; exit 1 if any check fails.",
    configure=configure_output,
    run=_run,
)
