"""``etl-craft doctor``: check a configuration end to end and report every problem."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from etl_craft import __version__
from etl_craft.cli.client import ApiClient
from etl_craft.cli.commands.common import Command, call, command_project, configure_output
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import ExitCode, UsageError
from etl_craft.engine.project_files import missing_imports
from etl_craft.services.doctor import Check, Status
from etl_craft.services.operations.agent_doctor import AgentDoctor, AgentImports
from etl_craft.services.operations.diagnostics import DoctorView
from etl_craft.services.operations.requests import NoInput


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
    if getattr(args, "agent", False):
        return check_agent_imports(args, out)
    done = call(args, ActionName.DOCTOR, NoInput())
    assert isinstance(done, DoctorView)

    def render() -> None:
        report(out, done.checks)
        out.agents(done.agents)

    out.result(done, args.output_format, text=render)
    return ExitCode.FAILURE if done.failed else ExitCode.SUCCESS


def configure(parser: argparse.ArgumentParser) -> None:
    """Inspect the calling agent's interpreter through its authenticated API connection."""
    parser.add_argument(
        "--agent",
        action="store_true",
        help="Check bundle imports on this agent host; requires --api and an agent token.",
    )
    configure_output(parser)


def check_agent_imports(args: argparse.Namespace, out: Output) -> int:
    """Collect local module availability as action input; the API owns the diagnostic result."""
    if not args.api:
        raise UsageError(
            "doctor --agent: no API address; expected --api or ETL_CRAFT_API "
            "with an agent token; provide the API address"
        )
    client = ApiClient(args.api, command_project(args))
    names = client.invoke(ActionName.AGENT_DOCTOR, AgentDoctor(version=__version__))
    assert isinstance(names, AgentImports)
    result = client.invoke(
        ActionName.AGENT_DOCTOR,
        AgentDoctor(
            version=__version__,
            bundle_id=names.bundle_id,
            missing=missing_imports(names.imports),
        ),
    )
    assert isinstance(result, AgentImports)
    out.result(
        result,
        args.output_format,
        text=lambda: out.line(
            f"Bundle {result.bundle_id}: missing imports "
            f"{', '.join(result.missing or ()) or 'none'}; "
            "install missing packages in the agent image (static imports; no project code executed)"
        ),
    )
    return ExitCode.FAILURE if result.missing else ExitCode.SUCCESS


COMMAND = Command(
    name="doctor",
    help="Check the configuration, secrets, connections and Engine DB; exit 1 if any check fails.",
    configure=configure,
    run=_run,
    actions=(ActionName.DOCTOR, ActionName.AGENT_DOCTOR),
)
