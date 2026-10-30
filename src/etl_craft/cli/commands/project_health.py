"""Bounded project health from the shared grant-filtered read action."""

import argparse

from etl_craft.cli.commands.common import Command, call, configure_output, request_from
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName
from etl_craft.services.operations.project_health import HealthInput, ProjectHealth


def configure(parser: argparse.ArgumentParser) -> None:
    """Offer JSON and bounded project pagination."""
    configure_output(parser)
    parser.add_argument("--json", dest="output_format", action="store_const", const="json")
    parser.add_argument("--after")
    parser.add_argument("--limit", type=int, default=25)


def run(args: argparse.Namespace, out: Output) -> int:
    """Render the same counts returned by the API and Home."""
    result = call(args, ActionName.PROJECT_HEALTH, request_from(args, HealthInput))
    assert isinstance(result, ProjectHealth)
    out.result(
        result,
        args.output_format,
        text=lambda: out.rows(
            (p.project, p.running, p.failed, p.sla_misses, p.queued_tasks) for p in result.projects
        ),
    )
    return 0


COMMAND = Command(
    "project-health",
    "Visible project health; failures and SLA misses in the last 24 h.",
    run,
    configure,
    actions=(ActionName.PROJECT_HEALTH,),
)
