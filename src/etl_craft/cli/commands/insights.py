"""``etl-craft insights``: the Pipeline Insights panels for a window of UTC days."""

import argparse
from datetime import date

from etl_craft.cli.commands.common import (
    Command,
    call,
    configure_output,
    configure_tags,
    request_from,
)
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations.insights import Insights, InsightsInput


def configure(parser: argparse.ArgumentParser) -> None:
    """Take the window, pipelines and tags."""
    configure_output(parser)
    configure_tags(parser)
    parser.add_argument(
        "--from", dest="start", type=date.fromisoformat, help="First UTC day (default: 6 days ago)."
    )
    parser.add_argument(
        "--to", dest="end", type=date.fromisoformat, help="Last UTC day (default: today)."
    )
    parser.add_argument(
        "--pipeline-code",
        dest="pipelines",
        action="append",
        default=[],
        help="Only this pipeline; repeat for more.",
    )


def _seconds(value: float | None) -> str:
    return "-" if value is None else f"{value:.1f}"


def _rate(value: float | None) -> str:
    return "-" if value is None else f"{value:.0%}"


def run(args: argparse.Namespace, out: Output) -> int:
    """Render the same document the API and the Insights page show."""
    result = call(args, ActionName.INSIGHTS, request_from(args, InsightsInput))
    assert isinstance(result, Insights)

    def render_text() -> None:
        out.line(
            f"window {result.window_start:%Y-%m-%d} to {result.window_end:%Y-%m-%d} "
            "(UTC, end excluded)"
        )
        out.rows(
            [
                (
                    "PIPELINE",
                    "RUNS",
                    "SUCCESS_RATE",
                    "AVG_SECONDS",
                    "SLA_BREACHED",
                    "RETRIES",
                    "ROWS_WRITTEN",
                )
            ]
        )
        out.rows(
            (
                p.pipeline_code or "ALL",
                p.runs,
                _rate(p.success_rate),
                _seconds(p.average_seconds),
                p.sla_breached,
                p.retries,
                "not measured" if p.rows_written is None else p.rows_written,
            )
            for p in (*result.per_pipeline, result.total)
        )

    out.result(result, args.output_format, text=render_text)
    return ExitCode.SUCCESS


COMMAND = Command(
    "insights",
    "Pipeline Insights: runs, durations, SLA, retries, rows and quality for a window.",
    run,
    configure,
    actions=(ActionName.INSIGHTS,),
)
