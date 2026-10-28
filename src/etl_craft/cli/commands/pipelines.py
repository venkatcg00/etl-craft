"""``etl-craft pipelines``: the active pipelines."""

from __future__ import annotations

import argparse

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
from etl_craft.services.operations.models import PipelineListView
from etl_craft.services.operations.pipeline_refresh import RefreshInput, RefreshResults
from etl_craft.services.operations.requests import TagFilter


def _configure(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    configure_tags(parser)
    verbs = parser.add_subparsers(dest="pipelines_verb")
    refresh = verbs.add_parser("refresh", help="Validate and pin the current pipeline definitions.")
    refresh.add_argument("--pipeline", dest="pipeline_code", help="Check only this pipeline.")
    configure_output(refresh)


def _run(args: argparse.Namespace, out: Output) -> int:
    if args.pipelines_verb == "refresh":
        return _refresh(args, out)
    done = call(args, ActionName.PIPELINES, request_from(args, TagFilter))
    assert isinstance(done, PipelineListView)

    def render_text() -> None:
        pipelines = done.pipelines
        if not pipelines:
            out.empty("no active pipelines")
            return
        header = (
            "PIPELINE_CODE",
            "PIPELINE_NAME",
            "REFRESH_TYPE",
            "RUN_SCHEDULE",
            "SLA_IN_HOURS",
            "TAGS",
        )
        out.rows([(*header, "PAUSED")])
        out.rows(
            (
                p.pipeline_code,
                p.pipeline_name,
                p.refresh_type,
                p.run_schedule,
                p.sla_in_hours,
                ", ".join(p.tags),
                p.paused.describe() if p.paused else "",
            )
            for p in pipelines
        )
        return

    out.result(done, args.output_format, text=render_text)
    return ExitCode.SUCCESS


def _refresh(args: argparse.Namespace, out: Output) -> int:
    done = call(args, ActionName.PIPELINE_REFRESH, request_from(args, RefreshInput))
    assert isinstance(done, RefreshResults)

    def render() -> None:
        for result in done.refreshes:
            if not result.changed:
                out.line(
                    f"{result.pipeline_code}: no changes since the last refresh at "
                    f"{result.previous_refreshed_at} ({result.previous_status})"
                )
            else:
                out.line(f"{result.pipeline_code}: {result.status} refresh={result.refresh_id}")
                for change in result.changes:
                    out.line(f"  changed: {change}")
            for finding in result.findings:
                line = "metadata" if finding.line is None else str(finding.line)
                out.line(f"  {finding.file}:{line}: {finding.message}; {finding.remedy}")

    out.result(done, args.output_format, text=render)
    return ExitCode.FAILURE if done.failed else ExitCode.SUCCESS


COMMAND = Command(
    name="pipelines",
    help="List the active pipelines.",
    configure=_configure,
    run=_run,
    actions=(ActionName.PIPELINES, ActionName.PIPELINE_REFRESH),
)
