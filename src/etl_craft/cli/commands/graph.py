"""``etl-craft graph``: a pipeline's task waves and what each task waits for."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands import Command
from etl_craft.cli.commands.common import command_context, configure_output
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations import PipelineRef, inspect


def _configure(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    parser.add_argument("--pipeline_code", required=True, help="the pipeline to show")


def _run(args: argparse.Namespace, out: Output) -> int:
    with command_context(args) as ctx:
        graph = inspect.pipeline_graph(ctx, PipelineRef(args.pipeline_code))
    if args.output_format == "json":
        out.document(graph)
        return ExitCode.SUCCESS
    out.line(f"Pipeline {graph.pipeline_code}")
    out.line("Waves, the order that is always safe:")
    if not graph.waves:
        out.line("  (no active tasks)")
    for number, wave in enumerate(graph.waves, start=1):
        out.line(f"  {number}: {', '.join(wave)}")
    if graph.conditional:
        out.line(
            "May start before their wave (an ANY or N run condition): "
            + ", ".join(graph.conditional)
        )
    out.line("Task dependencies:")
    edges = [(task, up, kind) for task, ups in graph.depends_on.items() for up, kind in ups]
    if not edges:
        out.line("  (none)")
    for task, upstream, kind in edges:
        out.line(f"  {task} <- {upstream} ({kind})")
    out.line("Pipeline dependencies:")
    if not graph.pipeline_dependencies:
        out.line("  (none)")
    for upstream, kind in graph.pipeline_dependencies:
        out.line(f"  {upstream} ({kind})")
    return ExitCode.SUCCESS


COMMAND = Command(
    name="graph",
    help="Show a pipeline's task waves and dependencies.",
    configure=_configure,
    run=_run,
)
