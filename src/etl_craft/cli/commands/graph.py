"""``etl-craft graph``: a pipeline's task waves and what each task waits for."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands import Command
from etl_craft.cli.commands.common import connect_engine_db, load_command_config
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode
from etl_craft.services.inspect import pipeline_graph


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pipeline_code", required=True, help="the pipeline to show")


def _run(args: argparse.Namespace, out: Output) -> int:
    engine = connect_engine_db(load_command_config(args))
    try:
        with engine.connect() as conn:
            graph = pipeline_graph(conn, args.pipeline_code)
    finally:
        engine.dispose()
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
