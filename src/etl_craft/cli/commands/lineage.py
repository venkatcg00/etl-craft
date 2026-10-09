"""``etl-craft lineage``: where tables and columns come from and where they go."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands.common import Command, command_context, configure_output
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode
from etl_craft.services.lineage import COPY, Edge, TaskLineage
from etl_craft.services.operations.diagnostics import trace_lineage


def _configure(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    parser.add_argument("--table", help="schema.table (or database.schema.table) to trace")
    parser.add_argument("--column", help="one column of --table to trace")
    direction = parser.add_mutually_exclusive_group()
    direction.add_argument("--upstream", action="store_true", help="only where it comes from")
    direction.add_argument("--downstream", action="store_true", help="only where it goes")
    parser.add_argument("--depth", type=int, help="stop after this many steps")
    parser.add_argument(
        "--refresh", action="store_true", help="work every task's lineage out again"
    )
    parser.add_argument(
        "--strict", action="store_true", help="exit 1 when any SQL task cannot be traced"
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    with command_context(args) as ctx:
        done = trace_lineage(
            ctx,
            table=args.table,
            column=args.column,
            upstream=not args.downstream,
            downstream=not args.upstream,
            depth=args.depth,
            refresh=args.refresh,
        )

    def render() -> None:
        if done.table is None:
            _summary(list(done.lineages), out)
        elif done.column is None:
            out.line(done.table)
            for enabled, label, paths in (
                (done.upstream, "upstream", done.upstream_tables),
                (done.downstream, "downstream", done.downstream_tables),
            ):
                if enabled:
                    out.line(f"  {label}:")
                    if not paths:
                        out.line("    (none)")
                    for level, other, task in paths:
                        out.line(f"    {'  ' * (level - 1)}{other}  ({task})")
        else:
            out.line(f"{done.table}.{done.column}")
            for level, edge in done.upstream_columns:
                source = (
                    f"{edge.source_object}.{edge.source_column}"
                    if edge.source_object
                    else "(no source)"
                )
                out.line(f"  {'  ' * (level - 1)}<- {source}  [{_how(edge)}]  ({edge.task})")
            for level, edge in done.downstream_columns:
                target = f"{edge.target_object}.{edge.target_column}"
                out.line(f"  {'  ' * (level - 1)}-> {target}  [{_how(edge)}]  ({edge.task})")
        failed = sum(1 for item in done.lineages if item.error)
        if failed and done.table is not None:
            out.line(f"({failed} SQL task(s) could not be traced; run lineage without --table)")

    out.result(done, args.output_format, text=render)
    return ExitCode.FAILURE if args.strict and done.failed else ExitCode.SUCCESS


def _summary(lineages: list[TaskLineage], out: Output) -> None:
    traced = [lineage for lineage in lineages if not lineage.error]
    if not lineages:
        out.empty("no active SQL tasks")
        return
    out.rows([("TASK", "TARGET", "COLUMNS", "SOURCE_TABLES")])
    for lineage in traced:
        columns = {e.target_column for e in lineage.edges}
        sources = sorted({e.source_object for e in lineage.edges if e.source_object})
        out.rows([(lineage.task, lineage.target_object, len(columns), ", ".join(sources))])
    for lineage in lineages:
        if lineage.error:
            out.line(f"not traced: {lineage.task}: {lineage.error}")


def _how(edge: Edge) -> str:
    return "copy" if edge.transformation == COPY else edge.transformation


COMMAND = Command(
    name="lineage",
    help="Show where tables and columns come from and where they go.",
    configure=_configure,
    run=_run,
)
