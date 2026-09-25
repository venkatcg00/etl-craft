"""``etl-craft lineage``: where tables and columns come from and where they go."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands import Command
from etl_craft.cli.commands.common import connect_engine_db, load_command_config
from etl_craft.cli.output import Output
from etl_craft.config.targets import active_catalog
from etl_craft.core.errors import ExitCode, UsageError
from etl_craft.services.lineage import COPY, Edge, LineageGraph, TaskLineage, collect, table_name


def _configure(parser: argparse.ArgumentParser) -> None:
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
    if args.column and not args.table:
        raise UsageError("--column needs --table")
    if args.depth is not None and args.depth < 1:
        raise UsageError(f"--depth must be 1 or more, got {args.depth}")
    config = load_command_config(args)
    engine = connect_engine_db(config)
    try:
        lineages = collect(engine, config, refresh=args.refresh)
    finally:
        engine.dispose()
    graph = LineageGraph.of(lineages)
    if args.table is None:
        _summary(lineages, out)
    else:
        catalog = active_catalog(config) if config.warehouse is not None else None
        table = table_name(args.table.split("."), catalog)
        up = not args.downstream
        down = not args.upstream
        if args.column:
            _column(graph, table, args.column.lower(), up, down, args.depth, out)
        else:
            _table(graph, table, up, down, out)
    failed = [lineage for lineage in lineages if lineage.error]
    if failed and args.table is not None:
        out.line(f"({len(failed)} SQL task(s) could not be traced; run lineage without --table)")
    return ExitCode.FAILURE if args.strict and failed else ExitCode.SUCCESS


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


def _table(graph: LineageGraph, table: str, up: bool, down: bool, out: Output) -> None:
    out.line(table)
    if up:
        out.line("  upstream:")
        found = graph.upstream_tables(table)
        if not found:
            out.line("    (none)")
        for level, other, task in found:
            out.line(f"    {'  ' * (level - 1)}{other}  ({task})")
    if down:
        out.line("  downstream:")
        found = graph.downstream_tables(table)
        if not found:
            out.line("    (none)")
        for level, other, task in found:
            out.line(f"    {'  ' * (level - 1)}{other}  ({task})")


def _how(edge: Edge) -> str:
    return "copy" if edge.transformation == COPY else edge.transformation


def _column(
    graph: LineageGraph,
    table: str,
    column: str,
    up: bool,
    down: bool,
    depth: int | None,
    out: Output,
) -> None:
    known = graph.columns(table)
    if column not in known:
        hint = f"; its traced columns: {', '.join(known)}" if known else ""
        raise UsageError(f"no lineage mentions {table}.{column}{hint}")
    out.line(f"{table}.{column}")
    if up:
        for level, e in graph.upstream(table, column, depth):
            source = f"{e.source_object}.{e.source_column}" if e.source_object else "(no source)"
            out.line(f"  {'  ' * (level - 1)}<- {source}  [{_how(e)}]  ({e.task})")
    if down:
        for level, e in graph.downstream(table, column, depth):
            target = f"{e.target_object}.{e.target_column}"
            out.line(f"  {'  ' * (level - 1)}-> {target}  [{_how(e)}]  ({e.task})")


COMMAND = Command(
    name="lineage",
    help="Show where tables and columns come from and where they go.",
    configure=_configure,
    run=_run,
)
