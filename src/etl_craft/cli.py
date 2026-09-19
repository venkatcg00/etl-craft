"""The `etl-craft` command-line entry point."""

# Per CLAUDE.md's CLI surface: `run`, `list`, and `graph` are wired up here
# so far (`configure`, `set-execution-mode`, `validate`, `generate-yml` are
# still unbuilt). Within `run`, `--task_code` given dispatches to
# runner.run_task (a single task); omitted dispatches to
# orchestrator.run_pipeline (the whole dependency graph, wave by wave).

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from sqlalchemy.engine import Engine

from etl_craft.cfg import (
    CfgError,
    fetch_all_pipelines,
    fetch_cross_pipeline_task_edges,
    fetch_pipeline_dependencies,
    fetch_pipeline_graph,
    fetch_task_codes,
    resolve_pipeline_id,
)
from etl_craft.config import ConfigError, ConnectorConfig, load_config
from etl_craft.db import build_engine
from etl_craft.orchestrator import run_pipeline
from etl_craft.resolver import build_graph
from etl_craft.runlog import RunLogError
from etl_craft.runner import DependenciesNotMetError, ForceNotAllowedError, run_task

# Every exception run_task/run_pipeline can raise for reasons short of a
# bug: bad --pipeline_code/--task_code, --force under Mode=orchestrator,
# unmet dependencies, or a pipeline with no logged run at all to bind to.
# Caught uniformly here as a clean one-line error rather than a raw
# traceback.
RUN_ERRORS = (CfgError, RunLogError, ForceNotAllowedError, DependenciesNotMetError)


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level `etl-craft` argument parser."""
    parser = argparse.ArgumentParser(prog="etl-craft")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run a pipeline or a single task")
    run_parser.add_argument("--pipeline_code", required=True)
    run_parser.add_argument("--task_code")
    run_parser.add_argument("--force", action="store_true")

    subparsers.add_parser("list", help="List pipelines")

    # [CHOICE] CLAUDE.md's CLI surface table literally writes this as
    # `graph --name <pipeline>`, unlike every other command's
    # --pipeline_code — an inconsistency in the source doc, not something
    # deliberately different here. --name is treated as a PIPELINE_CODE,
    # resolved the same way --pipeline_code is everywhere else, since
    # PIPELINE_CODE is the one designated CLI lookup key (open question #2).
    graph_parser = subparsers.add_parser(
        "graph", help="Print dependency chain / lineage for a pipeline"
    )
    graph_parser.add_argument("--name", required=True)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Parse `argv` (default: sys.argv[1:]) and dispatch to the matching command."""
    args = build_parser().parse_args(argv)
    try:
        config = load_config()
        engine = build_engine(config)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if args.command == "run":
        return _run_command(args, engine, config)
    if args.command == "list":
        return _list_command(engine)
    if args.command == "graph":
        return _graph_command(args, engine)
    # argparse's `required=True` on the subparsers guarantees args.command is
    # one of the branches above; this exists only to document that invariant
    # and satisfy the type checker, not as a path any test can reach.
    return 2  # pragma: no cover


def _run_command(args: argparse.Namespace, engine: Engine, config: ConnectorConfig) -> int:
    try:
        if args.task_code is None:
            outcome = run_pipeline(engine, config, args.pipeline_code, force=args.force)
        else:
            outcome = run_task(engine, config, args.pipeline_code, args.task_code, force=args.force)
    except RUN_ERRORS as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(outcome.message)
    return 0 if outcome.status in ("SUCCESS", "SKIPPED") else 1


def _list_command(engine: Engine) -> int:
    with engine.connect() as conn:
        pipelines = fetch_all_pipelines(conn)
    if not pipelines:
        print("(no active pipelines)")
        return 0
    for pipeline in pipelines:
        print(f"{pipeline.pipeline_code}\t{pipeline.pipeline_name}\t{pipeline.refresh_type}")
    return 0


def _graph_command(args: argparse.Namespace, engine: Engine) -> int:
    try:
        with engine.connect() as conn:
            pipeline_id = resolve_pipeline_id(conn, args.name)
            graph_data = fetch_pipeline_graph(conn, pipeline_id)
            task_codes = fetch_task_codes(conn, pipeline_id)
            pipeline_deps = fetch_pipeline_dependencies(conn, pipeline_id)
            cross_task_deps = fetch_cross_pipeline_task_edges(conn, pipeline_id)
    except CfgError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    graph = build_graph(graph_data.tasks, graph_data.same_pipeline_edges)
    print(f"Pipeline: {args.name}")
    print("Task waves (same-pipeline order):")
    if graph.task_ids:
        for wave_number, wave in enumerate(graph.waves(), start=1):
            codes = ", ".join(task_codes[task_id] for task_id in wave)
            print(f"  Wave {wave_number}: {codes}")
    else:
        print("  (no active tasks)")

    print("Pipeline dependencies:")
    if pipeline_deps:
        for dep in pipeline_deps:
            print(f"  {dep.depends_on_pipeline_code} ({dep.dependency_type})")
    else:
        print("  (none)")

    print("Cross-pipeline task dependencies:")
    if cross_task_deps:
        for dep in cross_task_deps:
            print(
                f"  {dep.task_code} -> {dep.depends_on_pipeline_code}."
                f"{dep.depends_on_task_code} ({dep.dependency_type})"
            )
    else:
        print("  (none)")
    return 0
