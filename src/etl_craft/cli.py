"""The `etl-craft` command-line entry point."""

# Per CLAUDE.md's CLI surface: `run`, `list`, `graph`, `set-execution-mode`,
# `configure --env`, `generate-yml`, and `validate` are wired up here so far
# (interactive `configure` with no --env is still unbuilt). Within
# `run`, `--task_code` dispatches to runner.run_task (a single task);
# `--init-only` dispatches to orchestrator.init_pipeline_run (mint/reuse the
# active run, no task execution — what a generated Airflow DAG's synthetic
# first step invokes); the bare form (neither given) dispatches to
# orchestrator.run_pipeline (the local wave-spawning scheduler, refused
# outright under Mode=orchestrator — see orchestrator.py's own comment).
#
# `set-execution-mode` and `configure` are handled *before* this module's
# usual load_config()/build_engine() setup, since both operate on a
# craft-connector.yml that may not exist yet or may not have a resolvable
# secret — unlike every other command, they don't need a working Engine DB
# connection at all, just the ability to read/write the YAML file itself.

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

import yaml
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.cfg import (
    CfgError,
    fetch_all_pipelines,
    fetch_cross_pipeline_task_edges,
    fetch_pipeline_dependencies,
    fetch_pipeline_graph,
    fetch_task_codes,
    resolve_pipeline_id,
)
from etl_craft.config import VALID_MODES, ConfigError, ConnectorConfig, load_config
from etl_craft.configure import configure_from_env, set_execution_mode
from etl_craft.db import build_engine
from etl_craft.generate_yml import generate_pipeline_dag
from etl_craft.orchestrator import OrchestratorModeRefusedError, init_pipeline_run, run_pipeline
from etl_craft.resolver import ResolverError, build_graph
from etl_craft.runlog import RunLogError
from etl_craft.runner import ForceNotAllowedError, run_task
from etl_craft.validate import validate_business_rule_keys, validate_graphs
from etl_craft.warehouse import build_data_engine

# Every exception run_task/run_pipeline/init_pipeline_run can raise for
# reasons short of a bug: bad --pipeline_code/--task_code, --force under
# Mode=orchestrator, run_pipeline itself under Mode=orchestrator, or a
# pipeline with no logged run at all to bind to. Caught uniformly here as a
# clean one-line error rather than a raw traceback. (An unmet dependency,
# same-pipeline or cross-pipeline, is no longer one of these — run_task/
# run_pipeline record it as a SKIPPED outcome instead of raising, per
# runner.py's own [DEVIATION] comment.)
RUN_ERRORS = (
    CfgError,
    RunLogError,
    ForceNotAllowedError,
    OrchestratorModeRefusedError,
)


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level `etl-craft` argument parser."""
    parser = argparse.ArgumentParser(prog="etl-craft")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run a pipeline or a single task")
    run_parser.add_argument("--pipeline_code", required=True)
    task_group = run_parser.add_mutually_exclusive_group()
    task_group.add_argument("--task_code")
    task_group.add_argument(
        "--init-only",
        action="store_true",
        help=(
            "Only mint/reuse the active run, no task execution — what a generated Airflow "
            "DAG's synthetic first step invokes. Legal under both Mode=local and "
            "Mode=orchestrator, unlike the bare (no --task_code) form."
        ),
    )
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

    mode_parser = subparsers.add_parser(
        "set-execution-mode", help="Lock Execution.Mode in craft-connector.yml"
    )
    mode_parser.add_argument("mode", choices=sorted(VALID_MODES))

    configure_parser = subparsers.add_parser("configure", help="Set up craft-connector.yml")
    configure_parser.add_argument(
        "--env", help="Path to an env file for non-interactive setup (required for now)"
    )

    generate_yml_parser = subparsers.add_parser(
        "generate-yml", help="Emit hand-rolled, Airflow-YAML-inspired DAG YAML for a pipeline"
    )
    generate_yml_parser.add_argument("--pipeline_code", required=True)
    generate_yml_parser.add_argument("--output", help="Write to this path instead of stdout")

    subparsers.add_parser("validate", help="Config integrity check")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Parse `argv` (default: sys.argv[1:]) and dispatch to the matching command."""
    args = build_parser().parse_args(argv)

    if args.command == "set-execution-mode":
        return _set_execution_mode_command(args)
    if args.command == "configure":
        return _configure_command(args)

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
    if args.command == "generate-yml":
        return _generate_yml_command(args, engine)
    if args.command == "validate":
        return _validate_command(engine, config)
    # argparse's `required=True` on the subparsers guarantees args.command is
    # one of the branches above; this exists only to document that invariant
    # and satisfy the type checker, not as a path any test can reach.
    return 2  # pragma: no cover


def _set_execution_mode_command(args: argparse.Namespace) -> int:
    try:
        set_execution_mode(args.mode)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(f"Execution.Mode set to {args.mode!r}")
    return 0


def _configure_command(args: argparse.Namespace) -> int:
    if args.env is None:
        print(
            "error: interactive `configure` (no --env) is not implemented yet — "
            "pass --env <path> for non-interactive setup from an env file",
            file=sys.stderr,
        )
        return 2
    try:
        configure_from_env(args.env)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(f"craft-connector.yml written from {args.env}")
    return 0


def _run_command(args: argparse.Namespace, engine: Engine, config: ConnectorConfig) -> int:
    try:
        if args.init_only:
            init_outcome = init_pipeline_run(engine, config, args.pipeline_code)
            print(init_outcome.message)
            return 0
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


def _generate_yml_command(args: argparse.Namespace, engine: Engine) -> int:
    try:
        with engine.connect() as conn:
            dag = generate_pipeline_dag(conn, args.pipeline_code)
    except (CfgError, ResolverError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    yaml_text = yaml.safe_dump(dag, sort_keys=False, default_flow_style=False)
    if args.output:
        Path(args.output).write_text(yaml_text)
        print(f"DAG YAML written to {args.output}")
    else:
        print(yaml_text, end="")
    return 0


def _validate_command(engine: Engine, config: ConnectorConfig) -> int:
    with engine.connect() as conn:
        issues = validate_graphs(conn)

        data_engine = None
        if config.warehouse is not None:
            try:
                data_engine = build_data_engine(config)
            except ConfigError as exc:
                print(f"error: {exc}", file=sys.stderr)
                return 2
        try:
            issues += validate_business_rule_keys(conn, data_engine)
        except SQLAlchemyError as exc:
            print(
                f"error: could not check business rules against the Data DB: {exc}", file=sys.stderr
            )
            return 2
        finally:
            if data_engine is not None:
                data_engine.dispose()

    if not issues:
        print("validate: OK — no issues found")
        return 0
    for issue in issues:
        print(f"[{issue.category}] {issue.message}")
    return 1
