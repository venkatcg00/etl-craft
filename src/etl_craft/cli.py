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
from importlib.metadata import version
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
    fetch_pipeline_run_history,
    fetch_pipeline_steps,
    fetch_table_lineage,
    fetch_task_codes,
    fetch_task_run_history,
    resolve_pipeline_id,
    resolve_task_id,
)
from etl_craft.column_lineage import column_lineage_for
from etl_craft.config import (
    VALID_MODES,
    ConfigError,
    ConnectorConfig,
    load_config,
    resolve_config_path,
)
from etl_craft.configure import set_execution_mode
from etl_craft.db import build_engine
from etl_craft.docs_generator import generate_docs
from etl_craft.doctor import run_checks
from etl_craft.documentation import fetch_history, refresh_all
from etl_craft.generate_yml import (
    GENERATED_HEADER,
    generate_global_dag,
    generate_pipeline_dag,
)
from etl_craft.init_db import InitDbError, init_db
from etl_craft.migrate import MigrationError, apply_pending_migrations
from etl_craft.orchestrator import (
    OrchestratorModeRefusedError,
    PipelineOutcome,
    finalize_active_run,
    init_pipeline_run,
    run_pipeline,
)
from etl_craft.resolver import ResolverError, build_graph
from etl_craft.runlog import RunLogError
from etl_craft.runner import ForceNotAllowedError, TaskOutcome, run_task
from etl_craft.setup_command import run_setup
from etl_craft.validate import (
    validate_business_rule_keys,
    validate_dependency_edges,
    validate_graphs,
    validate_read_only_sql,
    validate_task_lineage_declarations,
    validate_task_parameters,
    validate_warehouse_storage,
)
from etl_craft.warehouse import READ_ONLY_WAIT_SECONDS, data_db

# Every exception run_task/run_pipeline/init_pipeline_run can raise for
# reasons short of a bug: bad --pipeline_code/--task_code, --force under
# Mode=orchestrator, run_pipeline itself under Mode=orchestrator, or a
# pipeline with no logged run at all to bind to. Caught uniformly here as a
# clean one-line error rather than a raw traceback. (An unmet dependency,
# same-pipeline or cross-pipeline, is no longer one of these — run_task/
# run_pipeline record it as a SKIPPED outcome instead of raising, per
# runner.py's own [DEVIATION] comment.)
# [ADDITION, 2026-09-20, E2-49] ResolverError joins them. `run` builds the
# pipeline's graph, so a cyclic or malformed CFG_TASK_DEPENDENCY/
# CFG_TASKS.RUN_CONDITION config surfaced as a raw traceback — and E2-41 gave
# build_graph four new ways to raise, while --finalize-only started building a
# graph where it previously did not, so a config problem could take out a
# generated DAG's synthetic last step too. `validate` already reports the same
# condition as a clean [graph] issue, which is the behaviour every command
# should have.
RUN_ERRORS = (
    CfgError,
    RunLogError,
    ForceNotAllowedError,
    OrchestratorModeRefusedError,
    ResolverError,
)


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level `etl-craft` argument parser."""
    parser = argparse.ArgumentParser(prog="etl-craft")
    parser.add_argument(
        "--version",
        action="version",
        version=f"etl-craft {version('etl-craft')}",
    )
    parser.add_argument(
        "--config",
        help=(
            "Path to craft-connector.yml. Defaults to $ETL_CRAFT_CONFIG, then the "
            "nearest craft-connector.yml searching upward from the current directory."
        ),
    )
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
    task_group.add_argument(
        "--finalize-only",
        action="store_true",
        help=(
            "Only finalize the active run's SUCCESS/FAILED status from its tasks' current "
            "state, no task execution — what a generated Airflow DAG's synthetic last step "
            "invokes. Legal under both modes, like --init-only."
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
    # [DEVIATION, 2026-09-20, E2-36] --pipeline_code is accepted too. CLAUDE.md's
    # CLI table wrote this one command as `graph --name <pipeline>`, unlike
    # every other command's --pipeline_code — an inconsistency in the source
    # doc rather than a deliberate difference. Both spellings work rather than
    # breaking anything already scripted against --name.
    graph_target = graph_parser.add_mutually_exclusive_group(required=True)
    graph_target.add_argument("--name", dest="name")
    graph_target.add_argument("--pipeline_code", dest="name")

    mode_parser = subparsers.add_parser(
        "set-execution-mode", help="Lock Execution.Mode in craft-connector.yml"
    )
    mode_parser.add_argument("mode", choices=sorted(VALID_MODES))

    setup_parser = subparsers.add_parser(
        "setup",
        help="Set up or update this deployment: config, then schema/migrations",
    )
    setup_source = setup_parser.add_mutually_exclusive_group()
    setup_source.add_argument(
        "--env",
        help="Settings file to read (default: ./.env)",
    )
    setup_source.add_argument(
        "--from-environment",
        action="store_true",
        help="Read settings from the process environment instead of a file",
    )
    setup_parser.add_argument(
        "--migrations-dir",
        help="Directory of *.sql migration files (same resolution as `migrate`)",
    )

    generate_yml_parser = subparsers.add_parser(
        "generate-yml", help="Emit hand-rolled, Airflow-YAML-inspired DAG YAML"
    )
    generate_yml_target = generate_yml_parser.add_mutually_exclusive_group(required=True)
    generate_yml_target.add_argument("--pipeline_code")
    generate_yml_target.add_argument(
        "--global",
        action="store_true",
        dest="global_dag",
        help=(
            "Emit the optional cross-pipeline trigger DAG instead of one pipeline's own "
            "(requires [Orchestrator].Global_dag: true in craft-connector.yml)"
        ),
    )
    generate_yml_parser.add_argument("--output", help="Write to this path instead of stdout")

    subparsers.add_parser("validate", help="Config integrity check")

    # [ADDITION] Per explicit instruction ("take a target table and query
    # this ask cli to fetch dependencies, it should return all the tasks
    # that read the table and write the table") — one of the "read-only
    # query verbs conceptually agreed but not yet named or built" CLAUDE.md
    # already anticipated as "table-level lineage." [CHOICE] Named
    # `lineage`, not folded into `graph` (which is pipeline-scoped, not
    # cross-pipeline/table-scoped the way this query is).
    lineage_parser = subparsers.add_parser("lineage", help="Show table- or column-level lineage")
    lineage_target = lineage_parser.add_mutually_exclusive_group(required=True)
    lineage_target.add_argument("--table", help="schema.table, as declared in CFG_")
    lineage_target.add_argument(
        "--column",
        help="schema.table.column — parsed from SOURCE_SQL, cached in AUD_COLUMN_LINEAGE",
    )
    lineage_parser.add_argument(
        "--refresh",
        action="store_true",
        help="Re-parse instead of using cached column lineage",
    )

    docs_version_parser = subparsers.add_parser(
        "docs-version",
        help="Record a new version for any task whose DOCUMENTATION changed",
    )
    docs_version_parser.add_argument(
        "--task_code", help="Show one task's full documentation history instead"
    )
    docs_version_parser.add_argument("--pipeline_code", help="Required with --task_code")

    # [ADDITION] Closes CLAUDE.md open question #7 — see migrate.py's own
    # module docstring for scope/reasoning.
    migrate_parser = subparsers.add_parser(
        "migrate", help="Apply pending sql/migrations/*.sql files"
    )
    migrate_parser.add_argument(
        "--migrations-dir",
        help=(
            "Directory of *.sql migration files. Defaults to $ETL_CRAFT_MIGRATIONS_DIR, "
            "then ./sql/migrations, then the copy packaged with etl-craft."
        ),
    )

    subparsers.add_parser("doctor", help="Check config, secrets and every configured connection")

    init_db_parser = subparsers.add_parser(
        "init-db", help="Apply the packaged schema to an empty Engine DB"
    )
    init_db_parser.add_argument(
        "--force",
        action="store_true",
        help="Apply the schema even if engine tables already exist",
    )

    # [ADDITION] Close out CLAUDE.md's remaining "read-only query verbs
    # conceptually agreed but not yet named or built": steps-in-a-pipeline
    # and run history. Both are plain reads against CFG_/AUD_ tables, same
    # spirit as `list`/`graph`/`lineage` above — meant to replace ad hoc SQL
    # against the Engine DB, not to add any new mechanism.
    steps_parser = subparsers.add_parser(
        "steps", help="List a pipeline's active tasks and their declared parameters"
    )
    steps_parser.add_argument("--pipeline_code", required=True)

    history_parser = subparsers.add_parser(
        "history", help="Show recent run history for a pipeline, or one of its tasks"
    )
    history_parser.add_argument("--pipeline_code", required=True)
    history_parser.add_argument("--task_code")
    history_parser.add_argument("--limit", type=int, default=20)

    # [ADDITION] The "documentation generator" CLAUDE.md's own CLI surface
    # section anticipated: "the same read-layer queries as above, rendered
    # as a static, searchable site." See docs_generator.py's own docstring.
    docs_parser = subparsers.add_parser(
        "generate-docs", help="Emit a static, searchable documentation site"
    )
    docs_parser.add_argument(
        "--output", default="etl-craft-docs", help="Output directory (default: ./etl-craft-docs)"
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Parse `argv` (default: sys.argv[1:]) and dispatch to the matching command."""
    args = build_parser().parse_args(argv)
    # [ADDITION, 2026-09-20, E2-06] Resolved once, here, and threaded into
    # every command — including the two that *write* the file, so
    # `--config` means the same thing whichever verb is used.
    config_path = resolve_config_path(args.config)

    if args.command == "set-execution-mode":
        return _set_execution_mode_command(args, config_path)
    if args.command == "setup":
        return _setup_command(args, config_path)
    # doctor deliberately runs before build_engine: its entire job is to
    # diagnose a configuration that does not work yet, and the shared setup
    # below would exit 2 on an unresolvable secret before doctor said a word.
    if args.command == "doctor":
        return _doctor_command(config_path)

    try:
        config = load_config(config_path)
        engine = build_engine(config)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    # [ADDITION, 2026-09-20, E2-39] One catch around every command for the
    # most common real-world failure there is — Postgres unreachable, or
    # dropping mid-command. `build_engine` above constructs a lazy Engine with
    # a `creator`, so it never connects; the first real connection happens
    # inside each command, outside any DB-error guard. `list` and
    # `generate-docs` had no try/except at all, so an unreachable Engine DB
    # printed a raw traceback.
    try:
        if args.command == "run":
            return _run_command(args, engine, config)
        if args.command == "list":
            return _list_command(engine)
        if args.command == "graph":
            return _graph_command(args, engine)
        if args.command == "generate-yml":
            return _generate_yml_command(args, engine, config)
        if args.command == "validate":
            return _validate_command(engine, config)
        if args.command == "lineage":
            return _lineage_command(args, engine)
        if args.command == "docs-version":
            return _docs_version_command(args, engine)
        if args.command == "migrate":
            return _migrate_command(args, engine)
        if args.command == "init-db":
            return _init_db_command(args, engine)
        if args.command == "steps":
            return _steps_command(args, engine)
        if args.command == "history":
            return _history_command(args, engine)
        if args.command == "generate-docs":
            return _generate_docs_command(args, engine)
    except SQLAlchemyError as exc:
        print(f"error: Engine DB: {exc}", file=sys.stderr)
        return 2
    # argparse's `required=True` on the subparsers guarantees args.command is
    # one of the branches above; this exists only to document that invariant
    # and satisfy the type checker, not as a path any test can reach.
    return 2  # pragma: no cover


def _set_execution_mode_command(args: argparse.Namespace, config_path: Path) -> int:
    try:
        set_execution_mode(args.mode, config_path)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(f"Execution.Mode set to {args.mode!r}")
    return 0


def _setup_command(args: argparse.Namespace, config_path: Path) -> int:
    try:
        report = run_setup(
            config_path=config_path,
            env_path=args.env,
            from_environment=args.from_environment,
            migrations_dir=args.migrations_dir,
        )
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    print(f"  config   : {report.config_action}")
    print(f"  database : {report.database_action}")
    for applied in report.applied_migrations:
        print(f"             applied {applied}")
    if report.required_secrets:
        print("  secrets  : this configuration expects")
        for label, var in report.required_secrets:
            print(f"             {label:<22} {var}")
    for problem in report.problems:
        print(f"error: {problem}", file=sys.stderr)
    if not report.ok:
        return 1
    print("\nsetup: ready — run `etl-craft doctor` to verify every connection")
    return 0


def _outcome_of(outcome: PipelineOutcome | TaskOutcome) -> tuple[str, str]:
    """Reduce either outcome type to the (status, message) pair the CLI prints."""
    return outcome.status, outcome.message


def _run_command(args: argparse.Namespace, engine: Engine, config: ConnectorConfig) -> int:
    try:
        if args.init_only:
            init_outcome = init_pipeline_run(engine, config, args.pipeline_code)
            print(init_outcome.message)
            return 0
        if args.finalize_only:
            finalize_outcome = finalize_active_run(engine, config, args.pipeline_code)
            print(finalize_outcome.message)
            return 0 if finalize_outcome.status == "SUCCESS" else 1
        if args.task_code is None:
            status, message = _outcome_of(
                run_pipeline(engine, config, args.pipeline_code, force=args.force)
            )
        else:
            status, message = _outcome_of(
                run_task(engine, config, args.pipeline_code, args.task_code, force=args.force)
            )
    except RUN_ERRORS as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(message)
    return 0 if status in ("SUCCESS", "SKIPPED") else 1


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
        graph = build_graph(graph_data.tasks, graph_data.same_pipeline_edges)
    except (CfgError, ResolverError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(f"Pipeline: {args.name}")
    print("Task waves (same-pipeline order):")
    # [ADDITION, 2026-09-20, E2-51] Waves are the static ordering — see
    # resolver.waves()'s own [CHOICE]. A task whose RUN_CONDITION is ANY or N
    # can genuinely start before the wave shown here.
    if graph.task_ids:
        for wave_number, wave in enumerate(graph.waves(), start=1):
            codes = ", ".join(task_codes[task_id] for task_id in wave)
            print(f"  Wave {wave_number}: {codes}")
        conditional = sorted(
            task_codes[task.task_id]
            for task in graph_data.tasks
            if task.run_condition and task.run_condition != "ALL"
        )
        if conditional:
            print(
                "  (waves are the guaranteed-safe static order; these tasks have a "
                "RUN_CONDITION and may start earlier: " + ", ".join(conditional) + ")"
            )
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
        for cross_dep in cross_task_deps:
            print(
                f"  {cross_dep.task_code} -> {cross_dep.depends_on_pipeline_code}."
                f"{cross_dep.depends_on_task_code} ({cross_dep.dependency_type})"
            )
    else:
        print("  (none)")
    return 0


def _generate_yml_command(args: argparse.Namespace, engine: Engine, config: ConnectorConfig) -> int:
    if args.global_dag:
        if not config.orchestrator.global_dag:
            print(
                "error: the global DAG is disabled — set [Orchestrator].Global_dag: true "
                "in craft-connector.yml to enable it",
                file=sys.stderr,
            )
            return 2
        with engine.connect() as conn:
            dag = generate_global_dag(conn)
    else:
        try:
            with engine.connect() as conn:
                dag = generate_pipeline_dag(conn, config, args.pipeline_code)
        except (CfgError, ResolverError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1

    yaml_text = GENERATED_HEADER + yaml.safe_dump(dag, sort_keys=False, default_flow_style=False)
    if args.output:
        Path(args.output).write_text(yaml_text)
        print(f"DAG YAML written to {args.output}")
    else:
        print(yaml_text, end="")
    return 0


def _validate_command(engine: Engine, config: ConnectorConfig) -> int:
    with engine.connect() as conn:
        issues = validate_graphs(conn)
        issues += validate_task_lineage_declarations(conn)
        issues += validate_read_only_sql(conn)
        issues += validate_task_parameters(conn)
        issues += validate_dependency_edges(conn)

        if config.warehouse is None:
            issues += validate_business_rule_keys(conn, None)
        else:
            try:
                # [DEVIATION, 2026-09-21, E2-61] Through data_db, so a
                # single-writer warehouse queues briefly instead of failing
                # outright while a task is running. No-op for Postgres.
                with data_db(config, engine, wait_seconds=READ_ONLY_WAIT_SECONDS) as data_engine:
                    issues += validate_business_rule_keys(conn, data_engine)
                    issues += validate_warehouse_storage(conn, config, data_engine)
            except ConfigError as exc:
                print(f"error: {exc}", file=sys.stderr)
                return 2
            except SQLAlchemyError as exc:
                print(
                    f"error: could not check business rules against the Data DB: {exc}",
                    file=sys.stderr,
                )
                return 2

    if not issues:
        print("validate: OK — no issues found")
        return 0
    for issue in issues:
        print(f"[{issue.category}] {issue.message}")
    return 1


def _lineage_command(args: argparse.Namespace, engine: Engine) -> int:
    if args.column:
        return _column_lineage_command(args, engine)
    with engine.connect() as conn:
        entries = fetch_table_lineage(conn, args.table)
    if not entries:
        print(f"(no active task declares {args.table!r} as a SOURCE_OBJECT or TARGET_OBJECT)")
        return 0
    for entry in entries:
        print(f"{entry.pipeline_code}.{entry.task_code}\t{entry.role}")
    return 0


def _column_lineage_command(args: argparse.Namespace, engine: Engine) -> int:
    with engine.begin() as conn:
        try:
            produced_by, feeds = column_lineage_for(conn, args.column, refresh=args.refresh)
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2

    if not produced_by and not feeds:
        print(f"(no SQL task's SOURCE_SQL mentions {args.column!r})")
        return 0

    if produced_by:
        print(f"{args.column} is produced by:")
        for task in produced_by:
            for edge in task.edges:
                origin = (
                    f"{edge.source_object}.{edge.source_column}"
                    if edge.source_object and edge.source_column
                    else "(a literal or computed value)"
                )
                suffix = f"  [{edge.transformation}]" if edge.transformation else ""
                print(f"  {task.pipeline_code}.{task.task_code}  <- {origin}{suffix}")
    if feeds:
        print(f"{args.column} feeds:")
        for task in feeds:
            for edge in task.edges:
                print(
                    f"  {task.pipeline_code}.{task.task_code}  -> "
                    f"{edge.target_object}.{edge.target_column}"
                )
    return 0


def _docs_version_command(args: argparse.Namespace, engine: Engine) -> int:
    if args.task_code:
        if not args.pipeline_code:
            print("error: --pipeline_code is required with --task_code", file=sys.stderr)
            return 2
        with engine.connect() as conn:
            try:
                pipeline_id = resolve_pipeline_id(conn, args.pipeline_code)
                task_id = resolve_task_id(conn, pipeline_id, args.task_code)
            except CfgError as exc:
                print(f"error: {exc}", file=sys.stderr)
                return 1
            history = fetch_history(conn, task_id)
        if not history:
            print(f"(no recorded documentation for {args.pipeline_code}.{args.task_code})")
            return 0
        for version, documentation, recorded_at in history:
            print(f"v{version}\t{recorded_at}\n  {documentation}\n")
        return 0

    with engine.begin() as conn:
        results = refresh_all(conn)
    if not results:
        print("(no active task declares a DOCUMENTATION parameter)")
        return 0
    changed = [r for r in results if r[3]]
    for pipeline_code, task_code, version, was_changed in results:
        marker = "updated" if was_changed else "unchanged"
        print(f"{pipeline_code}.{task_code}\tv{version}\t{marker}")
    print(f"\ndocs-version: {len(changed)} of {len(results)} task(s) updated")
    return 0


def _doctor_command(config_path: Path) -> int:
    try:
        config = load_config(config_path)
    except ConfigError as exc:
        # Reported as a check, not as the usual exit-2 setup failure: a
        # missing or malformed config file is exactly what doctor exists to
        # tell you about.
        print(f"[FAIL] Configuration: {exc}", file=sys.stderr)
        return 1
    results = run_checks(config)
    for result in results:
        print(f"[{result.marker}] {result.name}: {result.detail}")
    failures = [r for r in results if not r.ok]
    if failures:
        print(f"\ndoctor: {len(failures)} check(s) failed", file=sys.stderr)
        return 1
    print("\ndoctor: all checks passed")
    return 0


def _init_db_command(args: argparse.Namespace, engine: Engine) -> int:
    try:
        count = init_db(engine, force=args.force)
    except (InitDbError, FileNotFoundError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"init-db: applied {count} statement(s) from the packaged schema")
    return 0


def _migrate_command(args: argparse.Namespace, engine: Engine) -> int:
    try:
        applied = apply_pending_migrations(engine, args.migrations_dir)
    except MigrationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    if not applied:
        print("migrate: already up to date")
    else:
        for version in applied:
            print(f"applied {version}")
    return 0


def _steps_command(args: argparse.Namespace, engine: Engine) -> int:
    try:
        with engine.connect() as conn:
            pipeline_id = resolve_pipeline_id(conn, args.pipeline_code)
            steps = fetch_pipeline_steps(conn, pipeline_id)
    except CfgError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    if not steps:
        print("(no active tasks)")
        return 0
    for step in steps:
        params = ", ".join(f"{name}={value}" for name, value in step.parameters.items())
        print(f"{step.task_code}\t{step.handler}\t{params}")
    return 0


def _history_command(args: argparse.Namespace, engine: Engine) -> int:
    try:
        with engine.connect() as conn:
            pipeline_id = resolve_pipeline_id(conn, args.pipeline_code)
            if args.task_code is None:
                pipeline_entries = fetch_pipeline_run_history(conn, pipeline_id, limit=args.limit)
                task_entries = None
            else:
                task_id = resolve_task_id(conn, pipeline_id, args.task_code)
                task_entries = fetch_task_run_history(conn, task_id, limit=args.limit)
                pipeline_entries = None
    except CfgError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if task_entries is not None:
        if not task_entries:
            print("(no logged runs)")
            return 0
        for entry in task_entries:
            print(
                f"pipeline_run_id={entry.pipeline_run_id}\t{entry.status}\t{entry.start_date}\t"
                f"{entry.end_date or ''}\t{entry.error_message or ''}"
            )
        return 0

    if not pipeline_entries:
        print("(no logged runs)")
        return 0
    for pipeline_entry in pipeline_entries:
        print(
            f"pipeline_run_id={pipeline_entry.pipeline_run_id}\t{pipeline_entry.status}\t"
            f"{pipeline_entry.start_date}\t{pipeline_entry.end_date or ''}"
        )
    return 0


def _generate_docs_command(args: argparse.Namespace, engine: Engine) -> int:
    output_dir = Path(args.output)
    with engine.connect() as conn:
        generate_docs(conn, output_dir)
    print(f"documentation site written to {output_dir}/")
    return 0
