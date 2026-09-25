"""``etl-craft generate-yml``: a pipeline's DAG, the global trigger DAG or the docs DAG, as YAML."""

from __future__ import annotations

import argparse
from pathlib import Path

from etl_craft.cli.commands import Command
from etl_craft.cli.commands.common import connect_engine_db, load_command_config
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode
from etl_craft.services.generate_yml import docs_dag, global_dag, pipeline_dag, to_yaml


def _configure(parser: argparse.ArgumentParser) -> None:
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--pipeline_code", help="the pipeline whose DAG to write")
    target.add_argument(
        "--global",
        action="store_true",
        dest="global_dag",
        help="write the DAG that triggers pipelines in dependency order (needs Global_dag: true)",
    )
    target.add_argument(
        "--docs",
        action="store_true",
        help="write the DAG that writes the catalog site again on Docs_site.Schedule",
    )
    parser.add_argument("--output", type=Path, metavar="PATH", help="write here, not to stdout")


def _run(args: argparse.Namespace, out: Output) -> int:
    config = load_command_config(args)
    if args.docs:
        dag = docs_dag(config)
    else:
        engine = connect_engine_db(config)
        try:
            with engine.connect() as conn:
                dag = (
                    global_dag(conn, config)
                    if args.global_dag
                    else pipeline_dag(conn, config, args.pipeline_code)
                )
        finally:
            engine.dispose()
    text = to_yaml(dag)
    if args.output is None:
        out.stdout.write(text)
        return ExitCode.SUCCESS
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(text, encoding="utf-8")
    out.line(f"wrote {args.output}")
    return ExitCode.SUCCESS


COMMAND = Command(
    name="generate-yml",
    help="Write a pipeline's DAG, the global trigger DAG or the docs DAG, as YAML.",
    configure=_configure,
    run=_run,
)
