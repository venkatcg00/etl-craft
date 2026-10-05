"""``etl-craft reconcile``: fence expired attempts and release expired idle supervisors."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands import Command
from etl_craft.cli.commands.common import connect_engine_db, load_command_config
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode, UsageError
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.repository.tasks import resolve_task_id
from etl_craft.execution.reconcile import reconcile


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pipeline_code", help="limit reconciliation to this pipeline")
    parser.add_argument("--task_code", help="limit reconciliation to this pipeline's task")


def _run(args: argparse.Namespace, out: Output) -> int:
    if args.task_code and not args.pipeline_code:
        raise UsageError("--task_code needs --pipeline_code")
    engine = connect_engine_db(load_command_config(args))
    try:
        with engine.connect() as conn:
            pipeline_id = (
                resolve_pipeline_id(conn, args.pipeline_code) if args.pipeline_code else None
            )
            task_id = (
                resolve_task_id(conn, pipeline_id, args.task_code)
                if pipeline_id is not None and args.task_code
                else None
            )
        report = reconcile(engine, pipeline_id=pipeline_id, task_id=task_id)
    finally:
        engine.dispose()
    out.line(
        f"reconcile: {len(report.lost)} attempt(s) LOST; "
        f"{len(report.released)} run lease(s) released"
    )
    return ExitCode.SUCCESS


COMMAND = Command(
    name="reconcile",
    help="Reconcile expired attempt and run leases.",
    configure=_configure,
    run=_run,
)
