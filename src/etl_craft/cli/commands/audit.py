"""Read command requests and metadata changes without changing the Engine DB."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime

from sqlalchemy import BigInteger, DateTime, bindparam, text

from etl_craft.cli.commands import Command
from etl_craft.cli.commands.common import connect_engine_db, load_command_config
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode
from etl_craft.engine.repository.pipelines import resolve_pipeline_id


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pipeline_code", help="filter requests and related metadata rows")
    parser.add_argument(
        "--since", type=datetime.fromisoformat, help="changes since an ISO date or timestamp"
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    since = args.since
    if since is not None and since.tzinfo is None:
        since = since.replace(tzinfo=UTC)
    engine = connect_engine_db(load_command_config(args))
    try:
        with engine.connect() as conn:
            pipeline = (
                None
                if args.pipeline_code is None
                else resolve_pipeline_id(conn, args.pipeline_code)
            )
            task_ids: set[int] = set()
            if pipeline is not None:
                task_ids.update(
                    conn.execute(
                        text(
                            "SELECT TASK_ID AS task_id FROM CFG_TASKS WHERE PIPELINE_ID=:pipeline"
                        ),
                        {"pipeline": pipeline},
                    ).scalars()
                )
                for before, after in conn.execute(
                    text(
                        "SELECT BEFORE_JSON AS before_json, AFTER_JSON AS after_json "
                        "FROM AUD_METADATA_CHANGES WHERE TABLE_NAME='CFG_TASKS'"
                    )
                ):
                    for captured in (before, after):
                        if captured is not None:
                            doc = captured if isinstance(captured, dict) else json.loads(captured)
                            if doc.get("pipeline_id") == pipeline:
                                task_ids.add(doc["task_id"])
            actions = conn.execute(
                text(
                    "SELECT STARTED_AT AS at, ACTOR AS actor, ACTOR_KIND AS kind, "
                    "COMMAND AS command, "
                    "OUTCOME AS outcome, ARGUMENTS AS arguments FROM AUD_ACTIONS "
                    "WHERE (:pipeline IS NULL OR PIPELINE_ID=:pipeline) "
                    "AND (:since IS NULL OR STARTED_AT>=:since) ORDER BY ACTION_ID"
                ).bindparams(
                    bindparam("pipeline", type_=BigInteger),
                    bindparam("since", type_=DateTime(timezone=True)),
                ),
                {"pipeline": pipeline, "since": since},
            ).all()
            # Metadata rows preserve deleted objects, so filter their captured identities.
            changes = conn.execute(
                text(
                    "SELECT CHANGED_AT AS at, ACTOR AS actor, ACTOR_KIND AS kind, "
                    "TABLE_NAME AS table_name, "
                    "ROW_KEY AS row_key, OPERATION AS operation, BEFORE_JSON AS before_json, "
                    "AFTER_JSON AS after_json, MIGRATION AS migration FROM AUD_METADATA_CHANGES "
                    "WHERE (:since IS NULL OR CHANGED_AT>=:since) ORDER BY CHANGE_ID"
                ).bindparams(bindparam("since", type_=DateTime(timezone=True))),
                {"since": since},
            ).all()
    finally:
        engine.dispose()
    out.rows([("AT", "ACTOR", "KIND", "COMMAND", "OUTCOME", "ARGUMENTS")])
    out.rows(tuple(row) for row in actions)
    out.rows([("AT", "ACTOR", "KIND", "TABLE", "ROW", "OPERATION", "BEFORE", "AFTER", "MIGRATION")])
    if pipeline is not None:
        filtered = []
        for row in changes:
            documents = [
                doc if isinstance(doc, dict) else json.loads(doc)
                for doc in (row.before_json, row.after_json)
                if doc is not None
            ]
            if any(
                doc.get("pipeline_id") == pipeline
                or doc.get("depends_on_pipeline_id") == pipeline
                or doc.get("task_id") in task_ids
                or doc.get("depends_on_task_id") in task_ids
                for doc in documents
            ):
                filtered.append(row)
        changes = filtered
    out.rows(tuple(row) for row in changes)
    return ExitCode.SUCCESS


COMMAND = Command(
    "audit", "Show command requests and metadata changes with their actors.", _configure, _run
)
