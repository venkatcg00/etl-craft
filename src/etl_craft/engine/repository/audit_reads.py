"""Read command and metadata audit records with preserved deleted-object identities."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, DateTime, bindparam, text
from sqlalchemy.engine import Connection


def document(value: Any) -> Any:
    """Decode SQLite JSON text while preserving PostgreSQL's decoded objects."""
    return json.loads(value) if isinstance(value, str) else value


def fetch_audit_records(
    conn: Connection,
    pipeline_id: int | None,
    since: datetime | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Read requests and metadata changes, including tasks no longer present in CFG_TASKS."""
    task_ids: set[int] = set()
    if pipeline_id is not None:
        task_ids.update(
            conn.execute(
                text("SELECT TASK_ID AS task_id FROM CFG_TASKS WHERE PIPELINE_ID=:pipeline"),
                {"pipeline": pipeline_id},
            ).scalars()
        )
        for before, after in conn.execute(
            text(
                "SELECT BEFORE_JSON AS before_json, AFTER_JSON AS after_json "
                "FROM AUD_METADATA_CHANGES WHERE TABLE_NAME='CFG_TASKS'"
            )
        ):
            for captured in (before, after):
                doc = document(captured)
                if doc is not None and doc.get("pipeline_id") == pipeline_id:
                    task_ids.add(doc["task_id"])
    actions = [
        dict(row._mapping)
        for row in conn.execute(
            text(
                "SELECT ACTION_ID AS action_id, PIPELINE_ID AS pipeline_id, TASK_ID AS task_id, "
                "STARTED_AT AS at, ACTOR AS actor, ACTOR_KIND AS kind, COMMAND AS command, "
                "OUTCOME AS outcome, ARGUMENTS AS arguments FROM AUD_ACTIONS "
                "WHERE (:pipeline IS NULL OR PIPELINE_ID=:pipeline) "
                "AND (:since IS NULL OR STARTED_AT>=:since) ORDER BY ACTION_ID"
            ).bindparams(
                bindparam("pipeline", type_=BigInteger),
                bindparam("since", type_=DateTime(timezone=True)),
            ),
            {"pipeline": pipeline_id, "since": since},
        )
    ]
    changes = [
        dict(row._mapping)
        for row in conn.execute(
            text(
                "SELECT CHANGE_ID AS change_id, CHANGED_AT AS at, ACTOR AS actor, "
                "ACTOR_KIND AS kind, TABLE_NAME AS table_name, ROW_KEY AS row_key, "
                "OPERATION AS operation, BEFORE_JSON AS before_json, AFTER_JSON AS after_json, "
                "MIGRATION AS migration FROM AUD_METADATA_CHANGES "
                "WHERE (:since IS NULL OR CHANGED_AT>=:since) ORDER BY CHANGE_ID"
            ).bindparams(bindparam("since", type_=DateTime(timezone=True))),
            {"since": since},
        )
    ]
    for action in actions:
        action["arguments"] = document(action["arguments"])
    for change in changes:
        for key in ("row_key", "before_json", "after_json"):
            change[key] = document(change[key])
    if pipeline_id is not None:
        changes = [
            change
            for change in changes
            if any(
                doc.get("pipeline_id") == pipeline_id
                or doc.get("depends_on_pipeline_id") == pipeline_id
                or doc.get("task_id") in task_ids
                or doc.get("depends_on_task_id") in task_ids
                for doc in (change["before_json"], change["after_json"])
                if doc is not None
            )
        ]
    return actions, changes
