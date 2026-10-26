"""Read command and metadata audit records with preserved deleted-object identities."""

from __future__ import annotations

import json
from collections.abc import Set
from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, DateTime, bindparam, text
from sqlalchemy.engine import Connection

from etl_craft.engine.queries import statement
from etl_craft.engine.repository.config_rows import TABLES


def document(value: Any) -> Any:
    """Decode SQLite JSON text while preserving PostgreSQL's decoded objects."""
    return json.loads(value) if isinstance(value, str) else value


def fetch_audit_records(
    conn: Connection,
    pipeline_id: int | None,
    since: datetime | None,
    *,
    pipeline_ids: Set[int] | None = None,
    project_id: int | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Read requests and metadata changes, including tasks no longer present in CFG_TASKS."""
    selected_ids = {pipeline_id} if pipeline_id is not None else pipeline_ids
    task_ids: set[int] = set()
    if selected_ids is not None:
        task_ids.update(
            row.task_id
            for row in conn.execute(statement(conn, "audit_task_pipelines"))
            if row.pipeline_id in selected_ids
        )
        for before, after in conn.execute(
            text(
                "SELECT BEFORE_JSON AS before_json, AFTER_JSON AS after_json "
                "FROM AUD_METADATA_CHANGES WHERE TABLE_NAME='CFG_TASKS'"
            )
        ):
            for captured in (before, after):
                doc = document(captured)
                if doc is not None and doc.get("pipeline_id") in selected_ids:
                    task_ids.add(doc["task_id"])
    actions = [
        dict(row._mapping)
        for row in conn.execute(
            statement(conn, "audit_actions").bindparams(
                bindparam("pipeline", type_=BigInteger),
                bindparam("project", type_=BigInteger),
                bindparam("since", type_=DateTime(timezone=True)),
            ),
            {"pipeline": pipeline_id, "since": since, "project": project_id},
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
    if project_id is not None:
        changes = project_changes(conn, project_id, changes)
    if selected_ids is not None:
        actions = [action for action in actions if action["pipeline_id"] in selected_ids]
        changes = [
            change
            for change in changes
            if any(
                doc.get("pipeline_id") in selected_ids
                or doc.get("depends_on_pipeline_id") in selected_ids
                or doc.get("task_id") in task_ids
                or doc.get("depends_on_task_id") in task_ids
                for doc in (change["before_json"], change["after_json"])
                if doc is not None
            )
        ]
    return actions, changes


def project_changes(
    conn: Connection, project_id: int, changes: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Keep the selected project's history and shared deployment configuration."""
    params = {"project_id": project_id}
    pipelines = set(conn.execute(statement(conn, "audit_project_pipeline_ids"), params).scalars())
    tables = set(conn.execute(statement(conn, "audit_project_table_ids"), params).scalars())
    tasks = {
        row.task_id
        for row in conn.execute(statement(conn, "audit_task_pipelines"))
        if row.pipeline_id in pipelines
    }
    for change in changes:
        if change["table_name"].upper() == "CFG_TASKS":
            tasks.update(
                doc["task_id"]
                for doc in (change["before_json"], change["after_json"])
                if doc is not None and doc.get("pipeline_id") in pipelines
            )
    scoped = {table.table for table in TABLES} | {"CFG_PROJECTS", "CFG_PIPELINE_GRANTS"}
    return [
        change
        for change in changes
        if change["table_name"].upper() not in scoped
        or any(
            doc.get("project_id") == project_id
            or doc.get("pipeline_id") in pipelines
            or doc.get("task_id") in tasks
            or doc.get("table_id") in tables
            for doc in (change["before_json"], change["after_json"])
            if doc is not None
        )
    ]
