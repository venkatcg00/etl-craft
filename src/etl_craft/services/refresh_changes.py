"""Describe changed metadata identities and project file contents between refreshes."""

import json
from datetime import datetime

from sqlalchemy.engine import Connection

from etl_craft.core.enums import RunStatus
from etl_craft.core.time import as_utc
from etl_craft.engine.bundles import Bundle
from etl_craft.engine.queries import statement


def differences(
    conn: Connection, pipeline_id: int, snapshot: str, bundle_id: str | None
) -> tuple[tuple[str, ...], datetime | None, RunStatus | None]:
    """Compare against the latest outcome, including failed outcomes."""
    previous = conn.execute(
        statement(conn, "refresh_previous"), {"pipeline_id": pipeline_id}
    ).one_or_none()
    old = {} if previous is None else json.loads(previous.snapshot)
    current = json.loads(snapshot)
    changes = []
    for table in sorted(old.keys() | current.keys()):
        before = old.get(table, {}).get("rows", [])
        after = current.get(table, {}).get("rows", [])
        if before == after:
            continue
        label = "tasks" if table in {"CFG_TASKS", "CFG_TASK_PARAMETERS"} else "tables"
        code = {"CFG_TASKS": "task_code", "CFG_TABLES": "table_name"}.get(table)
        previous_rows = {str(row.get(code)): row for row in before} if code else {}
        current_rows = {str(row.get(code)): row for row in after} if code else {}
        names = sorted(
            key
            for key in previous_rows.keys() | current_rows.keys()
            if previous_rows.get(key) != current_rows.get(key)
        )
        changes.append(f"{label}: {', '.join(names) if names else table}")
    old_files = (
        {}
        if previous is None or previous.bundle_id is None
        else dict(Bundle.load(conn, previous.bundle_id).files)
    )
    new_files = {} if bundle_id is None else dict(Bundle.load(conn, bundle_id).files)
    changes.extend(
        f"files: {name}"
        for name in sorted(old_files.keys() | new_files.keys())
        if old_files.get(name) != new_files.get(name)
    )
    return (
        tuple(changes),
        None if previous is None else as_utc(previous.refreshed_at),
        None if previous is None else RunStatus(previous.status),
    )
