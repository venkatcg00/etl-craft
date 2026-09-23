"""Per-task documentation and its version history.

[ADDITION, 2026-09-20] Per explicit instruction: "a task parameter for
documentation, which can be used for documentation, documentation versioning
... the documentation update is per task. the versioning should be per task.
updated on demand."

`CFG_TASK_PARAMETERS.DOCUMENTATION` holds the prose. It is deliberately an
ordinary parameter rather than a CFG_TASKS column, for the same reason
SCRIPT_NAME and SCHEMA_EVOLUTION moved there: CFG_TASKS holds only what is
true of every task.

[CHOICE] The version is derived from a hash of the text and bumped only when
the text genuinely changes, rather than being a second parameter an author
sets by hand. A hand-set version drifts out of sync the moment someone edits
one and not the other, and the entire point of a version here is to tell
current documentation from stale. Same reasoning as HASH_KEY for SCD change
detection: derive the change signal, do not ask for it.

Versions are recorded on demand — `etl-craft docs-version` and
`generate-docs` both refresh them — not on every task run, because
documentation changes when someone edits it, which has nothing to do with
whether a pipeline happened to execute.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.engine import Connection

DOCUMENTATION_PARAM = "DOCUMENTATION"


@dataclass(frozen=True)
class TaskDocumentation:
    """One task's current documentation, with the version it is at."""

    pipeline_code: str
    task_code: str
    task_id: int
    documentation: str
    version: int
    recorded_at: object


def documentation_hash(documentation: str) -> str:
    """Hash documentation text for change detection."""
    return hashlib.md5(documentation.strip().encode(), usedforsecurity=False).hexdigest()


def fetch_documented_tasks(conn: Connection) -> list[tuple[int, str, str, str]]:
    """Fetch (task_id, pipeline_code, task_code, documentation) for every documented task."""
    rows = conn.execute(
        text(
            "SELECT t.TASK_ID AS task_id, p.PIPELINE_CODE AS pipeline_code, "
            "t.TASK_CODE AS task_code, par.PARAMETER_VALUE AS documentation "
            "FROM CFG_TASK_PARAMETERS par "
            "JOIN CFG_TASKS t ON t.TASK_ID = par.TASK_ID "
            "JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID "
            "WHERE par.PARAMETER_NAME = :param AND par.ACTIVE_FLAG = 'Y' "
            "AND t.ACTIVE_FLAG = 'Y' AND p.ACTIVE_FLAG = 'Y' "
            "ORDER BY p.PIPELINE_CODE, t.TASK_CODE"
        ),
        {"param": DOCUMENTATION_PARAM},
    ).all()
    return [(r.task_id, r.pipeline_code, r.task_code, r.documentation) for r in rows]


def refresh_task_documentation(conn: Connection, task_id: int, documentation: str) -> int:
    """Record `documentation` for `task_id` if it changed. Returns its current version."""
    current = conn.execute(
        text(
            "SELECT VERSION AS version, DOCUMENTATION_HASH AS documentation_hash "
            "FROM AUD_TASK_DOCUMENTATION WHERE TASK_ID = :task_id "
            "ORDER BY VERSION DESC LIMIT 1"
        ),
        {"task_id": task_id},
    ).one_or_none()
    new_hash = documentation_hash(documentation)
    if current is not None and current.documentation_hash == new_hash:
        return int(current.version)

    next_version = 1 if current is None else int(current.version) + 1
    conn.execute(
        text(
            "INSERT INTO AUD_TASK_DOCUMENTATION "
            "(TASK_ID, VERSION, DOCUMENTATION_HASH, DOCUMENTATION) "
            "VALUES (:task_id, :version, :documentation_hash, :documentation)"
        ),
        {
            "task_id": task_id,
            "version": next_version,
            "documentation_hash": new_hash,
            "documentation": documentation,
        },
    )
    return next_version


def refresh_all(conn: Connection, *, record: bool = True) -> list[tuple[str, str, int, bool]]:
    """Refresh every documented task. Returns (pipeline, task, version, changed) per task.

    [ADDITION, 2026-09-20, E2-56] `record=False` reports the current versions
    without writing new ones, for `generate-docs` — a documentation build is a
    read-only verb, and its own connection never committed anyway, so every
    version it recorded was silently discarded.
    """
    results: list[tuple[str, str, int, bool]] = []
    for task_id, pipeline_code, task_code, documentation in fetch_documented_tasks(conn):
        before = current_version(conn, task_id)
        if not record:
            results.append((pipeline_code, task_code, before or 0, False))
            continue
        version = refresh_task_documentation(conn, task_id, documentation)
        results.append((pipeline_code, task_code, version, version != before))
    return results


def current_version(conn: Connection, task_id: int) -> int | None:
    """Return `task_id`'s current documentation version, or None if never recorded."""
    return conn.execute(
        text(
            "SELECT VERSION FROM AUD_TASK_DOCUMENTATION WHERE TASK_ID = :task_id "
            "ORDER BY VERSION DESC LIMIT 1"
        ),
        {"task_id": task_id},
    ).scalar_one_or_none()


def fetch_history(conn: Connection, task_id: int) -> list[tuple[int, str, object]]:
    """Return every recorded version of `task_id`'s documentation, newest first."""
    rows = conn.execute(
        text(
            "SELECT VERSION AS version, DOCUMENTATION AS documentation, "
            "RECORDED_AT AS recorded_at FROM AUD_TASK_DOCUMENTATION "
            "WHERE TASK_ID = :task_id ORDER BY VERSION DESC"
        ),
        {"task_id": task_id},
    ).all()
    return [(int(r.version), r.documentation, r.recorded_at) for r in rows]


def fetch_recorded_versions(conn: Connection) -> dict[tuple[str, str], int]:
    """Map (pipeline_code, task_code) -> its latest recorded documentation version.

    [DEVIATION, 2026-09-20, E2-56] Replaces `fetch_current_documentation`,
    which returned the recorded *text* as well. A documentation page should
    show what the DOCUMENTATION parameter says right now, not what was last
    recorded — otherwise an edit is invisible until someone runs
    `docs-version`. Only the version number needs the audit table, and it is
    simply absent until a version has been recorded.

    [DEVIATION, 2026-09-23, E3-03] Keyed by (pipeline_code, task_code), not
    bare task_code. TASK_CODE is only unique *per pipeline*
    (`ux_tasks_code_active` is a (PIPELINE_ID, TASK_CODE) index — schema.sql's
    own comment calls it out as "scoped per-pipeline, not global"). The SQL
    below already computes each version per TASK_ID correctly; collapsing
    that into a dict keyed by the bare code let two independently-authored
    pipelines that happen to share an ordinary task name (LOAD, VALIDATE, ...)
    silently overwrite each other's version badge, with whichever pipeline's
    page rendered last winning on both.
    """
    rows = conn.execute(
        text(
            "SELECT DISTINCT ON (d.TASK_ID) p.PIPELINE_CODE AS pipeline_code, "
            "t.TASK_CODE AS task_code, d.VERSION AS version "
            "FROM AUD_TASK_DOCUMENTATION d "
            "JOIN CFG_TASKS t ON t.TASK_ID = d.TASK_ID "
            "JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID "
            "ORDER BY d.TASK_ID, d.VERSION DESC"
        )
    ).all()
    return {(r.pipeline_code, r.task_code): int(r.version) for r in rows}
