"""Task documentation and its versions.

A task's ``DOCUMENTATION`` parameter holds its prose. ``docs-version`` records a new version in
``AUD_TASK_DOCUMENTATION`` whenever the text has changed since the last one, told apart by a hash
of the text with surrounding whitespace removed, so the version cannot drift from the text the
way a hand-set number would.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy.engine import Connection

from etl_craft.core.text import sha256_hex
from etl_craft.engine.queries import statement


@dataclass(frozen=True)
class DocumentationVersion:
    """A documented task's current version, and whether this call recorded it."""

    pipeline_code: str
    task_code: str
    version: int
    changed: bool


def documentation_hash(text: str) -> str:
    """Hash documentation text; whitespace around it does not count as a change."""
    return sha256_hex(text.strip().encode())


def refresh_versions(conn: Connection, *, record: bool = True) -> list[DocumentationVersion]:
    """Return every documented task's version, recording a new one where the text changed.

    Without ``record`` nothing is written, and a changed text reports its next version.
    """
    now = datetime.now(UTC)
    results: list[DocumentationVersion] = []
    for row in conn.execute(statement(conn, "documented_tasks")).all():
        latest = conn.execute(
            statement(conn, "latest_task_documentation"), {"task_id": row.task_id}
        ).one_or_none()
        digest = documentation_hash(row.documentation)
        if latest is not None and latest.documentation_hash == digest:
            results.append(
                DocumentationVersion(row.pipeline_code, row.task_code, int(latest.version), False)
            )
            continue
        version = 1 if latest is None else int(latest.version) + 1
        if record:
            conn.execute(
                statement(conn, "insert_task_documentation"),
                {
                    "task_id": row.task_id,
                    "version": version,
                    "documentation_hash": digest,
                    "documentation": row.documentation,
                    "now": now,
                },
            )
        results.append(DocumentationVersion(row.pipeline_code, row.task_code, version, True))
    return results
