"""Immutable event evidence and file consumption for successful task runs."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from sqlalchemy import bindparam
from sqlalchemy.engine import Connection

from etl_craft.core.actor import current_actor
from etl_craft.engine.queries import statement


def consumed_files(conn: Connection, task_id: int) -> set[str]:
    """Return canonical paths already consumed by this event task."""
    return set(
        conn.execute(statement(conn, "event_consumed_files"), {"task_id": task_id}).scalars()
    )


def consumed_keys(conn: Connection, task_id: int, keys: Iterable[str]) -> set[str]:
    """Return which of ``keys`` this event task already consumed, through its unique index."""
    wanted = list(keys)
    if not wanted:
        return set()
    query = statement(conn, "event_consumed_keys").bindparams(bindparam("keys", expanding=True))
    return set(conn.execute(query, {"task_id": task_id, "keys": wanted}).scalars())


def record_event(
    conn: Connection, task_id: int, task_run_id: int, evidence: dict[str, Any]
) -> bool:
    """Store evidence and reserve each file and key in the successful outcome's transaction.

    Returns ``False`` when another run consumed one of the keys first; the caller then rolls
    the whole outcome back and checks again.
    """
    conn.execute(
        statement(conn, "insert_task_event"),
        {
            "task_run_id": task_run_id,
            "evidence": json.dumps(evidence, default=str),
        },
    )
    for file in evidence.get("files", []):
        conn.execute(
            statement(conn, "consume_event_file"),
            {
                "task_id": task_id,
                "task_run_id": task_run_id,
                "file_path": file["path"],
            },
        )
    recorded = 0
    for key in evidence.get("keys", []):
        recorded += conn.execute(
            statement(conn, "consume_event_key"),
            {
                "task_id": task_id,
                "task_run_id": task_run_id,
                "event_key": key,
                "actor": current_actor().name,
            },
        ).rowcount
    return recorded == len(evidence.get("keys", []))


@dataclass(frozen=True)
class EventData:
    """Successful evidence under one exact run, shared by local and remote task contexts."""

    files: tuple[str, ...]
    keys: tuple[str, ...]
    rows: tuple[Mapping[str, Any], ...]


def run_event_files(conn: Connection, pipeline_run_id: int) -> tuple[str, ...]:
    """Return deduplicated paths from successful event tasks under this exact run."""
    return run_event_data(conn, pipeline_run_id).files


def run_event_data(conn: Connection, pipeline_run_id: int) -> EventData:
    """Read immutable evidence once, without consulting the live signal table."""
    evidence = [
        json.loads(row.evidence)
        for row in conn.execute(
            statement(conn, "run_event_files"), {"pipeline_run_id": pipeline_run_id}
        )
    ]
    return EventData(
        tuple(dict.fromkeys(file["path"] for item in evidence for file in item.get("files", []))),
        tuple(key for item in evidence for key in item.get("keys", [])),
        tuple(row for item in evidence for row in item.get("rows", [])),
    )
