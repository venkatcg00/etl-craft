"""Overseer process history and a working set bounded to active runs."""

from __future__ import annotations

import os
import socket
from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.engine import Engine

from etl_craft import __version__
from etl_craft.core.actor import current_actor


def active_overseer(engine: Engine) -> str:
    """Describe the latest unclosed process without treating history as a lock."""
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT OVERSEER_ID AS overseer_id, HOST AS host FROM AUD_OVERSEERS "
                "WHERE STOPPED_AT IS NULL ORDER BY OVERSEER_ID DESC LIMIT 1"
            )
        ).one_or_none()
    return (
        "another overseer is active"
        if row is None
        else (f"overseer {row.overseer_id} on {row.host} is active")
    )


def start(engine: Engine) -> int:
    """Record a leader only after its session lock has been acquired."""
    actor = current_actor()
    now = datetime.now(UTC)
    with engine.begin() as conn:
        return int(
            conn.execute(
                text(
                    "INSERT INTO AUD_OVERSEERS (HOST,PID,VERSION,STARTED_AT,HEARTBEAT_AT,"
                    "STARTED_BY,STARTED_BY_KIND) "
                    "VALUES (:host,:pid,:version,:now,:now,:actor,:kind) "
                    "RETURNING OVERSEER_ID AS overseer_id"
                ),
                {
                    "host": socket.gethostname(),
                    "pid": os.getpid(),
                    "version": __version__,
                    "now": now,
                    "actor": actor.name,
                    "kind": actor.kind.value,
                },
            ).scalar_one()
        )


def heartbeat(engine: Engine, overseer_id: int, *, stopped: bool = False) -> None:
    """Renew this process's history, optionally recording its clean shutdown."""
    actor = current_actor()
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE AUD_OVERSEERS SET HEARTBEAT_AT=:now, "
                "STOPPED_AT=CASE WHEN :stopped THEN :now ELSE STOPPED_AT END, "
                "STOPPED_BY=CASE WHEN :stopped THEN :actor ELSE STOPPED_BY END, "
                "STOPPED_BY_KIND=CASE WHEN :stopped THEN :kind ELSE STOPPED_BY_KIND END "
                "WHERE OVERSEER_ID=:id AND STOPPED_AT IS NULL"
            ),
            {
                "now": datetime.now(UTC),
                "stopped": stopped,
                "actor": actor.name,
                "kind": actor.kind.value,
                "id": overseer_id,
            },
        )
