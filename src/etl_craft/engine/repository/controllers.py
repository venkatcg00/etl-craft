"""Controller process history and a working set bounded to active runs."""

from __future__ import annotations

import os
import socket
from datetime import UTC, datetime

from sqlalchemy.engine import Engine

from etl_craft import __version__
from etl_craft.core.actor import current_actor
from etl_craft.engine.queries import statement


def active_controller(engine: Engine) -> str:
    """Describe the latest unclosed process without treating history as a lock."""
    with engine.connect() as conn:
        row = conn.execute(statement(conn, "controller_latest")).one_or_none()
    return (
        "another controller is active"
        if row is None
        else (f"controller {row.controller_id} on {row.host} is active")
    )


def start(engine: Engine) -> int:
    """Record a leader only after its session lock has been acquired."""
    actor = current_actor()
    now = datetime.now(UTC)
    with engine.begin() as conn:
        return int(
            conn.execute(
                statement(conn, "controller_start"),
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


def heartbeat(engine: Engine, controller_id: int, *, stopped: bool = False) -> None:
    """Renew this process's history, optionally recording its clean shutdown."""
    actor = current_actor()
    now = datetime.now(UTC)
    with engine.begin() as conn:
        conn.execute(
            statement(conn, "controller_heartbeat"),
            {
                "now": now,
                "stopped": stopped,
                "actor": actor.name,
                "kind": actor.kind.value,
                "id": controller_id,
            },
        )
    from etl_craft.engine.metrics import state

    metrics = state(engine)
    if metrics is not None:
        metrics.heartbeat = now.timestamp()
