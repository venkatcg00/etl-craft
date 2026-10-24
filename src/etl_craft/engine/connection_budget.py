"""Reserve the largest permitted process footprint before starting database work."""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psutil
from sqlalchemy.engine import Connection

from etl_craft.config import ConnectorConfig
from etl_craft.config.sizing import ceilings
from etl_craft.core.actor import current_actor
from etl_craft.core.enums import ProcessRole
from etl_craft.core.errors import ConfigurationError
from etl_craft.core.resources import discover, size
from etl_craft.dialects.engine import for_engine
from etl_craft.engine.queries import statement


@dataclass(frozen=True)
class Connections:
    """Maximum simultaneously open connections for one process and its local children."""

    pool: int
    sessions: int
    tasks: int
    roles: tuple[ProcessRole, ...] = ()

    @property
    def total(self) -> int:
        """Return the conservative maximum for this process and its children."""
        return self.pool + self.sessions + self.tasks


def footprint(config: ConnectorConfig, role: ProcessRole) -> Connections:
    """Count the request pool, dedicated sessions and locally permitted task children."""
    tasks = config.limits.max_parallel_tasks
    sizing = config.sizing
    if sizing is None and config.deployment is not None:
        sizing = size(discover(), config.deployment, ceilings(config))
    if sizing is not None:
        tasks = sizing.task_slots.value
    sessions = 3 if role == ProcessRole.CONTROLLER else 2 if role == ProcessRole.UI else 1
    return Connections(
        config.db_pool_size, sessions + 1 + int(role == ProcessRole.UI), tasks, (role,)
    )


def reservations(conn: Connection, now: datetime) -> Connections:
    """Count expired rows while their owning PostgreSQL session is still alive."""
    rows = conn.execute(statement(conn, "connection_reservations"), {"now": now}).all()
    return Connections(
        sum(row.pool_connections for row in rows),
        sum(row.session_connections for row in rows),
        sum(row.local_task_connections for row in rows),
        tuple(ProcessRole(row.role) for row in rows),
    )


def reserve(
    conn: Connection,
    role: ProcessRole,
    budget: Connections,
    now: datetime,
    *,
    lease_seconds: int = 60,
) -> str:
    """Serialize startup against other processes, then record this session's reservation."""
    limit = for_engine(conn).max_connections(conn)
    conn.execute(statement(conn, "connection_reservation_lock"))
    conn.execute(statement(conn, "connection_reservation_reap"), {"now": now})
    existing = reservations(conn, now)
    if limit is not None and (existing.total + budget.total) * 5 > limit * 4:
        raise ConfigurationError(
            f"Engine DB connection budget: {role} requests {budget.total} connections "
            f"with {existing.total} already reserved, max_connections={limit}; expected "
            "a deployment total at or below 80%; stop another process, lower "
            "Orchestration.Db_pool_size/local task slots, or raise max_connections"
        )
    session = conn.execute(statement(conn, "connection_reservation_session"), {"now": now}).one()
    identity = uuid4().hex
    conn.execute(statement(conn, "connection_reservation_tag"), {"identity": identity})
    actor = current_actor()
    conn.execute(
        statement(conn, "connection_reservation_insert"),
        {
            "identity": identity,
            "role": role,
            "host": socket.gethostname(),
            "pid": os.getpid(),
            "birth": psutil.Process().create_time(),
            "pool": budget.pool,
            "sessions": budget.sessions,
            "tasks": budget.tasks,
            "backend": session.backend_pid,
            "backend_started": session.backend_started,
            "now": now,
            "expires": now + timedelta(seconds=lease_seconds),
            "actor": actor.name,
            "kind": actor.kind,
        },
    )
    return identity


def heartbeat(
    conn: Connection, identity: str, now: datetime | None = None, *, lease_seconds: int = 60
) -> None:
    """Retain ownership; a closed reservation cannot silently restart."""
    now = now or datetime.now(UTC)
    session = conn.execute(statement(conn, "connection_reservation_session"), {"now": now}).one()
    result = conn.execute(
        statement(conn, "connection_reservation_heartbeat"),
        {
            "identity": identity,
            "now": now,
            "expires": now + timedelta(seconds=lease_seconds),
            "backend": session.backend_pid,
            "backend_started": session.backend_started,
        },
    )
    if result.rowcount != 1:
        raise ConfigurationError(
            f"connection reservation {identity}: ownership ended; expected one live reservation; "
            "stop this process and restart it to reserve capacity again"
        )
