"""Agent lifecycle writes: registration, heartbeats, expiry and an agent's claim of an attempt."""

from __future__ import annotations

from sqlalchemy.engine import Connection, Row

from etl_craft.engine.queries import statement


def claim_queued_attempt(
    conn: Connection, agent_id: int, pool: str, free: dict[str, bool]
) -> Row[tuple[object, ...]] | None:
    """Claim the highest-priority queued attempt this agent has a free slot for, or ``None``."""
    return conn.execute(
        statement(conn, "transition_claim_queued_attempt"),
        {"owner": str(agent_id), "agent_id": agent_id, "pool": pool, **free},
    ).one_or_none()


def register_agent(conn: Connection, params: dict[str, object]) -> Row[tuple[object, ...]] | None:
    """Record a live agent; ``None`` while another process holds the same registration."""
    return conn.execute(statement(conn, "transition_register_agent"), params).one_or_none()


def heartbeat_agent(conn: Connection, params: dict[str, object]) -> Row[tuple[object, ...]] | None:
    """Set a live agent's status and capacity; ``None`` once it is GONE or expired."""
    return conn.execute(statement(conn, "transition_heartbeat_agent"), params).one_or_none()


def expire_agents(conn: Connection) -> None:
    """Mark agents GONE whose heartbeat is older than three intervals."""
    conn.execute(statement(conn, "transition_expire_agents"))
