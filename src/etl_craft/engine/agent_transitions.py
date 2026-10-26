"""Agent lifecycle writes: registration, heartbeats, expiry and an agent's claim of an attempt."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import bindparam
from sqlalchemy.engine import Connection, Row

from etl_craft.core.actor import Actor
from etl_craft.engine import controls
from etl_craft.engine.metrics import lifecycle
from etl_craft.engine.queries import statement


def claim_queued_attempt(
    conn: Connection, agent_id: int, pool: str, free: dict[str, object]
) -> Row[tuple[object, ...]] | None:
    """Claim the highest-priority queued attempt this agent has a free slot for, or ``None``."""
    from etl_craft.engine.placement import claim_parameters

    claimed = conn.execute(
        statement(conn, "transition_claim_queued_attempt"),
        {
            **claim_parameters(),
            "owner": str(agent_id),
            "agent_id": agent_id,
            "pool": pool,
            **free,
        },
    ).one_or_none()
    if claimed is not None:
        lifecycle(conn, "dispatch", claimed.attempt_id)
    return claimed


def register_agent(conn: Connection, params: dict[str, object]) -> Row[tuple[object, ...]] | None:
    """Record a live agent; ``None`` while another process holds the same registration."""
    return conn.execute(statement(conn, "transition_register_agent"), params).one_or_none()


def heartbeat_agent(conn: Connection, params: dict[str, object]) -> Row[tuple[object, ...]] | None:
    """Set a live agent's status and capacity; ``None`` once it is GONE or expired."""
    return conn.execute(statement(conn, "transition_heartbeat_agent"), params).one_or_none()


def expire_agents(conn: Connection) -> None:
    """Mark agents GONE whose heartbeat is older than three intervals."""
    conn.execute(statement(conn, "transition_expire_agents"))


def claim_with_capacity(
    conn: Connection,
    attempt_id: int,
    actor: Actor,
    capacity: dict[str, object],
    *,
    wait: bool = False,
) -> bool:
    """Atomically reserve resources with ownership, leaving limited attempts queued."""
    from etl_craft.engine import transitions
    from etl_craft.engine.placement import claim_parameters

    controls.guard_attempt(conn, attempt_id)
    with conn.begin_nested():
        row = conn.execute(
            statement(conn, "transition_claim_attempt").bindparams(
                bindparam("candidate_ids", expanding=True)
            ),
            {
                **{"actor": actor.name, "actor_kind": actor.kind.value, "now": datetime.now(UTC)},
                **claim_parameters(),
                "owner": None,
                "lease": None,
                "host": None,
                "row_id": attempt_id,
                "candidate_ids": (attempt_id,),
                "warehouse_limit": None,
                "event_wait": False,
                "local_cpu": None,
                "local_memory_mb": None,
                "local_slots": None,
                "python_slots": None,
                "warehouse_slots": None,
                **capacity,
            },
        ).one_or_none()
        if row is None:
            if wait:
                return False
            raise transitions.stale_attempt(conn, attempt_id, "QUEUED (unowned)", None)
        transitions.sync_attempt_summary(conn, attempt_id)
        lifecycle(conn, "dispatch", attempt_id)
        return True
