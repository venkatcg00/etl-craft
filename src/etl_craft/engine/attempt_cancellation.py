"""Durable cancellation requests and acknowledgements under live ownership."""

from datetime import UTC, datetime

from sqlalchemy.engine import Connection

from etl_craft.core.errors import StaleTransitionError
from etl_craft.engine.queries import statement


def request_attempt_cancel(conn: Connection, attempt_id: int, owner: str | None) -> None:
    """Persist cancellation and wake the owning agent without relabelling live work."""
    from etl_craft.engine.notifications import agent_channel, pool_channel
    from etl_craft.engine.resource_locks import notify

    conn.execute(
        statement(conn, "agent_cancel"), {"attempt_id": attempt_id, "now": datetime.now(UTC)}
    )
    notify(conn, pool_channel("default"))
    if owner is not None and owner.isdecimal():
        notify(conn, agent_channel(int(owner)))


def acknowledge_cancellation(conn: Connection, attempt_id: int, owner: str) -> None:
    """Record confirmation only under the attempt's live owner and lease."""
    result = conn.execute(
        statement(conn, "agent_cancel_ack"), {"attempt_id": attempt_id, "owner": owner}
    )
    if result.rowcount != 1:
        raise StaleTransitionError(
            f"attempt {attempt_id}, owner={owner}: cancellation ownership ended or expired; "
            "expected a live owned claim; stop the child and reconcile before acknowledging"
        )
