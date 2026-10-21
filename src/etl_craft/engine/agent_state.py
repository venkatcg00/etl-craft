"""Atomic fencing of task protocol changes, using the Engine DB clock."""

from sqlalchemy.engine import Connection

from etl_craft.core.enums import AttemptStatus
from etl_craft.core.errors import StaleTransitionError
from etl_craft.engine.queries import statement


def fence(
    conn: Connection, attempt_id: int, owner: str, expected: AttemptStatus | None = None
) -> int:
    """Serialize a live owned request with reconciliation and every competing ending."""
    row = conn.execute(
        statement(conn, "transition_row_attempt"), {"row_id": attempt_id}
    ).one_or_none()
    if (
        row is not None
        and expected is None
        and row.status
        in {
            AttemptStatus.CLAIMED,
            AttemptStatus.RUNNING,
        }
    ):
        expected = AttemptStatus(row.status)
    found = None
    if expected is not None:
        found = conn.execute(
            statement(conn, "agent_fence"),
            {
                "attempt_id": attempt_id,
                "owner": owner,
                "expected": expected,
            },
        ).scalar_one_or_none()
    if found is None:
        actual = "missing" if row is None else f"status={row.status}, owner={row.owner_id}"
        raise StaleTransitionError(
            f"attempt {attempt_id}, requesting owner={owner}: {actual}; expected a live "
            f"{expected or 'CLAIMED/RUNNING'} claim owned by this token; "
            "stop the child and reconcile"
        )
    return int(found)
