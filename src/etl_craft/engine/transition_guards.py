"""Compare-and-set writes and their existing state diagnostics."""

from datetime import UTC, datetime

from sqlalchemy.engine import Connection

from etl_craft.core.actor import Actor
from etl_craft.core.errors import StaleTransitionError
from etl_craft.engine.queries import statement


def stale(
    conn: Connection, kind: str, row_id: int, expected: str, owner: str | None
) -> StaleTransitionError:
    """Describe the exact row and active attempt behind a refused transition."""
    row = (
        conn.execute(statement(conn, f"transition_row_{kind}"), {"row_id": row_id})
        .mappings()
        .one_or_none()
    )
    found = "missing" if row is None else repr(dict(row))
    if kind == "task":
        active = (
            conn.execute(statement(conn, "active_attempt"), {"task_run_id": row_id})
            .mappings()
            .one_or_none()
        )
        if active is not None:
            found += f"; active attempt {dict(active)!r}"
    return StaleTransitionError(
        f"{kind} row_id={row_id}: expected status {expected}, owner={owner!r}; found {found}. "
        "Read the run history and refresh the status and owner before retrying."
    )


def stale_attempt(
    conn: Connection, attempt_id: int, expected: str, owner: str | None
) -> StaleTransitionError:
    """Describe an attempt row that is not in the ``expected`` state for ``owner``."""
    return stale(conn, "attempt", attempt_id, expected, owner)


def actor_parameters(actor: Actor) -> dict[str, object]:
    """Bind the initiating actor to an attributed lifecycle write."""
    return {"actor": actor.name, "actor_kind": actor.kind.value, "now": datetime.now(UTC)}
