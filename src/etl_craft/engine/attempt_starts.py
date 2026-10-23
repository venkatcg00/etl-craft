"""Accept the one start of an attempt that its supervisor and its task process both report."""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.engine import Connection, Row

from etl_craft.core.actor import Actor
from etl_craft.core.enums import AttemptStatus
from etl_craft.core.errors import StaleTransitionError
from etl_craft.core.time import as_utc
from etl_craft.engine import runlog
from etl_craft.engine.queries import statement
from etl_craft.engine.transitions import stale_attempt, start_attempt


@dataclass(frozen=True)
class StartedProcess:
    """The task process an attempt runs in: its host, PID and process birth identity."""

    host: str
    pid: int
    start: str | None = None


def ensure_attempt_started(
    conn: Connection,
    attempt_id: int,
    actor: Actor,
    *,
    owner: str,
    process: StartedProcess,
    completed: bool = False,
) -> None:
    """Accept the same start acknowledged by parent and child, with no second write."""
    try:
        start_attempt(
            conn,
            attempt_id,
            actor,
            owner=owner,
            host=process.host,
            pid=process.pid,
            process_start=process.start,
        )
    except StaleTransitionError:
        row = conn.execute(
            statement(conn, "transition_row_attempt"), {"row_id": attempt_id}
        ).one_or_none()
        if not _started_as(row, owner, process.pid, process.start, completed=completed):
            raise


def assert_started(
    conn: Connection, row: Row[Any], *, owner: str, pid: int, process_start: str | None
) -> None:
    """Accept a start the other process already acknowledged, read as ``row``."""
    if not _started_as(row, owner, pid, process_start, completed=False):
        raise stale_attempt(conn, int(row.row_id), "RUNNING", owner)


def _started_as(
    row: Row[Any] | None,
    owner: str,
    pid: int,
    process_start: str | None,
    *,
    completed: bool,
) -> bool:
    """Whether ``row`` is this owner's process already started, with a live lease if running."""
    if row is None or row.owner_id != owner or row.pid != pid:
        return False
    if process_start is not None and row.process_start != process_start:
        return False
    if row.status == AttemptStatus.RUNNING:
        expiry = row.lease_expires_at
        instant = None if expiry is None else as_utc(expiry)
        return instant is not None and runlog.elapsed_hours(instant, datetime.now(UTC)) < 0
    return completed and row.status in {AttemptStatus.SUCCESS, AttemptStatus.FAILED}
