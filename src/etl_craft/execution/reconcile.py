"""Reconcile expired owners without reviving leases or adopting unrelated processes."""

from __future__ import annotations

import logging
import os
import signal
import socket
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import psutil
from sqlalchemy.engine import Engine

from etl_craft.core.actor import current_actor
from etl_craft.core.errors import StaleTransitionError
from etl_craft.engine import transitions
from etl_craft.engine.queries import statement
from etl_craft.execution.leases import LEASE_SECONDS, as_utc, process_start

logger = logging.getLogger(__name__)


@dataclass
class Reconciliation:
    """Attempts lost and supervisors released by this reconciliation call."""

    lost: list[int] = field(default_factory=list)
    released: list[int] = field(default_factory=list)


def stop_process(pid: int, birth: str, *, grace_seconds: float = 10) -> None:
    """Stop a verified child group; recheck its birth identity before each signal."""
    if process_start(pid) != birth:
        return
    try:
        process = psutil.Process(pid)
        known = {child.pid: process_start(child.pid) for child in process.children(recursive=True)}
        known[pid] = birth
        if os.name == "posix":
            if os.getpgid(pid) != pid or process_start(pid) != birth:
                return
            os.killpg(pid, signal.SIGTERM)
        else:
            for child_pid, started in known.items():
                if started is not None and process_start(child_pid) == started:
                    psutil.Process(child_pid).terminate()
        deadline = time.monotonic() + grace_seconds
        while time.monotonic() < deadline:
            if not any(
                started is not None and process_start(child_pid) == started
                for child_pid, started in known.items()
            ):
                return
            time.sleep(0.05)
        # The group leader can exit before its descendants. Signal only the birth
        # identities observed before termination, even if their parent has gone.
        for child_pid, started in known.items():
            if started is not None and process_start(child_pid) == started:
                try:
                    psutil.Process(child_pid).kill()
                except psutil.NoSuchProcess:
                    continue
    except (ProcessLookupError, psutil.NoSuchProcess):
        return


def reconcile(
    engine: Engine, *, pipeline_id: int | None = None, task_id: int | None = None
) -> Reconciliation:
    """Fence expired attempts, stop verified local children, then release expired idle runs."""
    now = datetime.now(UTC)
    report = Reconciliation()
    with engine.connect() as conn:
        attempts = conn.execute(
            statement(conn, "reconcile_attempts"), {"pipeline_id": pipeline_id, "task_id": task_id}
        ).all()
    for row in attempts:
        expires = as_utc(row.lease_expires_at or row.queued_at)
        if expires > now:
            continue
        local = row.host == socket.gethostname()
        if not local and expires + timedelta(seconds=2 * LEASE_SECONDS) > now:
            continue
        message = (
            f"attempt {row.attempt_number} was lost: owner {row.owner_id} stopped renewing "
            f"its lease at {row.heartbeat_at or row.lease_expires_at or 'unknown'}"
        )
        try:
            with engine.begin() as conn:
                transitions.lose_attempt(
                    conn, row.attempt_id, current_actor(), owner=row.owner_id, error_message=message
                )
                # Keep the loss invisible to another reconciler until the verified local
                # process has stopped, so it cannot release the run and admit a retry early.
                if local and row.pid is not None and row.process_start is not None:
                    stop_process(int(row.pid), str(row.process_start))
        except StaleTransitionError:
            continue
        report.lost.append(row.attempt_id)
        logger.warning("attempt_id=%s: %s", row.attempt_id, message)
    with engine.begin() as conn:
        runs = conn.execute(statement(conn, "reconcile_runs"), {"pipeline_id": pipeline_id}).all()
        for row in runs:
            if row.lease_expires_at is not None and as_utc(row.lease_expires_at) > now:
                continue
            if transitions.release_run_lease(
                conn, row.pipeline_run_id, owner=row.owner_id, expired=True
            ):
                report.released.append(row.pipeline_run_id)
    return report
