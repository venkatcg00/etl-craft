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

from etl_craft.config import ConnectorConfig
from etl_craft.core.actor import SYSTEM_ACTOR, acting_as, current_actor
from etl_craft.core.errors import RunStateError, StaleTransitionError
from etl_craft.core.time import as_utc
from etl_craft.dialects.engine import for_engine
from etl_craft.engine import agent_transitions, transitions
from etl_craft.engine.queries import statement
from etl_craft.execution.leases import LEASE_SECONDS, process_start

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
        if os.getpgid(pid) != pid or process_start(pid) != birth:
            return
        os.killpg(pid, signal.SIGTERM)
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
        deadline = time.monotonic() + max(1, grace_seconds)
        while any(
            started is not None and process_start(child_pid) == started
            for child_pid, started in known.items()
        ):
            if time.monotonic() >= deadline:
                raise RunStateError(
                    f"pid={pid}, birth={birth}: process group did not end after SIGKILL; "
                    "inspect the host process state before retrying or acknowledging a stop"
                )
            time.sleep(0.05)
    except (ProcessLookupError, psutil.NoSuchProcess):
        return


@acting_as(SYSTEM_ACTOR)
def reconcile(
    engine: Engine, *, pipeline_id: int | None = None, task_id: int | None = None
) -> Reconciliation:
    """Fence expired attempts, stop verified local children, then release expired idle runs."""
    if for_engine(engine).supports_api_replicas:
        with engine.begin() as conn:
            agent_transitions.expire_agents(conn)
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
        local = not row.agent_owned and row.host == socket.gethostname()
        if (
            not local
            and not row.agent_owned
            and expires + timedelta(seconds=2 * LEASE_SECONDS) > now
        ):
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


@acting_as(SYSTEM_ACTOR)
def settle_sources(engine: Engine, config: ConnectorConfig, source_codes: tuple[str, ...]) -> None:
    """Reconcile expired source attempts before a reader calculates its timestamp fence."""
    from etl_craft.engine.timestamp_windows import source_pipeline_ids
    from etl_craft.execution.control_flow import reconcile_endings

    with engine.connect() as conn:
        sources = source_pipeline_ids(conn, source_codes)
    for pipeline_id in sources:
        lost = reconcile(engine, pipeline_id=pipeline_id).lost
        if lost:
            reconcile_endings(engine, config, tuple(lost), pipeline_id=pipeline_id)
