"""Process identities and heartbeat scopes for run and attempt supervisors."""

from __future__ import annotations

import logging
import os
import socket
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import psutil
from sqlalchemy.engine import Engine

from etl_craft.core.actor import acting_as, current_actor
from etl_craft.core.errors import RunStateError, StaleTransitionError
from etl_craft.engine import transitions
from etl_craft.engine.queries import statement

logger = logging.getLogger(__name__)
LEASE_SECONDS = 60
HEARTBEAT_SECONDS = 15


def process_start(pid: int) -> str | None:
    """Read a process birth identity, including the boot identity on Linux."""
    try:
        proc = Path(f"/proc/{pid}/stat")
        if proc.exists():
            fields = proc.read_text(encoding="utf-8").rsplit(")", 1)[1].split()
            if fields[0] == "Z":
                return None
            boot = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip()
            return f"{boot}/{fields[19]}"
        process = psutil.Process(pid)
        if process.status() == psutil.STATUS_ZOMBIE:
            return None
        return str(process.create_time())
    except (OSError, psutil.Error, IndexError):
        return None


def owner_id() -> str:
    """Identify this supervisor's host, pid, birth time and random instance suffix."""
    birth = process_start(os.getpid())
    if birth is None:
        raise RunStateError(
            f"pid={os.getpid()}: cannot read process start time; cannot own a lease"
        )
    return f"{socket.gethostname()}:{os.getpid()}:{birth}:{uuid4().hex[:8]}"


def as_utc(value: object) -> datetime:
    """Read an Engine DB timestamp as an aware UTC instant."""
    result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    return result.replace(tzinfo=UTC) if result.tzinfo is None else result.astimezone(UTC)


@dataclass(frozen=True)
class RunSupervisor:
    """A run's exact owner and the cancellation shared by its wave tasks."""

    run_id: int
    owner: str
    cancel: threading.Event


_run: ContextVar[RunSupervisor | None] = ContextVar("run_supervisor", default=None)


def run_owner(run_id: int) -> str | None:
    """Return the supervisor in this execution scope for this exact run."""
    current = _run.get()
    return current.owner if current is not None and current.run_id == run_id else None


def run_cancel() -> threading.Event:
    """Return the supervising run's cancellation, or a standalone task event."""
    current = _run.get()
    return current.cancel if current is not None else threading.Event()


@contextmanager
def heartbeat(
    engine: Engine, kind: str, row_id: int, owner: str, cancel: threading.Event
) -> Iterator[None]:
    """Renew a lease while its process runs; stop work when ownership cannot be renewed."""
    stop = threading.Event()
    failures: list[Exception] = []
    actor = current_actor()

    def renew() -> None:
        while not stop.wait(HEARTBEAT_SECONDS):
            try:
                with acting_as(actor), engine.begin() as conn:
                    query = "run_lease" if kind == "run" else "transition_row_attempt"
                    row = conn.execute(statement(conn, query), {"row_id": row_id}).one_or_none()
                    active = {"IN-PROGRESS"} if kind == "run" else {"CLAIMED", "RUNNING"}
                    if row is not None and row.status == "LOST":
                        raise StaleTransitionError(
                            f"attempt {row_id}: owner {owner} was reconciled LOST; stop its child"
                        )
                    if row is not None and row.status not in active:
                        return
                    renewal = (
                        transitions.renew_run_lease if kind == "run" else transitions.renew_lease
                    )
                    renewal(
                        conn,
                        row_id,
                        actor,
                        owner=owner,
                        lease_expires_at=datetime.now(UTC) + timedelta(seconds=LEASE_SECONDS),
                    )
            except Exception as error:
                failures.append(error)
                cancel.set()
                logger.error("%s %s: heartbeat failed for owner %s: %s", kind, row_id, owner, error)
                return

    thread = threading.Thread(target=renew, name=f"etl-craft-{kind}-heartbeat", daemon=True)
    thread.start()
    try:
        yield
        if failures:
            raise StaleTransitionError(
                f"{kind} {row_id}: owner {owner} lost its heartbeat: {failures[0]}"
            )
    finally:
        stop.set()
        thread.join()


@contextmanager
def supervise_run(engine: Engine, run_id: int) -> Iterator[None]:
    """Claim and heartbeat one run; no other supervisor can adopt its live or expired lease."""
    owner = owner_id()
    with engine.begin() as conn:
        row = conn.execute(statement(conn, "run_lease"), {"row_id": run_id}).one()
        if row.owner_id is not None:
            raise RunStateError(
                f"run {run_id} is supervised by {row.owner_id} "
                f"(lease until {row.lease_expires_at}); "
                "wait for its supervisor, or reconcile an expired lease before resuming"
            )
        transitions.start_run(
            conn,
            run_id,
            current_actor(),
            owner=owner,
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=LEASE_SECONDS),
        )
    cancel = threading.Event()
    token = _run.set(RunSupervisor(run_id, owner, cancel))
    failed = False
    try:
        with heartbeat(engine, "run", run_id, owner, cancel):
            yield
    except BaseException:
        failed = True
        raise
    finally:
        _run.reset(token)
        try:
            with engine.begin() as conn:
                transitions.release_run_lease(conn, run_id, owner=owner)
        except Exception:
            if not failed:
                raise
            logger.exception("run %s: could not release owner %s", run_id, owner)
