"""Durable resource ownership shared by attempts and standalone actions."""

from __future__ import annotations

import logging
import os
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field

from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.config import ConnectorConfig
from etl_craft.core.actor import Actor, acting_as, current_actor
from etl_craft.core.errors import EtlCraftError, ExitCode, LockTimeoutError, StaleTransitionError
from etl_craft.core.ownership import attempt_holder
from etl_craft.core.time import as_utc
from etl_craft.dialects.engine import for_engine
from etl_craft.engine.agent_state import fence
from etl_craft.engine.queries import statement

logger = logging.getLogger(__name__)
LEASE_SECONDS = 60
RENEW_SECONDS = 15
FENCE_MARGIN_SECONDS = 15
WAIT_SECONDS = 30


@dataclass(frozen=True)
class Holder:
    """One attempt owner or one standalone action, with its requesting actor."""

    holder_id: str
    owner: str
    actor: Actor
    attempt_id: int | None = None

    def parameters(self) -> dict[str, object]:
        """Bind ownership without accepting an actor or owner from an HTTP request."""
        return {
            "holder": self.holder_id,
            "owner": self.owner,
            "actor": self.actor.name,
            "kind": self.actor.kind,
            "attempt_id": self.attempt_id,
            "seconds": LEASE_SECONDS,
        }


@dataclass
class Scope:
    """Nested target and warehouse locks share the same action identity."""

    holder: Holder
    held: set[int] = field(default_factory=set)


_scope: ContextVar[Scope | None] = ContextVar("resource_lock_scope", default=None)


@contextmanager
def attempt_scope(attempt_id: int, owner: str) -> Iterator[None]:
    """Bind all locks taken by a task process to its exact attempt owner."""
    holder = Holder(attempt_holder(attempt_id, owner), owner, current_actor(), attempt_id)
    token = _scope.set(Scope(holder))
    try:
        yield
    finally:
        _scope.reset(token)


def holder_tag(holder_id: str) -> str:
    """Return the label every warehouse session of one holder carries."""
    return f"etl-craft/{holder_id}"


def session_tag(identity: str) -> str:
    """Use an enclosing attempt/action scope when a writer already holds a resource."""
    scope = _scope.get()
    return holder_tag(scope.holder.holder_id if scope is not None else f"action:{identity}")


def notify(conn: Connection, channel: str = "etl_craft_events") -> None:
    """Wake resource waiters after the enclosing transaction commits."""
    if for_engine(conn).supports_api_replicas:
        conn.execute(
            statement(conn, "execution_notify"),
            {"channel": channel, "payload": "resources"},
        )


@dataclass(frozen=True)
class Acquisition:
    """One try: the lease budget when acquired, or the stale holder to confirm stopped first."""

    budget: float | None = None
    stale_holder: str | None = None


def acquire(
    conn: Connection, key: int, name: str, holder: Holder, confirmed: str | None = None
) -> Acquisition:
    """Insert the lock, or take over an ended or expired holder only once it is ``confirmed``.

    A live lock is never taken. A stale one passes in one statement, and only while it still
    names the holder whose warehouse statements the caller confirmed stopped.
    """
    if holder.attempt_id is not None:
        fence(conn, holder.attempt_id, holder.owner)
    row = conn.execute(
        statement(conn, "resource_lock_acquire"),
        {**holder.parameters(), "key": key, "name": name, "confirmed": confirmed},
    ).one_or_none()
    if row is not None:
        return Acquisition((as_utc(row.lease_expires_at) - as_utc(row.db_now)).total_seconds())
    current = conn.execute(statement(conn, "resource_lock_read"), {"key": key}).one_or_none()
    if current is not None and current.stale and current.holder_id != holder.holder_id:
        return Acquisition(stale_holder=current.holder_id)
    return Acquisition()


def release(conn: Connection, key: int, holder: Holder) -> None:
    """Release only this holder's reservation, leaving a successor untouched."""
    result = conn.execute(
        statement(conn, "resource_lock_release"), {**holder.parameters(), "key": key}
    )
    if result.rowcount:
        notify(conn)


def renew(conn: Connection, key: int, holder: Holder) -> float:
    """Return the database's remaining lease budget; refuse an expired owner."""
    row = conn.execute(
        statement(conn, "resource_lock_renew"), {**holder.parameters(), "key": key}
    ).one_or_none()
    if row is None:
        raise StaleTransitionError(
            f"resource lock {key}, holder={holder.holder_id}: ownership ended or expired; "
            "stop the writer and retry through its supervisor"
        )
    return (as_utc(row.lease_expires_at) - as_utc(row.db_now)).total_seconds()


def _fence(name: str) -> None:
    logger.critical("%s: cannot renew resource ownership; stopping before lease expiry", name)
    # A blocked warehouse call cannot be interrupted safely by a Python thread.
    os._exit(ExitCode.STALE_TRANSITION)


@contextmanager
def _heartbeat(
    engine: Engine, key: int, name: str, holder: Holder, started: float, budget: float
) -> Iterator[None]:
    stop = threading.Event()
    deadline = started + budget - FENCE_MARGIN_SECONDS

    def beat() -> None:
        nonlocal deadline
        while not stop.wait(min(RENEW_SECONDS, max(deadline - time.monotonic(), 0))):
            started = time.monotonic()
            if started >= deadline:
                _fence(name)
                return
            try:
                with acting_as(holder.actor), engine.begin() as conn:
                    budget = renew(conn, key, holder)
                deadline = started + budget - FENCE_MARGIN_SECONDS
            except StaleTransitionError:
                _fence(name)
                return
            except SQLAlchemyError:
                logger.warning("%s: resource renewal unavailable", name)

    def watch() -> None:
        while not stop.wait(max(deadline - time.monotonic(), 0)):
            if time.monotonic() >= deadline:
                _fence(name)
                return

    watchdog = threading.Thread(target=watch, name="resource-fence", daemon=True)
    worker = threading.Thread(target=beat, name="resource-lease", daemon=True)
    watchdog.start()
    worker.start()
    try:
        yield
    finally:
        stop.set()
        worker.join(timeout=1)
        watchdog.join(timeout=1)


def confirm_stale(confirm: Callable[[str], None], name: str, holder_id: str) -> None:
    """Stop a stale holder's warehouse statements; the lock stays with it unless confirmed."""
    try:
        confirm(holder_id)
    except EtlCraftError as error:
        raise LockTimeoutError(
            f"{name}: the lock stays with {holder_id}, whose warehouse statements are not "
            f"confirmed stopped: {error}"
        ) from error


def _busy(engine: Engine, key: int, name: str, waited: float) -> LockTimeoutError:
    with engine.connect() as conn:
        row = conn.execute(statement(conn, "resource_lock_read"), {"key": key}).one_or_none()
    detail = "holder changed while waiting"
    if row is not None:
        duration = (as_utc(row.db_now) - as_utc(row.acquired_at)).total_seconds()
        detail = f"holder={row.holder_id}, actor={row.actor}, held for {duration:g}s"
    return LockTimeoutError(
        f"{name}: lock unavailable after {waited:g}s; {detail}; "
        "wait for the holder to finish or inspect its attempt before retrying"
    )


@dataclass(frozen=True)
class Owner:
    """Where a holder's locks live, how it stops a stale holder, and its standalone identity."""

    engine: Engine
    config: ConnectorConfig
    confirm: Callable[[str], None]
    identity: str


@contextmanager
def hold(owner: Owner, key: int, name: str, wait_seconds: float) -> Iterator[None]:
    """Wait a bounded time for one resource, renewing ownership through the whole write."""
    from etl_craft.engine.notifications import notification_hub

    engine, config, confirm, identity = owner.engine, owner.config, owner.confirm, owner.identity

    scope = _scope.get()
    token = None
    if scope is None:
        scope = Scope(Holder(f"action:{identity}", identity, current_actor()))
        token = _scope.set(scope)
    try:
        if key in scope.held:
            yield
            return
        limit = wait_seconds if wait_seconds > 0 else WAIT_SECONDS
        deadline = time.monotonic() + limit
        hub = notification_hub(config, engine)
        confirmed: str | None = None
        while True:
            generation = hub.generation
            started = time.monotonic()
            with engine.begin() as conn:
                attempt = acquire(conn, key, name, scope.holder, confirmed)
            if attempt.budget is not None:
                acquired = attempt.budget
                break
            if attempt.stale_holder is not None and attempt.stale_holder != confirmed:
                confirm_stale(confirm, name, attempt.stale_holder)
                confirmed = attempt.stale_holder
                continue
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise _busy(engine, key, name, limit)
            hub.wait(generation, min(remaining, 1))
        scope.held.add(key)
        completed = False
        try:
            with _heartbeat(engine, key, name, scope.holder, started, acquired):
                yield
            completed = True
        finally:
            scope.held.remove(key)
            if not completed:
                confirm_stale(confirm, name, scope.holder.holder_id)
            with engine.begin() as conn:
                release(conn, key, scope.holder)
    finally:
        if token is not None:
            _scope.reset(token)
