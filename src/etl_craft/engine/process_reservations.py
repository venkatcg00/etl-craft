"""Keep a process reservation live on its own unpooled session until shutdown."""

from __future__ import annotations

import logging
import os
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import copy_context
from dataclasses import dataclass
from datetime import UTC, datetime
from time import monotonic
from weakref import WeakKeyDictionary, WeakSet

from sqlalchemy import event
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.engine.interfaces import DBAPIConnection
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import ConnectionPoolEntry

from etl_craft.config import ConnectorConfig
from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
from etl_craft.core.enums import ConnectionUse, ProcessRole
from etl_craft.core.errors import ConfigurationError, EtlCraftError
from etl_craft.core.faults import fault_point
from etl_craft.core.outage import OutageBudget
from etl_craft.dialects.engine import for_engine
from etl_craft.engine.connection_budget import Connections, heartbeat, reserve
from etl_craft.engine.queries import statement
from etl_craft.engine.retry import TRANSIENT_ERRORS

logger = logging.getLogger(__name__)
HEARTBEAT_SECONDS = 15


@dataclass(frozen=True)
class Allocation:
    """The capacity and cancellation signal belonging to one admitted process."""

    role: ProcessRole
    connections: Connections
    cancel: threading.Event
    lease_seconds: int = 60


@dataclass
class _Owner:
    identity: str
    connection: Connection
    deadline: float


_slots: WeakKeyDictionary[
    Engine, tuple[dict[ConnectionUse, threading.BoundedSemaphore], threading.Event]
] = WeakKeyDictionary()
_slots_lock = threading.Lock()
_starting: WeakSet[Engine] = WeakSet()


def task_capacity(
    engine: Engine,
) -> tuple[dict[ConnectionUse, threading.BoundedSemaphore], threading.Event] | None:
    """Return the capacity already reserved for this process engine, if any."""
    with _slots_lock:
        return _slots.get(engine)


@contextmanager
def connection_slot(engine: Engine, use: ConnectionUse = ConnectionUse.TASK) -> Iterator[bool]:
    """Share the reserved task or probe allowance across every caller in this process."""
    capacity = task_capacity(engine)
    if capacity is None:
        yield True
        return
    slots_by_use, cancel = capacity
    slots = slots_by_use[use]
    acquired = not cancel.is_set() and slots.acquire(blocking=False)
    try:
        yield acquired
    finally:
        if acquired:
            slots.release()


@contextmanager
def reserve_process(
    engine: Engine, config: ConnectorConfig, allocation: Allocation
) -> Iterator[None]:
    """Stop work on ownership loss; an expired owner cannot resume its reservation."""
    with _slots_lock:
        if engine in _slots or engine in _starting:
            raise ConfigurationError(
                "process already owns a connection reservation; expected one lifecycle per engine; "
                "reuse the running controller or API rather than starting another process role"
            )
        _starting.add(engine)
    try:
        with for_engine(engine).session_connection(engine, config) as conn:
            with conn.begin():
                identity = reserve(
                    conn,
                    allocation.role,
                    allocation.connections,
                    datetime.now(UTC),
                    lease_seconds=allocation.lease_seconds,
                )
            owner = _Owner(identity, conn, monotonic() + allocation.lease_seconds)
            try:
                fault_point("connections.after_reservation")
                with (
                    _task_capacity(engine, allocation, identity),
                    _renewing(engine, config, owner, allocation),
                ):
                    yield
            finally:
                _close_reservation(engine, owner.connection, identity)
    finally:
        with _slots_lock:
            _starting.discard(engine)


@contextmanager
def _task_capacity(engine: Engine, allocation: Allocation, identity: str) -> Iterator[None]:
    """Share child slots and inherited session identity, then drain before releasing them."""
    limits = {
        ConnectionUse.TASK: allocation.connections.tasks,
        ConnectionUse.PROBE: 1,
        ConnectionUse.HEALTH: int(allocation.role == ProcessRole.UI),
    }
    slots = {use: threading.BoundedSemaphore(limit) for use, limit in limits.items()}
    with _slots_lock:
        _slots[engine] = (slots, allocation.cancel)
    previous = os.environ.get("ETL_CRAFT_PROCESS_RESERVATION")
    os.environ["ETL_CRAFT_PROCESS_RESERVATION"] = identity
    try:
        yield
    finally:
        allocation.cancel.set()
        for use, limit in limits.items():
            for _ in range(limit):
                slots[use].acquire()
        if previous is None:
            os.environ.pop("ETL_CRAFT_PROCESS_RESERVATION", None)
        else:
            os.environ["ETL_CRAFT_PROCESS_RESERVATION"] = previous
        with _slots_lock:
            del _slots[engine]


@contextmanager
def _renewing(
    engine: Engine, config: ConnectorConfig, owner: _Owner, allocation: Allocation
) -> Iterator[None]:
    """Renew on the ownership session and fence new connections when ownership ends."""
    identity = owner.identity
    stop = threading.Event()
    failures: list[Exception] = []

    def fence(driver: DBAPIConnection, record: ConnectionPoolEntry, _proxy: object) -> None:
        if monotonic() >= owner.deadline or failures:
            allocation.cancel.set()
            raise ConfigurationError(
                f"connection reservation {identity}: ownership expired; expected a live "
                "reservation before opening a connection; restart this process"
            )
        if record.info.get("process_reservation") != identity:
            for_engine(engine).tag_process_connection(driver, identity)
            record.info["process_reservation"] = identity

    def renew() -> None:
        # Thread boundary: report ownership loss and stop serving or supervising work.
        outage = OutageBudget(allocation.lease_seconds - 60)
        delay: float = HEARTBEAT_SECONDS
        try:
            while not stop.wait(delay):
                try:
                    if owner.connection.invalidated or owner.connection.closed:
                        owner.connection.close()
                        owner.connection = for_engine(engine).session_connection(engine, config)
                    with acting_as(SYSTEM_ACTOR), owner.connection.begin():
                        heartbeat(
                            owner.connection, identity, lease_seconds=allocation.lease_seconds
                        )
                    owner.deadline = monotonic() + allocation.lease_seconds
                except TRANSIENT_ERRORS as error:
                    owner.connection.invalidate()
                    delay = outage.failed("renewing connection reservation", error)
                    continue
                outage.recovered("renewing connection reservation")
                delay = HEARTBEAT_SECONDS
        except (SQLAlchemyError, EtlCraftError) as error:
            failures.append(error)
            allocation.cancel.set()
            logger.exception("connection reservation %s lost; stopping process", identity)

    event.listen(engine, "checkout", fence)
    thread = threading.Thread(target=copy_context().run, args=(renew,), daemon=True)
    thread.start()
    successful = False
    try:
        yield
        successful = True
    finally:
        stop.set()
        thread.join()
        event.remove(engine, "checkout", fence)
    if successful and failures:
        raise ConfigurationError(
            f"connection reservation {identity}: lost its database session; expected "
            "continuous ownership; restore connectivity and restart this process"
        ) from failures[0]


def _close_reservation(engine: Engine, conn: Connection, identity: str) -> None:
    """Close pooled sessions before releasing capacity; orphan sessions retain their charge."""
    engine.dispose()
    if conn.invalidated or conn.closed:
        conn.close()
        return
    try:
        with conn.begin():
            conn.execute(
                statement(conn, "connection_reservation_stop"),
                {"identity": identity, "now": datetime.now(UTC)},
            )
    except SQLAlchemyError:
        logger.exception("connection reservation %s awaits session expiry", identity)
    finally:
        conn.close()
