"""One process owns a deployment through a session lock, never a heartbeat guess."""

from __future__ import annotations

import logging
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

import psycopg
from sqlalchemy.engine import Connection, Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.errors import LockTimeoutError, RunRefusedError
from etl_craft.core.filelock import file_lock
from etl_craft.dialects.engine import for_engine
from etl_craft.engine.locks import advisory_key
from etl_craft.engine.metrics import state
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.controllers import active_controller

logger = logging.getLogger(__name__)


@dataclass
class Leadership:
    """The held PostgreSQL listener, or SQLite's polling fallback."""

    connection: Connection | None = None
    engine: Engine | None = None
    key: int = 0
    config: ConnectorConfig | None = None

    def hold(self) -> None:
        """Take the PostgreSQL session lock again when a dropped connection took it away.

        Refuse when another controller took it meanwhile. SQLite's file lock never drops.
        """
        if self.engine is not None and self.connection is None:
            metrics = state(self.engine)
            if metrics is not None:
                metrics.leader = False
            self.connection = _acquire(self.engine, self.key, retaking=True, config=self.config)
            if metrics is not None:
                metrics.leader = True

    def wait(self, stop: threading.Event) -> None:
        """Wake on a committed execution notification or at the one-second poll.

        A dropped listener connection loses the session lock with it; it is taken again.
        """
        if self.connection is None:
            stop.wait(1)
            return
        try:
            for _ in for_engine(self.connection).notifications(self.connection, 1):
                break
        except psycopg.OperationalError as error:
            logger.warning("the leadership connection dropped (%s); taking leadership again", error)
            self.connection.invalidate()
            self.connection.close()
            self.connection = None
            self.hold()


def _acquire(
    engine: Engine, key: int, *, retaking: bool = False, config: ConnectorConfig | None = None
) -> Connection:
    """Hold the deployment's session lock and listen for execution notifications."""
    conn = (
        engine.connect()
        if config is None
        else for_engine(engine).session_connection(engine, config)
    ).execution_options(isolation_level="AUTOCOMMIT")
    try:
        held = conn.execute(statement(conn, "controller_try_lock"), {"key": key}).scalar_one()
        if not held:
            reason = (
                "; it took over while this controller's Engine DB connection was down, so this "
                "controller stops"
                if retaking
                else "; stop it before starting a controller"
            )
            raise RunRefusedError(active_controller(engine) + reason)
        conn.execute(statement(conn, "controller_listen"))
    except BaseException:
        conn.close()
        raise
    return conn


@contextmanager
def leadership(engine: Engine, config: ConnectorConfig | None = None) -> Iterator[Leadership]:
    """Hold a deployment-specific lock until all local agents have stopped."""
    path = for_engine(engine).controller_lock_path(engine)
    if path is not None:
        acquired = False
        try:
            with file_lock(path, wait_seconds=0.05):
                acquired = True
                yield Leadership()
        except LockTimeoutError:
            if acquired:
                raise
            raise RunRefusedError(
                active_controller(engine) + "; stop it before starting a controller"
            ) from None
        return
    with engine.connect() as conn:
        schema = str(conn.execute(statement(conn, "controller_schema")).scalar_one())
    key = advisory_key(f"etl-craft:controller:{schema}")
    leader = Leadership(_acquire(engine, key, config=config), engine, key, config)
    try:
        yield leader
    except BaseException:
        # Closing the connection ends the session lock; the original error is the one to report.
        if leader.connection is not None:
            leader.connection.invalidate()
            leader.connection.close()
        raise
    # The lock went with a dropped connection when it could not be taken again.
    if leader.connection is not None:
        _release(leader.connection, key)


def _release(conn: Connection, key: int) -> None:
    """Stop listening and release the session lock, then close the connection."""
    try:
        conn.execute(statement(conn, "controller_unlisten"))
        conn.execute(statement(conn, "controller_unlock"), {"key": key})
    except Exception:
        conn.invalidate()
        raise
    finally:
        conn.close()
