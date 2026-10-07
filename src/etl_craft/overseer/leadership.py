"""One process owns a deployment through a session lock, never a heartbeat guess."""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import blake2b
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from etl_craft.core.errors import LockTimeoutError, RunRefusedError
from etl_craft.core.filelock import file_lock
from etl_craft.engine.repository.overseers import active_overseer


@dataclass
class Leadership:
    """The held PostgreSQL listener, or SQLite's polling fallback."""

    connection: Connection | None = None

    def wait(self, stop: threading.Event) -> None:
        """Wake on a committed execution notification or at the one-second poll."""
        if self.connection is None:
            stop.wait(1)
        else:
            driver: Any = self.connection.connection.driver_connection
            for _ in driver.notifies(timeout=1, stop_after=1):
                break


@contextmanager
def leadership(engine: Engine) -> Iterator[Leadership]:
    """Hold a deployment-specific lock until all local workers have stopped."""
    if engine.dialect.name == "sqlite":
        database = engine.url.database
        if not database or database == ":memory:":
            raise RunRefusedError("server requires a file-backed SQLite Engine DB")
        acquired = False
        try:
            with file_lock(f"{database}.overseer.lock", wait_seconds=0.05):
                acquired = True
                yield Leadership()
        except LockTimeoutError:
            if acquired:
                raise
            raise RunRefusedError(
                active_overseer(engine) + "; stop it before starting a server"
            ) from None
        return
    with engine.connect() as raw:
        conn = raw.execution_options(isolation_level="AUTOCOMMIT")
        schema = str(conn.execute(text("SELECT current_schema() AS schema")).scalar_one())
        key = int.from_bytes(
            blake2b(f"etl-craft:overseer:{schema}".encode(), digest_size=8).digest(),
            "big",
            signed=True,
        )
        acquired = conn.execute(
            text("SELECT pg_try_advisory_lock(:key) AS held"), {"key": key}
        ).scalar_one()
        if not acquired:
            raise RunRefusedError(active_overseer(engine) + "; stop it before starting a server")
        try:
            conn.exec_driver_sql("LISTEN etl_craft_events")
            yield Leadership(conn)
        finally:
            try:
                conn.exec_driver_sql("UNLISTEN etl_craft_events")
                conn.execute(text("SELECT pg_advisory_unlock(:key) AS released"), {"key": key})
            except Exception:
                conn.invalidate()
                raise
