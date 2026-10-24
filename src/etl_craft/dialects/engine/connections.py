"""Keep local task interpreters within one physical Engine DB session at a time."""

from collections.abc import Callable
from threading import BoundedSemaphore

from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.engine.interfaces import DBAPIConnection
from sqlalchemy.pool import ConnectionPoolEntry


class SerialConnections:
    """Serialize connection creation through close, including lease-renewal threads."""

    def __init__(self) -> None:
        """Start with one physical connection available."""
        self.slot = BoundedSemaphore(1)

    def creator(self, create: Callable[[], DBAPIConnection]) -> Callable[[], DBAPIConnection]:
        """Wait before opening the physical connection, releasing failed opens immediately."""

        def connect() -> DBAPIConnection:
            self.slot.acquire()
            try:
                return create()
            except BaseException:
                self.slot.release()
                raise

        return connect

    def attach(self, engine: Engine) -> None:
        """Release the slot only after the driver confirms the connection is closed."""
        event.listen(engine, "close", self.closed)

    def closed(self, driver: DBAPIConnection, _record: ConnectionPoolEntry) -> None:
        """Close before waking the next opener; the pool's repeated close is idempotent."""
        try:
            driver.close()
        finally:
            self.slot.release()
