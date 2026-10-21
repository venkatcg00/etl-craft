"""Signal-driven shutdown shared by long-running CLI roles."""

import signal
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from types import FrameType


@contextmanager
def shutdown_event() -> Iterator[threading.Event]:
    """Set one stop event on SIGTERM or SIGINT and restore the caller's handlers on exit."""
    stop = threading.Event()

    def stopping(signum: int, frame: FrameType | None) -> None:
        stop.set()

    previous = {
        number: signal.signal(number, stopping) for number in (signal.SIGTERM, signal.SIGINT)
    }
    try:
        yield stop
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)
