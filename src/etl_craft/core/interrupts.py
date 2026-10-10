"""Deliver SIGINT, SIGTERM and SIGHUP as ``KeyboardInterrupt`` only where stopping is safe.

Python runs a signal handler between any two bytecodes of the main thread. A handler that raises
there can land inside an Engine DB transaction's cleanup: on SQLite the connection then returns
to the pool with its ``BEGIN IMMEDIATE`` still open, and every later write waits out the busy
timeout while the run is trying to stop its tasks.

Within ``deferred()``, a signal that arrives while the main thread works is held, and raised by
the next ``checkpoint()``, the next wait (``interruptible()`` or ``sleep()``), or the end of the
``with`` block. One that arrives while the main thread waits is raised at once. A second signal
while one is held is raised at once, so a run can always be stopped.
"""

from __future__ import annotations

import signal
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from types import FrameType

_held: str | None = None
_waiting = 0


def _hold(signum: int, frame: FrameType | None) -> None:
    global _held
    name = signal.Signals(signum).name
    if _waiting or _held is not None:
        _held = None
        raise KeyboardInterrupt(name)
    _held = name


def checkpoint() -> None:
    """Raise the held signal, if there is one, as ``KeyboardInterrupt``."""
    global _held
    if _held is not None:
        name, _held = _held, None
        raise KeyboardInterrupt(name)


@contextmanager
def interruptible() -> Iterator[None]:
    """Let a signal interrupt the main thread at once while the body waits.

    The body must only wait (sleep, poll a process, join): an interruption leaves nothing half
    done. In any other thread this does nothing, since signals reach only the main thread.
    """
    global _waiting
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    checkpoint()
    _waiting += 1
    try:
        yield
    finally:
        _waiting -= 1


def sleep(seconds: float) -> None:
    """``time.sleep`` that a held or arriving signal interrupts."""
    with interruptible():
        time.sleep(seconds)


@contextmanager
def deferred(*signals: signal.Signals) -> Iterator[None]:
    """Hold ``signals`` for the body until a safe point, then restore the previous handlers.

    A signal still held when the body ends is raised then. Outside the main thread, where
    handlers cannot be installed, the body runs unchanged.
    """
    global _held
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    previous = {sig: signal.signal(sig, _hold) for sig in signals}
    try:
        yield
        checkpoint()
    finally:
        _held = None
        for sig, handler in previous.items():
            signal.signal(sig, handler)
