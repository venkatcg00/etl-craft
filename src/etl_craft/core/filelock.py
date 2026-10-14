"""Shared or exclusive file locks coordinated across processes on the machine.

The operating system releases the lock when its holder exits, even when it is killed, so a
crashed process never leaves a stale lock behind.
"""

from __future__ import annotations

import fcntl
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import IO

from etl_craft.core import interrupts
from etl_craft.core.errors import LockTimeoutError

POLL_SECONDS = 0.05


@contextmanager
def file_lock(path: str | Path, wait_seconds: float = 0, *, shared: bool = False) -> Iterator[None]:
    """Hold a lock on ``path``, exclusive by default and creating its file when exclusive.

    Waits for another holder to release it: indefinitely when ``wait_seconds`` is 0, otherwise
    raising ``LockTimeoutError`` once ``wait_seconds`` have passed.
    Shared readers open an existing lock file read-only and exclude exclusive writers.
    """
    deadline = time.monotonic() + wait_seconds if wait_seconds else None
    handle = open(path, "rb" if shared else "a+b")  # noqa: SIM115 - closed in the finally below
    try:
        while not _try_lock(handle, shared=shared):
            if deadline is not None and time.monotonic() >= deadline:
                raise LockTimeoutError(
                    f"timed out after {wait_seconds}s waiting for {path}; "
                    "wait for the current holder to exit or stop it, then retry"
                )
            interrupts.sleep(POLL_SECONDS)
        try:
            yield
        finally:
            _unlock(handle)
    finally:
        handle.close()


def _try_lock(handle: IO[bytes], *, shared: bool = False) -> bool:
    try:
        kind = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
        fcntl.flock(handle.fileno(), kind | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    return True


def _unlock(handle: IO[bytes]) -> None:
    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
