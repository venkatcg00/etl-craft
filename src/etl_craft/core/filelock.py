"""An exclusive lock held on a file, shared by every process on the machine.

The operating system releases the lock when its holder exits, even when it is killed, so a
crashed process never leaves a stale lock behind.
"""

from __future__ import annotations

import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import IO

from etl_craft.core.errors import LockTimeoutError

POLL_SECONDS = 0.05


@contextmanager
def file_lock(path: str | Path, wait_seconds: float = 0) -> Iterator[None]:
    """Hold an exclusive lock on ``path``, creating the file if needed, for the ``with`` body.

    Waits for another holder to release it: indefinitely when ``wait_seconds`` is 0, otherwise
    raising ``LockTimeoutError`` once ``wait_seconds`` have passed.
    """
    deadline = time.monotonic() + wait_seconds if wait_seconds else None
    handle = open(path, "a+b")  # noqa: SIM115 - closed in the finally below
    try:
        while not _try_lock(handle):
            if deadline is not None and time.monotonic() >= deadline:
                raise LockTimeoutError(f"timed out after {wait_seconds}s waiting for {path}")
            time.sleep(POLL_SECONDS)
        try:
            yield
        finally:
            _unlock(handle)
    finally:
        handle.close()


if sys.platform == "win32":  # pragma: no cover - exercised on Windows only
    import msvcrt

    def _try_lock(handle: IO[bytes]) -> bool:
        handle.seek(0)
        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            return False
        return True

    def _unlock(handle: IO[bytes]) -> None:
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)

else:
    import fcntl

    def _try_lock(handle: IO[bytes]) -> bool:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        return True

    def _unlock(handle: IO[bytes]) -> None:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
