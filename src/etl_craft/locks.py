"""A named, cross-process lock held through the Engine DB.

[ADDITION, 2026-09-24] Two things in the engine have to be serialized across
processes: `migrate` (two concurrent runs must not double-apply a file) and
access to a single-writer warehouse (E2-61, DuckDB). Both were built on a
Postgres advisory lock in the Engine DB, because the Engine DB is the one thing
every contending process can reach. SQLite has no advisory locks, so for a
SQLite Engine DB the same lock is an OS file lock beside the database file.

That is a faithful substitute, not a weaker one, for the same reason SQLite is
acceptable as an Engine DB at all: every process that could contend is on the
machine that holds the file. Like an advisory lock it queues rather than
spinning on the protected resource, and like `pg_advisory_xact_lock` it is
released by the operating system if its holder dies.
"""

from __future__ import annotations

import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import IO

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

# How often a SQLite-side waiter re-tries a held file lock. Short: holders are
# tasks and migrations, and the wait is bounded by the caller anyway.
_FILE_LOCK_POLL_SECONDS = 0.1


class LockTimeout(Exception):
    """Raised when a lock could not be taken within the caller's bound."""


@contextmanager
def engine_lock(engine: Engine, key: int, name: str, *, wait_seconds: int = 0) -> Iterator[None]:
    """Hold the named lock for the duration of the block.

    `key` identifies the lock on Postgres (an advisory-lock key); `name` does
    on SQLite (it becomes part of the lock file's name). `wait_seconds` bounds
    the wait; 0 waits indefinitely.
    """
    if engine.dialect.name == "sqlite":
        with _file_lock(_lock_file_path(engine, name), wait_seconds):
            yield
        return

    with engine.begin() as lock_conn:
        if wait_seconds:
            # No bind parameter: SET takes a literal. wait_seconds is an int
            # from config/limits, never user text. Postgres's lock_timeout does
            # apply to pg_advisory_xact_lock -- verified, not assumed.
            lock_conn.execute(text(f"SET LOCAL lock_timeout = '{int(wait_seconds)}s'"))
        try:
            lock_conn.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": key})
        except OperationalError as exc:
            raise LockTimeout(f"timed out after {wait_seconds}s waiting for {name}") from exc
        # The caller's own work runs on other connections; this one exists
        # only to hold the lock until the transaction ends.
        yield


def _lock_file_path(engine: Engine, name: str) -> str:
    database = engine.url.database
    if not database:
        raise LockTimeout(f"cannot lock {name}: the SQLite Engine DB has no file path")
    return f"{database}.{name}.lock"


@contextmanager
def _file_lock(path: str, wait_seconds: int) -> Iterator[None]:
    deadline = time.monotonic() + wait_seconds if wait_seconds else None
    handle = open(path, "a+b")  # noqa: SIM115 - closed in the finally below
    try:
        while not _try_lock(handle):
            if deadline is not None and time.monotonic() >= deadline:
                raise LockTimeout(f"timed out after {wait_seconds}s waiting for {path}")
            time.sleep(_FILE_LOCK_POLL_SECONDS)
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
