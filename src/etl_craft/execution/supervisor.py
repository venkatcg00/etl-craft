"""Run child processes with a time limit, captured output and bounded parallelism.

Every child is a freshly started interpreter or program, never a fork of the engine, so it
inherits no open connections, locks or threads. Each child leads its own process group; on
timeout the whole group is stopped, including anything the child started.
"""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import IO

logger = logging.getLogger(__name__)

CANCEL_POLL_SECONDS = 0.5
"""How often a child that can be cancelled checks whether it has been."""

TAIL_BYTES = 64 * 1024
"""How much of the end of a child's output ``ChildResult.output_tail`` keeps."""

KILL_GRACE_SECONDS = 10.0
"""How long a timed-out child's process group has to exit after SIGTERM before SIGKILL."""


def etl_craft_argv(*args: str) -> tuple[str, ...]:
    """Return the command that runs ``etl-craft *args`` in this interpreter's environment."""
    return (sys.executable, "-m", "etl_craft", *args)


@dataclass(frozen=True)
class ChildSpec:
    """One child process to run.

    ``timeout_seconds`` of ``None`` or 0 means no limit. The child's standard output and
    standard error are appended to ``log_path`` when given, and to a temporary file otherwise.
    """

    argv: tuple[str, ...]
    timeout_seconds: float | None = None
    log_path: Path | None = None
    env: Mapping[str, str] | None = None
    cwd: Path | None = None


@dataclass(frozen=True)
class ChildResult:
    """How a child process ended.

    ``returncode`` is negative when a signal ended the child. ``output_tail`` is the end of
    what it wrote during this run, decoded as UTF-8 with invalid bytes replaced. ``cancelled``
    is true when the caller stopped it.
    """

    spec: ChildSpec
    returncode: int
    timed_out: bool
    elapsed_seconds: float
    output_tail: str
    cancelled: bool = False

    @property
    def succeeded(self) -> bool:
        """Whether the child exited with status 0 within its time limit."""
        return self.returncode == 0 and not self.timed_out

    def describe(self) -> str:
        """Say how the child ended, for example ``exited with code 3``."""
        if self.timed_out:
            return f"timed out after {self.spec.timeout_seconds:g}s and was killed"
        if self.cancelled:
            return "was stopped because the run was interrupted"
        if self.returncode < 0:
            return f"was killed by signal {_signal_name(-self.returncode)}"
        return f"exited with code {self.returncode}"


def run_child(
    spec: ChildSpec,
    *,
    tail_bytes: int = TAIL_BYTES,
    kill_grace_seconds: float = KILL_GRACE_SECONDS,
    cancel: threading.Event | None = None,
) -> ChildResult:
    """Run ``spec`` to completion and return how it ended.

    When the time limit passes, or ``cancel`` is set, the child's process group gets SIGTERM,
    then SIGKILL after ``kill_grace_seconds``. If waiting is interrupted, for example by Ctrl-C,
    the group is stopped the same way before the exception propagates.
    """
    with _output_file(spec.log_path) as output:
        start_offset = output.tell()
        started = time.monotonic()
        process = subprocess.Popen(
            spec.argv,
            stdin=subprocess.DEVNULL,
            stdout=output,
            stderr=subprocess.STDOUT,
            env=None if spec.env is None else dict(spec.env),
            cwd=spec.cwd,
            start_new_session=sys.platform != "win32",
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
        )
        logger.debug("started pid %s: %s", process.pid, " ".join(spec.argv))
        timed_out = cancelled = False
        try:
            cancelled = _wait(process, spec.timeout_seconds or None, cancel)
            if cancelled:
                logger.warning("stopping pid %s: the run was interrupted", process.pid)
                _stop_group(process, kill_grace_seconds)
        except subprocess.TimeoutExpired:
            timed_out = True
            logger.warning(
                "pid %s exceeded its %gs limit; stopping it", process.pid, spec.timeout_seconds
            )
            _stop_group(process, kill_grace_seconds)
        except BaseException:
            _stop_group(process, kill_grace_seconds)
            raise
        elapsed = time.monotonic() - started
        tail = _read_tail(output, start_offset, tail_bytes)
    result = ChildResult(
        spec=spec,
        returncode=process.returncode,
        timed_out=timed_out,
        elapsed_seconds=elapsed,
        output_tail=tail,
        cancelled=cancelled,
    )
    logger.debug("pid %s %s after %.1fs", process.pid, result.describe(), elapsed)
    return result


def run_children(
    specs: Sequence[ChildSpec],
    *,
    max_parallel: int,
    tail_bytes: int = TAIL_BYTES,
    kill_grace_seconds: float = KILL_GRACE_SECONDS,
) -> list[ChildResult]:
    """Run every spec, at most ``max_parallel`` at a time, and return results in spec order.

    A new child starts as soon as a running one ends. ``max_parallel`` below 1 counts as 1.
    """
    if not specs:
        return []
    workers = min(max(max_parallel, 1), len(specs))
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="etl-craft-child") as pool:
        futures = [
            pool.submit(
                run_child, spec, tail_bytes=tail_bytes, kill_grace_seconds=kill_grace_seconds
            )
            for spec in specs
        ]
        return [future.result() for future in futures]


def _wait(
    process: subprocess.Popen[bytes], timeout: float | None, cancel: threading.Event | None
) -> bool:
    """Wait for ``process``; return true when ``cancel`` was set first.

    Raises ``subprocess.TimeoutExpired`` when ``timeout`` passes first.
    """
    if cancel is None:
        process.wait(timeout=timeout)
        return False
    deadline = None if timeout is None else time.monotonic() + timeout
    while not cancel.is_set():
        step = CANCEL_POLL_SECONDS
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(process.args, timeout or 0)
            step = min(step, remaining)
        try:
            process.wait(timeout=step)
            return False
        except subprocess.TimeoutExpired:
            continue
    return process.poll() is None


@contextmanager
def _output_file(path: Path | None) -> Iterator[IO[bytes]]:
    """Open the log file for appending, or a temporary file that is removed afterwards."""
    if path is None:
        with tempfile.TemporaryFile() as handle:
            yield handle
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        yield handle


def _read_tail(output: IO[bytes], start_offset: int, tail_bytes: int) -> str:
    output.flush()
    end = output.seek(0, os.SEEK_END)
    output.seek(max(start_offset, end - tail_bytes))
    return output.read().decode("utf-8", errors="replace")


def _signal_name(number: int) -> str:
    try:
        return signal.Signals(number).name
    except ValueError:
        return str(number)


if sys.platform == "win32":  # pragma: no cover - exercised on Windows only

    def _stop_group(process: subprocess.Popen[bytes], grace_seconds: float) -> None:
        process.terminate()
        try:
            process.wait(timeout=grace_seconds)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()

else:

    def _stop_group(process: subprocess.Popen[bytes], grace_seconds: float) -> None:
        """Stop the child's whole process group: SIGTERM, then SIGKILL after the grace period.

        The group id is the child's pid. It stays taken while any process in the group lives,
        so the final SIGKILL reaches only what the child started, or nothing.
        """
        _signal_group(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=grace_seconds)
        except subprocess.TimeoutExpired:
            _signal_group(process.pid, signal.SIGKILL)
            process.wait()
        else:
            _signal_group(process.pid, signal.SIGKILL)

    def _signal_group(pgid: int, sig: signal.Signals) -> None:
        # ProcessLookupError: the group is gone. PermissionError: macOS reports a group of
        # zombies this way, and a group owned by another user is not ours to signal.
        with suppress(ProcessLookupError, PermissionError):
            os.killpg(pgid, sig)
