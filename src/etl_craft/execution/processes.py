"""Run child processes with a time limit, captured output and bounded parallelism.

Every child is a freshly started interpreter or program, never a fork of the engine, so it
inherits no open connections, locks or threads. Each child leads its own process group; on
timeout the whole group is stopped, including anything the child started.
"""

from __future__ import annotations

import logging
import os
import shutil
import signal
import subprocess
import tempfile
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import IO

from etl_craft.core import interrupts
from etl_craft.core.errors import ConfigurationError, RunStateError
from etl_craft.core.faults import fault_point
from etl_craft.execution.leases import process_start
from etl_craft.execution.reconcile import stop_process

logger = logging.getLogger(__name__)

CANCEL_POLL_SECONDS = 0.5
"""How often a child that can be cancelled checks whether it has been."""

TAIL_BYTES = 64 * 1024
"""How much of the end of a child's output ``ChildResult.output_tail`` keeps."""

KILL_GRACE_SECONDS = 10.0
"""How long a timed-out child's process group has to exit after SIGTERM before SIGKILL."""

MIN_FREE_LOG_BYTES = 64 * 1024 * 1024
"""The free space a child's log folder needs before the child starts."""


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
    queries_ended: bool = False

    @property
    def succeeded(self) -> bool:
        """Whether the child exited with status 0 within its time limit."""
        return self.returncode == 0 and not self.timed_out

    def describe(self) -> str:
        """Say how the child ended, for example ``exited with code 3``."""
        if self.timed_out:
            return f"timed out after {self.spec.timeout_seconds:g}s and was killed"
        if self.cancelled:
            return "was interrupted and stopped by its supervisor"
        if self.returncode < 0:
            return f"was killed by signal {_signal_name(-self.returncode)}"
        return f"exited with code {self.returncode}"


def run_child(
    spec: ChildSpec,
    *,
    tail_bytes: int = TAIL_BYTES,
    kill_grace_seconds: float = KILL_GRACE_SECONDS,
    cancel: threading.Event | None = None,
    on_started: Callable[[int], None] | None = None,
) -> ChildResult:
    """Run ``spec`` to completion and return how it ended.

    When the time limit passes, or ``cancel`` is set, the child's process group gets SIGTERM,
    then SIGKILL after ``kill_grace_seconds``. If waiting is interrupted, for example by Ctrl-C,
    the group is stopped the same way before the exception propagates.
    """
    with _output_file(spec.log_path) as output:
        start_offset = output.tell()
        started = time.monotonic()
        interrupts.checkpoint()
        process = subprocess.Popen(
            spec.argv,
            stdin=subprocess.DEVNULL,
            stdout=output,
            stderr=subprocess.STDOUT,
            env=None if spec.env is None else dict(spec.env),
            cwd=spec.cwd,
            start_new_session=True,
        )
        birth = process_start(process.pid)
        logger.debug("started pid %s: %s", process.pid, " ".join(spec.argv))
        timed_out = cancelled = False
        try:
            if on_started is not None:
                on_started(process.pid)
            fault_point("supervisor.after_popen")
            cancelled = _wait(process, spec.timeout_seconds or None, cancel)
            if cancelled:
                logger.warning("stopping pid %s: cancellation was requested", process.pid)
                _stop_group(process, birth, min(kill_grace_seconds, 1))
        except subprocess.TimeoutExpired:
            timed_out = True
            logger.warning(
                "pid %s exceeded its %gs limit; stopping it", process.pid, spec.timeout_seconds
            )
            _stop_group(process, birth, kill_grace_seconds)
        except BaseException:
            _stop_group(process, birth, kill_grace_seconds)
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


def _wait(
    process: subprocess.Popen[bytes], timeout: float | None, cancel: threading.Event | None
) -> bool:
    """Wait for ``process``; return true when ``cancel`` was set first.

    Raises ``subprocess.TimeoutExpired`` when ``timeout`` passes first.
    """
    if cancel is None:
        with interrupts.interruptible():
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
            with interrupts.interruptible():
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
    require_log_folder(path.parent)
    fault_point("supervisor.after_mkdir")
    with path.open("a+b") as handle:
        yield handle


def require_log_folder(folder: Path) -> None:
    """Refuse to start a child whose log folder cannot be written or has too little free space.

    The check runs before the child starts, so a full or read-only disk never interrupts a task
    halfway through its work.
    """
    remedy = "or point Orchestration.Log_dir at a writable disk with free space"
    try:
        folder.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise ConfigurationError(
            f"cannot create the log folder {folder}: {error.strerror}; "
            f"fix its permissions, {remedy}"
        ) from error
    if not os.access(folder, os.W_OK | os.X_OK):
        raise ConfigurationError(
            f"the log folder {folder} is not writable by this user; fix its permissions, {remedy}"
        )
    free = shutil.disk_usage(folder).free
    if free < MIN_FREE_LOG_BYTES:
        raise ConfigurationError(
            f"the disk holding the log folder {folder} has {free // 2**20} MiB free; a task needs "
            f"at least {MIN_FREE_LOG_BYTES // 2**20} MiB before it starts; free space, {remedy}"
        )


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


def _stop_group(process: subprocess.Popen[bytes], birth: str | None, grace_seconds: float) -> None:
    """Stop only the recorded process birth and its observed descendants, then reap it."""
    if birth is None and process.poll() is None:
        raise RunStateError(
            f"pid={process.pid}: process birth could not be verified; no signal sent. "
            "Inspect the host process before retrying or acknowledging cancellation."
        )
    if birth is not None:
        stop_process(process.pid, birth, grace_seconds=grace_seconds)
    process.wait()
