"""Stopping a local child signals only the process it recorded, and its recorded descendants."""

import contextlib
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from etl_craft.execution.leases import process_start
from etl_craft.execution.reconcile import stop_process

pytestmark = pytest.mark.unit


def alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return process_start(pid) is not None


def wait_for(predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition never held"
        time.sleep(0.02)


def test_a_process_whose_birth_identity_differs_is_never_signalled():
    child = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"], start_new_session=True
    )
    try:
        wait_for(lambda: process_start(child.pid) is not None)
        stop_process(child.pid, "a different process", grace_seconds=0.1)
        assert child.poll() is None
    finally:
        child.kill()
        child.wait(10)


def test_a_descendant_that_ignores_sigterm_is_killed_after_its_leader_exits(tmp_path: Path):
    pid_file = tmp_path / "descendant.pid"
    descendant = (
        "import os, signal, time\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        f"open({str(pid_file)!r}, 'w').write(str(os.getpid()))\n"
        "time.sleep(60)\n"
    )
    leader = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import subprocess, sys, time\n"
            f"subprocess.Popen([sys.executable, '-c', {descendant!r}])\n"
            "time.sleep(60)\n",
        ],
        start_new_session=True,
    )
    try:
        wait_for(lambda: pid_file.exists() and pid_file.read_text())
        grandchild = int(pid_file.read_text())
        wait_for(lambda: process_start(grandchild) is not None)
        stop_process(leader.pid, process_start(leader.pid), grace_seconds=0.5)
        leader.wait(10)
        wait_for(lambda: not alive(grandchild))
    finally:
        if leader.poll() is None:
            leader.kill()
            leader.wait(10)
        if pid_file.exists() and pid_file.read_text():
            with contextlib.suppress(ProcessLookupError):
                os.kill(int(pid_file.read_text()), signal.SIGKILL)
