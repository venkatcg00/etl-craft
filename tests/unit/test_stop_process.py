"""Stopping a local child signals only the process it recorded, and its recorded descendants."""

import contextlib
import os
import signal
import subprocess
import sys
import time
import types
from pathlib import Path

import psutil
import pytest

from etl_craft.core.errors import RunStateError
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


def test_a_process_that_is_not_its_group_leader_is_never_signalled():
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        wait_for(lambda: process_start(child.pid) is not None)
        stop_process(child.pid, process_start(child.pid), grace_seconds=0.1)
        assert child.poll() is None
    finally:
        child.kill()
        child.wait(10)


class Group:
    """A recorded process group that no signal ends, on a clock that advances per check."""

    def __init__(self, monkeypatch, *, gone=False):
        self.now = 0.0
        self.killed = []
        module = "etl_craft.execution.reconcile"
        monkeypatch.setattr(f"{module}.process_start", lambda pid: "birth")
        fake_os = types.SimpleNamespace(
            getpgid=lambda pid: pid, killpg=lambda pid, sig: self.killed.append(sig)
        )
        monkeypatch.setattr(f"{module}.os", fake_os)
        monkeypatch.setattr(
            f"{module}.time", types.SimpleNamespace(monotonic=self.tick, sleep=lambda s: None)
        )
        found = self.vanish if gone else self.process
        fake_psutil = types.SimpleNamespace(Process=found, NoSuchProcess=psutil.NoSuchProcess)
        monkeypatch.setattr(f"{module}.psutil", fake_psutil)

    def tick(self):
        self.now += 0.5
        return self.now

    def process(self, pid):
        def kill():
            self.killed.append(signal.SIGKILL)
            raise psutil.NoSuchProcess(pid)

        return types.SimpleNamespace(children=lambda recursive: [], kill=kill)

    def vanish(self, pid):
        raise psutil.NoSuchProcess(pid)


def test_a_group_that_outlives_sigkill_fails_naming_the_process(monkeypatch):
    group = Group(monkeypatch)
    with pytest.raises(RunStateError, match="pid=4242, birth=birth: process group did not end"):
        stop_process(4242, "birth", grace_seconds=1)
    assert group.killed == [signal.SIGTERM, signal.SIGKILL]


def test_a_process_that_ends_while_it_is_inspected_needs_no_signal(monkeypatch):
    group = Group(monkeypatch, gone=True)
    stop_process(4242, "birth", grace_seconds=1)
    assert group.killed == []
