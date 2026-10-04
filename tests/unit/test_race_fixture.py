"""Synchronization waits once per caller and restores the patched boundary."""

import threading
import time

import pytest

from fixtures.races import two_at_once

pytestmark = pytest.mark.unit


def checkpoint(value):
    return value


def test_both_callers_arrive_before_work_and_repeated_calls_do_not_wait(monkeypatch):
    arrived = set()

    def checked(value):
        assert len(arrived) == 2
        return value

    monkeypatch.setattr(__import__(__name__), "checkpoint", checked)

    def first():
        arrived.add(threading.get_ident())
        return checkpoint("one"), checkpoint("again")

    def second():
        arrived.add(threading.get_ident())
        return checkpoint("two")

    assert two_at_once(first, second, at=f"{__name__}.checkpoint") == (("one", "again"), "two")
    assert checkpoint is checked


@pytest.mark.parametrize("arrival_delay", [0, 0.15])
def test_a_caller_that_never_arrives_does_not_leave_the_other_waiting(arrival_delay):
    original = checkpoint

    def first():
        time.sleep(arrival_delay)
        return checkpoint(1)

    with pytest.raises(threading.BrokenBarrierError):
        two_at_once(first, lambda: 2, at=f"{__name__}.checkpoint", timeout=0.05)
    assert checkpoint is original
