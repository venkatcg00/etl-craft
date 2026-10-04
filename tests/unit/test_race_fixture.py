"""Synchronization waits once per caller and restores the patched boundary."""

import threading

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


def test_a_caller_that_never_arrives_does_not_leave_the_other_waiting():
    original = checkpoint
    with pytest.raises(threading.BrokenBarrierError):
        two_at_once(lambda: checkpoint(1), lambda: 2, at=f"{__name__}.checkpoint", timeout=0.05)
    assert checkpoint is original
