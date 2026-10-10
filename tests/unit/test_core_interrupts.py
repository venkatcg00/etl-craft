"""Signals reach the main thread as KeyboardInterrupt only at safe points."""

import os
import signal
import threading
import time

import pytest

from etl_craft.core import interrupts

pytestmark = pytest.mark.unit


def send(sig=signal.SIGTERM):
    os.kill(os.getpid(), sig)


def test_a_signal_during_work_is_held_until_the_next_checkpoint():
    with interrupts.deferred(signal.SIGTERM):
        send()
        time.sleep(0.01)  # work: the handler ran and held the signal
        with pytest.raises(KeyboardInterrupt, match="SIGTERM"):
            interrupts.checkpoint()
        interrupts.checkpoint()  # raised once


def test_a_signal_while_waiting_is_raised_at_once():
    with interrupts.deferred(signal.SIGTERM):
        timer = threading.Timer(0.05, send)
        timer.start()
        started = time.monotonic()
        with pytest.raises(KeyboardInterrupt, match="SIGTERM"):
            interrupts.sleep(10)
        assert time.monotonic() - started < 5
        timer.join()


def test_a_held_signal_is_raised_when_a_wait_begins():
    with interrupts.deferred(signal.SIGTERM):
        send()
        with pytest.raises(KeyboardInterrupt, match="SIGTERM"):
            interrupts.sleep(10)


def test_a_second_signal_stops_the_work_at_once():
    with interrupts.deferred(signal.SIGTERM, signal.SIGHUP):
        send()
        with pytest.raises(KeyboardInterrupt, match="SIGHUP"):
            send(signal.SIGHUP)


def test_a_signal_still_held_is_raised_when_the_block_ends_and_handlers_are_restored():
    before = signal.getsignal(signal.SIGHUP)
    with pytest.raises(KeyboardInterrupt, match="SIGHUP"), interrupts.deferred(signal.SIGHUP):
        send(signal.SIGHUP)
    assert signal.getsignal(signal.SIGHUP) is before


def test_waiting_in_another_thread_does_not_expose_the_main_thread():
    release = threading.Event()

    def wait():
        with interrupts.interruptible():
            release.wait(10)

    with interrupts.deferred(signal.SIGTERM):
        waiter = threading.Thread(target=wait)
        waiter.start()
        time.sleep(0.05)
        send()  # the main thread is working, whatever the other thread does
        time.sleep(0.01)
        release.set()
        waiter.join()
        with pytest.raises(KeyboardInterrupt, match="SIGTERM"):
            interrupts.checkpoint()
