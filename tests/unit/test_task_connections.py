"""Local task connections close before another renewal or state read opens one."""

import threading

import pytest

from etl_craft.dialects.engine.connections import SerialConnections
from fixtures.races import two_at_once

pytestmark = pytest.mark.unit


def test_concurrent_task_threads_never_open_two_physical_connections():
    serial = SerialConnections()
    active = peak = 0
    changed = threading.Lock()

    class Driver:
        def __init__(self):
            nonlocal active, peak
            with changed:
                active += 1
                peak = max(peak, active)

        def close(self):
            nonlocal active
            with changed:
                active -= 1

    create = serial.creator(Driver)

    def read():
        driver = create()
        try:
            return True
        finally:
            serial.closed(driver, None)

    assert two_at_once(read, read, at=(serial.slot, "acquire")) == (True, True)
    assert peak == 1
    assert active == 0


def test_a_refused_connect_does_not_spend_the_task_slot():
    serial = SerialConnections()

    def refused():
        raise OSError("connection refused")

    with pytest.raises(OSError, match="connection refused"):
        serial.creator(refused)()
    assert serial.slot.acquire(blocking=False)
    serial.slot.release()
