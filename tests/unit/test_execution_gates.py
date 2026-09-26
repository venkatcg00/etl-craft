"""Judging an upstream's last run, and pacing the wait for a running one."""

from datetime import UTC, datetime, timedelta

import pytest

from etl_craft.core.errors import GraphError
from etl_craft.engine.repository.trackers import FinishedRun, LatestRun
from etl_craft.execution import gates
from etl_craft.execution.gates import Clock, WaitBudget, judge, next_look_delay, satisfies

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("kind", "status", "has_data", "expected"),
    [
        ("SUCCESS", "SUCCESS", False, True),
        ("SUCCESS", "FAILED", False, False),
        ("FAILURE", "FAILED", False, True),
        ("FAILURE", "SUCCESS", False, False),
        ("ALWAYS", "SKIPPED", False, True),
        ("HAS_DATA", "SUCCESS", True, True),
        ("HAS_DATA", "SUCCESS", False, False),
        ("HAS_DATA", "FAILED", True, False),
    ],
)
def test_what_satisfies_each_dependency_type(kind, status, has_data, expected):
    assert satisfies(kind, FinishedRun(1, status, has_data)) is expected


def test_an_unknown_dependency_type():
    with pytest.raises(GraphError, match="unknown dependency_type"):
        satisfies("SOMETIMES", FinishedRun(1, "SUCCESS", False))


def test_the_last_finished_run_decides():
    assert judge("SUCCESS", None, None) == (None, "has no finished run")
    assert judge("SUCCESS", FinishedRun(5, "SUCCESS", False), None) == (
        5,
        "last finished run 5 ended SUCCESS",
    )
    assert judge("SUCCESS", FinishedRun(5, "SUCCESS", False), 5) == (
        None,
        "has no run since run 5, which was already consumed",
    )
    assert judge("SUCCESS", FinishedRun(6, "FAILED", False), 5) == (
        None,
        "last finished run 6 ended FAILED, which does not satisfy a SUCCESS dependency",
    )
    assert judge("HAS_DATA", FinishedRun(6, "SUCCESS", False), None)[1] == (
        "last finished run 6 ended SUCCESS with no rows written, which does not satisfy a "
        "HAS_DATA dependency"
    )


def test_look_delays():
    # Due at 70% of a 100 s average: 30 s in, the look is 40 s away.
    assert next_look_delay(100, 30, 0.7, 3600) == 40
    # Already overdue: the shortest pause, not a busy loop.
    assert next_look_delay(100, 500, 0.7, 3600) == gates.MIN_LOOK_INTERVAL_SECONDS
    # Never past the deadline.
    assert next_look_delay(100, 0, 0.7, 5) == 5
    assert next_look_delay(100, 0, 0.7, -1) == 0


class FakeClock:
    def __init__(self):
        self.now = datetime(2026, 9, 25, 10, tzinfo=UTC)
        self.slept = []

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.now += timedelta(seconds=seconds)

    def clock(self):
        return Clock(sleep=self.sleep, now=lambda: self.now)


def test_waiting_for_a_running_upstream_follows_its_average_length():
    fake = FakeClock()
    clock = fake.clock()
    started = fake.now
    looks = iter(["IN-PROGRESS", "IN-PROGRESS", "SUCCESS"])

    def latest():
        return LatestRun(9, next(looks), started)

    budget = WaitBudget.start(clock)
    gates._wait_while_running(latest, lambda: 100.0, "upstream", budget, clock)
    assert fake.slept == pytest.approx([70, 10])
    assert budget.looks_left == gates.MAX_LOOKS - 2


def test_waiting_stops_when_the_budget_runs_out():
    fake = FakeClock()
    clock = fake.clock()
    started = fake.now.replace(tzinfo=None)  # a naive start is read as UTC

    budget = WaitBudget(deadline=fake.now + timedelta(seconds=90), looks_left=gates.MAX_LOOKS)
    gates._wait_while_running(
        lambda: LatestRun(9, "IN-PROGRESS", started), lambda: None, "upstream", budget, clock
    )
    # The default length is assumed; the first look at 70% of it is cut to the deadline.
    assert fake.slept == [90]
    assert budget.exhausted(clock)


def test_an_upstream_that_is_not_running_is_not_waited_for():
    fake = FakeClock()
    clock = fake.clock()
    budget = WaitBudget.start(clock)
    gates._wait_while_running(lambda: None, lambda: 1.0, "upstream", budget, clock)
    gates._wait_while_running(
        lambda: LatestRun(1, "FAILED", fake.now), lambda: 1.0, "upstream", budget, clock
    )
    assert fake.slept == [] and budget.looks_left == gates.MAX_LOOKS


def test_the_unchecked_gate_is_never_definitive():
    check = gates.UncheckedGate().check(None, 1, 1)
    assert (check.satisfied_count, check.definitive) == (0, False)
    assert gates.UncheckedGate().consume(None, 1, 1, {}) is None
