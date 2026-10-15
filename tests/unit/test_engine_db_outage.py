"""An Engine DB outage is waited out within its budget, then reported with the setting."""

import psycopg
import pytest

from etl_craft.core.errors import EngineDbError
from etl_craft.core.outage import OutageBudget
from etl_craft.dialects.engine.postgres import riding_out_outages

pytestmark = pytest.mark.unit


class Clock:
    """Monotonic seconds that advance only when something sleeps."""

    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def connector(refuse):
    """A connect function that raises for the call numbers in ``refuse``."""
    calls = []

    def connect():
        calls.append(len(calls) + 1)
        if calls[-1] in refuse:
            raise psycopg.OperationalError("connection refused")
        return calls[-1]

    return connect, calls


def test_an_outage_is_waited_out_within_its_budget_then_named_with_the_setting():
    clock = Clock()
    outage = OutageBudget(3, clock)
    assert outage.failed("supervising runs", OSError("refused")) == 1.0
    clock.now = 2.5
    assert outage.failed("supervising runs", OSError("refused")) == 0.5
    clock.now = 3.0
    with pytest.raises(
        EngineDbError,
        match=r"not answered for 3s while supervising runs \(refused\); "
        r"Orchestration\.Engine_db_outage_seconds allows 3s",
    ):
        outage.failed("supervising runs", OSError("refused"))


def test_an_answer_ends_the_outage_so_the_next_one_gets_the_whole_budget():
    clock = Clock()
    outage = OutageBudget(3, clock)
    outage.failed("x", OSError("refused"))
    clock.now = 2.9
    outage.recovered("x")
    clock.now = 10.0
    assert outage.failed("x", OSError("refused")) == 1.0


def test_a_zero_budget_fails_at_the_first_refusal():
    with pytest.raises(EngineDbError, match="allows 0s"):
        OutageBudget(0, Clock()).failed("x", OSError("refused"))


def test_a_database_never_reached_fails_at_once():
    connect, calls = connector(refuse={1})
    clock = Clock()
    with pytest.raises(psycopg.OperationalError, match="connection refused"):
        riding_out_outages(connect, 120, sleep=clock.sleep, clock=clock)()
    assert (calls, clock.now) == ([1], 0.0)


def test_a_reached_database_is_waited_for_until_it_answers():
    connect, _ = connector(refuse={2, 3, 4})
    clock = Clock()
    open_connection = riding_out_outages(connect, 120, sleep=clock.sleep, clock=clock)
    assert open_connection() == 1
    assert open_connection() == 5
    assert clock.now == 3.0


def test_a_reached_database_that_outlasts_the_budget_names_the_setting():
    connect, _ = connector(refuse=set(range(2, 100)))
    clock = Clock()
    open_connection = riding_out_outages(connect, 2, sleep=clock.sleep, clock=clock)
    open_connection()
    with pytest.raises(EngineDbError, match=r"Engine_db_outage_seconds allows 2s"):
        open_connection()
    assert clock.now == 2.0
    with pytest.raises(EngineDbError, match=r"Engine_db_outage_seconds allows 2s"):
        open_connection()
    assert clock.now == 2.0, "a spent budget makes later connections fail without waiting"


def test_a_task_process_waits_out_an_outage_at_its_first_connection():
    connect, _ = connector(refuse={1, 2})
    clock = Clock()
    open_connection = riding_out_outages(connect, 120, reached=True, sleep=clock.sleep, clock=clock)
    assert open_connection() == 3
    assert clock.now == 2.0
