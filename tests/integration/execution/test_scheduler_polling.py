"""A running pipeline re-reads the Engine DB on a schedule, not on every scheduler tick."""

from datetime import UTC, datetime, timedelta

import pytest

from etl_craft.core.graph import build_graph
from etl_craft.engine.repository.dependencies import fetch_pipeline_graph
from etl_craft.execution.attempt_types import ChildOptions
from etl_craft.execution.gates import Clock
from etl_craft.execution.pools import Capacity, ExecutionHandle, HandleState, HandleStatus
from etl_craft.execution.scheduler import ENGINE_POLL_SECONDS, Scheduler
from fixtures.metadata import add_dependency, add_pipeline, add_task, start_run
from fixtures.round_trips import count_round_trips

pytestmark = pytest.mark.usefixtures("engine_actor")


class RunningPool:
    """One attempt that runs until ``end`` is set; its status is this process's own record."""

    name = "local"
    status_reads_engine = False

    def __init__(self):
        self.ended = False
        self.looks = 0

    def capacity(self):
        return Capacity({"python": 1, "warehouse": 1}, {"python": 0, "warehouse": 0})

    def status(self, handle):
        self.looks += 1
        state = HandleState.ENDED if self.ended else HandleState.RUNNING
        return HandleStatus(handle, state)


@pytest.mark.parametrize("reads_engine", [False, True])
def test_a_running_attempt_is_watched_without_reading_the_engine_db_every_tick(
    engine_db, reads_engine
):
    engine = engine_db.engine
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "slow", "PYTHON", SCRIPT_FILE="python_scripts/slow.py")
        run = start_run(conn, pipeline)
        data = fetch_pipeline_graph(conn, pipeline)
    now = datetime(2026, 10, 22, tzinfo=UTC)
    pool = RunningPool()
    pool.status_reads_engine = reads_engine
    scheduler = Scheduler(
        engine,
        engine_db.config,
        "P",
        pipeline,
        run,
        {task: "slow"},
        build_graph(data.tasks, data.same_pipeline_edges),
        force=False,
        clock=Clock(now=lambda: now),
        child=ChildOptions(),
        pool=pool,
        paused=lambda: False,
        settle=lambda failures: [],
    )
    scheduler.attempted.add(task)
    scheduler.jobs[task] = ExecutionHandle("local", 1)
    assert not scheduler.step()
    looks = pool.looks
    with count_round_trips(engine) as quiet:
        for _ in range(10):
            assert not scheduler.step()
    assert quiet.result()["engine_statements"] == 0
    # A pool whose status is an Engine DB read is not asked between due reads either.
    assert pool.looks == looks + (0 if reads_engine else 10)
    now += timedelta(seconds=ENGINE_POLL_SECONDS)
    with count_round_trips(engine) as due:
        assert not scheduler.step()
    assert due.result()["engine_statements"] > 0
    if not reads_engine:
        # An attempt that ends locally is acted on at the next tick, before the next due read.
        pool.ended = True
        with count_round_trips(engine) as ended:
            scheduler.step()
        assert task not in scheduler.jobs
        assert ended.result()["engine_statements"] > 0


def test_a_lifted_stop_does_not_keep_an_event_probe_cancelled(engine_db):
    import threading

    engine = engine_db.engine
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        slow = add_task(conn, pipeline, "slow", "PYTHON", SCRIPT_FILE="python_scripts/slow.py")
        task = add_task(conn, pipeline, "event", "EVENT_WAIT", EVENT_SQL="SELECT 1 AS ready")
        add_dependency(conn, pipeline, task, slow)
        run = start_run(conn, pipeline)
        data = fetch_pipeline_graph(conn, pipeline)
    pool = RunningPool()
    scheduler = Scheduler(
        engine,
        engine_db.config,
        "P",
        pipeline,
        run,
        {task: "event", slow: "slow"},
        build_graph(data.tasks, data.same_pipeline_edges),
        force=False,
        clock=Clock(),
        child=ChildOptions(),
        pool=pool,
        paused=lambda: False,
        settle=lambda failures: [],
    )
    scheduler.attempted.add(slow)
    scheduler.jobs[slow] = ExecutionHandle("local", 1)
    stopped = threading.Event()
    stopped.set()
    scheduler.event_stops[task] = stopped
    try:
        assert not scheduler.step()
        assert not stopped.is_set(), "a released control must allow the next event check"
    finally:
        pool.ended = True
        scheduler.close()
