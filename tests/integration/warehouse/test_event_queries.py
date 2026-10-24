"""Event queries and downstream file tokens on every local warehouse."""

import pytest
from sqlalchemy import text

from fixtures.event_waits import check_event_queries
from fixtures.sql_warehouse import warehouses


def test_event_query_and_file_token(sql_world):
    check_event_queries(sql_world, "event_source")


@warehouses("postgres")
def test_slow_sql_event_does_not_block_other_events_or_scheduler_shutdown(sql_world):
    import multiprocessing
    import time

    from etl_craft.core.graph import build_graph
    from etl_craft.engine.repository.dependencies import fetch_pipeline_graph
    from etl_craft.execution.attempt_types import ChildOptions
    from etl_craft.execution.gates import Clock
    from etl_craft.execution.pools import Capacity
    from etl_craft.execution.scheduler import Scheduler
    from fixtures.metadata import add_task

    world = sql_world
    with world.engine_db.begin() as conn:
        sql_task = add_task(
            conn, world.pipeline_id, "slow", "EVENT_WAIT", EVENT_SQL="SELECT pg_sleep(10) AS ready"
        )
        file_task = add_task(
            conn,
            world.pipeline_id,
            "file",
            "EVENT_WAIT",
            EVENT_FILE="ready.csv",
            EVENT_SETTLE_SECONDS="0",
        )
        data = fetch_pipeline_graph(conn, world.pipeline_id)
    world.new_run()
    (world.project / "ready.csv").write_text("ready")

    class FullPool:
        def capacity(self):
            return Capacity({"python": 1, "warehouse": 1}, {"python": 0, "warehouse": 0})

        def submit(self, spec):
            pytest.fail("event waits must never occupy an agent")

    scheduler = Scheduler(
        world.engine_db,
        world.config,
        "P",
        world.pipeline_id,
        world.pipeline_run_id,
        {sql_task: "slow", file_task: "file"},
        build_graph(data.tasks, data.same_pipeline_edges),
        force=False,
        clock=Clock(),
        child=ChildOptions(),
        pool=FullPool(),
        paused=lambda: False,
        settle=lambda failures: [],
    )
    children = {child.pid for child in multiprocessing.active_children()}
    try:
        started = time.monotonic()
        assert not scheduler.step()
        assert time.monotonic() - started < 2
        assert file_task in scheduler.completed
        # Wait for the background probe to establish its durable budget, before interrupting it.
        until = time.monotonic() + 5
        while time.monotonic() < until:
            with world.engine_db.connect() as conn:
                wait = conn.execute(
                    text(
                        "SELECT FIRST_CHECK_AT, WAIT_UNTIL FROM AUD_GATE_WAITS "
                        "WHERE TASK_ID=:id AND WAIT_KIND='EVENT'"
                    ),
                    {"id": sql_task},
                ).one_or_none()
            if wait is not None:
                break
            time.sleep(0.01)
        assert wait is not None
        started = time.monotonic()
        assert not scheduler.step()
        assert time.monotonic() - started < 2
    finally:
        started = time.monotonic()
        scheduler.close(interrupted=True)
        assert time.monotonic() - started < 3
    assert {child.pid for child in multiprocessing.active_children()} == children
    with world.engine_db.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM AUD_TASK_EVENTS")).scalar_one() == 1
    from etl_craft.core.time import as_utc
    from etl_craft.execution.events import check_event

    resumed = check_event(
        world.engine_db, world.config, world.pipeline_run_id, sql_task, as_utc(wait[1])
    )
    assert resumed.status == "FAILED"
    with world.engine_db.connect() as conn:
        restored = conn.execute(
            text(
                "SELECT FIRST_CHECK_AT, WAIT_UNTIL FROM AUD_GATE_WAITS "
                "WHERE TASK_ID=:id AND WAIT_KIND='EVENT'"
            ),
            {"id": sql_task},
        ).one()
    assert tuple(restored) == tuple(wait)


@warehouses("postgres")
@pytest.mark.parametrize("policy,status", [("FAIL", "FAILED"), ("SKIP", "SKIPPED")])
def test_slow_query_obeys_original_event_deadline(sql_world, policy, status):
    import time
    from datetime import UTC, datetime

    from etl_craft.execution.events import check_event
    from fixtures.metadata import add_task

    world = sql_world
    with world.engine_db.begin() as conn:
        task = add_task(
            conn,
            world.pipeline_id,
            "slow",
            "EVENT_WAIT",
            EVENT_SQL="SELECT pg_sleep(10) AS ready",
            EVENT_TIMEOUT_MINUTES="0.05",
            EVENT_ON_TIMEOUT=policy,
        )
    world.new_run()
    started = time.monotonic()
    result = check_event(
        world.engine_db, world.config, world.pipeline_run_id, task, datetime.now(UTC)
    )
    assert result.status == status
    assert time.monotonic() - started < 5
    with world.engine_db.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM AUD_TASK_EVENTS")).scalar_one() == 0


def test_sql_event_pipeline_finalizes_successfully(sql_world):
    from etl_craft.execution.pipeline import run_pipeline
    from fixtures.metadata import add_task

    world = sql_world
    with world.engine_db.begin() as conn:
        add_task(conn, world.pipeline_id, "wait", "EVENT_WAIT", EVENT_SQL="SELECT 1 AS ready")
    world.new_run()
    outcome = run_pipeline(world.engine_db, world.config, "P")
    with world.engine_db.connect() as conn:
        errors = conn.execute(text("SELECT ERROR_MESSAGE FROM AUD_TASK_ATTEMPTS")).scalars().all()
    assert outcome.status == "SUCCESS", errors
    with world.engine_db.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM AUD_TASK_EVENTS")).scalar_one() == 1


def _probe_in_parent(config, query):
    from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
    from etl_craft.execution.event_probe import probe_sql
    from etl_craft.handlers.event_wait import read_event

    event = read_event(config, {"EVENT_SQL": query})
    with acting_as(SYSTEM_ACTOR):
        probe_sql(config, event, 60, None)


@warehouses("postgres")
def test_sql_probe_exits_when_its_supervisor_is_killed(sql_world):
    import multiprocessing
    import time
    from uuid import uuid4

    import psutil

    from etl_craft.execution.leases import process_start

    context = multiprocessing.get_context("spawn")
    query = f"SELECT pg_sleep(10) AS crash_{uuid4().hex}"
    parent = context.Process(target=_probe_in_parent, args=(sql_world.config, query))
    orphan = None
    try:
        parent.start()
        until = time.monotonic() + 5
        while time.monotonic() < until:
            children = psutil.Process(parent.pid).children()
            if children:
                orphan = children[0]
                break
            time.sleep(0.01)
        assert orphan is not None
        until = time.monotonic() + 5
        while time.monotonic() < until:
            active = sql_world.rows(
                "SELECT count(*) FROM pg_stat_activity WHERE query LIKE :query AND state='active'",
                query=f"%{query}%",
            )[0][0]
            if active:
                break
            time.sleep(0.01)
        assert active == 1
        parent.kill()
        parent.join(timeout=2)
        assert not parent.is_alive()
        until = time.monotonic() + 5
        while process_start(orphan.pid) is not None and time.monotonic() < until:
            time.sleep(0.01)
        assert process_start(orphan.pid) is None
    finally:
        if parent.is_alive():
            parent.kill()
            parent.join()
        parent.close()
        if orphan is not None and process_start(orphan.pid) is not None:
            orphan.kill()
