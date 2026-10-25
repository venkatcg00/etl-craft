"""Explicit PostgreSQL million-attempt purge acceptance, outside ordinary test discovery."""

import contextvars
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from time import perf_counter

import pytest
from sqlalchemy import event, text

from etl_craft.config.model import Retention
from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.retention import PurgeInput
from etl_craft.services.retention import retaining
from fixtures.metadata import add_pipeline, add_task, upstream_run

pytestmark = [
    pytest.mark.harness,
    pytest.mark.timeout(420),
    pytest.mark.parametrize("empty_engine_db", ["postgresql"], indirect=True),
]


def test_million_attempts_purge_within_five_minutes_while_a_real_run_finishes(
    cli_project,
    record_testsuite_property,
):
    p = cli_project
    with p.engine.begin() as conn:
        pipeline = add_pipeline(conn, "expired")
        task = add_task(conn, pipeline, "old", handler="PYTHON")
        run, tasks = upstream_run(conn, pipeline, {task: ("SUCCESS",)})
        conn.execute(
            text(
                "INSERT INTO AUD_TASK_ATTEMPTS "
                "(TASK_RUN_ID,ATTEMPT_NUMBER,STATUS,STARTED_AT,ENDED_AT) "
                "SELECT :task,n,'SUCCESS','2000-01-01','2000-01-01' "
                "FROM generate_series(1,1000000) n"
            ),
            {"task": tasks[task]},
        )
    with retaining(), p.engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE AUD_PIPELINES_RUN_LOG SET END_DATE='2000-01-01' WHERE PIPELINE_RUN_ID=:run"
            ),
            {"run": run},
        )
    ctx = OperationContext(
        p.engine,
        replace(p.config, retention=Retention(run_history_days=90)),
        SYSTEM_ACTOR,
        role=ApiRole.ADMIN,
    )
    deleting, run_finished = threading.Event(), threading.Event()
    batches = []

    def hold_first_delete(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("DELETE FROM AUD_TASK_ATTEMPTS"):
            batches.append(cursor.rowcount)
            if not deleting.is_set():
                deleting.set()
                assert run_finished.wait(30), "a concurrent run blocked behind history deletion"

    event.listen(p.engine, "after_cursor_execute", hold_first_delete)
    started = perf_counter()
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(
                contextvars.copy_context().run, invoke, ctx, ActionName.PURGE, PurgeInput()
            )
            try:
                assert deleting.wait(30), "purge never reached an attempt batch"
                code, output = p.run("run", "--pipeline-code", "P")
                assert code == 0, output
            finally:
                run_finished.set()
            result = future.result(timeout=300)
    finally:
        event.remove(p.engine, "after_cursor_execute", hold_first_delete)
    elapsed = perf_counter() - started
    record_testsuite_property("purge_seconds", f"{elapsed:.3f}")
    assert elapsed < 300
    assert result.counts["AUD_TASK_ATTEMPTS"] == 1_000_000
    assert max(batches) <= 10_000
    with p.engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT COUNT(*) AS count FROM AUD_TASK_ATTEMPTS WHERE TASK_RUN_ID=:task"),
                {"task": tasks[task]},
            ).scalar_one()
            == 0
        )
        assert (
            conn.execute(
                text("SELECT COUNT(*) AS count FROM AUD_TASK_ATTEMPTS WHERE STATUS='SUCCESS'")
            ).scalar_one()
            == 1
        )
