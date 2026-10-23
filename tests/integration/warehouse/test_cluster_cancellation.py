"""Operators stop real SQL, Python and event queries, with physical query acknowledgements."""

import contextvars
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

import psutil
import pytest
from sqlalchemy import text

from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.enums import (
    ActionName,
    ApiRole,
    AttemptStatus,
    ControlAction,
    ControlStatus,
    RunStatus,
)
from etl_craft.engine import controls
from etl_craft.execution.pipeline import run_pipeline
from etl_craft.execution.runner import run_task
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.requests import ControlRequest
from fixtures.cancellation import wait_for
from fixtures.metadata import add_pipeline, add_task, start_run
from fixtures.retry_warehouse import save_project

SLOW = {
    "postgres": "SELECT 1 AS cancel_slow FROM pg_sleep(60)",
    "trino_iceberg": "SELECT SUM(mod(a.x*b.x*c.x*d.x,7)) AS cancel_slow FROM "
    "UNNEST(sequence(1,1000)) a(x) CROSS JOIN UNNEST(sequence(1,1000)) b(x) "
    "CROSS JOIN UNNEST(sequence(1,1000)) c(x) "
    "CROSS JOIN UNNEST(sequence(1,1000)) d(x) HAVING count(*)>0",
    "duckdb": "SELECT SUM(mod(a.range*b.range,7)) AS cancel_slow "
    "FROM range(1000000000) a CROSS JOIN range(1000) b HAVING count(*)>0",
}
SLOW["duckdb_iceberg"] = SLOW["duckdb"]


@pytest.mark.parametrize("kind", ["SQL", "PYTHON", "EVENT_WAIT"])
# Warehouse suites own this matrix; Engine DB cases must not also select it for engine jobs.
@pytest.mark.parametrize("empty_engine_db", ["sqlite", "postgresql"], indirect=True)
def test_operator_cancel_confirms_long_query_ended(
    sql_world, engine_db, tmp_path, monkeypatch, kind
):
    world = sql_world
    world.engine_db = engine_db.engine
    profile = engine_db.config.engine
    if engine_db.engine.url.get_backend_name() == "sqlite":
        profile = replace(profile, jdbc_url=f"jdbc:sqlite:{engine_db.engine.url.database}")
    world.config = replace(world.config, engine=profile)
    with world.engine_db.begin() as conn:
        world.pipeline_id = add_pipeline(conn, "CANCEL_QUERY")
        world.pipeline_run_id = start_run(conn, world.pipeline_id)
        params = (
            {
                "SQL_ACTION": "CREATE_TABLE",
                "TARGET_OBJECT": world.name("cancelled"),
                "SELECT_QUERY": SLOW[world.kind],
            }
            if kind == "SQL"
            else {"EVENT_SQL": SLOW[world.kind]}
            if kind == "EVENT_WAIT"
            else {"SCRIPT_FILE": "python_scripts/cancel.py"}
        )
        task = add_task(conn, world.pipeline_id, "slow", kind, **params)
    save_project(world)
    scripts = world.config.python_scripts_dir
    scripts.mkdir(exist_ok=True)
    (scripts / "cancel.py").write_text(
        "from sqlalchemy import text\nfrom etl_craft.scripting import ScriptResult\n"
        "def run(task):\n"
        "    with task.warehouse() as warehouse, warehouse.connect() as conn:\n"
        f"        conn.execute(text({SLOW[world.kind]!r})).all()\n"
        "    return ScriptResult(0)\n"
    )
    instrumentation = tmp_path / "instrumentation"
    instrumentation.mkdir()
    (instrumentation / "sitecustomize.py").write_text(
        "from fixtures.cancellation import install_query_marker\ninstall_query_marker()\n"
    )
    marker = tmp_path / "query-started"
    monkeypatch.setenv(
        "PYTHONPATH", os.pathsep.join((str(instrumentation), str(Path(__file__).parents[2])))
    )
    monkeypatch.setenv("ETL_CRAFT_TEST_QUERY_STARTED", str(marker))
    ctx = OperationContext(world.engine_db, world.config, SYSTEM_ACTOR, role=ApiRole.ADMIN)

    def execute():
        if kind == "EVENT_WAIT":
            return run_pipeline(world.engine_db, world.config, "CANCEL_QUERY")
        return run_task(
            world.engine_db, world.config, "CANCEL_QUERY", "slow", automatic_retries=False
        )

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(contextvars.copy_context().run, execute)
        try:

            def ready():
                if future.done():
                    pytest.fail(f"task ended before its query started: {future.result()}")
                return marker.exists()

            wait_for(ready)
            observed = json.loads(marker.read_text())
            if world.kind.startswith("duckdb"):

                def computing():
                    cpu = psutil.Process(observed["pid"]).cpu_times()
                    return cpu.user + cpu.system > observed["cpu"] + 0.05

                wait_for(computing)
            else:
                from etl_craft.core.ownership import attempt_holder
                from etl_craft.engine.resource_locks import holder_tag
                from etl_craft.warehouse.connection import warehouse_dialect

                with world.engine_db.connect() as conn:
                    if kind == "EVENT_WAIT":
                        holder = controls.event_probes(conn, world.pipeline_run_id, task)[
                            0
                        ].holder_id
                    else:
                        row = conn.execute(
                            text(
                                "SELECT a.ATTEMPT_ID AS attempt_id, a.OWNER_ID AS owner_id "
                                "FROM AUD_TASK_ATTEMPTS a JOIN AUD_TASK_RUN_LOG t "
                                "ON t.TASK_RUN_ID=a.TASK_RUN_ID WHERE t.TASK_ID=:task "
                                "AND t.PIPELINE_RUN_ID=:run"
                            ),
                            {"task": task, "run": world.pipeline_run_id},
                        ).one()
                        holder = attempt_holder(row.attempt_id, row.owner_id)

                def running():
                    with world.warehouse.begin() as conn:
                        return warehouse_dialect(world.config).running_tagged(
                            conn, holder_tag(holder)
                        )

                wait_for(running)
            started = time.monotonic()
            pending = invoke(
                ctx,
                ActionName.CONTROL,
                ControlRequest(
                    pipeline_code="CANCEL_QUERY",
                    run_id=world.pipeline_run_id,
                    task_code="slow",
                    action=ControlAction.ABORT,
                    reason="stop the long warehouse query",
                ),
            )
            result = future.result(timeout=15)

            def completed():
                # Standalone tasks have no controller tick; advance observations explicitly.
                from etl_craft.execution.control_flow import advance

                advance(world.engine_db, world.pipeline_run_id, observed=True, config=world.config)
                with world.engine_db.connect() as conn:
                    view = controls.get(conn, pending.control_id)
                return view if view.status == ControlStatus.COMPLETED else None

            done = wait_for(completed, seconds=10)
            assert time.monotonic() - started < 8
            assert done.completed_at is not None
            if kind != "EVENT_WAIT":
                assert result.status == RunStatus.CANCELLED
                with world.engine_db.connect() as conn:
                    attempt = conn.execute(
                        text(
                            "SELECT STATUS AS status, CANCEL_REQUESTED_AT AS requested, "
                            "CANCEL_ACKNOWLEDGED_AT AS acknowledged FROM AUD_TASK_ATTEMPTS "
                            "WHERE TASK_RUN_ID=:id"
                        ),
                        {"id": result.task_run_id},
                    ).one()
                assert attempt.status == AttemptStatus.CANCELLED
                assert attempt.requested is not None and attempt.acknowledged is not None
                assert done.attempts[0].process_ended_at is not None
                assert done.attempts[0].queries_ended_at is not None
            with world.engine_db.connect() as conn:
                task_row = conn.execute(
                    text(
                        "SELECT STATUS AS status FROM AUD_TASK_RUN_LOG "
                        "WHERE TASK_ID=:task AND PIPELINE_RUN_ID=:run"
                    ),
                    {"task": task, "run": world.pipeline_run_id},
                ).one()
            assert task_row.status == RunStatus.CANCELLED
        finally:
            if not future.done():
                invoke(
                    ctx,
                    ActionName.CONTROL,
                    ControlRequest(
                        pipeline_code="CANCEL_QUERY",
                        run_id=world.pipeline_run_id,
                        action=ControlAction.ABORT,
                        reason="test cleanup",
                    ),
                )
                future.result(timeout=65)
