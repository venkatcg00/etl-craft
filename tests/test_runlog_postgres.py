"""Integration tests for etl_craft.runlog against a real Postgres.

test_runlog.py already covers the control-flow logic against SQLite. This
file exists for the one thing that can't be proven there: that concurrent
callers of find_or_create_active_run genuinely converge on a single run via
the database's own partial unique index, not via anything in application
code. See tests/conftest.py for how the Postgres connection is resolved
(and skipped if none is reachable).
"""

import threading

from sqlalchemy import text

from etl_craft.runlog import (
    find_or_create_active_run,
    find_or_create_task_run,
    resolve_run_for_task,
    update_task_run,
)


def test_find_or_create_active_run_against_real_schema(pg_conn, cfg_pipeline):
    first = find_or_create_active_run(pg_conn, cfg_pipeline)
    second = find_or_create_active_run(pg_conn, cfg_pipeline)
    assert first == second


def test_find_or_create_active_run_mints_fresh_run_after_terminal(pg_conn, cfg_pipeline):
    first = find_or_create_active_run(pg_conn, cfg_pipeline)
    pg_conn.execute(
        text("UPDATE AUD_PIPELINES_RUN_LOG SET STATUS = 'SUCCESS' WHERE PIPELINE_RUN_ID = :id"),
        {"id": first},
    )
    second = find_or_create_active_run(pg_conn, cfg_pipeline)
    assert second != first


def test_resolve_run_for_task_dev_fallback_against_real_schema(pg_conn, cfg_pipeline):
    run_id = find_or_create_active_run(pg_conn, cfg_pipeline)
    pg_conn.execute(
        text("UPDATE AUD_PIPELINES_RUN_LOG SET STATUS = 'FAILED' WHERE PIPELINE_RUN_ID = :id"),
        {"id": run_id},
    )
    resolved = resolve_run_for_task(pg_conn, cfg_pipeline)
    assert resolved == run_id


def test_find_or_create_task_run_short_circuits_on_success(pg_conn, cfg_pipeline, cfg_task):
    run_id = find_or_create_active_run(pg_conn, cfg_pipeline)
    binding = find_or_create_task_run(pg_conn, cfg_task, run_id)
    update_task_run(pg_conn, binding.task_run_id, status="SUCCESS", target_count=7)

    rebound = find_or_create_task_run(pg_conn, cfg_task, run_id)
    assert rebound.task_run_id == binding.task_run_id
    assert rebound.status == "SUCCESS"


def test_concurrent_find_or_create_active_run_converges_on_one_run(
    postgres_engine, committed_pipeline
):
    """The DB's ux_pipeline_run_one_active — not app logic — must resolve this race."""
    results: list[int] = []
    errors: list[Exception] = []
    racer_count = 8
    barrier = threading.Barrier(racer_count)

    def race() -> None:
        try:
            barrier.wait(timeout=5)
            with postgres_engine.connect() as conn, conn.begin():
                results.append(find_or_create_active_run(conn, committed_pipeline))
        except Exception as exc:  # surfaced via `errors`, not swallowed
            errors.append(exc)

    threads = [threading.Thread(target=race) for _ in range(racer_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert not errors, errors
    assert len(results) == racer_count
    assert len(set(results)) == 1, "every racer must converge on the exact same run id"
