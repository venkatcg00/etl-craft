"""Generated keys survive concurrent task processes and interrupted allocation."""

import multiprocessing
from dataclasses import replace

import pytest

from etl_craft.core.errors import HandlerError
from etl_craft.handlers.sql.session import Session
from fixtures.engine_db import apply_schema
from fixtures.metadata import add_pipeline, start_run
from fixtures.sql_row_ids import check_row_id_generation, run_paused_append


def test_generated_row_ids_survive_writes_and_evolution(sql_world):
    check_row_id_generation(sql_world, "numbered")


@pytest.mark.parametrize("engine_kind", ["sqlite", "postgresql"])
def test_concurrent_appends_keep_row_ids_unique_across_processes(sql_world, request, engine_kind):
    w = sql_world
    if w.kind != "trino_iceberg":
        pytest.skip("Trino supports separate task processes sharing one Iceberg target")
    if engine_kind == "postgresql":
        db = request.getfixturevalue("postgres_database")
        apply_schema(db.engine)
        w.config = replace(w.config, engine=db.config.engine)
        w.engine_db = db.engine
        with db.engine.begin() as conn:
            w.pipeline_id = add_pipeline(conn, "P", refresh_type="INCREMENTAL")
            w.pipeline_run_id = start_run(conn, w.pipeline_id)
    w.setup("events", "SELECT 1 AS id", "APPEND_TABLE")
    params = {"SQL_ACTION": "APPEND_TABLE", "TARGET_OBJECT": "events"}
    w.run("seed", SOURCE_SQL="SELECT 0 AS id", **params)
    contexts = [
        w.task("first", SOURCE_SQL="SELECT 1 AS id UNION ALL SELECT 2", **params),
        w.task(
            "second",
            SOURCE_SQL="SELECT 3 AS id UNION ALL SELECT 4",
            **{**params, "TARGET_OBJECT": w.name("events").upper()},
        ),
    ]
    # Both references normalize to the same target lock; each process builds its own pools.
    spawn = multiprocessing.get_context("spawn")
    attempted = [spawn.Event(), spawn.Event()]
    allocated = [spawn.Event(), spawn.Event()]
    release = spawn.Event()
    results = spawn.Queue()
    processes = [
        spawn.Process(
            target=run_paused_append,
            args=(context, i == 0, attempted[i], allocated[i], release, results),
        )
        for i, context in enumerate(contexts)
    ]
    try:
        processes[0].start()
        assert allocated[0].wait(20), "first append never read its ROW_ID base"
        processes[1].start()
        assert attempted[1].wait(20), "second append never attempted the target lock"
        assert not allocated[1].wait(0.3), (
            "second append read MAX(ROW_ID) before the first committed"
        )
        release.set()
        for process in processes:
            process.join(30)
            assert process.exitcode == 0
        assert [results.get(timeout=5) for _ in processes] == [("ok", 2), ("ok", 2)]
        rows = w.rows(f"SELECT id, row_id FROM {w.name('events')}")
        assert sorted(row[0] for row in rows) == [0, 1, 2, 3, 4]
        assert sorted(row[1] for row in rows) == [1, 2, 3, 4, 5]
        assert {row[1] for row in rows if row[0] in {1, 2}} == {2, 3}
        assert {row[1] for row in rows if row[0] in {3, 4}} == {4, 5}
    finally:
        release.set()
        for process in processes:
            if process.pid is not None and process.is_alive():
                process.terminate()
                process.join(5)
        results.close()


def test_failed_computed_allocation_releases_the_target_lock(sql_world, monkeypatch):
    w = sql_world
    if w.kind not in {"trino_iceberg", "duckdb_iceberg"}:
        pytest.skip("computed generators allocate from the current maximum")
    w.setup("events", "SELECT 1 AS id", "APPEND_TABLE")
    original = Session.count

    def fail_after_max(session, statement, *, step):
        result = original(session, statement, step=step)
        if step == "largest ROW_ID":
            raise HandlerError("allocation interrupted before insert")
        return result

    params = {
        "SQL_ACTION": "APPEND_TABLE",
        "TARGET_OBJECT": "events",
        "SOURCE_SQL": "SELECT 1 AS id",
    }
    with monkeypatch.context() as patch:
        patch.setattr(Session, "count", fail_after_max)
        with pytest.raises(HandlerError, match="allocation interrupted"):
            w.run("load", **params)
    assert not w.rows(f"SELECT id FROM {w.name('events')}")
    assert w.run("load", **params).insert_count == 1
    assert w.rows(f"SELECT row_id FROM {w.name('events')}") == [(1,)]
