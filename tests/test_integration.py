"""The opt-in, real-Postgres integration suite — needs Docker (see Makefile).

Skips itself with a clear message (see tests/conftest.py's postgres_engine
fixture) if nothing is reachable, so plain `pytest -q` never requires it.
Organized by source module, one section per module, since combining them
loses nothing (no fixture/helper name collisions) and keeps file count down.
"""

import threading

import pytest
from sqlalchemy import text

from conftest import insert_committed_dependency, insert_committed_task, seed_active_run
from etl_craft.cfg import (
    CfgError,
    fetch_pipeline_graph,
    fetch_task_handler,
    resolve_pipeline_id,
    resolve_task_id,
)
from etl_craft.config import (
    CloningConfig,
    ConnectionProfile,
    ConnectionSection,
    ConnectorConfig,
    SourceConfig,
)
from etl_craft.runlog import (
    find_or_create_active_run,
    find_or_create_task_run,
    resolve_run_for_task,
    update_task_run,
)
from etl_craft.runner import DependenciesNotMetError, ForceNotAllowedError, run_task

# ==============================================================================
# runlog.py — against real Postgres
# ==============================================================================
#
# test_unit.py already covers the control-flow logic against SQLite. This
# section exists for the one thing that can't be proven there: that
# concurrent callers of find_or_create_active_run genuinely converge on a
# single run via the database's own partial unique index, not via anything
# in application code.


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


# ==============================================================================
# cfg.py — against real Postgres
# ==============================================================================
#
# Purely SELECT-based against sql/schema.sql, so there's no separate
# non-Postgres logic worth stand-in testing.


def test_resolve_pipeline_id(pg_conn, cfg_pipeline):
    assert resolve_pipeline_id(pg_conn, "TEST_PL") == cfg_pipeline


def test_resolve_pipeline_id_unknown_code_raises(pg_conn):
    with pytest.raises(CfgError):
        resolve_pipeline_id(pg_conn, "NO_SUCH_PIPELINE")


def test_resolve_pipeline_id_ignores_inactive(pg_conn, cfg_pipeline):
    pg_conn.execute(
        text("UPDATE CFG_PIPELINES SET ACTIVE_FLAG = 'N' WHERE PIPELINE_ID = :id"),
        {"id": cfg_pipeline},
    )
    with pytest.raises(CfgError):
        resolve_pipeline_id(pg_conn, "TEST_PL")


def test_resolve_task_id(pg_conn, cfg_pipeline, cfg_task):
    assert resolve_task_id(pg_conn, cfg_pipeline, "test_task") == cfg_task


def test_resolve_task_id_unknown_code_raises(pg_conn, cfg_pipeline):
    with pytest.raises(CfgError):
        resolve_task_id(pg_conn, cfg_pipeline, "no_such_task")


def test_fetch_task_handler(pg_conn, cfg_task):
    assert fetch_task_handler(pg_conn, cfg_task) == "SQL"


def test_fetch_pipeline_graph_no_dependencies(pg_conn, cfg_pipeline, cfg_task):
    data = fetch_pipeline_graph(pg_conn, cfg_pipeline)
    assert [t.task_id for t in data.tasks] == [cfg_task]
    assert data.same_pipeline_edges == []
    assert data.cross_pipeline_task_ids == frozenset()


def test_fetch_pipeline_graph_same_pipeline_edge(pg_conn, cfg_pipeline, cfg_task):
    task_b = pg_conn.execute(
        text(
            "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
            "VALUES ('task_b', 'ETL', :pipeline_id, 'SQL') RETURNING TASK_ID"
        ),
        {"pipeline_id": cfg_pipeline},
    ).scalar_one()
    pg_conn.execute(
        text(
            "INSERT INTO CFG_TASK_DEPENDENCY "
            "(PIPELINE_ID, TASK_ID, DEPENDS_ON_PIPELINE_ID, DEPENDS_ON_TASK_ID, DEPENDENCY_TYPE) "
            "VALUES (:pipeline_id, :task_b, :pipeline_id, :cfg_task, 'SUCCESS')"
        ),
        {"pipeline_id": cfg_pipeline, "task_b": task_b, "cfg_task": cfg_task},
    )

    data = fetch_pipeline_graph(pg_conn, cfg_pipeline)
    assert {t.task_id for t in data.tasks} == {cfg_task, task_b}
    assert len(data.same_pipeline_edges) == 1
    edge = data.same_pipeline_edges[0]
    assert edge.task_id == task_b
    assert edge.depends_on_task_id == cfg_task
    assert edge.dependency_type == "SUCCESS"
    assert data.cross_pipeline_task_ids == frozenset()


def test_fetch_pipeline_graph_cross_pipeline_edge_excluded(pg_conn, cfg_pipeline, cfg_task):
    other_pipeline = pg_conn.execute(
        text(
            "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
            "VALUES ('TEST_OTHER_PL', 'Other Pipeline', 'FULL') RETURNING PIPELINE_ID"
        )
    ).scalar_one()
    other_task = pg_conn.execute(
        text(
            "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
            "VALUES ('other_task', 'ETL', :pipeline_id, 'SQL') RETURNING TASK_ID"
        ),
        {"pipeline_id": other_pipeline},
    ).scalar_one()
    pg_conn.execute(
        text(
            "INSERT INTO CFG_TASK_DEPENDENCY "
            "(PIPELINE_ID, TASK_ID, DEPENDS_ON_PIPELINE_ID, DEPENDS_ON_TASK_ID, DEPENDENCY_TYPE) "
            "VALUES (:pipeline_id, :task_id, :other_pipeline, :other_task, 'SUCCESS')"
        ),
        {
            "pipeline_id": cfg_pipeline,
            "task_id": cfg_task,
            "other_pipeline": other_pipeline,
            "other_task": other_task,
        },
    )

    data = fetch_pipeline_graph(pg_conn, cfg_pipeline)
    assert data.same_pipeline_edges == []
    assert data.cross_pipeline_task_ids == frozenset({cfg_task})


# ==============================================================================
# runner.py — against real Postgres
# ==============================================================================
#
# run_task opens its own connections internally (mirroring how it'll really
# be invoked — once per `etl-craft run --task_code`), so every test here
# uses genuinely committed data via conftest.py's committed_pipeline
# helpers, not the rolled-back pg_conn used in the sections above.


def make_config(mode: str = "local") -> ConnectorConfig:
    profile = ConnectionProfile(
        section="POSTGRES",
        name="dev",
        jdbc_url="jdbc:postgresql://localhost:55432/etl_craft",
        user="etl_craft",
        auth_mode="password",
    )
    return ConnectorConfig(
        mode=mode,
        source=SourceConfig(type="environment"),
        postgres=ConnectionSection(active_profile="dev", profiles={"dev": profile}),
        cloning=CloningConfig(),
    )


def test_run_task_with_no_dependencies_hits_stub_handler_and_fails(
    postgres_engine, committed_pipeline
):
    # HANDLER=SQL has no real implementation yet (handlers.py is a stub
    # registry) — so a task with no blocking dependencies should get all
    # the way through the dependency check and binding, then fail on
    # dispatch. That's the expected outcome until real handlers exist.
    insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(), "TEST_CONCURRENT_PL", "task_a")

    assert outcome.status == "FAILED"
    assert "HANDLER='SQL'" in outcome.message

    with postgres_engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT STATUS AS status, ERROR_MESSAGE AS error_message "
                "FROM AUD_TASK_RUN_LOG t "
                "JOIN CFG_TASKS c ON c.TASK_ID = t.TASK_ID "
                "WHERE c.TASK_CODE = 'task_a' AND c.PIPELINE_ID = :pid"
            ),
            {"pid": committed_pipeline},
        ).one()
    assert row.status == "FAILED"
    assert row.error_message is not None


def test_run_task_short_circuits_on_existing_success(postgres_engine, committed_pipeline):
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    seed_active_run(postgres_engine, committed_pipeline)
    # Run once (fails on the stub handler), then force it to SUCCESS by
    # hand to simulate a real handler having completed, and confirm a
    # second invocation short-circuits without touching the row again.
    run_task(postgres_engine, make_config(), "TEST_CONCURRENT_PL", "task_a")
    with postgres_engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_TASK_RUN_LOG SET STATUS = 'SUCCESS' WHERE TASK_ID = :task_id"),
            {"task_id": task_id},
        )

    outcome = run_task(postgres_engine, make_config(), "TEST_CONCURRENT_PL", "task_a")

    assert outcome.status == "SKIPPED"


def test_run_task_raises_when_dependency_not_met(postgres_engine, committed_pipeline):
    # task_b depends on task_a via SUCCESS; task_a hasn't been run at all.
    task_a = insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    task_b = insert_committed_task(postgres_engine, committed_pipeline, "task_b")
    insert_committed_dependency(postgres_engine, committed_pipeline, task_b, task_a)
    seed_active_run(postgres_engine, committed_pipeline)

    with pytest.raises(DependenciesNotMetError):
        run_task(postgres_engine, make_config(), "TEST_CONCURRENT_PL", "task_b")


def test_run_task_proceeds_once_dependency_satisfied(postgres_engine, committed_pipeline):
    task_a = insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    task_b = insert_committed_task(postgres_engine, committed_pipeline, "task_b")
    insert_committed_dependency(postgres_engine, committed_pipeline, task_b, task_a)
    seed_active_run(postgres_engine, committed_pipeline)

    # task_a first, so it logs (fails on the stub, but that's a terminal
    # status — which is all the SUCCESS-typed edge cares about being met).
    # To actually satisfy a SUCCESS edge, force task_a's row to SUCCESS.
    run_task(postgres_engine, make_config(), "TEST_CONCURRENT_PL", "task_a")
    with postgres_engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_TASK_RUN_LOG SET STATUS = 'SUCCESS' WHERE TASK_ID = :id"),
            {"id": task_a},
        )

    outcome = run_task(postgres_engine, make_config(), "TEST_CONCURRENT_PL", "task_b")

    # Gets past the dependency check and fails on the stub handler, same
    # as any other task — proving it was the dependency gate that mattered.
    assert outcome.status == "FAILED"


def test_run_task_force_bypasses_dependency_check(postgres_engine, committed_pipeline):
    task_a = insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    task_b = insert_committed_task(postgres_engine, committed_pipeline, "task_b")
    insert_committed_dependency(postgres_engine, committed_pipeline, task_b, task_a)
    seed_active_run(postgres_engine, committed_pipeline)

    # task_a was never run, so without --force this would raise
    # DependenciesNotMetError (see test_run_task_raises_when_dependency_not_met).
    outcome = run_task(postgres_engine, make_config(), "TEST_CONCURRENT_PL", "task_b", force=True)

    assert outcome.status == "FAILED"  # got past the (skipped) dependency check to the stub handler


def test_run_task_force_refused_under_orchestrator_mode(postgres_engine, committed_pipeline):
    insert_committed_task(postgres_engine, committed_pipeline, "task_a")

    with pytest.raises(ForceNotAllowedError):
        run_task(
            postgres_engine,
            make_config(mode="orchestrator"),
            "TEST_CONCURRENT_PL",
            "task_a",
            force=True,
        )
