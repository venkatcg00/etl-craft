"""The opt-in, real-Postgres integration suite — needs Docker (see Makefile).

Skips itself with a clear message (see tests/conftest.py's postgres_engine
fixture) if nothing is reachable, so plain `pytest -q` never requires it.
Organized by source module, one section per module, since combining them
loses nothing (no fixture/helper name collisions) and keeps file count down.
"""

import threading
import time

import pytest
import yaml
from sqlalchemy import text

from conftest import (
    CRAFT_CONNECTOR_YAML,
    insert_committed_dependency,
    insert_committed_task,
    seed_active_run,
)
from etl_craft.cfg import (
    CfgError,
    fetch_all_pipelines,
    fetch_cross_pipeline_task_edges,
    fetch_pipeline_dependencies,
    fetch_pipeline_detail,
    fetch_pipeline_graph,
    fetch_task_handler,
    resolve_pipeline_id,
    resolve_task_id,
)
from etl_craft.cli import main as cli_main
from etl_craft.config import (
    CloningConfig,
    ConnectionProfile,
    ConnectionSection,
    ConnectorConfig,
    SourceConfig,
)
from etl_craft.generate_yml import generate_pipeline_dag
from etl_craft.handlers import HandlerResult
from etl_craft.orchestrator import OrchestratorModeRefusedError, init_pipeline_run, run_pipeline
from etl_craft.resolver import ResolverError
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


def test_find_or_create_active_run_second_transaction_hits_integrity_error_path(
    postgres_engine, committed_pipeline
):
    """Deterministically forces find_or_create_active_run's except-IntegrityError branch.

    The 8-racer test above proves the DB guarantee holds under real load, but
    its timing is best-effort — depending on connection-pool/network
    scheduling, a "losing" thread's own SELECT can end up running *after*
    the winner has already committed, so it never actually attempts the
    conflicting INSERT that triggers the except branch. This test instead
    controls the interleaving explicitly so that branch is exercised for
    real, every time.
    """
    holder_inserted = threading.Event()
    results: dict[str, int] = {}
    errors: list[Exception] = []

    def holder() -> None:
        try:
            with postgres_engine.connect() as conn, conn.begin():
                run_id = conn.execute(
                    text(
                        "INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS) "
                        "VALUES (:id, 'IN-PROGRESS') RETURNING PIPELINE_RUN_ID"
                    ),
                    {"id": committed_pipeline},
                ).scalar_one()
                results["holder"] = run_id
                holder_inserted.set()
                # Stay uncommitted long enough for the racer to see nothing
                # on its own SELECT, then block on our still-open insert.
                time.sleep(0.3)
        except Exception as exc:  # surfaced via `errors`, not swallowed
            errors.append(exc)

    def racer() -> None:
        try:
            assert holder_inserted.wait(timeout=5)
            time.sleep(0.05)
            with postgres_engine.connect() as conn, conn.begin():
                results["racer"] = find_or_create_active_run(conn, committed_pipeline)
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=holder), threading.Thread(target=racer)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert not errors, errors
    assert results["holder"] == results["racer"]


def test_find_or_create_task_run_second_transaction_hits_integrity_error_path(
    postgres_engine, committed_pipeline
):
    """Deterministically forces find_or_create_task_run's except-IntegrityError branch.

    Same rationale as the pipeline-run version above, at task-run grain.
    """
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    pipeline_run_id = seed_active_run(postgres_engine, committed_pipeline)

    holder_inserted = threading.Event()
    results: dict[str, int] = {}
    errors: list[Exception] = []

    def holder() -> None:
        try:
            with postgres_engine.connect() as conn, conn.begin():
                task_run_id = conn.execute(
                    text(
                        "INSERT INTO AUD_TASK_RUN_LOG (TASK_ID, PIPELINE_RUN_ID, STATUS) "
                        "VALUES (:task_id, :pipeline_run_id, 'IN-PROGRESS') "
                        "RETURNING TASK_RUN_ID"
                    ),
                    {"task_id": task_id, "pipeline_run_id": pipeline_run_id},
                ).scalar_one()
                results["holder"] = task_run_id
                holder_inserted.set()
                time.sleep(0.3)
        except Exception as exc:
            errors.append(exc)

    def racer() -> None:
        try:
            assert holder_inserted.wait(timeout=5)
            time.sleep(0.05)
            with postgres_engine.connect() as conn, conn.begin():
                binding = find_or_create_task_run(conn, task_id, pipeline_run_id)
                results["racer"] = binding.task_run_id
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=holder), threading.Thread(target=racer)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert not errors, errors
    assert results["holder"] == results["racer"]


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


def test_fetch_all_pipelines_includes_active_excludes_inactive(pg_conn, cfg_pipeline):
    pg_conn.execute(
        text(
            "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE, ACTIVE_FLAG) "
            "VALUES ('TEST_INACTIVE_PL', 'Inactive Pipeline', 'FULL', 'N')"
        )
    )

    pipelines = fetch_all_pipelines(pg_conn)

    codes = {p.pipeline_code for p in pipelines}
    assert "TEST_PL" in codes
    assert "TEST_INACTIVE_PL" not in codes


def test_fetch_pipeline_dependencies(pg_conn, cfg_pipeline):
    other_pipeline = pg_conn.execute(
        text(
            "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
            "VALUES ('TEST_UPSTREAM_PL', 'Upstream Pipeline', 'FULL') RETURNING PIPELINE_ID"
        )
    ).scalar_one()
    pg_conn.execute(
        text(
            "INSERT INTO CFG_PIPELINE_DEPENDENCY "
            "(PIPELINE_ID, DEPENDS_ON_PIPELINE_ID, DEPENDENCY_TYPE) "
            "VALUES (:pipeline_id, :other_pipeline, 'HAS_DATA')"
        ),
        {"pipeline_id": cfg_pipeline, "other_pipeline": other_pipeline},
    )

    deps = fetch_pipeline_dependencies(pg_conn, cfg_pipeline)

    assert len(deps) == 1
    assert deps[0].depends_on_pipeline_code == "TEST_UPSTREAM_PL"
    assert deps[0].dependency_type == "HAS_DATA"


def test_fetch_cross_pipeline_task_edges(pg_conn, cfg_pipeline, cfg_task):
    other_pipeline = pg_conn.execute(
        text(
            "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
            "VALUES ('TEST_UPSTREAM_PL2', 'Upstream Pipeline 2', 'FULL') RETURNING PIPELINE_ID"
        )
    ).scalar_one()
    other_task = pg_conn.execute(
        text(
            "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
            "VALUES ('upstream_task', 'ETL', :pipeline_id, 'SQL') RETURNING TASK_ID"
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

    edges = fetch_cross_pipeline_task_edges(pg_conn, cfg_pipeline)

    assert len(edges) == 1
    assert edges[0].task_code == "test_task"
    assert edges[0].depends_on_pipeline_code == "TEST_UPSTREAM_PL2"
    assert edges[0].depends_on_task_code == "upstream_task"
    assert edges[0].dependency_type == "SUCCESS"


def test_fetch_pipeline_detail(pg_conn, cfg_pipeline):
    pg_conn.execute(
        text(
            "UPDATE CFG_PIPELINES SET DESCRIPTION = 'A test pipeline', "
            "RUN_SCHEDULE = '0 6 * * *', SLA_IN_HOURS = 1.5 WHERE PIPELINE_ID = :id"
        ),
        {"id": cfg_pipeline},
    )

    detail = fetch_pipeline_detail(pg_conn, cfg_pipeline)

    assert detail.pipeline_code == "TEST_PL"
    assert detail.description == "A test pipeline"
    assert detail.run_schedule == "0 6 * * *"
    assert detail.sla_in_hours == 1.5
    assert isinstance(detail.sla_in_hours, float)
    assert detail.refresh_type == "INCREMENTAL"


def test_fetch_pipeline_detail_nullable_fields_default_none(pg_conn, cfg_pipeline):
    detail = fetch_pipeline_detail(pg_conn, cfg_pipeline)
    assert detail.description is None
    assert detail.run_schedule is None
    assert detail.sla_in_hours is None


# ==============================================================================
# generate_yml.py — against real Postgres
# ==============================================================================


def test_generate_pipeline_dag_linear_chain(pg_conn, cfg_pipeline, cfg_task):
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

    dag = generate_pipeline_dag(pg_conn, "TEST_PL")

    assert dag["dag_id"] == "TEST_PL"
    assert dag["refresh_type"] == "INCREMENTAL"
    assert set(dag["tasks"]) == {"__init__", "test_task", "task_b"}
    assert dag["tasks"]["__init__"]["depends_on"] == []
    assert (
        dag["tasks"]["__init__"]["bash_command"]
        == "etl-craft run --pipeline_code TEST_PL --init-only"
    )
    assert dag["tasks"]["test_task"]["depends_on"] == [
        {"task": "__init__", "dependency_type": "ALWAYS"}
    ]
    assert dag["tasks"]["task_b"]["depends_on"] == [
        {"task": "test_task", "dependency_type": "SUCCESS"}
    ]
    assert "pipeline_dependencies" not in dag
    assert "cross_pipeline_task_dependencies" not in dag


def test_generate_pipeline_dag_with_no_tasks_still_has_init(pg_conn, cfg_pipeline):
    dag = generate_pipeline_dag(pg_conn, "TEST_PL")
    assert set(dag["tasks"]) == {"__init__"}


def test_generate_pipeline_dag_rejects_cycle(pg_conn, cfg_pipeline, cfg_task):
    task_b = pg_conn.execute(
        text(
            "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
            "VALUES ('task_b', 'ETL', :pipeline_id, 'SQL') RETURNING TASK_ID"
        ),
        {"pipeline_id": cfg_pipeline},
    ).scalar_one()
    for task_id, depends_on in [(task_b, cfg_task), (cfg_task, task_b)]:
        pg_conn.execute(
            text(
                "INSERT INTO CFG_TASK_DEPENDENCY "
                "(PIPELINE_ID, TASK_ID, DEPENDS_ON_PIPELINE_ID, DEPENDS_ON_TASK_ID, "
                "DEPENDENCY_TYPE) "
                "VALUES (:pipeline_id, :task_id, :pipeline_id, :depends_on, 'SUCCESS')"
            ),
            {"pipeline_id": cfg_pipeline, "task_id": task_id, "depends_on": depends_on},
        )

    with pytest.raises(ResolverError):
        generate_pipeline_dag(pg_conn, "TEST_PL")


def test_generate_pipeline_dag_unknown_pipeline_raises(pg_conn):
    with pytest.raises(CfgError):
        generate_pipeline_dag(pg_conn, "NO_SUCH_PIPELINE")


def test_generate_pipeline_dag_includes_pipeline_dependencies(pg_conn, cfg_pipeline):
    other_pipeline = pg_conn.execute(
        text(
            "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
            "VALUES ('TEST_GENYML_UPSTREAM', 'Upstream', 'FULL') RETURNING PIPELINE_ID"
        )
    ).scalar_one()
    pg_conn.execute(
        text(
            "INSERT INTO CFG_PIPELINE_DEPENDENCY "
            "(PIPELINE_ID, DEPENDS_ON_PIPELINE_ID, DEPENDENCY_TYPE) "
            "VALUES (:pipeline_id, :other_pipeline, 'HAS_DATA')"
        ),
        {"pipeline_id": cfg_pipeline, "other_pipeline": other_pipeline},
    )

    dag = generate_pipeline_dag(pg_conn, "TEST_PL")

    assert dag["pipeline_dependencies"] == [
        {"depends_on_pipeline": "TEST_GENYML_UPSTREAM", "dependency_type": "HAS_DATA"}
    ]


def test_generate_pipeline_dag_includes_cross_pipeline_task_dependencies(
    pg_conn, cfg_pipeline, cfg_task
):
    other_pipeline = pg_conn.execute(
        text(
            "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
            "VALUES ('TEST_GENYML_UPSTREAM2', 'Upstream 2', 'FULL') RETURNING PIPELINE_ID"
        )
    ).scalar_one()
    other_task = pg_conn.execute(
        text(
            "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
            "VALUES ('upstream_task', 'ETL', :pipeline_id, 'SQL') RETURNING TASK_ID"
        ),
        {"pipeline_id": other_pipeline},
    ).scalar_one()
    pg_conn.execute(
        text(
            "INSERT INTO CFG_TASK_DEPENDENCY (PIPELINE_ID, TASK_ID, DEPENDS_ON_PIPELINE_ID, "
            "DEPENDS_ON_TASK_ID, DEPENDENCY_TYPE) "
            "VALUES (:pipeline_id, :task_id, :other_pipeline, :other_task, 'SUCCESS')"
        ),
        {
            "pipeline_id": cfg_pipeline,
            "task_id": cfg_task,
            "other_pipeline": other_pipeline,
            "other_task": other_task,
        },
    )

    dag = generate_pipeline_dag(pg_conn, "TEST_PL")

    assert dag["cross_pipeline_task_dependencies"] == [
        {
            "task": "test_task",
            "depends_on_pipeline": "TEST_GENYML_UPSTREAM2",
            "depends_on_task": "upstream_task",
            "dependency_type": "SUCCESS",
        }
    ]


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


def test_run_task_marks_success_and_stamps_counts_when_handler_succeeds(
    monkeypatch, postgres_engine, committed_pipeline
):
    # Every real HANDLER is still a stub (handlers.py), so dispatch() always
    # raises — run_task's own SUCCESS-finalizing code has nothing to
    # exercise it through the real dispatch path yet. Patching dispatch
    # directly proves that code works without needing a real handler built.
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    seed_active_run(postgres_engine, committed_pipeline)
    monkeypatch.setattr(
        "etl_craft.runner.dispatch",
        lambda handler: HandlerResult(source_count=10, target_count=9, insert_count=9),
    )

    outcome = run_task(postgres_engine, make_config(), "TEST_CONCURRENT_PL", "task_a")

    assert outcome.status == "SUCCESS"
    with postgres_engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT STATUS AS status, SOURCE_COUNT AS source_count, "
                "TARGET_COUNT AS target_count, INSERT_COUNT AS insert_count "
                "FROM AUD_TASK_RUN_LOG WHERE TASK_ID = :id"
            ),
            {"id": task_id},
        ).one()
    assert row.status == "SUCCESS"
    assert row.source_count == 10
    assert row.target_count == 9
    assert row.insert_count == 9


# ==============================================================================
# orchestrator.py — against real Postgres, spawning real subprocesses
# ==============================================================================
#
# Every task here fails on the stub handler (HANDLER=SQL has no real
# implementation yet), so a fully-SUCCESS pipeline run can't be
# demonstrated until real handlers exist. These instead prove the
# orchestration mechanics: wave computation, subprocess spawning,
# retry-skip on an already-settled task, stuck-detection, and --force.


def test_run_pipeline_runs_independent_tasks_and_finalizes_failed(
    craft_connector_on_disk, postgres_engine, committed_pipeline
):
    insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    insert_committed_task(postgres_engine, committed_pipeline, "task_b")

    outcome = run_pipeline(postgres_engine, make_config(), "TEST_CONCURRENT_PL")

    assert outcome.status == "FAILED"
    with postgres_engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT STATUS AS status FROM AUD_TASK_RUN_LOG t "
                "JOIN CFG_TASKS c ON c.TASK_ID = t.TASK_ID WHERE c.PIPELINE_ID = :pid"
            ),
            {"pid": committed_pipeline},
        ).all()
    assert {row.status for row in rows} == {"FAILED"}
    assert len(rows) == 2  # both tasks actually got spawned and logged


def test_run_pipeline_never_spawns_downstream_of_a_failed_dependency(
    craft_connector_on_disk, postgres_engine, committed_pipeline
):
    task_a = insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    task_b = insert_committed_task(postgres_engine, committed_pipeline, "task_b")
    insert_committed_dependency(postgres_engine, committed_pipeline, task_b, task_a)

    outcome = run_pipeline(postgres_engine, make_config(), "TEST_CONCURRENT_PL")

    assert outcome.status == "FAILED"
    assert "stuck" in outcome.message
    with postgres_engine.connect() as conn:
        task_b_rows = conn.execute(
            text("SELECT COUNT(*) FROM AUD_TASK_RUN_LOG WHERE TASK_ID = :id"), {"id": task_b}
        ).scalar_one()
    assert task_b_rows == 0  # never became ready, so never even got bound


def test_run_pipeline_skips_already_succeeded_task(postgres_engine, committed_pipeline):
    task_a = insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    seed_active_run(postgres_engine, committed_pipeline)
    with postgres_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO AUD_TASK_RUN_LOG (TASK_ID, PIPELINE_RUN_ID, STATUS) "
                "SELECT :task_id, PIPELINE_RUN_ID, 'SUCCESS' FROM AUD_PIPELINES_RUN_LOG "
                "WHERE PIPELINE_ID = :pipeline_id AND STATUS = 'IN-PROGRESS'"
            ),
            {"task_id": task_a, "pipeline_id": committed_pipeline},
        )

    outcome = run_pipeline(postgres_engine, make_config(), "TEST_CONCURRENT_PL")

    # Never spawned (already SUCCESS), so the whole pipeline finalizes
    # SUCCESS without ever touching the stub handler.
    assert outcome.status == "SUCCESS"


def test_run_pipeline_force_bypasses_dependency_check(
    craft_connector_on_disk, postgres_engine, committed_pipeline
):
    task_a = insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    task_b = insert_committed_task(postgres_engine, committed_pipeline, "task_b")
    insert_committed_dependency(postgres_engine, committed_pipeline, task_b, task_a)

    outcome = run_pipeline(postgres_engine, make_config(), "TEST_CONCURRENT_PL", force=True)

    assert outcome.status == "FAILED"  # both hit the stub handler
    with postgres_engine.connect() as conn:
        task_b_rows = conn.execute(
            text("SELECT COUNT(*) FROM AUD_TASK_RUN_LOG WHERE TASK_ID = :id"), {"id": task_b}
        ).scalar_one()
    assert task_b_rows == 1  # force spawned it despite task_a never succeeding


def test_run_pipeline_refused_under_orchestrator_mode_with_force(
    postgres_engine, committed_pipeline
):
    insert_committed_task(postgres_engine, committed_pipeline, "task_a")

    with pytest.raises(OrchestratorModeRefusedError):
        run_pipeline(
            postgres_engine, make_config(mode="orchestrator"), "TEST_CONCURRENT_PL", force=True
        )


def test_run_pipeline_refused_under_orchestrator_mode_without_force(
    postgres_engine, committed_pipeline
):
    # The whole point of the change: this is refused even without --force,
    # since under real Airflow nothing should invoke the local wave-spawning
    # scheduler at all — see orchestrator.py's own [CHOICE] comment.
    insert_committed_task(postgres_engine, committed_pipeline, "task_a")

    with pytest.raises(OrchestratorModeRefusedError):
        run_pipeline(postgres_engine, make_config(mode="orchestrator"), "TEST_CONCURRENT_PL")


def test_init_pipeline_run_mints_active_run(postgres_engine, committed_pipeline):
    outcome = init_pipeline_run(postgres_engine, make_config(), "TEST_CONCURRENT_PL")

    with postgres_engine.connect() as conn:
        row = conn.execute(
            text("SELECT STATUS AS status FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID = :id"),
            {"id": outcome.pipeline_run_id},
        ).one()
    assert row.status == "IN-PROGRESS"


def test_init_pipeline_run_reuses_existing_active_run(postgres_engine, committed_pipeline):
    first = init_pipeline_run(postgres_engine, make_config(), "TEST_CONCURRENT_PL")
    second = init_pipeline_run(postgres_engine, make_config(), "TEST_CONCURRENT_PL")
    assert first.pipeline_run_id == second.pipeline_run_id


def test_init_pipeline_run_works_under_orchestrator_mode(postgres_engine, committed_pipeline):
    # Unlike run_pipeline, init_pipeline_run is legal under both modes — it's
    # exactly what Mode=orchestrator's synthetic first step is meant to call.
    outcome = init_pipeline_run(
        postgres_engine, make_config(mode="orchestrator"), "TEST_CONCURRENT_PL"
    )
    assert outcome.pipeline_run_id is not None


def test_run_pipeline_with_no_active_tasks_finalizes_success(postgres_engine, committed_pipeline):
    outcome = run_pipeline(postgres_engine, make_config(), "TEST_CONCURRENT_PL")
    assert outcome.status == "SUCCESS"


# ==============================================================================
# cli.py — against real Postgres, via a real craft-connector.yml on disk
# ==============================================================================


def test_cli_run_requires_pipeline_code(craft_connector_on_disk):
    with pytest.raises(SystemExit) as exc_info:
        cli_main(["run", "--task_code", "t1"])
    assert exc_info.value.code == 2


def test_cli_main_reports_config_error_when_craft_connector_missing(tmp_path, monkeypatch, capsys):
    # No craft_connector_on_disk here — the point is that nothing was
    # written, so load_config() raises before any command dispatch happens.
    monkeypatch.chdir(tmp_path)

    exit_code = cli_main(["list"])

    assert exit_code == 2
    assert "error:" in capsys.readouterr().err


def test_cli_run_end_to_end_hits_stub_handler(
    craft_connector_on_disk, postgres_engine, committed_pipeline, capsys
):
    insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    seed_active_run(postgres_engine, committed_pipeline)

    exit_code = cli_main(["run", "--pipeline_code", "TEST_CONCURRENT_PL", "--task_code", "task_a"])

    assert exit_code == 1
    assert "HANDLER='SQL'" in capsys.readouterr().out


def test_cli_run_unknown_pipeline_code(craft_connector_on_disk):
    exit_code = cli_main(["run", "--pipeline_code", "NO_SUCH_PIPELINE", "--task_code", "t1"])
    assert exit_code == 1


def test_cli_run_without_task_code_dispatches_to_pipeline_orchestration(
    craft_connector_on_disk, postgres_engine, committed_pipeline
):
    insert_committed_task(postgres_engine, committed_pipeline, "task_a")

    exit_code = cli_main(["run", "--pipeline_code", "TEST_CONCURRENT_PL"])

    # No --task_code: goes through orchestrator.run_pipeline, not the old
    # "not implemented" stub. task_a fails on the stub handler, so the
    # whole pipeline finalizes FAILED — proving this path is live now.
    assert exit_code == 1


def test_cli_run_init_only(craft_connector_on_disk, committed_pipeline, capsys):
    exit_code = cli_main(["run", "--pipeline_code", "TEST_CONCURRENT_PL", "--init-only"])

    assert exit_code == 0
    assert "pipeline_run_id=" in capsys.readouterr().out


def test_cli_run_init_only_and_task_code_are_mutually_exclusive(craft_connector_on_disk):
    with pytest.raises(SystemExit) as exc_info:
        cli_main(
            ["run", "--pipeline_code", "TEST_CONCURRENT_PL", "--init-only", "--task_code", "t1"]
        )
    assert exc_info.value.code == 2


def test_cli_run_bare_form_refused_under_orchestrator_mode(
    tmp_path, monkeypatch, committed_pipeline
):
    orchestrator_yaml = CRAFT_CONNECTOR_YAML.replace("Mode: local", "Mode: orchestrator")
    (tmp_path / "craft-connector.yml").write_text(orchestrator_yaml)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ETL_CRAFT_POSTGRES_DEV_SECRET", "etl_craft")

    exit_code = cli_main(["run", "--pipeline_code", "TEST_CONCURRENT_PL"])

    assert exit_code == 1


def test_cli_list_prints_active_pipelines(
    craft_connector_on_disk, postgres_engine, committed_pipeline, capsys
):
    # cli_main opens its own connection, so this needs genuinely committed
    # data (committed_pipeline), not the rolled-back pg_conn/cfg_pipeline.
    exit_code = cli_main(["list"])

    assert exit_code == 0
    out = capsys.readouterr().out
    assert "TEST_CONCURRENT_PL" in out
    assert "Concurrent Test Pipeline" in out
    assert "INCREMENTAL" in out


def test_cli_list_with_no_pipelines(craft_connector_on_disk, monkeypatch, capsys):
    # Monkeypatching the query result (rather than deactivating every real
    # pipeline in the shared Docker Postgres) keeps this test from touching
    # any other test's data — there's no safe way to make "zero active
    # pipelines" true for real without a blast radius across the whole DB.
    monkeypatch.setattr("etl_craft.cli.fetch_all_pipelines", lambda conn: [])

    exit_code = cli_main(["list"])

    assert exit_code == 0
    assert capsys.readouterr().out.strip() == "(no active pipelines)"


def test_cli_graph_requires_name(craft_connector_on_disk):
    with pytest.raises(SystemExit) as exc_info:
        cli_main(["graph"])
    assert exc_info.value.code == 2


def test_cli_graph_unknown_pipeline(craft_connector_on_disk, capsys):
    exit_code = cli_main(["graph", "--name", "NO_SUCH_PIPELINE"])
    assert exit_code == 1
    assert "error:" in capsys.readouterr().err


def test_cli_graph_with_no_active_tasks(craft_connector_on_disk, committed_pipeline, capsys):
    exit_code = cli_main(["graph", "--name", "TEST_CONCURRENT_PL"])

    assert exit_code == 0
    out = capsys.readouterr().out
    assert "(no active tasks)" in out
    assert "Pipeline dependencies:\n  (none)" in out
    assert "Cross-pipeline task dependencies:\n  (none)" in out


def test_cli_graph_prints_waves_and_dependencies(
    craft_connector_on_disk, postgres_engine, committed_pipeline, capsys
):
    task_a = insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    task_b = insert_committed_task(postgres_engine, committed_pipeline, "task_b")
    insert_committed_dependency(postgres_engine, committed_pipeline, task_b, task_a)

    with postgres_engine.begin() as conn:
        other_pipeline_id = conn.execute(
            text(
                "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
                "VALUES ('TEST_GRAPH_UPSTREAM2', 'Graph Upstream 2', 'FULL') RETURNING PIPELINE_ID"
            )
        ).scalar_one()
        other_task_id = conn.execute(
            text(
                "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
                "VALUES ('upstream_task', 'ETL', :pid, 'SQL') RETURNING TASK_ID"
            ),
            {"pid": other_pipeline_id},
        ).scalar_one()
        conn.execute(
            text(
                "INSERT INTO CFG_PIPELINE_DEPENDENCY "
                "(PIPELINE_ID, DEPENDS_ON_PIPELINE_ID, DEPENDENCY_TYPE) "
                "VALUES (:pid, :other_pid, 'HAS_DATA')"
            ),
            {"pid": committed_pipeline, "other_pid": other_pipeline_id},
        )
        conn.execute(
            text(
                "INSERT INTO CFG_TASK_DEPENDENCY (PIPELINE_ID, TASK_ID, DEPENDS_ON_PIPELINE_ID, "
                "DEPENDS_ON_TASK_ID, DEPENDENCY_TYPE) "
                "VALUES (:pid, :task_a, :other_pid, :other_task, 'SUCCESS')"
            ),
            {
                "pid": committed_pipeline,
                "task_a": task_a,
                "other_pid": other_pipeline_id,
                "other_task": other_task_id,
            },
        )

    try:
        exit_code = cli_main(["graph", "--name", "TEST_CONCURRENT_PL"])

        assert exit_code == 0
        out = capsys.readouterr().out
        assert "Wave 1: task_a" in out
        assert "Wave 2: task_b" in out
        assert "TEST_GRAPH_UPSTREAM2 (HAS_DATA)" in out
        assert "task_a -> TEST_GRAPH_UPSTREAM2.upstream_task (SUCCESS)" in out
    finally:
        with postgres_engine.begin() as conn:
            conn.execute(
                text("DELETE FROM CFG_TASK_DEPENDENCY WHERE PIPELINE_ID = :pid"),
                {"pid": committed_pipeline},
            )
            conn.execute(
                text("DELETE FROM CFG_PIPELINE_DEPENDENCY WHERE PIPELINE_ID = :pid"),
                {"pid": committed_pipeline},
            )
            conn.execute(
                text("DELETE FROM CFG_TASKS WHERE PIPELINE_ID = :pid"), {"pid": other_pipeline_id}
            )
            conn.execute(
                text("DELETE FROM CFG_PIPELINES WHERE PIPELINE_ID = :pid"),
                {"pid": other_pipeline_id},
            )
    assert "Pipeline dependencies:" in out
    assert "Cross-pipeline task dependencies:" in out


def test_cli_generate_yml_prints_to_stdout(
    craft_connector_on_disk, postgres_engine, committed_pipeline, capsys
):
    insert_committed_task(postgres_engine, committed_pipeline, "task_a")

    exit_code = cli_main(["generate-yml", "--pipeline_code", "TEST_CONCURRENT_PL"])

    assert exit_code == 0
    out = capsys.readouterr().out
    parsed = yaml.safe_load(out)
    assert parsed["dag_id"] == "TEST_CONCURRENT_PL"
    assert set(parsed["tasks"]) == {"__init__", "task_a"}


def test_cli_generate_yml_writes_to_output_file(
    craft_connector_on_disk, postgres_engine, committed_pipeline, tmp_path, capsys
):
    insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    output_path = tmp_path / "dag.yml"

    exit_code = cli_main(
        ["generate-yml", "--pipeline_code", "TEST_CONCURRENT_PL", "--output", str(output_path)]
    )

    assert exit_code == 0
    assert "DAG YAML written" in capsys.readouterr().out
    parsed = yaml.safe_load(output_path.read_text())
    assert parsed["dag_id"] == "TEST_CONCURRENT_PL"


def test_cli_generate_yml_unknown_pipeline(craft_connector_on_disk, capsys):
    exit_code = cli_main(["generate-yml", "--pipeline_code", "NO_SUCH_PIPELINE"])

    assert exit_code == 1
    assert "error:" in capsys.readouterr().err


def test_cli_generate_yml_rejects_cycle(
    craft_connector_on_disk, postgres_engine, committed_pipeline
):
    task_a = insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    task_b = insert_committed_task(postgres_engine, committed_pipeline, "task_b")
    insert_committed_dependency(postgres_engine, committed_pipeline, task_b, task_a)
    insert_committed_dependency(postgres_engine, committed_pipeline, task_a, task_b)

    exit_code = cli_main(["generate-yml", "--pipeline_code", "TEST_CONCURRENT_PL"])

    assert exit_code == 1
