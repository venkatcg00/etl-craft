"""Integration tests for etl_craft.runner against a real Postgres.

run_task opens its own connections internally (mirroring how it'll really
be invoked — once per `etl-craft run --task_code`), so every fixture here
uses genuinely committed data via tests/conftest.py's committed_pipeline
helpers, not the rolled-back pg_conn used elsewhere.
"""

import pytest
from sqlalchemy import text

from conftest import insert_committed_dependency, insert_committed_task, seed_active_run
from etl_craft.config import (
    CloningConfig,
    ConnectionProfile,
    ConnectionSection,
    ConnectorConfig,
    SourceConfig,
)
from etl_craft.runner import (
    DependenciesNotMetError,
    ForceNotAllowedError,
    run_task,
)


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
            text("UPDATE AUD_TASK_RUN_LOG SET STATUS = 'SUCCESS' " "WHERE TASK_ID = :task_id"),
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
