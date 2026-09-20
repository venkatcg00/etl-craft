"""The opt-in, real-Postgres integration suite — needs Docker (see Makefile).

Skips itself with a clear message (see tests/conftest.py's postgres_engine
fixture) if nothing is reachable, so plain `pytest -q` never requires it.
Organized by source module, one section per module, since combining them
loses nothing (no fixture/helper name collisions) and keeps file count down.
"""

import json
import os
import threading
import time
from datetime import UTC, datetime, timedelta

import pytest
import yaml
from sqlalchemy import create_engine, inspect, text

from conftest import (
    CRAFT_CONNECTOR_YAML,
    insert_committed_business_rule,
    insert_committed_cross_pipeline_task_dependency,
    insert_committed_dependency,
    insert_committed_pipeline_dependency,
    insert_committed_pipeline_run,
    insert_committed_task,
    insert_committed_task_parameters,
    insert_committed_task_run,
    seed_active_run,
)
from etl_craft.cfg import (
    CfgError,
    fetch_all_pipeline_dependency_edges,
    fetch_all_pipelines,
    fetch_business_rule_targets,
    fetch_cross_pipeline_task_edges,
    fetch_failure_watch_messages,
    fetch_pipeline_dependencies,
    fetch_pipeline_dependency_edge_ids,
    fetch_pipeline_detail,
    fetch_pipeline_graph,
    fetch_pipeline_run_history,
    fetch_pipeline_steps,
    fetch_task_cross_pipeline_dependency_ids,
    fetch_task_handler,
    fetch_task_run_history,
    resolve_pipeline_id,
    resolve_task_id,
)
from etl_craft.cli import main as cli_main
from etl_craft.cloning import run_cloning_if_enabled
from etl_craft.config import (
    CloningConfig,
    ConnectionProfile,
    ConnectionSection,
    ConnectorConfig,
    EmailConfig,
    EmailProfile,
    OrchestratorConfig,
    SourceConfig,
)
from etl_craft.crosspipe import (
    _wait_for_pipeline_dependency_to_settle,
    _wait_for_task_dependency_to_settle,
    check_pipeline_dependencies,
    check_task_cross_pipeline_dependencies,
    consume_pipeline_dependency_edges,
    consume_task_dependency_edges,
)
from etl_craft.docs_generator import collect_docs, generate_docs
from etl_craft.execution import HandlerResult
from etl_craft.generate_yml import GLOBAL_DAG_ID, generate_global_dag, generate_pipeline_dag
from etl_craft.migrate import MigrationError, apply_pending_migrations
from etl_craft.orchestrator import (
    OrchestratorModeRefusedError,
    finalize_active_run,
    init_pipeline_run,
    run_pipeline,
)
from etl_craft.resolver import ResolverError
from etl_craft.runlog import (
    RunLogError,
    find_or_create_active_run,
    find_or_create_task_run,
    resolve_run_for_task,
    update_task_run,
)
from etl_craft.runner import ForceNotAllowedError, run_task
from etl_craft.validate import validate_business_rule_keys, validate_graphs
from etl_craft.warehouse import build_data_engine

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
# warehouse.py — against real Postgres, standing in for "any dialect"
# ==============================================================================
#
# There's no second real warehouse engine available in this Docker setup to
# prove connectivity against a genuinely different dialect — but the whole
# point of build_data_engine's design is that it never hardcodes a driver;
# it asks SQLAlchemy's own resolved dialect for connect args at pool-
# checkout time (see warehouse.py's module docstring). Pointing it at this
# same Postgres container with dialect "postgresql+psycopg" still proves
# that exact generic mechanism executes for real, end to end — a mocked
# DBAPI (as in test_unit.py) can't prove that part.


def test_build_data_engine_connects_for_real(monkeypatch, postgres_engine):
    # postgres_engine is otherwise unused here — build_data_engine opens its
    # own connection independently of it — but depending on it is what runs
    # the skip-if-unreachable check before this test tries to connect for
    # real. Same class of gap as craft_connector_on_disk's own fix above.
    monkeypatch.setenv("ETL_CRAFT_WAREHOUSE_DEV_SECRET", "etl_craft")
    profile = ConnectionProfile(
        section="WAREHOUSE",
        name="dev",
        jdbc_url="jdbc:postgresql://localhost:55432/etl_craft",
        user="etl_craft",
        auth_mode="password",
    )
    config = ConnectorConfig(
        mode="local",
        source=SourceConfig(type="environment"),
        postgres=ConnectionSection(
            active_profile="dev",
            profiles={
                "dev": ConnectionProfile(
                    section="POSTGRES",
                    name="dev",
                    jdbc_url="jdbc:postgresql://localhost:55432/etl_craft",
                    user="etl_craft",
                    auth_mode="password",
                )
            },
        ),
        cloning=CloningConfig(),
        warehouse=ConnectionSection(active_profile="dev", profiles={"dev": profile}),
    )

    engine = build_data_engine(config)
    try:
        with engine.connect() as conn:
            assert conn.execute(text("SELECT 1")).scalar_one() == 1
    finally:
        engine.dispose()


def test_build_data_engine_connects_to_real_clickhouse(monkeypatch, clickhouse_engine):
    # Unlike the Postgres-standing-in-for-"some dialect" test above, this
    # genuinely proves the generic, dialect-agnostic connect mechanism
    # against a real *different* SQLAlchemy dialect — the actual point of
    # warehouse.py never hardcoding a driver. Skips itself (via
    # clickhouse_engine) if ClickHouse or the `clickhouse` extra isn't
    # available, same as the Postgres suite skips when Docker is down.
    monkeypatch.setenv("ETL_CRAFT_WAREHOUSE_DEV_SECRET", "etl_craft")
    profile = ConnectionProfile(
        section="WAREHOUSE",
        name="dev",
        jdbc_url="jdbc:clickhouse://localhost:58123/etl_craft",
        user="etl_craft",
        auth_mode="password",
    )
    config = ConnectorConfig(
        mode="local",
        source=SourceConfig(type="environment"),
        postgres=ConnectionSection(
            active_profile="dev",
            profiles={
                "dev": ConnectionProfile(
                    section="POSTGRES",
                    name="dev",
                    jdbc_url="jdbc:postgresql://localhost:55432/etl_craft",
                    user="etl_craft",
                    auth_mode="password",
                )
            },
        ),
        cloning=CloningConfig(),
        warehouse=ConnectionSection(active_profile="dev", profiles={"dev": profile}),
    )

    engine = build_data_engine(config)
    try:
        with engine.connect() as conn:
            assert conn.execute(text("SELECT 1")).scalar_one() == 1
    finally:
        engine.dispose()


# ==============================================================================
# cloning.py — against real Postgres (Engine DB always) and, for the actual
# copy mechanism, real ClickHouse as the Data DB -- proving the generic
# mirroring mechanism against a genuinely different dialect, the same "prove
# it for real" bar warehouse.py's own ClickHouse test already set. Testing
# against Postgres-as-both-roles is deliberately *not* done for the real
# copy path: since every mirrored table keeps its Engine DB name, doing so
# would mean truncating and reinserting the actual CFG_/AUD_ tables from
# their own reflection -- exactly the destructive scenario
# cloning.run_cloning_if_enabled's own same-database guard exists to refuse.
# ==============================================================================


def _clickhouse_warehouse_config(monkeypatch, *, cloning: CloningConfig) -> ConnectorConfig:
    monkeypatch.setenv("ETL_CRAFT_WAREHOUSE_DEV_SECRET", "etl_craft")
    return ConnectorConfig(
        mode="local",
        source=SourceConfig(type="environment"),
        postgres=ConnectionSection(
            active_profile="dev",
            profiles={
                "dev": ConnectionProfile(
                    section="POSTGRES",
                    name="dev",
                    jdbc_url="jdbc:postgresql://localhost:55432/etl_craft",
                    user="etl_craft",
                    auth_mode="password",
                )
            },
        ),
        cloning=cloning,
        warehouse=ConnectionSection(
            active_profile="dev",
            profiles={
                "dev": ConnectionProfile(
                    section="WAREHOUSE",
                    name="dev",
                    jdbc_url="jdbc:clickhouse://localhost:58123/etl_craft",
                    user="etl_craft",
                    auth_mode="password",
                )
            },
        ),
    )


@pytest.fixture
def clickhouse_cfg_tables_cleanup(clickhouse_engine):
    """Drop every table cloning.py's 'cfg' scope could have mirrored into ClickHouse."""
    yield
    with clickhouse_engine.begin() as conn:
        for table_name in (
            "cfg_pipelines",
            "cfg_pipeline_dependency",
            "cfg_tasks",
            "cfg_task_dependency",
            "cfg_task_parameters",
            "cfg_business_rules",
            "aud_pipelines_run_log",
            "aud_task_run_log",
        ):
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))


def test_run_cloning_if_enabled_noop_when_disabled(postgres_engine):
    config = ConnectorConfig(
        mode="local",
        source=SourceConfig(type="environment"),
        postgres=ConnectionSection(
            active_profile="dev",
            profiles={
                "dev": ConnectionProfile(
                    section="POSTGRES",
                    name="dev",
                    jdbc_url="jdbc:postgresql://localhost:55432/etl_craft",
                    user="etl_craft",
                    auth_mode="password",
                )
            },
        ),
        cloning=CloningConfig(enabled=False),
    )
    run_cloning_if_enabled(postgres_engine, config)  # must not raise


def test_run_cloning_if_enabled_raises_when_no_warehouse_configured(postgres_engine):
    config = ConnectorConfig(
        mode="local",
        source=SourceConfig(type="environment"),
        postgres=ConnectionSection(
            active_profile="dev",
            profiles={
                "dev": ConnectionProfile(
                    section="POSTGRES",
                    name="dev",
                    jdbc_url="jdbc:postgresql://localhost:55432/etl_craft",
                    user="etl_craft",
                    auth_mode="password",
                )
            },
        ),
        cloning=CloningConfig(enabled=True, scope="cfg"),
    )
    with pytest.raises(ValueError, match="no \\[Warehouse\\]"):
        run_cloning_if_enabled(postgres_engine, config)


def test_run_cloning_refuses_when_warehouse_is_the_same_database_as_engine(postgres_engine):
    # A real, plausible misconfiguration this guard exists specifically to
    # catch -- see cloning._same_database's own docstring for why this
    # would otherwise be destructive, not merely redundant.
    config = make_config(cloning=CloningConfig(enabled=True, scope="cfg"), warehouse=True)
    with pytest.raises(ValueError, match="same database"):
        run_cloning_if_enabled(postgres_engine, config)


def test_run_cloning_clones_cfg_tables_into_real_clickhouse(
    monkeypatch,
    postgres_engine,
    clickhouse_engine,
    committed_pipeline,
    clickhouse_cfg_tables_cleanup,
):
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "t")
    insert_committed_task_parameters(
        postgres_engine, task_id, {"SQL_ACTION": "CREATE_TABLE", "TARGET_OBJECT": "public.x"}
    )
    config = _clickhouse_warehouse_config(
        monkeypatch, cloning=CloningConfig(enabled=True, scope="cfg")
    )

    run_cloning_if_enabled(postgres_engine, config)

    with clickhouse_engine.connect() as conn:
        pipelines = list(
            conn.execute(
                text("SELECT pipeline_code FROM cfg_pipelines WHERE pipeline_id = :id"),
                {"id": committed_pipeline},
            )
        )
        tasks = list(
            conn.execute(
                text("SELECT task_code, handler FROM cfg_tasks WHERE task_id = :id"),
                {"id": task_id},
            )
        )
    assert pipelines == [("TEST_CONCURRENT_PL",)]
    assert tasks == [("t", "SQL")]


def test_run_cloning_serializes_jsonb_column_to_text(
    monkeypatch,
    postgres_engine,
    clickhouse_engine,
    committed_pipeline,
    clickhouse_cfg_tables_cleanup,
):
    with postgres_engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_PIPELINES SET PIPELINE_PARAMETERS = :params WHERE PIPELINE_ID = :id"),
            {"params": json.dumps({"CATCHUP": True}), "id": committed_pipeline},
        )
    config = _clickhouse_warehouse_config(
        monkeypatch, cloning=CloningConfig(enabled=True, scope="cfg")
    )

    run_cloning_if_enabled(postgres_engine, config)

    with clickhouse_engine.connect() as conn:
        params = conn.execute(
            text("SELECT pipeline_parameters FROM cfg_pipelines WHERE pipeline_id = :id"),
            {"id": committed_pipeline},
        ).scalar_one()
    assert json.loads(params) == {"CATCHUP": True}


def test_run_cloning_is_idempotent_across_repeated_runs(
    monkeypatch,
    postgres_engine,
    clickhouse_engine,
    committed_pipeline,
    clickhouse_cfg_tables_cleanup,
):
    config = _clickhouse_warehouse_config(
        monkeypatch, cloning=CloningConfig(enabled=True, scope="cfg")
    )

    run_cloning_if_enabled(postgres_engine, config)
    run_cloning_if_enabled(postgres_engine, config)

    with clickhouse_engine.connect() as conn:
        count = conn.execute(
            text("SELECT count(*) FROM cfg_pipelines WHERE pipeline_id = :id"),
            {"id": committed_pipeline},
        ).scalar_one()
    assert count == 1


@pytest.fixture
def second_postgres_database(postgres_engine):
    """A genuinely separate, schema-less Postgres database on the same server as the Engine DB.

    For proving cloning's generic (non-ClickHouse) create-target-table path
    for real: pointing [Warehouse] at *this* same Postgres server's default
    "etl_craft" database would hit run_cloning_if_enabled's own same-
    database refusal (correctly), so this fixture creates a second,
    disposable one instead -- a genuinely different database, the same
    Postgres dialect, no schema.sql applied to it.
    """
    db_name = "etl_craft_clone_target"
    with postgres_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f"DROP DATABASE IF EXISTS {db_name}"))
        conn.execute(text(f"CREATE DATABASE {db_name}"))
    yield db_name
    with postgres_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f"DROP DATABASE IF EXISTS {db_name}"))


def test_run_cloning_creates_generic_target_table_on_a_different_postgres_database(
    postgres_engine, committed_pipeline, second_postgres_database
):
    config = ConnectorConfig(
        mode="local",
        source=SourceConfig(type="environment"),
        postgres=ConnectionSection(
            active_profile="dev",
            profiles={
                "dev": ConnectionProfile(
                    section="POSTGRES",
                    name="dev",
                    jdbc_url="jdbc:postgresql://localhost:55432/etl_craft",
                    user="etl_craft",
                    auth_mode="password",
                )
            },
        ),
        cloning=CloningConfig(enabled=True, scope="cfg"),
        warehouse=ConnectionSection(
            active_profile="dev",
            profiles={
                "dev": ConnectionProfile(
                    section="WAREHOUSE",
                    name="dev",
                    jdbc_url=f"jdbc:postgresql://localhost:55432/{second_postgres_database}",
                    user="etl_craft",
                    auth_mode="password",
                )
            },
        ),
    )

    run_cloning_if_enabled(postgres_engine, config)

    target_engine = create_engine(
        f"postgresql+psycopg://etl_craft:etl_craft@localhost:55432/{second_postgres_database}"
    )
    try:
        with target_engine.connect() as conn:
            pipeline_code = conn.execute(
                text("SELECT pipeline_code FROM cfg_pipelines WHERE pipeline_id = :id"),
                {"id": committed_pipeline},
            ).scalar_one()
        assert pipeline_code == "TEST_CONCURRENT_PL"
    finally:
        target_engine.dispose()


def test_run_cloning_aud_scope_clones_only_aud_tables(
    monkeypatch,
    postgres_engine,
    clickhouse_engine,
    committed_pipeline,
    clickhouse_cfg_tables_cleanup,
):
    seed_active_run(postgres_engine, committed_pipeline)
    config = _clickhouse_warehouse_config(
        monkeypatch, cloning=CloningConfig(enabled=True, scope="aud")
    )

    run_cloning_if_enabled(postgres_engine, config)

    with clickhouse_engine.connect() as conn:
        assert not inspect(clickhouse_engine).has_table("cfg_pipelines")
        count = conn.execute(
            text("SELECT count(*) FROM aud_pipelines_run_log WHERE pipeline_id = :id"),
            {"id": committed_pipeline},
        ).scalar_one()
    assert count == 1


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


def test_fetch_all_pipeline_dependency_edges(pg_conn, cfg_pipeline):
    other_pipeline = pg_conn.execute(
        text(
            "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
            "VALUES ('TEST_GLOBAL_UPSTREAM', 'Upstream', 'FULL') RETURNING PIPELINE_ID"
        )
    ).scalar_one()
    pg_conn.execute(
        text(
            "INSERT INTO CFG_PIPELINE_DEPENDENCY "
            "(PIPELINE_ID, DEPENDS_ON_PIPELINE_ID, DEPENDENCY_TYPE) "
            "VALUES (:pipeline_id, :other_pipeline, 'SUCCESS')"
        ),
        {"pipeline_id": cfg_pipeline, "other_pipeline": other_pipeline},
    )

    edges = fetch_all_pipeline_dependency_edges(pg_conn)

    assert len(edges) == 1
    assert edges[0].pipeline_code == "TEST_PL"
    assert edges[0].depends_on_pipeline_code == "TEST_GLOBAL_UPSTREAM"
    assert edges[0].dependency_type == "SUCCESS"


def test_fetch_all_pipeline_dependency_edges_excludes_inactive(pg_conn, cfg_pipeline):
    other_pipeline = pg_conn.execute(
        text(
            "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
            "VALUES ('TEST_GLOBAL_UPSTREAM2', 'Upstream', 'FULL') RETURNING PIPELINE_ID"
        )
    ).scalar_one()
    pg_conn.execute(
        text(
            "INSERT INTO CFG_PIPELINE_DEPENDENCY "
            "(PIPELINE_ID, DEPENDS_ON_PIPELINE_ID, DEPENDENCY_TYPE, ACTIVE_FLAG) "
            "VALUES (:pipeline_id, :other_pipeline, 'SUCCESS', 'N')"
        ),
        {"pipeline_id": cfg_pipeline, "other_pipeline": other_pipeline},
    )

    assert fetch_all_pipeline_dependency_edges(pg_conn) == []


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
    # Auto-stamped by trg_set_audit_columns to current_user on insert, never
    # actually NULL in practice — the test role connecting here.
    assert detail.created_by == "etl_craft"


def test_fetch_pipeline_detail_nullable_fields_default_none(pg_conn, cfg_pipeline):
    detail = fetch_pipeline_detail(pg_conn, cfg_pipeline)
    assert detail.description is None
    assert detail.run_schedule is None
    assert detail.sla_in_hours is None
    assert detail.created_by == "etl_craft"


def test_fetch_business_rule_targets(pg_conn, cfg_pipeline, cfg_task):
    pg_conn.execute(
        text(
            "INSERT INTO CFG_BUSINESS_RULES (BUSINESS_RULE_NAME, PIPELINE_ID, TASK_ID, "
            "BUSINESS_RULE_SQL, BUSINESS_RULE_TYPE, BUSINESS_RULE_KEY_COLUMN, TARGET_TABLE, "
            "SEQUENCE_NUMBER) VALUES ('br1', :pipeline_id, :task_id, 'SELECT 1', 'REJECT', "
            "'id', 'public.my_table', 1)"
        ),
        {"pipeline_id": cfg_pipeline, "task_id": cfg_task},
    )

    targets = fetch_business_rule_targets(pg_conn)

    assert len(targets) == 1
    assert targets[0].business_rule_name == "br1"
    assert targets[0].target_table == "public.my_table"
    assert targets[0].key_column == "id"


def test_fetch_business_rule_targets_excludes_inactive(pg_conn, cfg_pipeline, cfg_task):
    pg_conn.execute(
        text(
            "INSERT INTO CFG_BUSINESS_RULES (BUSINESS_RULE_NAME, PIPELINE_ID, TASK_ID, "
            "BUSINESS_RULE_SQL, BUSINESS_RULE_TYPE, BUSINESS_RULE_KEY_COLUMN, TARGET_TABLE, "
            "SEQUENCE_NUMBER, ACTIVE_FLAG) VALUES ('br1', :pipeline_id, :task_id, 'SELECT 1', "
            "'REJECT', 'id', 'public.my_table', 1, 'N')"
        ),
        {"pipeline_id": cfg_pipeline, "task_id": cfg_task},
    )

    assert fetch_business_rule_targets(pg_conn) == []


def test_fetch_pipeline_dependency_edge_ids(pg_conn, cfg_pipeline):
    other_id = pg_conn.execute(
        text(
            "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
            "VALUES ('TEST_XPIPE_UP', 'Upstream', 'INCREMENTAL') RETURNING PIPELINE_ID"
        )
    ).scalar_one()
    edge_id = pg_conn.execute(
        text(
            "INSERT INTO CFG_PIPELINE_DEPENDENCY (PIPELINE_ID, DEPENDS_ON_PIPELINE_ID, "
            "DEPENDENCY_TYPE) VALUES (:pid, :other, 'SUCCESS') RETURNING PIPELINE_DEPENDENCY_ID"
        ),
        {"pid": cfg_pipeline, "other": other_id},
    ).scalar_one()

    edges = fetch_pipeline_dependency_edge_ids(pg_conn, cfg_pipeline)

    assert len(edges) == 1
    assert edges[0].pipeline_dependency_id == edge_id
    assert edges[0].depends_on_pipeline_id == other_id
    assert edges[0].dependency_type == "SUCCESS"


def test_fetch_task_cross_pipeline_dependency_ids(pg_conn, cfg_pipeline, cfg_task):
    other_pipeline_id = pg_conn.execute(
        text(
            "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
            "VALUES ('TEST_XTASK_UP', 'Upstream', 'INCREMENTAL') RETURNING PIPELINE_ID"
        )
    ).scalar_one()
    other_task_id = pg_conn.execute(
        text(
            "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
            "VALUES ('upstream_task', 'ETL', :pid, 'SQL') RETURNING TASK_ID"
        ),
        {"pid": other_pipeline_id},
    ).scalar_one()
    edge_id = pg_conn.execute(
        text(
            "INSERT INTO CFG_TASK_DEPENDENCY (PIPELINE_ID, TASK_ID, DEPENDS_ON_PIPELINE_ID, "
            "DEPENDS_ON_TASK_ID, DEPENDENCY_TYPE) VALUES (:pid, :task, :other_pid, :other_task, "
            "'FAILURE') RETURNING TASK_DEPENDENCY_ID"
        ),
        {
            "pid": cfg_pipeline,
            "task": cfg_task,
            "other_pid": other_pipeline_id,
            "other_task": other_task_id,
        },
    ).scalar_one()

    edges = fetch_task_cross_pipeline_dependency_ids(pg_conn, cfg_task)

    assert len(edges) == 1
    assert edges[0].task_dependency_id == edge_id
    assert edges[0].pipeline_id == cfg_pipeline
    assert edges[0].depends_on_pipeline_id == other_pipeline_id
    assert edges[0].depends_on_task_id == other_task_id
    assert edges[0].dependency_type == "FAILURE"


def test_fetch_task_cross_pipeline_dependency_ids_excludes_same_pipeline_edges(
    pg_conn, cfg_pipeline, cfg_task
):
    other_task_id = pg_conn.execute(
        text(
            "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
            "VALUES ('task_b', 'ETL', :pid, 'SQL') RETURNING TASK_ID"
        ),
        {"pid": cfg_pipeline},
    ).scalar_one()
    pg_conn.execute(
        text(
            "INSERT INTO CFG_TASK_DEPENDENCY (PIPELINE_ID, TASK_ID, DEPENDS_ON_PIPELINE_ID, "
            "DEPENDS_ON_TASK_ID, DEPENDENCY_TYPE) "
            "VALUES (:pid, :task, :pid, :other_task, 'SUCCESS')"
        ),
        {"pid": cfg_pipeline, "task": cfg_task, "other_task": other_task_id},
    )

    assert fetch_task_cross_pipeline_dependency_ids(pg_conn, cfg_task) == []


# ==============================================================================
# validate.py — against real Postgres
# ==============================================================================


def test_validate_graphs_empty_when_no_pipelines(pg_conn):
    assert validate_graphs(pg_conn) == []


def test_validate_graphs_ok_for_well_formed_pipeline(pg_conn, cfg_pipeline, cfg_task):
    assert validate_graphs(pg_conn) == []


def test_validate_graphs_reports_cycle(pg_conn, cfg_pipeline):
    task_a = pg_conn.execute(
        text(
            "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
            "VALUES ('task_a', 'ETL', :pipeline_id, 'SQL') RETURNING TASK_ID"
        ),
        {"pipeline_id": cfg_pipeline},
    ).scalar_one()
    task_b = pg_conn.execute(
        text(
            "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
            "VALUES ('task_b', 'ETL', :pipeline_id, 'SQL') RETURNING TASK_ID"
        ),
        {"pipeline_id": cfg_pipeline},
    ).scalar_one()
    for a, b in [(task_a, task_b), (task_b, task_a)]:
        pg_conn.execute(
            text(
                "INSERT INTO CFG_TASK_DEPENDENCY (PIPELINE_ID, TASK_ID, DEPENDS_ON_PIPELINE_ID, "
                "DEPENDS_ON_TASK_ID, DEPENDENCY_TYPE) "
                "VALUES (:pid, :a, :pid, :b, 'SUCCESS')"
            ),
            {"pid": cfg_pipeline, "a": a, "b": b},
        )

    issues = validate_graphs(pg_conn)

    assert len(issues) == 1
    assert issues[0].category == "graph"
    assert "TEST_PL" in issues[0].message


def test_validate_business_rule_keys_empty_when_no_active_rules(pg_conn):
    assert validate_business_rule_keys(pg_conn, None) == []


def test_validate_business_rule_keys_reports_missing_warehouse(pg_conn, cfg_pipeline, cfg_task):
    pg_conn.execute(
        text(
            "INSERT INTO CFG_BUSINESS_RULES (BUSINESS_RULE_NAME, PIPELINE_ID, TASK_ID, "
            "BUSINESS_RULE_SQL, BUSINESS_RULE_TYPE, BUSINESS_RULE_KEY_COLUMN, TARGET_TABLE, "
            "SEQUENCE_NUMBER) VALUES ('br1', :pipeline_id, :task_id, 'SELECT 1', 'REJECT', "
            "'id', 'public.my_table', 1)"
        ),
        {"pipeline_id": cfg_pipeline, "task_id": cfg_task},
    )

    issues = validate_business_rule_keys(pg_conn, None)

    assert len(issues) == 1
    assert "Warehouse" in issues[0].message


def _insert_business_rule(pg_conn, pipeline_id, task_id, name, target_table, key_column):
    pg_conn.execute(
        text(
            "INSERT INTO CFG_BUSINESS_RULES (BUSINESS_RULE_NAME, PIPELINE_ID, TASK_ID, "
            "BUSINESS_RULE_SQL, BUSINESS_RULE_TYPE, BUSINESS_RULE_KEY_COLUMN, TARGET_TABLE, "
            "SEQUENCE_NUMBER) VALUES (:name, :pipeline_id, :task_id, 'SELECT 1', 'REJECT', "
            ":key_column, :target_table, 1)"
        ),
        {
            "name": name,
            "pipeline_id": pipeline_id,
            "task_id": task_id,
            "key_column": key_column,
            "target_table": target_table,
        },
    )


def test_validate_business_rule_keys_table_does_not_exist(
    pg_conn, cfg_pipeline, cfg_task, postgres_engine
):
    _insert_business_rule(pg_conn, cfg_pipeline, cfg_task, "br1", "does_not_exist_xyz", "id")

    issues = validate_business_rule_keys(pg_conn, postgres_engine)

    assert len(issues) == 1
    assert "does not exist" in issues[0].message


def test_validate_business_rule_keys_matching_single_column_pk_is_ok(
    pg_conn, cfg_pipeline, cfg_task, postgres_engine
):
    with postgres_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS validate_pk_test_good"))
        conn.execute(text("CREATE TABLE validate_pk_test_good (id INT PRIMARY KEY, val INT)"))
    try:
        # Uppercase key_column ("ID") against Postgres's real (lowercase,
        # unquoted-identifier-folded) "id" — proves the comparison is
        # case-insensitive, not just a lucky exact-string match.
        _insert_business_rule(
            pg_conn, cfg_pipeline, cfg_task, "br1", "public.validate_pk_test_good", "ID"
        )

        issues = validate_business_rule_keys(pg_conn, postgres_engine)

        assert issues == []
    finally:
        with postgres_engine.begin() as conn:
            conn.execute(text("DROP TABLE validate_pk_test_good"))


def test_validate_business_rule_keys_no_pk_reported(
    pg_conn, cfg_pipeline, cfg_task, postgres_engine
):
    with postgres_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS validate_pk_test_nopk"))
        conn.execute(text("CREATE TABLE validate_pk_test_nopk (id INT, val INT)"))
    try:
        _insert_business_rule(pg_conn, cfg_pipeline, cfg_task, "br1", "validate_pk_test_nopk", "id")

        issues = validate_business_rule_keys(pg_conn, postgres_engine)

        assert len(issues) == 1
        assert "exactly one primary key column" in issues[0].message
    finally:
        with postgres_engine.begin() as conn:
            conn.execute(text("DROP TABLE validate_pk_test_nopk"))


def test_validate_business_rule_keys_composite_pk_reported(
    pg_conn, cfg_pipeline, cfg_task, postgres_engine
):
    with postgres_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS validate_pk_test_composite"))
        conn.execute(
            text("CREATE TABLE validate_pk_test_composite (a INT, b INT, PRIMARY KEY (a, b))")
        )
    try:
        _insert_business_rule(
            pg_conn, cfg_pipeline, cfg_task, "br1", "validate_pk_test_composite", "a"
        )

        issues = validate_business_rule_keys(pg_conn, postgres_engine)

        assert len(issues) == 1
        assert "exactly one primary key column" in issues[0].message
    finally:
        with postgres_engine.begin() as conn:
            conn.execute(text("DROP TABLE validate_pk_test_composite"))


def test_validate_business_rule_keys_mismatched_column_reported(
    pg_conn, cfg_pipeline, cfg_task, postgres_engine
):
    with postgres_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS validate_pk_test_mismatch"))
        conn.execute(text("CREATE TABLE validate_pk_test_mismatch (id INT PRIMARY KEY, val INT)"))
    try:
        _insert_business_rule(
            pg_conn, cfg_pipeline, cfg_task, "br1", "validate_pk_test_mismatch", "val"
        )

        issues = validate_business_rule_keys(pg_conn, postgres_engine)

        assert len(issues) == 1
        assert "does not match" in issues[0].message
    finally:
        with postgres_engine.begin() as conn:
            conn.execute(text("DROP TABLE validate_pk_test_mismatch"))


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

    dag = generate_pipeline_dag(pg_conn, make_config(), "TEST_PL")

    assert dag["dag_id"] == "TEST_PL"
    assert dag["refresh_type"] == "INCREMENTAL"
    assert dag["catchup"] is False
    assert dag["tags"] == ["incremental"]
    assert dag["default_args"] == {
        "owner": "etl_craft",
        "retries": 1,
        "retry_delay_minutes": 5,
        "depends_on_past": False,
        "email_on_failure": False,
    }
    assert set(dag["tasks"]) == {"__init__", "test_task", "task_b", "__finalize__"}
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
    # __finalize__ depends only on the leaf (task_b) — test_task has
    # something downstream of it, so it isn't a leaf.
    assert dag["tasks"]["__finalize__"]["depends_on"] == [
        {"task": "task_b", "dependency_type": "ALWAYS"}
    ]
    assert (
        dag["tasks"]["__finalize__"]["bash_command"]
        == "etl-craft run --pipeline_code TEST_PL --finalize-only"
    )
    assert "pipeline_dependencies" not in dag
    assert "cross_pipeline_task_dependencies" not in dag


def test_generate_pipeline_dag_finalize_depends_on_every_leaf_in_a_diamond(
    pg_conn, cfg_pipeline, cfg_task
):
    # cfg_task -> {branch_a, branch_b} -> nothing further: both branches are
    # leaves, so __finalize__ must wait on both, not just one.
    branch_a = pg_conn.execute(
        text(
            "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
            "VALUES ('branch_a', 'ETL', :pipeline_id, 'SQL') RETURNING TASK_ID"
        ),
        {"pipeline_id": cfg_pipeline},
    ).scalar_one()
    branch_b = pg_conn.execute(
        text(
            "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
            "VALUES ('branch_b', 'ETL', :pipeline_id, 'SQL') RETURNING TASK_ID"
        ),
        {"pipeline_id": cfg_pipeline},
    ).scalar_one()
    for branch in (branch_a, branch_b):
        pg_conn.execute(
            text(
                "INSERT INTO CFG_TASK_DEPENDENCY (PIPELINE_ID, TASK_ID, DEPENDS_ON_PIPELINE_ID, "
                "DEPENDS_ON_TASK_ID, DEPENDENCY_TYPE) "
                "VALUES (:pipeline_id, :branch, :pipeline_id, :cfg_task, 'SUCCESS')"
            ),
            {"pipeline_id": cfg_pipeline, "branch": branch, "cfg_task": cfg_task},
        )

    dag = generate_pipeline_dag(pg_conn, make_config(), "TEST_PL")

    assert dag["tasks"]["__finalize__"]["depends_on"] == [
        {"task": "branch_a", "dependency_type": "ALWAYS"},
        {"task": "branch_b", "dependency_type": "ALWAYS"},
    ]


def test_generate_pipeline_dag_with_no_tasks_still_has_init(pg_conn, cfg_pipeline):
    dag = generate_pipeline_dag(pg_conn, make_config(), "TEST_PL")
    assert set(dag["tasks"]) == {"__init__", "__finalize__"}
    # No real tasks at all -> __finalize__ falls back to depending on
    # __init__ directly, same as any real task with no dependencies would.
    assert dag["tasks"]["__finalize__"]["depends_on"] == [
        {"task": "__init__", "dependency_type": "ALWAYS"}
    ]


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
        generate_pipeline_dag(pg_conn, make_config(), "TEST_PL")


def test_generate_pipeline_dag_unknown_pipeline_raises(pg_conn):
    with pytest.raises(CfgError):
        generate_pipeline_dag(pg_conn, make_config(), "NO_SUCH_PIPELINE")


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

    dag = generate_pipeline_dag(pg_conn, make_config(), "TEST_PL")

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

    dag = generate_pipeline_dag(pg_conn, make_config(), "TEST_PL")

    assert dag["cross_pipeline_task_dependencies"] == [
        {
            "task": "test_task",
            "depends_on_pipeline": "TEST_GENYML_UPSTREAM2",
            "depends_on_task": "upstream_task",
            "dependency_type": "SUCCESS",
        }
    ]


def _config_with_orchestrator(**overrides) -> ConnectorConfig:
    base = make_config()
    orchestrator = OrchestratorConfig(**overrides)
    return ConnectorConfig(
        mode=base.mode,
        source=base.source,
        postgres=base.postgres,
        cloning=base.cloning,
        warehouse=base.warehouse,
        orchestrator=orchestrator,
    )


def test_generate_pipeline_dag_pipeline_level_override_wins(pg_conn, cfg_pipeline, cfg_task):
    # Tier 1 (CFG_PIPELINES column) beats both tier 2 (global config) and
    # tier 3 (hardcoded default), even when a global default is also set.
    pg_conn.execute(
        text(
            "UPDATE CFG_PIPELINES SET PIPELINE_PARAMETERS = "
            '\'{"CATCHUP": true, "TAGS": ["from-pipeline"], "RETRIES": 7}\'::jsonb '
            "WHERE PIPELINE_ID = :id"
        ),
        {"id": cfg_pipeline},
    )
    config = _config_with_orchestrator(catchup=False, tags=["from-global"], retries=2)

    dag = generate_pipeline_dag(pg_conn, config, "TEST_PL")

    assert dag["catchup"] is True
    assert dag["tags"] == ["from-pipeline"]
    assert dag["default_args"]["retries"] == 7


def test_generate_pipeline_dag_falls_back_to_global_orchestrator_config(
    pg_conn, cfg_pipeline, cfg_task
):
    # Tier 2: nothing set at the pipeline level, so the [Orchestrator]
    # global default is used instead of the final hardcoded default.
    config = _config_with_orchestrator(
        catchup=True,
        tags=["from-global"],
        retries=9,
        retry_delay_minutes=20,
        depends_on_past=True,
        email_on_failure=True,
        email_recipients=["oncall@example.com"],
    )

    dag = generate_pipeline_dag(pg_conn, config, "TEST_PL")

    assert dag["catchup"] is True
    assert dag["tags"] == ["from-global"]
    assert dag["default_args"] == {
        "owner": "etl_craft",
        "retries": 9,
        "retry_delay_minutes": 20,
        "depends_on_past": True,
        "email_on_failure": True,
        "email": ["oncall@example.com"],
    }


def test_generate_pipeline_dag_email_key_absent_when_email_on_failure_false(
    pg_conn, cfg_pipeline, cfg_task
):
    dag = generate_pipeline_dag(pg_conn, make_config(), "TEST_PL")
    assert "email" not in dag["default_args"]


def test_generate_global_dag_includes_pipelines_on_either_side_of_an_edge(pg_conn, cfg_pipeline):
    upstream_id = pg_conn.execute(
        text(
            "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
            "VALUES ('TEST_GLOBALDAG_UP', 'Upstream', 'FULL') RETURNING PIPELINE_ID"
        )
    ).scalar_one()
    pg_conn.execute(
        text(
            "INSERT INTO CFG_PIPELINE_DEPENDENCY "
            "(PIPELINE_ID, DEPENDS_ON_PIPELINE_ID, DEPENDENCY_TYPE) "
            "VALUES (:pipeline_id, :upstream_id, 'SUCCESS')"
        ),
        {"pipeline_id": cfg_pipeline, "upstream_id": upstream_id},
    )

    dag = generate_global_dag(pg_conn)

    assert dag["dag_id"] == GLOBAL_DAG_ID
    assert dag["pipelines"]["TEST_PL"] == {
        "trigger_dag_id": "TEST_PL",
        "depends_on": [{"pipeline": "TEST_GLOBALDAG_UP", "dependency_type": "SUCCESS"}],
    }
    # The upstream pipeline is included too (nothing it depends on itself),
    # since something else depending on it still needs a node to trigger.
    assert dag["pipelines"]["TEST_GLOBALDAG_UP"] == {
        "trigger_dag_id": "TEST_GLOBALDAG_UP",
        "depends_on": [],
    }


def test_generate_global_dag_excludes_pipelines_with_no_dependency_edges(pg_conn, cfg_pipeline):
    dag = generate_global_dag(pg_conn)
    assert "TEST_PL" not in dag["pipelines"]


# ==============================================================================
# crosspipe.py — against real Postgres
# ==============================================================================
#
# crosspipe.py's functions each open their own connections (see its own
# module docstring on why — never one held open across a poll's real
# sleep), so every row a test here sets up must be genuinely committed via
# postgres_engine/two_committed_pipelines, not the rolled-back pg_conn used
# elsewhere in this file.


class _FakeClock:
    """A controllable clock: sleep() advances it instead of actually waiting."""

    def __init__(self, start: datetime):
        self._now = start
        self.sleeps: list[float] = []

    def now(self) -> datetime:
        return self._now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self._now += timedelta(seconds=seconds)


def test_check_pipeline_dependencies_satisfied_when_no_edges(
    postgres_engine, two_committed_pipelines
):
    downstream_id, _ = two_committed_pipelines
    assert check_pipeline_dependencies(postgres_engine, downstream_id) is None


def test_check_pipeline_dependencies_not_satisfied_with_no_upstream_run(
    postgres_engine, two_committed_pipelines
):
    downstream_id, upstream_id = two_committed_pipelines
    insert_committed_pipeline_dependency(postgres_engine, downstream_id, upstream_id, "SUCCESS")

    reason = check_pipeline_dependencies(postgres_engine, downstream_id)

    assert reason is not None
    assert f"pipeline_id={upstream_id}" in reason


def test_check_pipeline_dependencies_success_type_satisfied_by_success_run(
    postgres_engine, two_committed_pipelines
):
    downstream_id, upstream_id = two_committed_pipelines
    insert_committed_pipeline_dependency(postgres_engine, downstream_id, upstream_id, "SUCCESS")
    insert_committed_pipeline_run(
        postgres_engine, upstream_id, "FAILED", end_date=datetime.now(UTC)
    )
    insert_committed_pipeline_run(
        postgres_engine, upstream_id, "SUCCESS", end_date=datetime.now(UTC)
    )

    assert check_pipeline_dependencies(postgres_engine, downstream_id) is None


def test_check_pipeline_dependencies_failure_type_satisfied_by_failed_run(
    postgres_engine, two_committed_pipelines
):
    downstream_id, upstream_id = two_committed_pipelines
    insert_committed_pipeline_dependency(postgres_engine, downstream_id, upstream_id, "FAILURE")
    insert_committed_pipeline_run(
        postgres_engine, upstream_id, "SUCCESS", end_date=datetime.now(UTC)
    )

    assert check_pipeline_dependencies(postgres_engine, downstream_id) is not None

    insert_committed_pipeline_run(
        postgres_engine, upstream_id, "FAILED", end_date=datetime.now(UTC)
    )

    assert check_pipeline_dependencies(postgres_engine, downstream_id) is None


def test_check_pipeline_dependencies_always_type_satisfied_by_any_terminal_status(
    postgres_engine, two_committed_pipelines
):
    downstream_id, upstream_id = two_committed_pipelines
    insert_committed_pipeline_dependency(postgres_engine, downstream_id, upstream_id, "ALWAYS")
    insert_committed_pipeline_run(
        postgres_engine, upstream_id, "SKIPPED", end_date=datetime.now(UTC)
    )

    assert check_pipeline_dependencies(postgres_engine, downstream_id) is None


def test_check_pipeline_dependencies_has_data_satisfied_by_any_task_with_data(
    postgres_engine, two_committed_pipelines
):
    # [CHOICE] confirmed with the user: pipeline-level HAS_DATA means "the
    # run succeeded AND at least one of its own tasks reported
    # TARGET_COUNT > 0" — AUD_PIPELINES_RUN_LOG has no TARGET_COUNT itself.
    downstream_id, upstream_id = two_committed_pipelines
    upstream_task_id = insert_committed_task(postgres_engine, upstream_id, "upstream_task")
    insert_committed_pipeline_dependency(postgres_engine, downstream_id, upstream_id, "HAS_DATA")
    run_id = insert_committed_pipeline_run(
        postgres_engine, upstream_id, "SUCCESS", end_date=datetime.now(UTC)
    )

    assert check_pipeline_dependencies(postgres_engine, downstream_id) is not None

    insert_committed_task_run(postgres_engine, upstream_task_id, run_id, "SUCCESS", target_count=5)

    assert check_pipeline_dependencies(postgres_engine, downstream_id) is None


def test_consume_pipeline_dependency_edges_updates_tracker_and_blocks_reconsumption(
    postgres_engine, two_committed_pipelines
):
    downstream_id, upstream_id = two_committed_pipelines
    edge_id = insert_committed_pipeline_dependency(
        postgres_engine, downstream_id, upstream_id, "SUCCESS"
    )
    run_id = insert_committed_pipeline_run(
        postgres_engine, upstream_id, "SUCCESS", end_date=datetime.now(UTC)
    )

    consume_pipeline_dependency_edges(postgres_engine, downstream_id)

    with postgres_engine.connect() as conn:
        tracked = conn.execute(
            text(
                "SELECT LAST_CONSUMED_PIPELINE_RUN_ID FROM AUD_PIPELINE_DEPENDENCY_TRACKER "
                "WHERE PIPELINE_DEPENDENCY_ID = :id"
            ),
            {"id": edge_id},
        ).scalar_one()
    assert tracked == run_id
    # Re-checking now (no newer qualifying run since) correctly reports unmet.
    assert check_pipeline_dependencies(postgres_engine, downstream_id) is not None


def test_consume_pipeline_dependency_edges_is_a_noop_when_unsatisfied(
    postgres_engine, two_committed_pipelines
):
    downstream_id, upstream_id = two_committed_pipelines
    edge_id = insert_committed_pipeline_dependency(
        postgres_engine, downstream_id, upstream_id, "SUCCESS"
    )

    consume_pipeline_dependency_edges(postgres_engine, downstream_id)

    with postgres_engine.connect() as conn:
        count = conn.execute(
            text(
                "SELECT COUNT(*) FROM AUD_PIPELINE_DEPENDENCY_TRACKER "
                "WHERE PIPELINE_DEPENDENCY_ID = :id"
            ),
            {"id": edge_id},
        ).scalar_one()
    assert count == 0


def test_wait_for_pipeline_dependency_polls_then_settles(postgres_engine, two_committed_pipelines):
    _, upstream_id = two_committed_pipelines
    insert_committed_pipeline_run(postgres_engine, upstream_id, "IN-PROGRESS")

    calls = {"n": 0}

    def fake_sleep(seconds):
        calls["n"] += 1
        if calls["n"] == 2:
            with postgres_engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE AUD_PIPELINES_RUN_LOG SET STATUS = 'SUCCESS', END_DATE = now() "
                        "WHERE PIPELINE_ID = :pid"
                    ),
                    {"pid": upstream_id},
                )

    _wait_for_pipeline_dependency_to_settle(
        postgres_engine, upstream_id, sleep=fake_sleep, now=lambda: datetime.now(UTC)
    )

    assert calls["n"] == 2


def test_wait_for_pipeline_dependency_gives_up_after_poll_cap(
    postgres_engine, two_committed_pipelines
):
    _, upstream_id = two_committed_pipelines
    start = datetime.now(UTC)
    insert_committed_pipeline_run(postgres_engine, upstream_id, "IN-PROGRESS", start_date=start)

    clock = _FakeClock(start)
    _wait_for_pipeline_dependency_to_settle(
        postgres_engine, upstream_id, sleep=clock.sleep, now=clock.now
    )

    assert len(clock.sleeps) == 30
    assert (clock.now() - start).total_seconds() < 3600


def test_wait_for_pipeline_dependency_stops_at_deadline_mid_loop(
    postgres_engine, two_committed_pipelines
):
    # Distinct from the poll-cap test above: this hits the wall-clock
    # deadline check at the *top* of a later loop iteration, before the
    # poll count (30) would ever be reached — the other way the loop can end.
    _, upstream_id = two_committed_pipelines
    start = datetime.now(UTC)
    insert_committed_pipeline_run(postgres_engine, upstream_id, "IN-PROGRESS", start_date=start)

    clock = _FakeClock(start)

    def jump_sleep(seconds: float) -> None:
        clock.sleeps.append(seconds)
        clock._now += timedelta(hours=2)  # blow well past the 1-hour deadline in one jump

    _wait_for_pipeline_dependency_to_settle(
        postgres_engine, upstream_id, sleep=jump_sleep, now=clock.now
    )

    assert len(clock.sleeps) == 1


def test_check_task_cross_pipeline_dependencies_satisfied_when_no_edges(
    postgres_engine, two_committed_pipelines
):
    downstream_id, _ = two_committed_pipelines
    downstream_task_id = insert_committed_task(postgres_engine, downstream_id, "task_a")
    assert check_task_cross_pipeline_dependencies(postgres_engine, downstream_task_id) is None


def test_check_task_cross_pipeline_dependencies_success_type(
    postgres_engine, two_committed_pipelines
):
    downstream_id, upstream_id = two_committed_pipelines
    downstream_task_id = insert_committed_task(postgres_engine, downstream_id, "task_a")
    upstream_task_id = insert_committed_task(postgres_engine, upstream_id, "upstream_task")
    run_id = insert_committed_pipeline_run(postgres_engine, upstream_id, "IN-PROGRESS")
    insert_committed_cross_pipeline_task_dependency(
        postgres_engine, downstream_id, downstream_task_id, upstream_id, upstream_task_id, "SUCCESS"
    )

    reason = check_task_cross_pipeline_dependencies(postgres_engine, downstream_task_id)
    assert reason is not None

    insert_committed_task_run(postgres_engine, upstream_task_id, run_id, "SUCCESS")

    assert check_task_cross_pipeline_dependencies(postgres_engine, downstream_task_id) is None


def test_check_task_cross_pipeline_dependencies_has_data_native(
    postgres_engine, two_committed_pipelines
):
    downstream_id, upstream_id = two_committed_pipelines
    downstream_task_id = insert_committed_task(postgres_engine, downstream_id, "task_a")
    upstream_task_id = insert_committed_task(postgres_engine, upstream_id, "upstream_task")
    # Both pipeline runs are minted terminal (not IN-PROGRESS) — this test
    # only cares about task-level status, and ux_pipeline_run_one_active
    # would block a second concurrent IN-PROGRESS run for the same pipeline.
    run_id = insert_committed_pipeline_run(
        postgres_engine, upstream_id, "SUCCESS", end_date=datetime.now(UTC)
    )
    insert_committed_cross_pipeline_task_dependency(
        postgres_engine,
        downstream_id,
        downstream_task_id,
        upstream_id,
        upstream_task_id,
        "HAS_DATA",
    )
    insert_committed_task_run(postgres_engine, upstream_task_id, run_id, "SUCCESS", target_count=0)

    assert check_task_cross_pipeline_dependencies(postgres_engine, downstream_task_id) is not None

    # A second, later run of the upstream task that genuinely reported data
    # — ux_task_run_one_per_pipeline_run means one row per (task, run), so
    # this needs its own pipeline run, same as a real second execution would.
    second_run_id = insert_committed_pipeline_run(
        postgres_engine, upstream_id, "SUCCESS", end_date=datetime.now(UTC)
    )
    insert_committed_task_run(
        postgres_engine, upstream_task_id, second_run_id, "SUCCESS", target_count=5
    )

    assert check_task_cross_pipeline_dependencies(postgres_engine, downstream_task_id) is None


def test_consume_task_dependency_edges_updates_tracker(postgres_engine, two_committed_pipelines):
    downstream_id, upstream_id = two_committed_pipelines
    downstream_task_id = insert_committed_task(postgres_engine, downstream_id, "task_a")
    upstream_task_id = insert_committed_task(postgres_engine, upstream_id, "upstream_task")
    run_id = insert_committed_pipeline_run(postgres_engine, upstream_id, "IN-PROGRESS")
    edge_id = insert_committed_cross_pipeline_task_dependency(
        postgres_engine, downstream_id, downstream_task_id, upstream_id, upstream_task_id, "SUCCESS"
    )
    task_run_id = insert_committed_task_run(postgres_engine, upstream_task_id, run_id, "SUCCESS")

    consume_task_dependency_edges(postgres_engine, downstream_task_id)

    with postgres_engine.connect() as conn:
        tracked = conn.execute(
            text(
                "SELECT LAST_CONSUMED_TASK_RUN_ID FROM AUD_TASK_DEPENDENCY_TRACKER "
                "WHERE TASK_DEPENDENCY_ID = :id"
            ),
            {"id": edge_id},
        ).scalar_one()
    assert tracked == task_run_id


def test_wait_for_task_dependency_polls_then_settles(postgres_engine, two_committed_pipelines):
    downstream_id, upstream_id = two_committed_pipelines
    upstream_task_id = insert_committed_task(postgres_engine, upstream_id, "upstream_task")
    run_id = insert_committed_pipeline_run(postgres_engine, upstream_id, "IN-PROGRESS")
    insert_committed_task_run(postgres_engine, upstream_task_id, run_id, "IN-PROGRESS")

    calls = {"n": 0}

    def fake_sleep(seconds):
        calls["n"] += 1
        if calls["n"] == 2:
            with postgres_engine.begin() as conn:
                conn.execute(
                    text("UPDATE AUD_TASK_RUN_LOG SET STATUS = 'SUCCESS' WHERE TASK_ID = :task_id"),
                    {"task_id": upstream_task_id},
                )

    _wait_for_task_dependency_to_settle(
        postgres_engine, upstream_task_id, sleep=fake_sleep, now=lambda: datetime.now(UTC)
    )

    assert calls["n"] == 2


def test_wait_for_task_dependency_stops_at_deadline_mid_loop(
    postgres_engine, two_committed_pipelines
):
    _, upstream_id = two_committed_pipelines
    upstream_task_id = insert_committed_task(postgres_engine, upstream_id, "upstream_task")
    run_id = insert_committed_pipeline_run(postgres_engine, upstream_id, "IN-PROGRESS")
    start = datetime.now(UTC)
    insert_committed_task_run(
        postgres_engine, upstream_task_id, run_id, "IN-PROGRESS", start_date=start
    )

    clock = _FakeClock(start)

    def jump_sleep(seconds: float) -> None:
        clock.sleeps.append(seconds)
        clock._now += timedelta(hours=2)

    _wait_for_task_dependency_to_settle(
        postgres_engine, upstream_task_id, sleep=jump_sleep, now=clock.now
    )

    assert len(clock.sleeps) == 1


# ==============================================================================
# runner.py — against real Postgres
# ==============================================================================
#
# run_task opens its own connections internally (mirroring how it'll really
# be invoked — once per `etl-craft run --task_code`), so every test here
# uses genuinely committed data via conftest.py's committed_pipeline
# helpers, not the rolled-back pg_conn used in the sections above.


def make_config(
    mode: str = "local",
    *,
    warehouse: bool = False,
    email: bool = False,
    email_auth_mode: str = "none",
    cloning: CloningConfig | None = None,
) -> ConnectorConfig:
    profile = ConnectionProfile(
        section="POSTGRES",
        name="dev",
        jdbc_url="jdbc:postgresql://localhost:55432/etl_craft",
        user="etl_craft",
        auth_mode="password",
    )
    warehouse_section = None
    if warehouse:
        # Same test Postgres, standing in as the Data DB — same pattern
        # test_build_data_engine_connects_for_real above uses. Needs
        # ETL_CRAFT_WAREHOUSE_DEV_SECRET set (see the warehouse_config fixture).
        warehouse_section = ConnectionSection(
            active_profile="dev",
            profiles={
                "dev": ConnectionProfile(
                    section="WAREHOUSE",
                    name="dev",
                    jdbc_url="jdbc:postgresql://localhost:55432/etl_craft",
                    user="etl_craft",
                    auth_mode="password",
                )
            },
        )
    email_section = None
    if email:
        # No real SMTP server is part of this project's test infra — every
        # email_alert.py test mocks smtplib.SMTP itself, so host/port here
        # are never actually dialed.
        email_section = EmailConfig(
            active_profile="dev",
            profiles={
                "dev": EmailProfile(
                    section="EMAIL",
                    name="dev",
                    host="smtp.test.invalid",
                    port=587,
                    from_address="etl-craft@test.invalid",
                    auth_mode=email_auth_mode,
                    user="alerts@test.invalid" if email_auth_mode == "password" else None,
                )
            },
        )
    return ConnectorConfig(
        mode=mode,
        source=SourceConfig(type="environment"),
        email=email_section,
        cloning=cloning or CloningConfig(),
        postgres=ConnectionSection(active_profile="dev", profiles={"dev": profile}),
        warehouse=warehouse_section,
    )


def test_run_task_with_no_dependencies_hits_stub_handler_and_fails(
    postgres_engine, committed_pipeline
):
    # make_config() has no [Warehouse] section — a task with no blocking
    # dependencies should get all the way through the dependency check and
    # binding, then fail on dispatch (HANDLER=SQL needs a Data DB to run
    # against). Proves the failure path end to end without needing a real
    # [Warehouse] configured.
    insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(), "TEST_CONCURRENT_PL", "task_a")

    assert outcome.status == "FAILED"
    assert "[Warehouse]" in outcome.message

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


def test_run_task_skips_when_same_pipeline_dependency_not_met(postgres_engine, committed_pipeline):
    # task_b depends on task_a via SUCCESS; task_a hasn't been run at all.
    # [DEVIATION] used to raise DependenciesNotMetError (exit 1); now
    # records SKIPPED (exit 0) instead — see runner.py's own docstring.
    task_a = insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    task_b = insert_committed_task(postgres_engine, committed_pipeline, "task_b")
    insert_committed_dependency(postgres_engine, committed_pipeline, task_b, task_a)
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(), "TEST_CONCURRENT_PL", "task_b")

    assert outcome.status == "SKIPPED"
    with postgres_engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT STATUS AS status, ERROR_MESSAGE AS error_message FROM AUD_TASK_RUN_LOG "
                "WHERE TASK_ID = :id"
            ),
            {"id": task_b},
        ).one()
    assert row.status == "SKIPPED"
    assert row.error_message is not None


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

    # task_a was never run, so without --force this would be SKIPPED (see
    # test_run_task_skips_when_same_pipeline_dependency_not_met).
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


def test_run_task_skips_when_bound_pipeline_run_is_itself_skipped(
    postgres_engine, committed_pipeline
):
    # Simulates what orchestrator.py leaves behind when a pipeline-level
    # cross-pipeline dependency was never met: the run is minted but
    # immediately finalized SKIPPED. Every task bound to it should also
    # come back SKIPPED, without attempting any dependency check of its own.
    task_a = insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    run_id = seed_active_run(postgres_engine, committed_pipeline)
    with postgres_engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_PIPELINES_RUN_LOG SET STATUS = 'SKIPPED' WHERE PIPELINE_RUN_ID = :id"),
            {"id": run_id},
        )

    outcome = run_task(postgres_engine, make_config(), "TEST_CONCURRENT_PL", "task_a")

    assert outcome.status == "SKIPPED"
    with postgres_engine.connect() as conn:
        status = conn.execute(
            text("SELECT STATUS FROM AUD_TASK_RUN_LOG WHERE TASK_ID = :id"), {"id": task_a}
        ).scalar_one()
    assert status == "SKIPPED"


def test_run_task_skips_when_cross_pipeline_task_dependency_not_met(
    postgres_engine, two_committed_pipelines
):
    downstream_id, upstream_id = two_committed_pipelines
    downstream_task_id = insert_committed_task(postgres_engine, downstream_id, "task_a")
    upstream_task_id = insert_committed_task(postgres_engine, upstream_id, "upstream_task")
    insert_committed_cross_pipeline_task_dependency(
        postgres_engine, downstream_id, downstream_task_id, upstream_id, upstream_task_id, "SUCCESS"
    )
    seed_active_run(postgres_engine, downstream_id)

    outcome = run_task(postgres_engine, make_config(), "TEST_XPIPE_DOWN", "task_a")

    assert outcome.status == "SKIPPED"
    assert "cross-pipeline" in outcome.message


def test_run_task_proceeds_when_cross_pipeline_task_dependency_satisfied(
    postgres_engine, two_committed_pipelines
):
    downstream_id, upstream_id = two_committed_pipelines
    downstream_task_id = insert_committed_task(postgres_engine, downstream_id, "task_a")
    upstream_task_id = insert_committed_task(postgres_engine, upstream_id, "upstream_task")
    insert_committed_cross_pipeline_task_dependency(
        postgres_engine, downstream_id, downstream_task_id, upstream_id, upstream_task_id, "SUCCESS"
    )
    upstream_run_id = insert_committed_pipeline_run(
        postgres_engine, upstream_id, "SUCCESS", end_date=datetime.now(UTC)
    )
    insert_committed_task_run(postgres_engine, upstream_task_id, upstream_run_id, "SUCCESS")
    seed_active_run(postgres_engine, downstream_id)

    outcome = run_task(postgres_engine, make_config(), "TEST_XPIPE_DOWN", "task_a")

    # Gets past the cross-pipeline gate and fails on the stub handler, same
    # as any other task — proving it was the gate itself that mattered.
    assert outcome.status == "FAILED"


def test_run_task_marks_success_and_stamps_counts_when_handler_succeeds(
    monkeypatch, postgres_engine, committed_pipeline
):
    # Patches dispatch directly so run_task's own SUCCESS-finalizing code is
    # exercised in isolation, independent of any one HANDLER's real
    # implementation (sql_actions.py/business_rules.py/scripts.py each get
    # their own dedicated tests). Fork duplicates this patch into the child
    # process — see runner.py's own [CHOICE] comment on why that's exactly
    # what makes this monkeypatch approach work at all.
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    seed_active_run(postgres_engine, committed_pipeline)
    monkeypatch.setattr(
        "etl_craft.runner.dispatch",
        lambda engine, ctx: HandlerResult(source_count=10, target_count=9, insert_count=9),
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


def test_run_task_detects_a_child_that_dies_unannounced(
    monkeypatch, postgres_engine, committed_pipeline
):
    # Proves the actual crash-detection path from CLAUDE.md's "Crash
    # detection" section: a child that dies before writing its own outcome
    # (os._exit, no exception the parent could observe) still leaves the
    # row FAILED, written by the still-alive parent. fork is what makes
    # this monkeypatch visible inside the forked child at all — see
    # runner.py's own [CHOICE] comment on why spawn wouldn't work here.
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    seed_active_run(postgres_engine, committed_pipeline)

    def _crash(handler):
        os._exit(1)

    monkeypatch.setattr("etl_craft.runner.dispatch", _crash)

    outcome = run_task(postgres_engine, make_config(), "TEST_CONCURRENT_PL", "task_a")

    assert outcome.status == "FAILED"
    assert "died unexpectedly" in outcome.message
    with postgres_engine.connect() as conn:
        row = conn.execute(
            text("SELECT STATUS AS status FROM AUD_TASK_RUN_LOG WHERE TASK_ID = :id"),
            {"id": task_id},
        ).one()
    assert row.status == "FAILED"


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


def test_finalize_active_run_raises_when_no_active_run(postgres_engine, committed_pipeline):
    with pytest.raises(RunLogError):
        finalize_active_run(postgres_engine, make_config(), "TEST_CONCURRENT_PL")


def test_finalize_active_run_marks_success_when_all_tasks_settled(
    postgres_engine, committed_pipeline
):
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    run_id = seed_active_run(postgres_engine, committed_pipeline)
    with postgres_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO AUD_TASK_RUN_LOG (TASK_ID, PIPELINE_RUN_ID, STATUS) "
                "VALUES (:task_id, :run_id, 'SUCCESS')"
            ),
            {"task_id": task_id, "run_id": run_id},
        )

    outcome = finalize_active_run(postgres_engine, make_config(), "TEST_CONCURRENT_PL")

    assert outcome.status == "SUCCESS"
    with postgres_engine.connect() as conn:
        status = conn.execute(
            text("SELECT STATUS FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID = :id"),
            {"id": run_id},
        ).scalar_one()
    assert status == "SUCCESS"


def test_finalize_active_run_marks_failed_when_a_task_is_unsettled(
    postgres_engine, committed_pipeline
):
    # task_a never ran at all (no AUD_TASK_RUN_LOG row) — exactly what a
    # real Airflow task that failed to even bind would look like.
    insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = finalize_active_run(postgres_engine, make_config(), "TEST_CONCURRENT_PL")

    assert outcome.status == "FAILED"


def test_finalize_active_run_works_under_orchestrator_mode(postgres_engine, committed_pipeline):
    # Unlike run_pipeline, finalize_active_run is legal under both modes —
    # it's exactly what Mode=orchestrator's synthetic last step calls.
    seed_active_run(postgres_engine, committed_pipeline)
    outcome = finalize_active_run(
        postgres_engine, make_config(mode="orchestrator"), "TEST_CONCURRENT_PL"
    )
    assert outcome.status == "SUCCESS"


def test_finalize_active_run_invokes_cloning(postgres_engine, committed_pipeline, monkeypatch):
    seed_active_run(postgres_engine, committed_pipeline)
    calls = []
    monkeypatch.setattr(
        "etl_craft.orchestrator.run_cloning_if_enabled",
        lambda engine, config: calls.append((engine, config)),
    )
    config = make_config()

    outcome = finalize_active_run(postgres_engine, config, "TEST_CONCURRENT_PL")

    assert outcome.status == "SUCCESS"
    assert calls == [(postgres_engine, config)]


def test_finalize_active_run_cloning_failure_does_not_fail_the_pipeline(
    postgres_engine, committed_pipeline, monkeypatch, capsys
):
    seed_active_run(postgres_engine, committed_pipeline)

    def _raise(engine, config):
        raise ValueError("Data DB unreachable")

    monkeypatch.setattr("etl_craft.orchestrator.run_cloning_if_enabled", _raise)

    outcome = finalize_active_run(postgres_engine, make_config(), "TEST_CONCURRENT_PL")

    assert outcome.status == "SUCCESS"
    assert "warning: cloning failed" in capsys.readouterr().err


def test_init_pipeline_run_mints_and_finalizes_skipped_when_dependency_unmet(
    postgres_engine, two_committed_pipelines
):
    downstream_id, upstream_id = two_committed_pipelines
    insert_committed_pipeline_dependency(postgres_engine, downstream_id, upstream_id, "SUCCESS")

    outcome = init_pipeline_run(postgres_engine, make_config(), "TEST_XPIPE_DOWN")

    assert "SKIPPED" in outcome.message
    with postgres_engine.connect() as conn:
        status = conn.execute(
            text("SELECT STATUS FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID = :id"),
            {"id": outcome.pipeline_run_id},
        ).scalar_one()
    assert status == "SKIPPED"


def test_run_pipeline_finalizes_skipped_when_dependency_unmet(
    postgres_engine, two_committed_pipelines
):
    downstream_id, upstream_id = two_committed_pipelines
    insert_committed_pipeline_dependency(postgres_engine, downstream_id, upstream_id, "SUCCESS")
    insert_committed_task(postgres_engine, downstream_id, "task_a")

    outcome = run_pipeline(postgres_engine, make_config(), "TEST_XPIPE_DOWN")

    assert outcome.status == "SKIPPED"


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
    # CRAFT_CONNECTOR_YAML (conftest.py) has no [Warehouse] section — a
    # HANDLER=SQL task correctly fails needing a Data DB it has none of.
    insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    seed_active_run(postgres_engine, committed_pipeline)

    exit_code = cli_main(["run", "--pipeline_code", "TEST_CONCURRENT_PL", "--task_code", "task_a"])

    assert exit_code == 1
    assert "[Warehouse]" in capsys.readouterr().out


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


def test_cli_run_finalize_only(
    craft_connector_on_disk, postgres_engine, committed_pipeline, capsys
):
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    run_id = seed_active_run(postgres_engine, committed_pipeline)
    with postgres_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO AUD_TASK_RUN_LOG (TASK_ID, PIPELINE_RUN_ID, STATUS) "
                "VALUES (:task_id, :run_id, 'SUCCESS')"
            ),
            {"task_id": task_id, "run_id": run_id},
        )

    exit_code = cli_main(["run", "--pipeline_code", "TEST_CONCURRENT_PL", "--finalize-only"])

    assert exit_code == 0
    assert "SUCCESS" in capsys.readouterr().out


def test_cli_run_finalize_only_reports_failed_with_nonzero_exit(
    craft_connector_on_disk, postgres_engine, committed_pipeline, capsys
):
    insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    seed_active_run(postgres_engine, committed_pipeline)

    exit_code = cli_main(["run", "--pipeline_code", "TEST_CONCURRENT_PL", "--finalize-only"])

    assert exit_code == 1
    assert "FAILED" in capsys.readouterr().out


def test_cli_run_finalize_only_and_task_code_are_mutually_exclusive(craft_connector_on_disk):
    with pytest.raises(SystemExit) as exc_info:
        cli_main(
            [
                "run",
                "--pipeline_code",
                "TEST_CONCURRENT_PL",
                "--finalize-only",
                "--task_code",
                "t1",
            ]
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
    assert set(parsed["tasks"]) == {"__init__", "task_a", "__finalize__"}


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


def test_cli_generate_yml_global_disabled_by_default(craft_connector_on_disk, capsys):
    # craft_connector_on_disk's CRAFT_CONNECTOR_YAML has no [Orchestrator]
    # section at all, so Global_dag defaults to false, per explicit
    # instruction ("the global dag option ... defaults to false").
    exit_code = cli_main(["generate-yml", "--global"])

    assert exit_code == 2
    assert "disabled" in capsys.readouterr().err


def test_cli_generate_yml_global_enabled(
    tmp_path, monkeypatch, postgres_engine, committed_pipeline, capsys
):
    (tmp_path / "craft-connector.yml").write_text(
        CRAFT_CONNECTOR_YAML + "\nOrchestrator:\n  Global_dag: true\n"
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ETL_CRAFT_POSTGRES_DEV_SECRET", "etl_craft")
    with postgres_engine.begin() as conn:
        other_id = conn.execute(
            text(
                "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
                "VALUES ('TEST_CLI_GLOBAL_UP', 'Upstream', 'FULL') RETURNING PIPELINE_ID"
            )
        ).scalar_one()
        conn.execute(
            text(
                "INSERT INTO CFG_PIPELINE_DEPENDENCY "
                "(PIPELINE_ID, DEPENDS_ON_PIPELINE_ID, DEPENDENCY_TYPE) "
                "VALUES (:pid, :other_id, 'SUCCESS')"
            ),
            {"pid": committed_pipeline, "other_id": other_id},
        )

    try:
        exit_code = cli_main(["generate-yml", "--global"])
    finally:
        with postgres_engine.begin() as conn:
            conn.execute(
                text("DELETE FROM CFG_PIPELINE_DEPENDENCY WHERE PIPELINE_ID = :pid"),
                {"pid": committed_pipeline},
            )
            conn.execute(
                text("DELETE FROM CFG_PIPELINES WHERE PIPELINE_ID = :id"), {"id": other_id}
            )

    assert exit_code == 0
    out = capsys.readouterr().out
    assert GLOBAL_DAG_ID in out
    assert "TEST_CONCURRENT_PL" in out
    assert "TEST_CLI_GLOBAL_UP" in out


def test_cli_generate_yml_requires_pipeline_code_or_global(craft_connector_on_disk):
    with pytest.raises(SystemExit) as exc_info:
        cli_main(["generate-yml"])
    assert exc_info.value.code == 2


def test_cli_generate_yml_pipeline_code_and_global_are_mutually_exclusive(craft_connector_on_disk):
    with pytest.raises(SystemExit) as exc_info:
        cli_main(["generate-yml", "--pipeline_code", "X", "--global"])
    assert exc_info.value.code == 2


def test_cli_validate_ok_when_no_issues(
    craft_connector_on_disk, postgres_engine, committed_pipeline, capsys
):
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {"SOURCE_OBJECT": "public.src", "TARGET_OBJECT": "public.tgt"},
    )

    exit_code = cli_main(["validate"])

    assert exit_code == 0
    assert "OK" in capsys.readouterr().out


def test_cli_validate_reports_task_missing_lineage_declarations(
    craft_connector_on_disk, postgres_engine, committed_pipeline, capsys
):
    insert_committed_task(postgres_engine, committed_pipeline, "task_a")

    exit_code = cli_main(["validate"])

    assert exit_code == 1
    out = capsys.readouterr().out
    assert "[task_lineage]" in out
    assert "SOURCE_OBJECT" in out and "TARGET_OBJECT" in out


def test_cli_validate_reports_cycle(
    craft_connector_on_disk, postgres_engine, committed_pipeline, capsys
):
    task_a = insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    task_b = insert_committed_task(postgres_engine, committed_pipeline, "task_b")
    insert_committed_dependency(postgres_engine, committed_pipeline, task_b, task_a)
    insert_committed_dependency(postgres_engine, committed_pipeline, task_a, task_b)

    exit_code = cli_main(["validate"])

    assert exit_code == 1
    assert "[graph]" in capsys.readouterr().out


def test_cli_validate_reports_missing_warehouse_for_active_business_rule(
    craft_connector_on_disk, postgres_engine, committed_pipeline, capsys
):
    # craft_connector_on_disk's CRAFT_CONNECTOR_YAML has no [Warehouse]
    # section, so an active business rule with nothing to check its
    # TARGET_TABLE against is itself a reportable integrity issue.
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    insert_committed_business_rule(
        postgres_engine, committed_pipeline, task_id, "br1", "public.some_table", "id"
    )

    exit_code = cli_main(["validate"])

    assert exit_code == 1
    out = capsys.readouterr().out
    assert "[business_rule_pk]" in out
    assert "Warehouse" in out


WAREHOUSE_YAML_SUFFIX = """
Warehouse:
  Active_profile: dev
  Profiles:
    dev:
      jdbc_url: jdbc:postgresql://localhost:55432/etl_craft
      user: etl_craft
      auth_mode: password
"""

UNREACHABLE_WAREHOUSE_YAML_SUFFIX = """
Warehouse:
  Active_profile: dev
  Profiles:
    dev:
      jdbc_url: jdbc:postgresql://localhost:1/etl_craft
      user: etl_craft
      auth_mode: password
"""


def test_cli_validate_ok_with_warehouse_configured_and_matching_pk(
    tmp_path, monkeypatch, postgres_engine, committed_pipeline, capsys
):
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {"SOURCE_OBJECT": "public.src", "TARGET_OBJECT": "public.validate_cli_test_good"},
    )
    with postgres_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS validate_cli_test_good"))
        conn.execute(text("CREATE TABLE validate_cli_test_good (id INT PRIMARY KEY)"))
    insert_committed_business_rule(
        postgres_engine, committed_pipeline, task_id, "br1", "validate_cli_test_good", "id"
    )
    (tmp_path / "craft-connector.yml").write_text(CRAFT_CONNECTOR_YAML + WAREHOUSE_YAML_SUFFIX)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ETL_CRAFT_POSTGRES_DEV_SECRET", "etl_craft")
    monkeypatch.setenv("ETL_CRAFT_WAREHOUSE_DEV_SECRET", "etl_craft")

    try:
        exit_code = cli_main(["validate"])
    finally:
        with postgres_engine.begin() as conn:
            conn.execute(text("DROP TABLE validate_cli_test_good"))

    assert exit_code == 0
    assert "OK" in capsys.readouterr().out


def test_cli_validate_reports_config_error_building_data_engine(
    tmp_path, monkeypatch, postgres_engine, committed_pipeline, capsys
):
    (tmp_path / "craft-connector.yml").write_text(CRAFT_CONNECTOR_YAML + WAREHOUSE_YAML_SUFFIX)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ETL_CRAFT_POSTGRES_DEV_SECRET", "etl_craft")
    monkeypatch.delenv("ETL_CRAFT_WAREHOUSE_DEV_SECRET", raising=False)

    exit_code = cli_main(["validate"])

    assert exit_code == 2
    assert "error:" in capsys.readouterr().err


def test_cli_validate_reports_connection_error_checking_business_rules(
    tmp_path, monkeypatch, postgres_engine, committed_pipeline, capsys
):
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    insert_committed_business_rule(
        postgres_engine, committed_pipeline, task_id, "br1", "public.some_table", "id"
    )
    (tmp_path / "craft-connector.yml").write_text(
        CRAFT_CONNECTOR_YAML + UNREACHABLE_WAREHOUSE_YAML_SUFFIX
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ETL_CRAFT_POSTGRES_DEV_SECRET", "etl_craft")
    monkeypatch.setenv("ETL_CRAFT_WAREHOUSE_DEV_SECRET", "etl_craft")

    exit_code = cli_main(["validate"])

    assert exit_code == 2
    assert "error:" in capsys.readouterr().err


# ==============================================================================
# sql_actions.py / business_rules.py / scripts.py — the execution engine
# ==============================================================================
#
# All against real Postgres, standing in as *both* the Engine DB (its usual
# role) and the Data DB / [Warehouse] (make_config(warehouse=True) points
# both at the same instance) — the same "one dialect stands in for the
# generic mechanism" spirit as warehouse.py's own tests. Every test drives
# the real `run_task()` entry point end to end (CFG_ setup -> dispatch ->
# AUD_TASK_RUN_LOG), the same path a real deployment uses, rather than
# calling sql_actions.execute()/business_rules.execute() directly — that
# exercises handlers.py's own wiring (including its HandlerError-wrapping of
# ConfigError/SQLAlchemyError) for free, not just the leaf modules.
#
# data_db_tables (conftest.py) tracks/drops every real table a test creates
# in the Data DB side of this same Postgres instance — separate from
# committed_pipeline's own CFG_/AUD_ cleanup.


def _task_run_row(engine, task_id: int):
    """Fetch task_id's most recent AUD_TASK_RUN_LOG row (one per pipeline_run_id it's seen)."""
    with engine.connect() as conn:
        return conn.execute(
            text(
                "SELECT STATUS AS status, ERROR_MESSAGE AS error_message, "
                "SOURCE_COUNT AS source_count, TARGET_COUNT AS target_count, "
                "INSERT_COUNT AS insert_count, UPDATE_COUNT AS update_count, "
                "DELETE_COUNT AS delete_count, TASK_LOG AS task_log "
                "FROM AUD_TASK_RUN_LOG WHERE TASK_ID = :task_id "
                "ORDER BY START_DATE DESC LIMIT 1"
            ),
            {"task_id": task_id},
        ).one()


def finalize_pipeline_run_stub(conn, pipeline_run_id):
    conn.execute(
        text("UPDATE AUD_PIPELINES_RUN_LOG SET STATUS = 'SUCCESS' WHERE PIPELINE_RUN_ID = :id"),
        {"id": pipeline_run_id},
    )


def test_sql_create_table_stamps_pipeline_run_id_and_counts(
    postgres_engine, committed_pipeline, data_db_tables
):
    target = f"public.sqlx_create_{committed_pipeline}"
    data_db_tables.append(target)
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "t")
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {
            "SQL_ACTION": "CREATE_TABLE",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": "SELECT id, name FROM (VALUES (1,'a'),(2,'b')) AS v(id, name) WHERE 1=1",
        },
    )
    run_id = seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "t")

    assert outcome.status == "SUCCESS"
    with postgres_engine.connect() as conn:
        rows = conn.execute(
            text(f"SELECT id, name, pipeline_run_id FROM {target} ORDER BY id")
        ).all()
    assert rows == [(1, "a", run_id), (2, "b", run_id)]
    row = _task_run_row(postgres_engine, task_id)
    assert (row.source_count, row.target_count, row.insert_count) == (2, 2, 2)


def test_sql_setup_table_infers_audit_columns_from_scd2_sibling(
    postgres_engine, committed_pipeline, data_db_tables
):
    target = f"public.sqlx_setup_scd2_{committed_pipeline}"
    data_db_tables.append(target)
    setup_id = insert_committed_task(postgres_engine, committed_pipeline, "setup")
    insert_committed_task_parameters(
        postgres_engine,
        setup_id,
        {
            "SQL_ACTION": "SETUP_TABLE",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": "SELECT id, name FROM (VALUES (1,'a')) AS v(id, name) WHERE 1=1",
        },
    )
    sibling_id = insert_committed_task(postgres_engine, committed_pipeline, "merger")
    insert_committed_task_parameters(
        postgres_engine,
        sibling_id,
        {
            "SQL_ACTION": "SCD2_MERGE",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": "SELECT id, name FROM whatever",
            "MERGE_KEY": "id",
            "MERGE_COMPARE_COLUMNS": "name",
        },
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "setup")

    assert outcome.status == "SUCCESS"
    with postgres_engine.connect() as conn:
        cols = (
            conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = :t ORDER BY ordinal_position"
                ),
                {"t": target.split(".", 1)[1]},
            )
            .scalars()
            .all()
        )
        count = conn.execute(text(f"SELECT COUNT(*) FROM {target}")).scalar_one()
    assert cols == [
        "id",
        "name",
        "pipeline_run_id",
        "hash_key",
        "create_date",
        "created_by",
        "update_date",
        "updated_by",
        "delete_flag",
        "active_flag",
    ]
    assert count == 0


def test_sql_setup_table_no_sibling_falls_back_to_no_audit_columns(
    postgres_engine, committed_pipeline, data_db_tables
):
    target = f"public.sqlx_setup_solo_{committed_pipeline}"
    data_db_tables.append(target)
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "setup")
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {
            "SQL_ACTION": "SETUP_TABLE",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": "SELECT id, name FROM (VALUES (1,'a')) AS v(id, name) WHERE 1=1",
        },
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "setup")

    assert outcome.status == "SUCCESS"
    with postgres_engine.connect() as conn:
        cols = (
            conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = :t ORDER BY ordinal_position"
                ),
                {"t": target.split(".", 1)[1]},
            )
            .scalars()
            .all()
        )
    assert cols == ["id", "name", "pipeline_run_id"]


def test_sql_overwrite_table_truncates_and_reinserts(
    postgres_engine, committed_pipeline, data_db_tables
):
    target = f"public.sqlx_over_{committed_pipeline}"
    src = f"sqlx_over_src_{committed_pipeline}"
    data_db_tables.extend([target, src])
    with postgres_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE {src} (id int, name varchar)"))
        conn.execute(text(f"INSERT INTO {src} VALUES (1, 'a'), (2, 'b')"))

    setup_id = insert_committed_task(postgres_engine, committed_pipeline, "setup")
    insert_committed_task_parameters(
        postgres_engine,
        setup_id,
        {
            "SQL_ACTION": "SETUP_TABLE",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": f"SELECT * FROM {src} WHERE 1=1",
        },
    )
    over_id = insert_committed_task(postgres_engine, committed_pipeline, "over")
    insert_committed_task_parameters(
        postgres_engine,
        over_id,
        {
            "SQL_ACTION": "OVERWRITE_TABLE",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": f"SELECT * FROM {src} WHERE 1=1",
        },
    )
    run1 = seed_active_run(postgres_engine, committed_pipeline)
    assert (
        run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "setup").status
        == "SUCCESS"
    )
    assert (
        run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "over").status
        == "SUCCESS"
    )
    with postgres_engine.connect() as conn:
        assert conn.execute(text(f"SELECT id, name FROM {target} ORDER BY id")).all() == [
            (1, "a"),
            (2, "b"),
        ]

    with postgres_engine.begin() as conn:
        finalize_pipeline_run_stub(conn, run1)
        conn.execute(text(f"DELETE FROM {src}"))
        conn.execute(text(f"INSERT INTO {src} VALUES (3, 'c')"))
    seed_active_run(postgres_engine, committed_pipeline)
    assert (
        run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "over").status
        == "SUCCESS"
    )
    with postgres_engine.connect() as conn:
        assert conn.execute(text(f"SELECT id, name FROM {target} ORDER BY id")).all() == [(3, "c")]


def test_sql_overwrite_table_missing_target_fails_clearly(
    postgres_engine, committed_pipeline, data_db_tables
):
    target = f"public.sqlx_over_missing_{committed_pipeline}"
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "over")
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {
            "SQL_ACTION": "OVERWRITE_TABLE",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": "SELECT 1 AS id WHERE 1=1",
        },
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "over")

    assert outcome.status == "FAILED"
    assert "does not exist" in outcome.message
    assert "SETUP_TABLE or CREATE_TABLE" in outcome.message


def test_sql_scd1_merge_inserts_updates_and_skips_unchanged(
    postgres_engine, committed_pipeline, data_db_tables
):
    target = f"public.sqlx_scd1_{committed_pipeline}"
    src = f"sqlx_scd1_src_{committed_pipeline}"
    data_db_tables.extend([target, src])
    with postgres_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE {src} (id int, name varchar)"))
        conn.execute(text(f"INSERT INTO {src} VALUES (1, 'a'), (2, 'b')"))

    setup_id = insert_committed_task(postgres_engine, committed_pipeline, "setup")
    insert_committed_task_parameters(
        postgres_engine,
        setup_id,
        {
            "SQL_ACTION": "SETUP_TABLE",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": f"SELECT * FROM {src} WHERE 1=1",
        },
    )
    merge_id = insert_committed_task(postgres_engine, committed_pipeline, "merge")
    insert_committed_task_parameters(
        postgres_engine,
        merge_id,
        {
            "SQL_ACTION": "SCD1_MERGE",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": f"SELECT * FROM {src} WHERE 1=1",
            "MERGE_KEY": "id",
            "MERGE_COMPARE_COLUMNS": "name",
        },
    )
    run1 = seed_active_run(postgres_engine, committed_pipeline)
    assert (
        run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "setup").status
        == "SUCCESS"
    )
    assert (
        run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "merge").status
        == "SUCCESS"
    )
    with postgres_engine.connect() as conn:
        assert conn.execute(text(f"SELECT id, name FROM {target} ORDER BY id")).all() == [
            (1, "a"),
            (2, "b"),
        ]

    with postgres_engine.begin() as conn:
        finalize_pipeline_run_stub(conn, run1)
        conn.execute(text(f"UPDATE {src} SET name = 'a-updated' WHERE id = 1"))
        conn.execute(text(f"INSERT INTO {src} VALUES (3, 'c')"))
    seed_active_run(postgres_engine, committed_pipeline)
    outcome = run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "merge")
    assert outcome.status == "SUCCESS"
    with postgres_engine.connect() as conn:
        assert conn.execute(text(f"SELECT id, name FROM {target} ORDER BY id")).all() == [
            (1, "a-updated"),
            (2, "b"),
            (3, "c"),
        ]
    row = _task_run_row(postgres_engine, merge_id)
    assert row.insert_count == 1
    assert row.update_count == 1


def test_sql_scd2_merge_deactivates_and_inserts_new_version(
    postgres_engine, committed_pipeline, data_db_tables
):
    target = f"public.sqlx_scd2_{committed_pipeline}"
    src = f"sqlx_scd2_src_{committed_pipeline}"
    data_db_tables.extend([target, src])
    with postgres_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE {src} (id int, name varchar)"))
        conn.execute(text(f"INSERT INTO {src} VALUES (1, 'x')"))

    setup_id = insert_committed_task(postgres_engine, committed_pipeline, "setup")
    insert_committed_task_parameters(
        postgres_engine,
        setup_id,
        {
            "SQL_ACTION": "SETUP_TABLE",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": f"SELECT * FROM {src} WHERE 1=1",
        },
    )
    merge_id = insert_committed_task(postgres_engine, committed_pipeline, "merge")
    insert_committed_task_parameters(
        postgres_engine,
        merge_id,
        {
            "SQL_ACTION": "SCD2_MERGE",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": f"SELECT * FROM {src} WHERE 1=1",
            "MERGE_KEY": "id",
            "MERGE_COMPARE_COLUMNS": "name",
        },
    )
    run1 = seed_active_run(postgres_engine, committed_pipeline)
    assert (
        run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "setup").status
        == "SUCCESS"
    )
    assert (
        run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "merge").status
        == "SUCCESS"
    )

    with postgres_engine.begin() as conn:
        finalize_pipeline_run_stub(conn, run1)
        conn.execute(text(f"UPDATE {src} SET name = 'x-updated' WHERE id = 1"))
    seed_active_run(postgres_engine, committed_pipeline)
    outcome = run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "merge")

    assert outcome.status == "SUCCESS"
    with postgres_engine.connect() as conn:
        rows = conn.execute(
            text(f"SELECT id, name, active_flag FROM {target} ORDER BY active_flag, name")
        ).all()
    assert rows == [(1, "x", "N"), (1, "x-updated", "Y")]


def test_sql_scd_merge_missing_merge_key_fails(postgres_engine, committed_pipeline, data_db_tables):
    target = f"public.sqlx_scd_nomkey_{committed_pipeline}"
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "merge")
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {
            "SQL_ACTION": "SCD1_MERGE",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": "SELECT 1 AS id, 'a' AS name",
            "MERGE_COMPARE_COLUMNS": "name",
        },
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "merge")

    assert outcome.status == "FAILED"
    assert "MERGE_KEY" in outcome.message


def test_sql_drop_table_succeeds_with_create_table_sibling(
    postgres_engine, committed_pipeline, data_db_tables
):
    target = f"public.sqlx_drop_ok_{committed_pipeline}"
    data_db_tables.append(target)
    create_id = insert_committed_task(postgres_engine, committed_pipeline, "creator")
    insert_committed_task_parameters(
        postgres_engine,
        create_id,
        {
            "SQL_ACTION": "CREATE_TABLE",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": "SELECT 1 AS id WHERE 1=1",
        },
    )
    drop_id = insert_committed_task(postgres_engine, committed_pipeline, "dropper")
    insert_committed_task_parameters(
        postgres_engine, drop_id, {"SQL_ACTION": "DROP_TABLE", "TARGET_OBJECT": target}
    )
    seed_active_run(postgres_engine, committed_pipeline)
    assert (
        run_task(
            postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "creator"
        ).status
        == "SUCCESS"
    )

    outcome = run_task(
        postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "dropper"
    )

    assert outcome.status == "SUCCESS"
    with postgres_engine.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM information_schema.tables WHERE table_name = :t"),
            {"t": target.split(".", 1)[1]},
        ).scalar_one_or_none()
    assert exists is None


def test_sql_drop_table_refused_without_create_table_sibling(
    postgres_engine, committed_pipeline, data_db_tables
):
    target = f"public.sqlx_drop_refuse_{committed_pipeline}"
    bare_table = target.split(".", 1)[1]
    data_db_tables.append(target)
    with postgres_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE {bare_table} (id int)"))
    drop_id = insert_committed_task(postgres_engine, committed_pipeline, "dropper")
    insert_committed_task_parameters(
        postgres_engine, drop_id, {"SQL_ACTION": "DROP_TABLE", "TARGET_OBJECT": target}
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(
        postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "dropper"
    )

    assert outcome.status == "FAILED"
    assert "refused" in outcome.message
    with postgres_engine.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM information_schema.tables WHERE table_name = :t"),
            {"t": bare_table},
        ).scalar_one_or_none()
    assert exists is not None


def test_sql_drop_table_refused_when_create_table_sibling_has_not_run_yet(
    postgres_engine, committed_pipeline, data_db_tables
):
    # A CREATE_TABLE sibling exists in CFG_ (the earlier test covers that
    # part) but hasn't actually executed under *this* run — "created by
    # this pipeline using create_table before this drop table step," per
    # explicit instruction, not just declared somewhere in config.
    target = f"public.sqlx_drop_not_run_{committed_pipeline}"
    data_db_tables.append(target)
    creator_id = insert_committed_task(postgres_engine, committed_pipeline, "creator")
    insert_committed_task_parameters(
        postgres_engine,
        creator_id,
        {"SQL_ACTION": "CREATE_TABLE", "TARGET_OBJECT": target, "SOURCE_SQL": "SELECT 1 AS id"},
    )
    # Deliberately never run_task(..., "creator") — the whole point is that
    # its AUD_TASK_RUN_LOG has no SUCCESS row yet under this run.
    drop_id = insert_committed_task(postgres_engine, committed_pipeline, "dropper")
    insert_committed_task_parameters(
        postgres_engine, drop_id, {"SQL_ACTION": "DROP_TABLE", "TARGET_OBJECT": target}
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(
        postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "dropper"
    )

    assert outcome.status == "FAILED"
    assert "hasn't completed successfully yet" in outcome.message


def test_sql_delete_rows_hard_and_soft(postgres_engine, committed_pipeline, data_db_tables):
    target = f"public.sqlx_delete_{committed_pipeline}"
    data_db_tables.append(target)
    setup_id = insert_committed_task(postgres_engine, committed_pipeline, "setup")
    insert_committed_task_parameters(
        postgres_engine,
        setup_id,
        {
            "SQL_ACTION": "SETUP_TABLE",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": "SELECT id, name FROM (VALUES (1,'a')) AS v(id, name) WHERE 1=1",
        },
    )
    merge_id = insert_committed_task(postgres_engine, committed_pipeline, "merge")
    insert_committed_task_parameters(
        postgres_engine,
        merge_id,
        {
            "SQL_ACTION": "SCD1_MERGE",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": "SELECT id, name FROM (VALUES (1,'a'),(2,'b')) AS v(id, name) WHERE 1=1",
            "MERGE_KEY": "id",
            "MERGE_COMPARE_COLUMNS": "name",
        },
    )
    soft_id = insert_committed_task(postgres_engine, committed_pipeline, "soft")
    insert_committed_task_parameters(
        postgres_engine,
        soft_id,
        {
            "SQL_ACTION": "DELETE_ROWS",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": f"SELECT id FROM {target} WHERE id = 1",
            "MERGE_KEY": "id",
        },
    )
    hard_id = insert_committed_task(postgres_engine, committed_pipeline, "hard")
    insert_committed_task_parameters(
        postgres_engine,
        hard_id,
        {
            "SQL_ACTION": "DELETE_ROWS",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": f"SELECT id FROM {target} WHERE id = 2",
            "MERGE_KEY": "id",
            "HARD_DELETE": "true",
        },
    )
    seed_active_run(postgres_engine, committed_pipeline)
    assert (
        run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "setup").status
        == "SUCCESS"
    )
    assert (
        run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "merge").status
        == "SUCCESS"
    )

    soft_outcome = run_task(
        postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "soft"
    )
    hard_outcome = run_task(
        postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "hard"
    )

    assert soft_outcome.status == "SUCCESS"
    assert hard_outcome.status == "SUCCESS"
    with postgres_engine.connect() as conn:
        rows = conn.execute(text(f"SELECT id, delete_flag FROM {target} ORDER BY id")).all()
    assert rows == [(1, "Y")]
    assert _task_run_row(postgres_engine, soft_id).delete_count == 1
    assert _task_run_row(postgres_engine, hard_id).delete_count == 1


def test_sql_schema_check_fails_when_stage_missing_a_target_column(
    postgres_engine, committed_pipeline, data_db_tables
):
    target = f"public.sqlx_missing_col_{committed_pipeline}"
    data_db_tables.append(target)
    setup_id = insert_committed_task(postgres_engine, committed_pipeline, "setup")
    insert_committed_task_parameters(
        postgres_engine,
        setup_id,
        {
            "SQL_ACTION": "SETUP_TABLE",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": "SELECT id, name FROM (VALUES (1,'a')) AS v(id, name) WHERE 1=1",
        },
    )
    over_id = insert_committed_task(
        postgres_engine, committed_pipeline, "over", schema_evolution=True
    )
    insert_committed_task_parameters(
        postgres_engine,
        over_id,
        {
            "SQL_ACTION": "OVERWRITE_TABLE",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": "SELECT id FROM (VALUES (1)) AS v(id) WHERE 1=1",  # missing "name"
        },
    )
    seed_active_run(postgres_engine, committed_pipeline)
    assert (
        run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "setup").status
        == "SUCCESS"
    )

    outcome = run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "over")

    assert outcome.status == "FAILED"
    assert "missing column" in outcome.message
    assert "['name']" in outcome.message


def test_sql_schema_evolution_disabled_fails_on_new_column(
    postgres_engine, committed_pipeline, data_db_tables
):
    target = f"public.sqlx_evolve_off_{committed_pipeline}"
    data_db_tables.append(target)
    setup_id = insert_committed_task(postgres_engine, committed_pipeline, "setup")
    insert_committed_task_parameters(
        postgres_engine,
        setup_id,
        {
            "SQL_ACTION": "SETUP_TABLE",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": "SELECT id, name FROM (VALUES (1,'a')) AS v(id, name) WHERE 1=1",
        },
    )
    over_id = insert_committed_task(
        postgres_engine, committed_pipeline, "over", schema_evolution=False
    )
    insert_committed_task_parameters(
        postgres_engine,
        over_id,
        {
            "SQL_ACTION": "OVERWRITE_TABLE",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": (
                "SELECT id, name, extra FROM (VALUES (1,'a','z')) AS v(id, name, extra) "
                "WHERE 1=1"
            ),
        },
    )
    seed_active_run(postgres_engine, committed_pipeline)
    assert (
        run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "setup").status
        == "SUCCESS"
    )

    outcome = run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "over")

    assert outcome.status == "FAILED"
    assert "new column(s)" in outcome.message
    assert "SCHEMA_EVOLUTION is false" in outcome.message


def test_sql_schema_evolution_enabled_adds_column_at_right_position(
    postgres_engine, committed_pipeline, data_db_tables
):
    target = f"public.sqlx_evolve_on_{committed_pipeline}"
    data_db_tables.extend([target, f"{target}__etl_evolve"])
    setup_id = insert_committed_task(postgres_engine, committed_pipeline, "setup")
    insert_committed_task_parameters(
        postgres_engine,
        setup_id,
        {
            "SQL_ACTION": "SETUP_TABLE",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": "SELECT id, name FROM (VALUES (1,'a')) AS v(id, name) WHERE 1=1",
        },
    )
    over_id = insert_committed_task(
        postgres_engine, committed_pipeline, "over", schema_evolution=True
    )
    insert_committed_task_parameters(
        postgres_engine,
        over_id,
        {
            "SQL_ACTION": "OVERWRITE_TABLE",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": (
                "SELECT id, name, extra FROM (VALUES (1,'a','z')) AS v(id, name, extra) "
                "WHERE 1=1"
            ),
        },
    )
    seed_active_run(postgres_engine, committed_pipeline)
    assert (
        run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "setup").status
        == "SUCCESS"
    )

    outcome = run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "over")

    assert outcome.status == "SUCCESS"
    with postgres_engine.connect() as conn:
        cols = (
            conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = :t ORDER BY ordinal_position"
                ),
                {"t": target.split(".", 1)[1]},
            )
            .scalars()
            .all()
        )
        data = conn.execute(text(f"SELECT id, name, extra FROM {target}")).all()
    # "extra" lands right after "name" (the SELECT's own column order), not
    # appended after the engine-managed columns.
    assert cols == ["id", "name", "extra", "pipeline_run_id", "update_date"]
    assert data == [(1, "a", "z")]


def test_sql_overwrite_table_missing_audit_column_fails_clearly(
    postgres_engine, committed_pipeline, data_db_tables
):
    # A target that predates this convention (or was hand-built) has its
    # business columns and PIPELINE_RUN_ID, but never got UPDATE_DATE — the
    # column OVERWRITE_TABLE itself needs to stamp. This must fail up front
    # with a clear message, not partway through the real UPDATE/INSERT with
    # a raw "column update_date does not exist".
    target = f"public.sqlx_over_missing_audit_{committed_pipeline}"
    data_db_tables.append(target)
    with postgres_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE {target} (id int, name varchar, pipeline_run_id bigint)"))
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "over")
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {
            "SQL_ACTION": "OVERWRITE_TABLE",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": "SELECT 1 AS id, 'a' AS name WHERE 1=1",
        },
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "over")

    assert outcome.status == "FAILED"
    assert "missing the audit column(s)" in outcome.message
    assert "UPDATE_DATE" in outcome.message


def test_sql_scd1_merge_missing_audit_column_fails_even_with_schema_evolution(
    postgres_engine, committed_pipeline, data_db_tables
):
    # This check must fire regardless of SCHEMA_EVOLUTION: that flag only
    # ever governs new *business* columns the staged SELECT introduces, never
    # repairing a target's own missing engine-managed columns.
    target = f"public.sqlx_scd1_missing_audit_{committed_pipeline}"
    data_db_tables.append(target)
    with postgres_engine.begin() as conn:
        # Business columns + PIPELINE_RUN_ID, but no HASH_KEY/CREATE_DATE/
        # CREATED_BY/UPDATE_DATE/UPDATED_BY/DELETE_FLAG at all.
        conn.execute(text(f"CREATE TABLE {target} (id int, name varchar, pipeline_run_id bigint)"))
    task_id = insert_committed_task(
        postgres_engine, committed_pipeline, "merge", schema_evolution=True
    )
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {
            "SQL_ACTION": "SCD1_MERGE",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": "SELECT 1 AS id, 'a' AS name WHERE 1=1",
            "MERGE_KEY": "id",
            "MERGE_COMPARE_COLUMNS": "name",
        },
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "merge")

    assert outcome.status == "FAILED"
    assert "missing the audit column(s)" in outcome.message
    assert "HASH_KEY" in outcome.message


def test_sql_delete_rows_soft_delete_missing_delete_flag_fails_clearly(
    postgres_engine, committed_pipeline, data_db_tables
):
    # DELETE_ROWS never goes through _check_or_evolve_schema (it only
    # matches on MERGE_KEY, no shape comparison) — its soft-delete path gets
    # its own DELETE_FLAG-presence check for the same reason.
    target = f"public.sqlx_delete_missing_flag_{committed_pipeline}"
    data_db_tables.append(target)
    with postgres_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE {target} (id int, pipeline_run_id bigint)"))
        conn.execute(text(f"INSERT INTO {target} (id, pipeline_run_id) VALUES (1, 1)"))
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "soft")
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {
            "SQL_ACTION": "DELETE_ROWS",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": f"SELECT id FROM {target} WHERE id = 1",
            "MERGE_KEY": "id",
        },
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "soft")

    assert outcome.status == "FAILED"
    assert "DELETE_FLAG" in outcome.message
    assert "HARD_DELETE=true" in outcome.message


def test_sql_delete_rows_hard_delete_ignores_missing_delete_flag(
    postgres_engine, committed_pipeline, data_db_tables
):
    # HARD_DELETE=true issues a real DELETE and never touches DELETE_FLAG at
    # all -- unlike the soft-delete path above, a target that never had that
    # column must NOT be rejected by the audit-column check.
    target = f"public.sqlx_hard_delete_no_flag_{committed_pipeline}"
    data_db_tables.append(target)
    with postgres_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE {target} (id int, pipeline_run_id bigint)"))
        conn.execute(text(f"INSERT INTO {target} (id, pipeline_run_id) VALUES (1, 1)"))
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "hard")
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {
            "SQL_ACTION": "DELETE_ROWS",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": f"SELECT id FROM {target} WHERE id = 1",
            "MERGE_KEY": "id",
            "HARD_DELETE": "true",
        },
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "hard")

    assert outcome.status == "SUCCESS"
    with postgres_engine.connect() as conn:
        rows = conn.execute(text(f"SELECT id FROM {target}")).all()
    assert rows == []


def test_sql_unknown_action_and_missing_params_fail_clearly(
    postgres_engine, committed_pipeline, data_db_tables
):
    bad_action_id = insert_committed_task(postgres_engine, committed_pipeline, "bad_action")
    insert_committed_task_parameters(
        postgres_engine,
        bad_action_id,
        {"SQL_ACTION": "BOGUS", "TARGET_OBJECT": "public.whatever"},
    )
    no_target_id = insert_committed_task(postgres_engine, committed_pipeline, "no_target")
    insert_committed_task_parameters(postgres_engine, no_target_id, {"SQL_ACTION": "CREATE_TABLE"})
    no_source_id = insert_committed_task(postgres_engine, committed_pipeline, "no_source")
    insert_committed_task_parameters(
        postgres_engine,
        no_source_id,
        {"SQL_ACTION": "CREATE_TABLE", "TARGET_OBJECT": "public.whatever2"},
    )
    seed_active_run(postgres_engine, committed_pipeline)

    bad_action = run_task(
        postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "bad_action"
    )
    no_target = run_task(
        postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "no_target"
    )
    no_source = run_task(
        postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "no_source"
    )

    assert bad_action.status == "FAILED" and "SQL_ACTION" in bad_action.message
    assert no_target.status == "FAILED" and "TARGET_OBJECT" in no_target.message
    assert no_source.status == "FAILED" and "SOURCE_SQL" in no_source.message


def test_sql_delete_rows_missing_source_sql_fails(postgres_engine, committed_pipeline):
    # DELETE_ROWS has its own required-SOURCE_SQL check (separate code path
    # from every other action's, since it doesn't share their common
    # "build select_sql up front" branch in execute()).
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "delete")
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {"SQL_ACTION": "DELETE_ROWS", "TARGET_OBJECT": "public.whatever4", "MERGE_KEY": "id"},
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "delete")

    assert outcome.status == "FAILED"
    assert "SOURCE_SQL" in outcome.message


def test_sql_malformed_source_sql_wrapped_as_handler_error_not_a_crash(
    postgres_engine, committed_pipeline
):
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "bad_sql")
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {
            "SQL_ACTION": "CREATE_TABLE",
            "TARGET_OBJECT": "public.whatever3",
            "SOURCE_SQL": "SELECT this is not valid sql",
        },
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(
        postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "bad_sql"
    )

    assert outcome.status == "FAILED"
    assert "died unexpectedly" not in outcome.message


# ------------------------------------------------------------------------------
# business_rules.py
# ------------------------------------------------------------------------------


def test_business_rules_flags_deactivates_and_skips_rerun(
    postgres_engine, committed_pipeline, data_db_tables
):
    target = f"public.brx_target_{committed_pipeline}"
    bare_table = target.split(".", 1)[1]
    data_db_tables.append(target)
    with postgres_engine.begin() as conn:
        conn.execute(
            text(f"CREATE TABLE {bare_table} (id int, flag varchar, pipeline_run_id bigint)")
        )

    task_id = insert_committed_task(postgres_engine, committed_pipeline, "check", "BUSINESS_RULES")
    insert_committed_business_rule(
        postgres_engine,
        committed_pipeline,
        task_id,
        "bad_flag",
        target,
        "id",
        business_rule_sql="SELECT 1 WHERE t.flag = 'BAD'",
        business_rule_type="REJECT",
    )
    run1 = seed_active_run(postgres_engine, committed_pipeline)
    with postgres_engine.begin() as conn:
        conn.execute(
            text(f"INSERT INTO {target} (id, flag, pipeline_run_id) VALUES (1, 'BAD', :run)"),
            {"run": run1},
        )
        conn.execute(
            text(f"INSERT INTO {target} (id, flag, pipeline_run_id) VALUES (2, 'OK', :run)"),
            {"run": run1},
        )

    outcome = run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "check")
    assert outcome.status == "SUCCESS"
    with postgres_engine.connect() as conn:
        results = conn.execute(
            text(
                "SELECT BUSINESS_RULE_KEY AS key, STATUS AS status, ACTIVE_FLAG AS active_flag "
                "FROM AUD_BUSINESS_RULES_RESULTS"
            )
        ).all()
    assert [(r.key, r.status, r.active_flag) for r in results] == [("1", "REJECT", "Y")]

    # Rerun (same still-active run -> SUCCESS binding short-circuits) proves
    # nothing duplicates; finalize and start a fresh run where id=1 now
    # passes, to prove deactivation.
    with postgres_engine.begin() as conn:
        finalize_pipeline_run_stub(conn, run1)
    run2 = seed_active_run(postgres_engine, committed_pipeline)
    with postgres_engine.begin() as conn:
        conn.execute(
            text(f"UPDATE {target} SET flag = 'OK', pipeline_run_id = :run WHERE id = 1"),
            {"run": run2},
        )
        conn.execute(
            text(f"UPDATE {target} SET pipeline_run_id = :run WHERE id = 2"), {"run": run2}
        )

    outcome2 = run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "check")
    assert outcome2.status == "SUCCESS"
    with postgres_engine.connect() as conn:
        results2 = conn.execute(
            text(
                "SELECT BUSINESS_RULE_KEY AS key, ACTIVE_FLAG AS active_flag "
                "FROM AUD_BUSINESS_RULES_RESULTS WHERE BUSINESS_RULE_KEY = '1'"
            )
        ).all()
    assert [(r.key, r.active_flag) for r in results2] == [("1", "N")]


def test_business_rules_force_scans_all_data(postgres_engine, committed_pipeline, data_db_tables):
    target = f"public.brx_force_{committed_pipeline}"
    bare_table = target.split(".", 1)[1]
    data_db_tables.append(target)
    with postgres_engine.begin() as conn:
        conn.execute(
            text(f"CREATE TABLE {bare_table} (id int, flag varchar, pipeline_run_id bigint)")
        )
        # A row stamped with a pipeline_run_id that will never match the
        # active run -> only found if the check scans everything (--force).
        conn.execute(text(f"INSERT INTO {target} VALUES (99, 'BAD', -1)"))

    task_id = insert_committed_task(postgres_engine, committed_pipeline, "check", "BUSINESS_RULES")
    insert_committed_business_rule(
        postgres_engine,
        committed_pipeline,
        task_id,
        "bad_flag",
        target,
        "id",
        business_rule_sql="SELECT 1 WHERE t.flag = 'BAD'",
        business_rule_type="REPORT",
    )
    seed_active_run(postgres_engine, committed_pipeline)

    scoped = run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "check")
    assert scoped.status == "SUCCESS"
    with postgres_engine.connect() as conn:
        assert (
            conn.execute(text("SELECT COUNT(*) FROM AUD_BUSINESS_RULES_RESULTS")).scalar_one() == 0
        )

    forced = run_task(
        postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "check", force=True
    )
    assert forced.status == "SUCCESS"
    with postgres_engine.connect() as conn:
        results = conn.execute(
            text(
                "SELECT BUSINESS_RULE_KEY AS key, STATUS AS status FROM AUD_BUSINESS_RULES_RESULTS"
            )
        ).all()
    assert [(r.key, r.status) for r in results] == [("99", "REPORT")]


def test_business_rules_same_sequence_number_rules_run_as_one_wave(
    postgres_engine, committed_pipeline, data_db_tables
):
    # "it sequence would be like a dense rank. run in waves. every rule
    # sharing same number for a task can run parallel" — two rules at the
    # same SEQUENCE_NUMBER exercise the ThreadPoolExecutor wave path
    # (single-rule waves take a separate, sequential fast path), and both
    # must still produce correct, independent results.
    target = f"public.brx_wave_{committed_pipeline}"
    bare_table = target.split(".", 1)[1]
    data_db_tables.append(target)
    with postgres_engine.begin() as conn:
        conn.execute(
            text(f"CREATE TABLE {bare_table} (id int, flag varchar, pipeline_run_id bigint)")
        )

    task_id = insert_committed_task(postgres_engine, committed_pipeline, "check", "BUSINESS_RULES")
    insert_committed_business_rule(
        postgres_engine,
        committed_pipeline,
        task_id,
        "flag_a",
        target,
        "id",
        business_rule_sql="SELECT 1 WHERE t.flag = 'A'",
        business_rule_type="REJECT",
        sequence_number=1,
    )
    insert_committed_business_rule(
        postgres_engine,
        committed_pipeline,
        task_id,
        "flag_b",
        target,
        "id",
        business_rule_sql="SELECT 1 WHERE t.flag = 'B'",
        business_rule_type="INCOMPLETE",
        sequence_number=1,
    )
    run_id = seed_active_run(postgres_engine, committed_pipeline)
    with postgres_engine.begin() as conn:
        conn.execute(
            text(f"INSERT INTO {target} VALUES (1, 'A', :run), (2, 'B', :run)"), {"run": run_id}
        )

    outcome = run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "check")

    assert outcome.status == "SUCCESS"
    with postgres_engine.connect() as conn:
        results = conn.execute(
            text(
                "SELECT BUSINESS_RULE_KEY AS key, STATUS AS status FROM AUD_BUSINESS_RULES_RESULTS "
                "ORDER BY BUSINESS_RULE_KEY"
            )
        ).all()
    assert [(r.key, r.status) for r in results] == [("1", "REJECT"), ("2", "INCOMPLETE")]


def test_business_rules_one_bad_rule_in_a_wave_does_not_block_its_wave_mate(
    postgres_engine, committed_pipeline, data_db_tables
):
    # "every rule sharing same number for a task can run parallel" — one
    # rule in the wave has malformed SQL; its wave-mate is independent and
    # must still run to completion and keep its own result, even though the
    # task as a whole still ends up FAILED because of the broken one.
    target = f"public.brx_wave_fail_{committed_pipeline}"
    bare_table = target.split(".", 1)[1]
    data_db_tables.append(target)
    with postgres_engine.begin() as conn:
        conn.execute(
            text(f"CREATE TABLE {bare_table} (id int, flag varchar, pipeline_run_id bigint)")
        )

    task_id = insert_committed_task(postgres_engine, committed_pipeline, "check", "BUSINESS_RULES")
    insert_committed_business_rule(
        postgres_engine,
        committed_pipeline,
        task_id,
        "good_rule",
        target,
        "id",
        business_rule_sql="SELECT 1 WHERE t.flag = 'BAD'",
        business_rule_type="REJECT",
        sequence_number=1,
    )
    bad_rule_id = insert_committed_business_rule(
        postgres_engine,
        committed_pipeline,
        task_id,
        "bad_rule",
        target,
        "id",
        business_rule_sql="this is not valid sql",
        business_rule_type="REJECT",
        sequence_number=1,
    )
    run_id = seed_active_run(postgres_engine, committed_pipeline)
    with postgres_engine.begin() as conn:
        conn.execute(text(f"INSERT INTO {target} VALUES (1, 'BAD', :run)"), {"run": run_id})

    outcome = run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "check")

    assert outcome.status == "FAILED"
    assert "bad_rule" in outcome.message
    with postgres_engine.connect() as conn:
        good_result = conn.execute(
            text(
                "SELECT COUNT(*) FROM AUD_BUSINESS_RULES_RESULTS WHERE BUSINESS_RULE_KEY = '1' "
                "AND BUSINESS_RULE_ID <> :bad_id"
            ),
            {"bad_id": bad_rule_id},
        ).scalar_one()
        bad_rule_status = conn.execute(
            text("SELECT STATUS FROM AUD_BUSINESS_RULES_RUN_LOG WHERE BUSINESS_RULE_ID = :id"),
            {"id": bad_rule_id},
        ).scalar_one()
    assert good_result == 1
    assert bad_rule_status == "FAILED"


def test_business_rules_bad_rule_sql_fails_and_marks_run_log_failed(
    postgres_engine, committed_pipeline, data_db_tables
):
    target = f"public.brx_bad_{committed_pipeline}"
    bare_table = target.split(".", 1)[1]
    data_db_tables.append(target)
    with postgres_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE {bare_table} (id int, pipeline_run_id bigint)"))

    task_id = insert_committed_task(postgres_engine, committed_pipeline, "check", "BUSINESS_RULES")
    business_rule_id = insert_committed_business_rule(
        postgres_engine,
        committed_pipeline,
        task_id,
        "broken",
        target,
        "id",
        business_rule_sql="SELECT this is not valid sql",
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "check")

    assert outcome.status == "FAILED"
    assert "broken" in outcome.message
    with postgres_engine.connect() as conn:
        status = conn.execute(
            text("SELECT STATUS FROM AUD_BUSINESS_RULES_RUN_LOG WHERE BUSINESS_RULE_ID = :id"),
            {"id": business_rule_id},
        ).scalar_one()
    assert status == "FAILED"


# ------------------------------------------------------------------------------
# scripts.py — HANDLER=PYTHON
# ------------------------------------------------------------------------------


def _write_script(tmp_path, name: str, body: str) -> str:
    path = tmp_path / name
    path.write_text(body)
    return str(path)


PYTHON_RETURN_VALUES = "INGESTION_COUNT|LATEST_OFFSET_UPDATE"


def test_python_handler_runs_script_and_records_ingestion_count(
    postgres_engine, committed_pipeline, tmp_path
):
    script = _write_script(
        tmp_path,
        "ok.py",
        "import json\n"
        "print('doing work')\n"
        "print(json.dumps({'INGESTION_COUNT': 7, "
        "'LATEST_OFFSET_UPDATE': '2023-01-01 00:00:00|timestamp'}))\n",
    )
    task_id = insert_committed_task(
        postgres_engine,
        committed_pipeline,
        "ingest",
        "PYTHON",
        script_name=script,
        return_values=PYTHON_RETURN_VALUES,
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(), "TEST_CONCURRENT_PL", "ingest")

    assert outcome.status == "SUCCESS"
    row = _task_run_row(postgres_engine, task_id)
    assert row.source_count == 7
    # "log all the variables... as rows with variable = value semantics"
    assert "INGESTION_COUNT = 7" in row.task_log
    assert "LATEST_OFFSET_UPDATE = 2023-01-01 00:00:00|timestamp" in row.task_log


def test_python_handler_script_failure_becomes_handler_error(
    postgres_engine, committed_pipeline, tmp_path
):
    script = _write_script(
        tmp_path, "bad.py", "import sys\nprint('boom', file=sys.stderr)\nsys.exit(3)\n"
    )
    insert_committed_task(
        postgres_engine,
        committed_pipeline,
        "ingest",
        "PYTHON",
        script_name=script,
        return_values=PYTHON_RETURN_VALUES,
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(), "TEST_CONCURRENT_PL", "ingest")

    assert outcome.status == "FAILED"
    assert "boom" in outcome.message
    assert "exited 3" in outcome.message


def test_python_handler_offset_tracker_insert_then_update(
    postgres_engine, committed_pipeline, tmp_path
):
    script = _write_script(
        tmp_path,
        "offset.py",
        "import json, os\n"
        "value = os.environ.get('ETL_CRAFT_TEST_OFFSET', '100')\n"
        "print(json.dumps({'INGESTION_COUNT': 1, "
        "'LATEST_OFFSET_UPDATE': value + '|number'}))\n",
    )
    task_id = insert_committed_task(
        postgres_engine,
        committed_pipeline,
        "ingest",
        "PYTHON",
        script_name=script,
        return_values=PYTHON_RETURN_VALUES,
    )
    run1 = seed_active_run(postgres_engine, committed_pipeline)

    os.environ["ETL_CRAFT_TEST_OFFSET"] = "100"
    outcome1 = run_task(postgres_engine, make_config(), "TEST_CONCURRENT_PL", "ingest")
    assert outcome1.status == "SUCCESS"
    with postgres_engine.connect() as conn:
        first = conn.execute(
            text(
                "SELECT OFFSET_TYPE AS offset_type, OFFSET_VALUE AS offset_value "
                "FROM AUD_TASK_OFFSET_TRACKER WHERE TASK_ID = :id"
            ),
            {"id": task_id},
        ).one()
    assert (first.offset_type, first.offset_value) == ("NUMBER", "100")

    with postgres_engine.begin() as conn:
        finalize_pipeline_run_stub(conn, run1)
    seed_active_run(postgres_engine, committed_pipeline)
    os.environ["ETL_CRAFT_TEST_OFFSET"] = "200"
    outcome2 = run_task(postgres_engine, make_config(), "TEST_CONCURRENT_PL", "ingest")
    assert outcome2.status == "SUCCESS"
    with postgres_engine.connect() as conn:
        second = conn.execute(
            text("SELECT OFFSET_VALUE AS v FROM AUD_TASK_OFFSET_TRACKER WHERE TASK_ID = :id"),
            {"id": task_id},
        ).scalar_one()
    assert second == "200"
    del os.environ["ETL_CRAFT_TEST_OFFSET"]


def test_python_handler_invalid_offset_type_fails(postgres_engine, committed_pipeline, tmp_path):
    script = _write_script(
        tmp_path,
        "badoffset.py",
        "import json\n"
        "print(json.dumps({'INGESTION_COUNT': 1, 'LATEST_OFFSET_UPDATE': 'x|bogus'}))\n",
    )
    insert_committed_task(
        postgres_engine,
        committed_pipeline,
        "ingest",
        "PYTHON",
        script_name=script,
        return_values=PYTHON_RETURN_VALUES,
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(), "TEST_CONCURRENT_PL", "ingest")

    assert outcome.status == "FAILED"
    assert "BOGUS" in outcome.message


def test_python_handler_no_trailing_json_fails_missing_mandatory_variables(
    postgres_engine, committed_pipeline, tmp_path
):
    # Under the new contract, INGESTION_COUNT/LATEST_OFFSET_UPDATE are
    # always mandatory — a script reporting neither is a real failure, not
    # a quiet no-op the way it was before this contract firmed up.
    script = _write_script(tmp_path, "quiet.py", "print('just a log line, no JSON')\n")
    insert_committed_task(
        postgres_engine,
        committed_pipeline,
        "ingest",
        "PYTHON",
        script_name=script,
        return_values=PYTHON_RETURN_VALUES,
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(), "TEST_CONCURRENT_PL", "ingest")

    assert outcome.status == "FAILED"
    assert "INGESTION_COUNT" in outcome.message
    assert "LATEST_OFFSET_UPDATE" in outcome.message


def test_python_handler_return_values_not_declared_fails_before_running_script(
    postgres_engine, committed_pipeline, tmp_path
):
    # CFG_TASKS.RETURN_VALUES omitted entirely -> caught before the
    # subprocess even runs, per "all of the variable names to expect should
    # be enlisted."
    script = _write_script(tmp_path, "unreachable.py", "raise SystemExit('should never run')\n")
    insert_committed_task(
        postgres_engine, committed_pipeline, "ingest", "PYTHON", script_name=script
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(), "TEST_CONCURRENT_PL", "ingest")

    assert outcome.status == "FAILED"
    assert "RETURN_VALUES" in outcome.message


def test_python_handler_offset_update_missing_pipe_separator_fails(
    postgres_engine, committed_pipeline, tmp_path
):
    script = _write_script(
        tmp_path,
        "nopipe.py",
        "import json\n"
        "print(json.dumps({'INGESTION_COUNT': 1, 'LATEST_OFFSET_UPDATE': 'no_separator_here'}))\n",
    )
    insert_committed_task(
        postgres_engine,
        committed_pipeline,
        "ingest",
        "PYTHON",
        script_name=script,
        return_values=PYTHON_RETURN_VALUES,
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(), "TEST_CONCURRENT_PL", "ingest")

    assert outcome.status == "FAILED"
    assert "value|datatype" in outcome.message


def test_python_handler_ingestion_count_non_numeric_fails(
    postgres_engine, committed_pipeline, tmp_path
):
    script = _write_script(
        tmp_path,
        "badcount.py",
        "import json\n"
        "print(json.dumps({'INGESTION_COUNT': 'not-a-number', "
        "'LATEST_OFFSET_UPDATE': '1|number'}))\n",
    )
    insert_committed_task(
        postgres_engine,
        committed_pipeline,
        "ingest",
        "PYTHON",
        script_name=script,
        return_values=PYTHON_RETURN_VALUES,
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(), "TEST_CONCURRENT_PL", "ingest")

    assert outcome.status == "FAILED"
    assert "expected a number" in outcome.message


# ------------------------------------------------------------------------------
# email_alert.py — HANDLER=EMAIL_ALERT, against real Postgres (Engine DB reads
# only, same as every other handler section here) with smtplib.SMTP mocked —
# no real SMTP server is part of this project's test infra, and proving the
# handler builds/sends the right message doesn't require actually delivering
# one anywhere.
# ------------------------------------------------------------------------------


@pytest.fixture
def fake_smtp(tmp_path, monkeypatch):
    """Replace smtplib.SMTP with a fake that appends each call's effect to a file.

    run_task() forks the actual handler dispatch into a child process
    (runner.py's crash detection) — an in-memory list a fake SMTP client
    appended to would only ever be visible inside that child's own
    copy-on-write memory, never back in this test's own process. A shared
    file on disk is the same workaround the project's own crash-detection
    test already uses for the identical reason (see runner.py's own
    [CHOICE] on why fork, not spawn, and CLAUDE.md's "Bug caught and fixed"
    note on that test) — a real cross-process-visible side effect, not an
    in-memory one.
    """
    log_path = tmp_path / "fake_smtp_events.jsonl"

    class _FakeSMTP:
        def __init__(self, host, port):
            self.host = host
            self.port = port

        def __enter__(self):
            return self

        def __exit__(self, *exc_info):
            return False

        def _log(self, event: dict) -> None:
            with log_path.open("a") as f:
                f.write(json.dumps(event) + "\n")

        def starttls(self):
            self._log({"event": "starttls"})

        def login(self, user, password):
            self._log({"event": "login", "user": user, "password": password})

        def sendmail(self, from_addr, to_addrs, message):
            self._log({"event": "sendmail", "from": from_addr, "to": to_addrs, "message": message})

    monkeypatch.setattr("smtplib.SMTP", _FakeSMTP)
    return log_path


def _read_smtp_events(log_path) -> list[dict]:
    if not log_path.exists():
        return []
    return [json.loads(line) for line in log_path.read_text().splitlines() if line.strip()]


def test_email_alert_sends_with_substituted_subject_and_body(
    postgres_engine, committed_pipeline, fake_smtp
):
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "alert", "EMAIL_ALERT")
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {
            "EMAIL_TO": "a@example.com|b@example.com",
            "EMAIL_SUBJECT": "Pipeline $$pipeline_code failed",
            "EMAIL_BODY": "Run $$pipeline_id, task $$task_code: $$error_message",
        },
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(email=True), "TEST_CONCURRENT_PL", "alert")

    assert outcome.status == "SUCCESS"
    events = _read_smtp_events(fake_smtp)
    sent = [e for e in events if e["event"] == "sendmail"]
    assert len(sent) == 1
    assert sent[0]["to"] == ["a@example.com", "b@example.com"]
    assert "Pipeline TEST_CONCURRENT_PL failed" in sent[0]["message"]
    assert "task alert" in sent[0]["message"]
    row = _task_run_row(postgres_engine, task_id)
    assert "RECIPIENT_COUNT = 2" in row.task_log


def test_email_alert_pulls_error_message_from_watched_failure_task(
    postgres_engine, committed_pipeline, fake_smtp
):
    watched_id = insert_committed_task(postgres_engine, committed_pipeline, "watched")
    insert_committed_task_parameters(
        postgres_engine,
        watched_id,
        {"SQL_ACTION": "CREATE_TABLE", "TARGET_OBJECT": "public.whatever"},
    )
    alert_id = insert_committed_task(postgres_engine, committed_pipeline, "alert", "EMAIL_ALERT")
    insert_committed_task_parameters(
        postgres_engine,
        alert_id,
        {
            "EMAIL_TO": "a@example.com",
            "EMAIL_SUBJECT": "alert",
            "EMAIL_BODY": "reason: $$error_message",
        },
    )
    insert_committed_dependency(
        postgres_engine, committed_pipeline, alert_id, watched_id, dependency_type="FAILURE"
    )
    run_id = seed_active_run(postgres_engine, committed_pipeline)
    watched_run_id = insert_committed_task_run(postgres_engine, watched_id, run_id, "FAILED")
    with postgres_engine.begin() as conn:
        update_task_run(conn, watched_run_id, status="FAILED", error_message="table already exists")

    outcome = run_task(postgres_engine, make_config(email=True), "TEST_CONCURRENT_PL", "alert")

    assert outcome.status == "SUCCESS"
    sent = [e for e in _read_smtp_events(fake_smtp) if e["event"] == "sendmail"]
    assert "reason: table already exists" in sent[0]["message"]


def test_email_alert_password_auth_mode_logs_in(postgres_engine, committed_pipeline, fake_smtp):
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "alert", "EMAIL_ALERT")
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {"EMAIL_TO": "a@example.com", "EMAIL_SUBJECT": "s", "EMAIL_BODY": "b"},
    )
    seed_active_run(postgres_engine, committed_pipeline)
    os.environ.setdefault("ETL_CRAFT_EMAIL_DEV_SECRET", "smtp-secret")

    outcome = run_task(
        postgres_engine,
        make_config(email=True, email_auth_mode="password"),
        "TEST_CONCURRENT_PL",
        "alert",
    )

    assert outcome.status == "SUCCESS"
    events = _read_smtp_events(fake_smtp)
    assert any(e["event"] == "starttls" for e in events)
    logins = [(e["user"], e["password"]) for e in events if e["event"] == "login"]
    assert logins == [("alerts@test.invalid", "smtp-secret")]


def test_email_alert_missing_email_to_fails_clearly(postgres_engine, committed_pipeline, fake_smtp):
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "alert", "EMAIL_ALERT")
    insert_committed_task_parameters(
        postgres_engine, task_id, {"EMAIL_SUBJECT": "s", "EMAIL_BODY": "b"}
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(email=True), "TEST_CONCURRENT_PL", "alert")

    assert outcome.status == "FAILED"
    assert "EMAIL_TO" in outcome.message


def test_email_alert_missing_subject_or_body_fails_clearly(
    postgres_engine, committed_pipeline, fake_smtp
):
    no_subject_id = insert_committed_task(
        postgres_engine, committed_pipeline, "no_subject", "EMAIL_ALERT"
    )
    insert_committed_task_parameters(
        postgres_engine, no_subject_id, {"EMAIL_TO": "a@example.com", "EMAIL_BODY": "b"}
    )
    no_body_id = insert_committed_task(
        postgres_engine, committed_pipeline, "no_body", "EMAIL_ALERT"
    )
    insert_committed_task_parameters(
        postgres_engine, no_body_id, {"EMAIL_TO": "a@example.com", "EMAIL_SUBJECT": "s"}
    )
    seed_active_run(postgres_engine, committed_pipeline)

    no_subject = run_task(
        postgres_engine, make_config(email=True), "TEST_CONCURRENT_PL", "no_subject"
    )
    no_body = run_task(postgres_engine, make_config(email=True), "TEST_CONCURRENT_PL", "no_body")

    assert no_subject.status == "FAILED" and "EMAIL_SUBJECT" in no_subject.message
    assert no_body.status == "FAILED" and "EMAIL_BODY" in no_body.message


def test_email_alert_no_email_section_configured_fails_clearly(
    postgres_engine, committed_pipeline, fake_smtp
):
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "alert", "EMAIL_ALERT")
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {"EMAIL_TO": "a@example.com", "EMAIL_SUBJECT": "s", "EMAIL_BODY": "b"},
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(), "TEST_CONCURRENT_PL", "alert")

    assert outcome.status == "FAILED"
    assert "[Email]" in outcome.message
    assert not _read_smtp_events(fake_smtp)


def test_email_alert_smtp_failure_becomes_handler_error(
    postgres_engine, committed_pipeline, monkeypatch
):
    def _raise(host, port):
        raise OSError("connection refused")

    monkeypatch.setattr("smtplib.SMTP", _raise)
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "alert", "EMAIL_ALERT")
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {"EMAIL_TO": "a@example.com", "EMAIL_SUBJECT": "s", "EMAIL_BODY": "b"},
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(email=True), "TEST_CONCURRENT_PL", "alert")

    assert outcome.status == "FAILED"
    assert "failed to send" in outcome.message


def test_email_alert_pipelines_digest_for_one_named_pipeline(
    postgres_engine, committed_pipeline, fake_smtp
):
    subject_task = insert_committed_task(postgres_engine, committed_pipeline, "watched")
    insert_committed_task_parameters(
        postgres_engine, subject_task, {"SQL_ACTION": "CREATE_TABLE", "TARGET_OBJECT": "public.x"}
    )
    alert_id = insert_committed_task(postgres_engine, committed_pipeline, "alert", "EMAIL_ALERT")
    insert_committed_task_parameters(
        postgres_engine,
        alert_id,
        {
            "EMAIL_TO": "a@example.com",
            "EMAIL_SUBJECT": "digest",
            "EMAIL_PIPELINES": "TEST_CONCURRENT_PL",
        },
    )
    run_id = seed_active_run(postgres_engine, committed_pipeline)
    watched_run_id = insert_committed_task_run(postgres_engine, subject_task, run_id, "SUCCESS")
    with postgres_engine.begin() as conn:
        update_task_run(conn, watched_run_id, status="SUCCESS", target_count=1)

    outcome = run_task(postgres_engine, make_config(email=True), "TEST_CONCURRENT_PL", "alert")

    assert outcome.status == "SUCCESS"
    sent = [e for e in _read_smtp_events(fake_smtp) if e["event"] == "sendmail"]
    assert len(sent) == 1
    message = sent[0]["message"]
    assert "Content-Type: text/html" in message
    assert "TEST_CONCURRENT_PL" in message
    assert "<details>" in message and "<summary>" in message
    assert "watched" in message  # per-task breakdown inside the collapsible section


def test_email_alert_pipelines_digest_unknown_pipeline_does_not_fail_the_task(
    postgres_engine, committed_pipeline, fake_smtp
):
    alert_id = insert_committed_task(postgres_engine, committed_pipeline, "alert", "EMAIL_ALERT")
    insert_committed_task_parameters(
        postgres_engine,
        alert_id,
        {
            "EMAIL_TO": "a@example.com",
            "EMAIL_SUBJECT": "digest",
            "EMAIL_PIPELINES": "TEST_CONCURRENT_PL|NO_SUCH_PIPELINE",
        },
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(email=True), "TEST_CONCURRENT_PL", "alert")

    assert outcome.status == "SUCCESS"
    sent = [e for e in _read_smtp_events(fake_smtp) if e["event"] == "sendmail"]
    assert "NO_SUCH_PIPELINE" in sent[0]["message"]


def test_email_alert_pipelines_all_includes_every_active_pipeline(
    postgres_engine, committed_pipeline, fake_smtp
):
    alert_id = insert_committed_task(postgres_engine, committed_pipeline, "alert", "EMAIL_ALERT")
    insert_committed_task_parameters(
        postgres_engine,
        alert_id,
        {"EMAIL_TO": "a@example.com", "EMAIL_SUBJECT": "digest", "EMAIL_PIPELINES": "ALL"},
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(email=True), "TEST_CONCURRENT_PL", "alert")

    assert outcome.status == "SUCCESS"
    sent = [e for e in _read_smtp_events(fake_smtp) if e["event"] == "sendmail"]
    assert "TEST_CONCURRENT_PL" in sent[0]["message"]


def test_email_alert_pipelines_digest_never_run_pipeline(
    postgres_engine, committed_pipeline, fake_smtp
):
    with postgres_engine.begin() as conn:
        never_run_id = conn.execute(
            text(
                "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
                "VALUES ('TEST_NEVER_RUN_PL', 'Never Run', 'INCREMENTAL') RETURNING PIPELINE_ID"
            )
        ).scalar_one()
    try:
        alert_id = insert_committed_task(
            postgres_engine, committed_pipeline, "alert", "EMAIL_ALERT"
        )
        insert_committed_task_parameters(
            postgres_engine,
            alert_id,
            {
                "EMAIL_TO": "a@example.com",
                "EMAIL_SUBJECT": "digest",
                "EMAIL_PIPELINES": "TEST_NEVER_RUN_PL",
            },
        )
        seed_active_run(postgres_engine, committed_pipeline)

        outcome = run_task(postgres_engine, make_config(email=True), "TEST_CONCURRENT_PL", "alert")

        assert outcome.status == "SUCCESS"
        sent = [e for e in _read_smtp_events(fake_smtp) if e["event"] == "sendmail"]
        assert "NEVER_RUN" in sent[0]["message"]
    finally:
        with postgres_engine.begin() as conn:
            conn.execute(
                text("DELETE FROM CFG_PIPELINES WHERE PIPELINE_ID = :id"), {"id": never_run_id}
            )


def test_email_alert_intro_and_digest_both_present(postgres_engine, committed_pipeline, fake_smtp):
    alert_id = insert_committed_task(postgres_engine, committed_pipeline, "alert", "EMAIL_ALERT")
    insert_committed_task_parameters(
        postgres_engine,
        alert_id,
        {
            "EMAIL_TO": "a@example.com",
            "EMAIL_SUBJECT": "digest",
            "EMAIL_BODY": "Nightly status for $$pipeline_code",
            "EMAIL_PIPELINES": "TEST_CONCURRENT_PL",
        },
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(email=True), "TEST_CONCURRENT_PL", "alert")

    assert outcome.status == "SUCCESS"
    message = [e for e in _read_smtp_events(fake_smtp) if e["event"] == "sendmail"][0]["message"]
    assert "Nightly status for TEST_CONCURRENT_PL" in message
    assert "Pipeline status summary" in message


# ------------------------------------------------------------------------------
# lineage CLI command — cfg.fetch_table_lineage
# ------------------------------------------------------------------------------


def test_cli_lineage_lists_source_and_target_tasks_across_pipelines(
    craft_connector_on_disk, postgres_engine, committed_pipeline, capsys
):
    # Two tasks in the same pipeline: one reads the table (SOURCE_OBJECT),
    # one writes it (TARGET_OBJECT) — "return all the tasks that read the
    # table and write the table," per explicit instruction. A pipe-separated
    # SOURCE_OBJECT with a second, unrelated table proves the multi-value
    # convention is actually parsed, not just exact-matched whole.
    reader_id = insert_committed_task(postgres_engine, committed_pipeline, "reader")
    insert_committed_task_parameters(
        postgres_engine,
        reader_id,
        {
            "SOURCE_OBJECT": "public.lineage_target|public.other_table",
            "TARGET_OBJECT": "public.out",
        },
    )
    writer_id = insert_committed_task(postgres_engine, committed_pipeline, "writer")
    insert_committed_task_parameters(
        postgres_engine,
        writer_id,
        {"SOURCE_OBJECT": "public.in", "TARGET_OBJECT": "public.lineage_target"},
    )
    unrelated_id = insert_committed_task(postgres_engine, committed_pipeline, "unrelated")
    insert_committed_task_parameters(
        postgres_engine,
        unrelated_id,
        {"SOURCE_OBJECT": "public.something_else", "TARGET_OBJECT": "public.another"},
    )

    exit_code = cli_main(["lineage", "--table", "public.lineage_target"])

    assert exit_code == 0
    out = capsys.readouterr().out
    assert "TEST_CONCURRENT_PL.reader\tSOURCE" in out
    assert "TEST_CONCURRENT_PL.writer\tTARGET" in out
    assert "unrelated" not in out


def test_cli_lineage_reports_nothing_for_an_undeclared_table(craft_connector_on_disk):
    exit_code = cli_main(["lineage", "--table", "public.nobody_uses_this"])

    assert exit_code == 0


# ------------------------------------------------------------------------------
# docs_generator.py -- against real Postgres for collect_docs (the one
# DB-touching entry point); rendering itself is covered in test_unit.py.
# ------------------------------------------------------------------------------


def test_collect_docs_includes_pipeline_with_waves_and_steps(postgres_engine, committed_pipeline):
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "t")
    insert_committed_task_parameters(
        postgres_engine, task_id, {"SQL_ACTION": "CREATE_TABLE", "TARGET_OBJECT": "public.x"}
    )

    with postgres_engine.connect() as conn:
        docs = collect_docs(conn)

    entry = next(d for d in docs if d[0].pipeline_code == "TEST_CONCURRENT_PL")
    summary, data = entry
    assert summary.pipeline_name == "Concurrent Test Pipeline"
    assert data.waves == [["t"]]
    assert data.steps[0].task_code == "t"


def test_generate_docs_writes_expected_files(postgres_engine, committed_pipeline, tmp_path):
    output_dir = tmp_path / "docs-site"
    with postgres_engine.connect() as conn:
        generate_docs(conn, output_dir)

    assert (output_dir / "index.html").is_file()
    assert (output_dir / "style.css").is_file()
    assert (output_dir / "search.js").is_file()
    assert (output_dir / "search-index.json").is_file()
    assert (output_dir / "TEST_CONCURRENT_PL.html").is_file()

    index = json.loads((output_dir / "search-index.json").read_text())
    assert any(e["pipeline_code"] == "TEST_CONCURRENT_PL" for e in index)
    assert "TEST_CONCURRENT_PL" in (output_dir / "index.html").read_text()


def test_cli_generate_docs_writes_site_and_reports_output_dir(
    craft_connector_on_disk, committed_pipeline, tmp_path, capsys
):
    output_dir = tmp_path / "cli-docs-site"

    exit_code = cli_main(["generate-docs", "--output", str(output_dir)])

    assert exit_code == 0
    assert (output_dir / "index.html").is_file()
    assert str(output_dir) in capsys.readouterr().out


# ------------------------------------------------------------------------------
# migrate.py
# ------------------------------------------------------------------------------


@pytest.fixture
def migrations_cleanup(postgres_engine):
    """Delete any SCHEMA_MIGRATIONS rows this test's own migration files added."""
    versions: list[str] = []
    yield versions
    if versions:
        with postgres_engine.begin() as conn:
            conn.execute(
                text("DELETE FROM SCHEMA_MIGRATIONS WHERE VERSION = ANY(:versions)"),
                {"versions": versions},
            )


def _write_migration(tmp_path, name: str, body: str):
    (tmp_path / name).write_text(body)


def test_apply_pending_migrations_applies_in_order_and_records_them(
    postgres_engine, tmp_path, migrations_cleanup
):
    _write_migration(
        tmp_path,
        "0001_create_table.sql",
        "CREATE TABLE migrate_test_t1 (id int);",
    )
    _write_migration(
        tmp_path,
        "0002_add_column.sql",
        # Multiple statements in one file, semicolon-split.
        "ALTER TABLE migrate_test_t1 ADD COLUMN name varchar; "
        "INSERT INTO migrate_test_t1 (id, name) VALUES (1, 'a');",
    )
    migrations_cleanup.extend(["0001_create_table.sql", "0002_add_column.sql"])

    try:
        applied = apply_pending_migrations(postgres_engine, tmp_path)
        assert applied == ["0001_create_table.sql", "0002_add_column.sql"]
        with postgres_engine.connect() as conn:
            row = conn.execute(text("SELECT id, name FROM migrate_test_t1")).one()
            assert (row.id, row.name) == (1, "a")
            versions = (
                conn.execute(
                    text(
                        "SELECT VERSION FROM SCHEMA_MIGRATIONS WHERE VERSION LIKE '000%' "
                        "ORDER BY VERSION"
                    )
                )
                .scalars()
                .all()
            )
            assert versions == ["0001_create_table.sql", "0002_add_column.sql"]

        # Idempotent on rerun — nothing pending, nothing re-applied.
        assert apply_pending_migrations(postgres_engine, tmp_path) == []
    finally:
        with postgres_engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS migrate_test_t1"))


def test_apply_pending_migrations_stops_and_does_not_record_a_failed_file(
    postgres_engine, tmp_path, migrations_cleanup
):
    _write_migration(tmp_path, "0001_bad.sql", "this is not valid sql;")
    migrations_cleanup.append("0001_bad.sql")

    with pytest.raises(MigrationError, match="0001_bad.sql"):
        apply_pending_migrations(postgres_engine, tmp_path)

    with postgres_engine.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM SCHEMA_MIGRATIONS WHERE VERSION = '0001_bad.sql'")
        ).scalar_one_or_none()
    assert exists is None


def test_cli_migrate_reports_up_to_date_with_no_pending_files(craft_connector_on_disk, capsys):
    # sql/migrations/ is genuinely empty by design (see its own README) —
    # this exercises the real default directory, not a test-scoped one.
    exit_code = cli_main(["migrate"])

    assert exit_code == 0
    assert "up to date" in capsys.readouterr().out


def test_cli_migrate_reports_applied_files(craft_connector_on_disk, monkeypatch, capsys):
    # Real success-with-results and failure paths both mock
    # apply_pending_migrations directly (same spirit as the runner.dispatch
    # monkeypatch elsewhere) — the function itself is already proven for
    # real against Postgres above; this just proves the CLI wires its
    # result/exception into the right message and exit code.
    monkeypatch.setattr(
        "etl_craft.cli.apply_pending_migrations", lambda engine: ["0001_x.sql", "0002_y.sql"]
    )

    exit_code = cli_main(["migrate"])

    assert exit_code == 0
    out = capsys.readouterr().out
    assert "applied 0001_x.sql" in out
    assert "applied 0002_y.sql" in out


def test_cli_migrate_reports_error_on_failed_migration(
    craft_connector_on_disk, monkeypatch, capsys
):
    def _raise(engine):
        raise MigrationError("0001_bad.sql failed to apply: syntax error")

    monkeypatch.setattr("etl_craft.cli.apply_pending_migrations", _raise)

    exit_code = cli_main(["migrate"])

    assert exit_code == 1
    assert "0001_bad.sql" in capsys.readouterr().err


# ==============================================================================
# cfg.py / cli.py -- `steps` and `history`, the two remaining read-only query
# verbs CLAUDE.md's CLI surface section listed as "conceptually agreed but
# not yet named or built" (dependency graph and table-level lineage were
# already closed by `graph`/`lineage`).
# ==============================================================================


def test_fetch_pipeline_steps_returns_active_tasks_with_their_parameters(
    postgres_engine, committed_pipeline
):
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "t")
    insert_committed_task_parameters(
        postgres_engine, task_id, {"SQL_ACTION": "CREATE_TABLE", "TARGET_OBJECT": "public.x"}
    )

    with postgres_engine.connect() as conn:
        steps = fetch_pipeline_steps(conn, committed_pipeline)

    assert len(steps) == 1
    assert steps[0].task_code == "t"
    assert steps[0].handler == "SQL"
    assert steps[0].parameters == {"SQL_ACTION": "CREATE_TABLE", "TARGET_OBJECT": "public.x"}


def test_fetch_pipeline_run_history_orders_newest_first_and_respects_limit(
    postgres_engine, committed_pipeline
):
    older = insert_committed_pipeline_run(
        postgres_engine,
        committed_pipeline,
        "SUCCESS",
        start_date=datetime.now(UTC) - timedelta(hours=2),
    )
    newer = insert_committed_pipeline_run(postgres_engine, committed_pipeline, "FAILED")

    with postgres_engine.connect() as conn:
        entries = fetch_pipeline_run_history(conn, committed_pipeline, limit=1)

    assert [e.pipeline_run_id for e in entries] == [newer]
    assert entries[0].status == "FAILED"
    assert older != newer  # sanity: the two runs really are distinct rows


def test_fetch_task_run_history_includes_error_message(postgres_engine, committed_pipeline):
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "t")
    run_id = seed_active_run(postgres_engine, committed_pipeline)
    task_run_id = insert_committed_task_run(postgres_engine, task_id, run_id, "FAILED")
    with postgres_engine.begin() as conn:
        update_task_run(conn, task_run_id, status="FAILED", error_message="boom")

    with postgres_engine.connect() as conn:
        entries = fetch_task_run_history(conn, task_id)

    assert len(entries) == 1
    assert entries[0].pipeline_run_id == run_id
    assert entries[0].error_message == "boom"


def test_fetch_failure_watch_messages_reads_latest_error_from_watched_task(
    postgres_engine, committed_pipeline
):
    watched_id = insert_committed_task(postgres_engine, committed_pipeline, "watched")
    alert_id = insert_committed_task(
        postgres_engine, committed_pipeline, "alert", handler="EMAIL_ALERT"
    )
    insert_committed_dependency(
        postgres_engine, committed_pipeline, alert_id, watched_id, dependency_type="FAILURE"
    )
    run_id = seed_active_run(postgres_engine, committed_pipeline)
    watched_run_id = insert_committed_task_run(postgres_engine, watched_id, run_id, "FAILED")
    with postgres_engine.begin() as conn:
        update_task_run(conn, watched_run_id, status="FAILED", error_message="disk full")

    with postgres_engine.connect() as conn:
        messages = fetch_failure_watch_messages(conn, alert_id)

    assert len(messages) == 1
    assert messages[0].depends_on_task_code == "watched"
    assert messages[0].error_message == "disk full"


def test_cli_steps_lists_tasks_and_parameters(
    craft_connector_on_disk, postgres_engine, committed_pipeline, capsys
):
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "t")
    insert_committed_task_parameters(postgres_engine, task_id, {"SQL_ACTION": "CREATE_TABLE"})

    exit_code = cli_main(["steps", "--pipeline_code", "TEST_CONCURRENT_PL"])

    assert exit_code == 0
    out = capsys.readouterr().out
    assert "t\tSQL\tSQL_ACTION=CREATE_TABLE" in out


def test_cli_steps_unknown_pipeline_errors(craft_connector_on_disk, capsys):
    exit_code = cli_main(["steps", "--pipeline_code", "NO_SUCH_PIPELINE"])

    assert exit_code == 1
    assert "no active pipeline" in capsys.readouterr().err


def test_cli_history_pipeline_level(
    craft_connector_on_disk, postgres_engine, committed_pipeline, capsys
):
    insert_committed_pipeline_run(postgres_engine, committed_pipeline, "SUCCESS")

    exit_code = cli_main(["history", "--pipeline_code", "TEST_CONCURRENT_PL"])

    assert exit_code == 0
    out = capsys.readouterr().out
    assert "SUCCESS" in out


def test_cli_history_task_level(
    craft_connector_on_disk, postgres_engine, committed_pipeline, capsys
):
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "t")
    run_id = seed_active_run(postgres_engine, committed_pipeline)
    task_run_id = insert_committed_task_run(postgres_engine, task_id, run_id, "FAILED")
    with postgres_engine.begin() as conn:
        update_task_run(conn, task_run_id, status="FAILED", error_message="kaboom")

    exit_code = cli_main(["history", "--pipeline_code", "TEST_CONCURRENT_PL", "--task_code", "t"])

    assert exit_code == 0
    out = capsys.readouterr().out
    assert "FAILED" in out
    assert "kaboom" in out


def test_cli_history_unknown_task_errors(craft_connector_on_disk, committed_pipeline, capsys):
    exit_code = cli_main(
        ["history", "--pipeline_code", "TEST_CONCURRENT_PL", "--task_code", "no_such_task"]
    )

    assert exit_code == 1
    assert "no active task" in capsys.readouterr().err


def test_cli_history_no_logged_runs_prints_placeholder(
    craft_connector_on_disk, committed_pipeline, capsys
):
    exit_code = cli_main(["history", "--pipeline_code", "TEST_CONCURRENT_PL"])

    assert exit_code == 0
    assert "(no logged runs)" in capsys.readouterr().out


def test_cli_steps_no_active_tasks_prints_placeholder(
    craft_connector_on_disk, committed_pipeline, capsys
):
    exit_code = cli_main(["steps", "--pipeline_code", "TEST_CONCURRENT_PL"])

    assert exit_code == 0
    assert "(no active tasks)" in capsys.readouterr().out


def test_cli_history_task_level_no_logged_runs_prints_placeholder(
    craft_connector_on_disk, postgres_engine, committed_pipeline, capsys
):
    insert_committed_task(postgres_engine, committed_pipeline, "t")

    exit_code = cli_main(["history", "--pipeline_code", "TEST_CONCURRENT_PL", "--task_code", "t"])

    assert exit_code == 0
    assert "(no logged runs)" in capsys.readouterr().out
