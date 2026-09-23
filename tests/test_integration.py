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
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
import yaml
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import OperationalError

import etl_craft.orchestrator as orchestrator_module
import etl_craft.sql_actions as sql_actions_module
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
    fetch_task_run_history,
    resolve_pipeline_id,
    resolve_task_id,
)
from etl_craft.cli import main as cli_main
from etl_craft.cloning import run_cloning_if_enabled
from etl_craft.column_lineage import lineage_for_tasks
from etl_craft.config import (
    CloningConfig,
    ConnectionProfile,
    ConnectionSection,
    ConnectorConfig,
    EmailConfig,
    EmailProfile,
    OrchestratorConfig,
    SourceConfig,
    load_config,
)
from etl_craft.crosspipe import (
    PollBudget,
    _wait_for_pipeline_dependency_to_settle,
    _wait_for_task_dependency_to_settle,
    check_pipeline_dependencies,
    check_task_cross_pipeline_dependencies,
    consume_pipeline_dependency_edges,
    consume_task_dependency_edges,
)
from etl_craft.db import ConnectionError_
from etl_craft.docs_generator import collect_docs, generate_docs
from etl_craft.doctor import run_checks
from etl_craft.documentation import fetch_history, refresh_all, refresh_task_documentation
from etl_craft.execution import HandlerError, HandlerResult
from etl_craft.generate_yml import GLOBAL_DAG_ID, generate_global_dag, generate_pipeline_dag
from etl_craft.init_db import InitDbError, init_db
from etl_craft.migrate import (
    MigrationError,
    apply_pending_migrations,
    mark_packaged_migrations_applied,
)
from etl_craft.orchestrator import (
    OrchestratorModeRefusedError,
    finalize_active_run,
    init_pipeline_run,
    run_pipeline,
    settle_unsatisfiable_tasks,
)
from etl_craft.packaged_sql import packaged_migrations_dir
from etl_craft.resolver import ResolverError, build_graph
from etl_craft.runlog import (
    RunLogError,
    find_or_create_active_run,
    find_or_create_task_run,
    resolve_run_for_task,
    update_task_run,
)
from etl_craft.runner import ForceNotAllowedError, run_task
from etl_craft.setup_command import run_setup
from etl_craft.validate import (
    requested_table_formats,
    validate_business_rule_key_stability,
    validate_business_rule_keys,
    validate_graphs,
    validate_task_parameters,
    validate_warehouse_storage,
)
from etl_craft.warehouse import build_warehouse_engine, open_warehouse, verify_iceberg_catalog

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
    # [DEVIATION, E2-48] The fallback refuses a finished run by default now —
    # binding to one rewrites audit rows that have already been reported on.
    with pytest.raises(RunLogError, match="already FAILED"):
        resolve_run_for_task(pg_conn, cfg_pipeline)

    assert resolve_run_for_task(pg_conn, cfg_pipeline, force=True) == run_id


def test_resolve_run_for_task_advice_is_mode_aware(pg_conn, cfg_pipeline):
    # E2-55. The message used to recommend --force unconditionally -- which
    # Mode=orchestrator refuses outright. So in the mode a real deployment runs
    # in, "clear a failed task and re-run it" had no route AND the error
    # pointed at a flag that would be rejected. Reproduced across all four
    # mode/force combinations during round 3.
    run_id = find_or_create_active_run(pg_conn, cfg_pipeline)
    pg_conn.execute(
        text("UPDATE AUD_PIPELINES_RUN_LOG SET STATUS = 'FAILED' WHERE PIPELINE_RUN_ID = :id"),
        {"id": run_id},
    )

    with pytest.raises(RunLogError) as local_exc:
        resolve_run_for_task(pg_conn, cfg_pipeline, mode="local")
    assert "--force" in str(local_exc.value)

    with pytest.raises(RunLogError) as orch_exc:
        resolve_run_for_task(pg_conn, cfg_pipeline, mode="orchestrator")
    message = str(orch_exc.value)
    assert "--init-only" in message
    assert "NEW pipeline_run_id" in message
    # It must not recommend a flag this mode refuses.
    assert "pass --force" not in message


def test_resolve_run_for_task_still_binds_to_a_skipped_run(pg_conn, cfg_pipeline):
    # E2-48's guard must not catch SKIPPED. orchestrator.py writes that status
    # when a pipeline's own cross-pipeline gate was never met, precisely so
    # every task binding to it records SKIPPED in turn — binding there is
    # additive and intended, unlike binding to a SUCCESS or FAILED run.
    run_id = find_or_create_active_run(pg_conn, cfg_pipeline)
    pg_conn.execute(
        text("UPDATE AUD_PIPELINES_RUN_LOG SET STATUS = 'SKIPPED' WHERE PIPELINE_RUN_ID = :id"),
        {"id": run_id},
    )

    assert resolve_run_for_task(pg_conn, cfg_pipeline) == run_id


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
# point of build_warehouse_engine's design is that it never hardcodes a driver;
# it asks SQLAlchemy's own resolved dialect for connect args at pool-
# checkout time (see warehouse.py's module docstring). Pointing it at this
# same Postgres container with dialect "postgresql+psycopg" still proves
# that exact generic mechanism executes for real, end to end — a mocked
# DBAPI (as in test_unit.py) can't prove that part.


def test_build_warehouse_engine_connects_for_real(monkeypatch, postgres_engine):
    # postgres_engine is otherwise unused here — build_warehouse_engine opens its
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

    engine = build_warehouse_engine(config)
    try:
        with engine.connect() as conn:
            assert conn.execute(text("SELECT 1")).scalar_one() == 1
    finally:
        engine.dispose()


def test_hash_expression_yields_the_same_32_hex_chars_on_both_warehouses(
    postgres_engine, duckdb_engine
):
    # [DEVIATION, 2026-09-20] Was a ClickHouse test proving its MD5 needed
    # hex()-ing. ClickHouse is gone; the point now is the opposite and
    # stronger one -- Postgres and DuckDB produce the *identical* HASH_KEY for
    # the same row, so an SCD target is portable between them and
    # VARCHAR(32) is right for both.
    expr = sql_actions_module._hash_expression(["a", "b"], "s")
    sql = f"SELECT {expr} FROM (SELECT 'x' AS a, CAST(NULL AS VARCHAR) AS b) AS s"

    with postgres_engine.connect() as conn:
        pg_value = conn.execute(text(sql)).scalar_one()
    with duckdb_engine.connect() as conn:
        duck_value = conn.execute(text(sql)).scalar_one()

    assert len(pg_value) == 32
    assert set(pg_value) <= set("0123456789abcdef")
    assert pg_value == duck_value


def test_build_warehouse_engine_connects_to_real_duckdb(tmp_path):
    # Unlike the Postgres-standing-in-for-"some dialect" test above, this
    # genuinely proves the generic, dialect-agnostic connect mechanism against
    # a real *different* SQLAlchemy dialect -- the actual point of
    # warehouse.py never hardcoding a driver.
    #
    # It also covers auth_mode='none': DuckDB is a file, so there is no user
    # to be and no password to present, and requiring one would mean inventing
    # a secret that authenticates nothing.
    path = tmp_path / "warehouse.duckdb"
    profile = ConnectionProfile(
        section="WAREHOUSE",
        name="dev",
        jdbc_url=f"jdbc:duckdb:{path}",
        user="",
        auth_mode="none",
    )
    config = ConnectorConfig(
        mode="local",
        source=SourceConfig(type="environment"),
        postgres=ConnectionSection(active_profile="dev", profiles={"dev": profile}),
        cloning=CloningConfig(enabled=False),
        warehouse=ConnectionSection(active_profile="dev", profiles={"dev": profile}),
    )

    engine = build_warehouse_engine(config)
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE probe AS SELECT 1 AS id"))
            assert conn.execute(text("SELECT id FROM probe")).scalar_one() == 1
            # The catalog DuckDB derives from the file stem, which is what
            # qualify()'s three-part catalog.schema.table form needs.
            assert conn.execute(text("SELECT current_catalog()")).scalar_one() == "warehouse"
    finally:
        engine.dispose()


# ==============================================================================
# cloning.py — against real Postgres (Engine DB always) and, for the actual
# copy mechanism, real DuckDB as the warehouse -- proving the generic mirroring
# mechanism against a genuinely different dialect, the same "prove it for
# real" bar warehouse.py's own second-warehouse test already set. Testing
# against Postgres-as-both-roles is deliberately *not* done for the real
# copy path: since every mirrored table keeps its Engine DB name, doing so
# would mean truncating and reinserting the actual CFG_/AUD_ tables from
# their own reflection -- exactly the destructive scenario
# cloning.run_cloning_if_enabled's own same-database guard exists to refuse.
# ==============================================================================


def _duckdb_warehouse_config(tmp_path, *, cloning: CloningConfig) -> ConnectorConfig:
    """Engine DB on real Postgres, warehouse on a throwaway DuckDB file."""
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
                    jdbc_url=f"jdbc:duckdb:{tmp_path / 'warehouse.duckdb'}",
                    user="",
                    auth_mode="none",
                )
            },
        ),
    )


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


def test_run_cloning_clones_cfg_tables_into_real_duckdb(
    tmp_path,
    postgres_engine,
    committed_pipeline,
):
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "t")
    insert_committed_task_parameters(
        postgres_engine, task_id, {"SQL_ACTION": "CREATE_TABLE", "TARGET_OBJECT": "public.x"}
    )
    config = _duckdb_warehouse_config(tmp_path, cloning=CloningConfig(enabled=True, scope="cfg"))

    run_cloning_if_enabled(postgres_engine, config)

    with build_warehouse_engine(config).connect() as conn:
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
    tmp_path,
    postgres_engine,
    committed_pipeline,
):
    with postgres_engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_PIPELINES SET PIPELINE_PARAMETERS = :params WHERE PIPELINE_ID = :id"),
            {"params": json.dumps({"CATCHUP": True}), "id": committed_pipeline},
        )
    config = _duckdb_warehouse_config(tmp_path, cloning=CloningConfig(enabled=True, scope="cfg"))

    run_cloning_if_enabled(postgres_engine, config)

    with build_warehouse_engine(config).connect() as conn:
        params = conn.execute(
            text("SELECT pipeline_parameters FROM cfg_pipelines WHERE pipeline_id = :id"),
            {"id": committed_pipeline},
        ).scalar_one()
    assert json.loads(params) == {"CATCHUP": True}


def test_run_cloning_is_idempotent_across_repeated_runs(
    tmp_path,
    postgres_engine,
    committed_pipeline,
):
    config = _duckdb_warehouse_config(tmp_path, cloning=CloningConfig(enabled=True, scope="cfg"))

    run_cloning_if_enabled(postgres_engine, config)
    run_cloning_if_enabled(postgres_engine, config)

    with build_warehouse_engine(config).connect() as conn:
        count = conn.execute(
            text("SELECT count(*) FROM cfg_pipelines WHERE pipeline_id = :id"),
            {"id": committed_pipeline},
        ).scalar_one()
    assert count == 1


@pytest.fixture
def second_postgres_database(postgres_engine):
    """A genuinely separate, schema-less Postgres database on the same server as the Engine DB.

    For proving cloning's create-target-table path against Postgres itself
    for real against a second Postgres: pointing [Warehouse] at *this* same server's default
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
    tmp_path,
    postgres_engine,
    committed_pipeline,
):
    seed_active_run(postgres_engine, committed_pipeline)
    config = _duckdb_warehouse_config(tmp_path, cloning=CloningConfig(enabled=True, scope="aud"))

    run_cloning_if_enabled(postgres_engine, config)

    warehouse_engine = build_warehouse_engine(config)
    with warehouse_engine.connect() as conn:
        assert not inspect(warehouse_engine).has_table("cfg_pipelines")
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


def test_validate_business_rule_keys_sees_a_duckdb_primary_key(
    pg_conn, cfg_pipeline, cfg_task, duckdb_engine
):
    # duckdb_engine does not reflect primary keys: Inspector.get_pk_constraint
    # returns an empty constrained_columns list even for a table DuckDB is
    # genuinely enforcing one on (verified directly -- a duplicate insert
    # fails at commit). Taken at face value that makes validate report *every*
    # target on the newly-primary warehouse as having no primary key, so the
    # check that enforces CLAUDE.md's single-column-PK convention fails
    # exactly where the convention is being honoured. Same shape as E2-53: a
    # code path that only ever ran against one dialect.
    with duckdb_engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA staging"))
        conn.execute(text("CREATE TABLE staging.dim (id BIGINT PRIMARY KEY, val TEXT)"))
    _insert_business_rule(pg_conn, cfg_pipeline, cfg_task, "br1", "staging.dim", "ID")

    assert validate_business_rule_keys(pg_conn, duckdb_engine) == []


def test_validate_business_rule_keys_reports_a_duckdb_table_with_no_pk(
    pg_conn, cfg_pipeline, cfg_task, duckdb_engine
):
    # The counterpart, so the fix above cannot be "return [] on duckdb": a
    # DuckDB table genuinely without a primary key must still be reported.
    with duckdb_engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA staging"))
        conn.execute(text("CREATE TABLE staging.dim (id BIGINT, val TEXT)"))
    _insert_business_rule(pg_conn, cfg_pipeline, cfg_task, "br1", "staging.dim", "ID")

    issues = validate_business_rule_keys(pg_conn, duckdb_engine)

    assert len(issues) == 1
    assert "must have exactly one primary key column" in issues[0].message


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


def test_validate_business_rule_key_need_not_be_the_primary_key(
    pg_conn, cfg_pipeline, cfg_task, postgres_engine
):
    # [DEVIATION, 2026-09-22] This asserted the opposite until it turned out to
    # force the broken configuration. E2-54 made ROW_ID the primary key of
    # every engine-created table, so requiring BUSINESS_RULE_KEY_COLUMN to
    # *equal* the primary key meant every rule had to key on ROW_ID -- which a
    # full-replace action regenerates, so its flags could never be deactivated.
    # Naming the stable business key, which is the correct choice, failed
    # validation.
    #
    # CLAUDE.md's convention is that a target *has* a single-column primary
    # key, "which is why BUSINESS_RULE_KEY_COLUMN can safely stay a single
    # column rather than a list" -- never that they are the same column.
    with postgres_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS validate_pk_test_natural"))
        conn.execute(
            text(
                "CREATE TABLE validate_pk_test_natural "
                "(ROW_ID INT PRIMARY KEY, cust_id INT, val INT)"
            )
        )
    try:
        _insert_business_rule(
            pg_conn, cfg_pipeline, cfg_task, "br1", "validate_pk_test_natural", "cust_id"
        )

        assert validate_business_rule_keys(pg_conn, postgres_engine) == []
    finally:
        with postgres_engine.begin() as conn:
            conn.execute(text("DROP TABLE validate_pk_test_natural"))


def test_validate_business_rule_key_must_exist_on_the_target(
    pg_conn, cfg_pipeline, cfg_task, postgres_engine
):
    # Relaxing "must be the primary key" must not relax "must be a real
    # column" -- a typo'd key column silently flags nothing.
    with postgres_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS validate_pk_test_missing_col"))
        conn.execute(
            text("CREATE TABLE validate_pk_test_missing_col (ROW_ID INT PRIMARY KEY, val INT)")
        )
    try:
        _insert_business_rule(
            pg_conn, cfg_pipeline, cfg_task, "br1", "validate_pk_test_missing_col", "no_such_col"
        )

        issues = validate_business_rule_keys(pg_conn, postgres_engine)

        assert len(issues) == 1
        assert "no_such_col" in issues[0].message
    finally:
        with postgres_engine.begin() as conn:
            conn.execute(text("DROP TABLE validate_pk_test_missing_col"))


def test_validate_flags_a_business_rule_keyed_on_a_regenerated_row_id(
    pg_conn, cfg_pipeline, cfg_task
):
    # The defect this whole check exists for. A flag is recorded against a
    # BUSINESS_RULE_KEY_COLUMN value and deactivated only for keys the "no
    # longer violates" query returns -- which means keys still in the target.
    # OVERWRITE_TABLE regenerates every ROW_ID, so a flagged key never comes
    # back: on Postgres the flag points at nothing forever, and on Iceberg the
    # value is reused, so it comes back pointing at a different row entirely.
    # Reproduced both ways against real warehouses.
    _insert_task_parameters(
        pg_conn,
        cfg_task,
        {"SQL_ACTION": "OVERWRITE_TABLE", "TARGET_OBJECT": "public.brk_target"},
    )
    _insert_business_rule(pg_conn, cfg_pipeline, cfg_task, "br1", "public.brk_target", "ROW_ID")

    issues = validate_business_rule_key_stability(pg_conn)

    assert len(issues) == 1
    assert "regenerates every ROW_ID" in issues[0].message


def test_validate_allows_row_id_as_a_key_on_a_merge_target(pg_conn, cfg_pipeline, cfg_task):
    # SCD1_MERGE updates rows in place, so ROW_ID survives and is a legitimate
    # key there. The check must not fire for it.
    _insert_task_parameters(
        pg_conn,
        cfg_task,
        {"SQL_ACTION": "SCD1_MERGE", "TARGET_OBJECT": "public.brk_merge"},
    )
    _insert_business_rule(pg_conn, cfg_pipeline, cfg_task, "br1", "public.brk_merge", "ROW_ID")

    assert validate_business_rule_key_stability(pg_conn) == []


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
    # E2-48: all_success, not all_done — a root task whose run was never
    # minted must not start and fall back into the previous, finalized run.
    assert dag["tasks"]["test_task"]["depends_on"] == ["__init__"]
    assert dag["tasks"]["test_task"]["trigger_rule"] == "all_success"
    assert dag["tasks"]["task_b"]["depends_on"] == ["test_task"]
    assert dag["tasks"]["task_b"]["trigger_rule"] == "all_success"
    # __finalize__ depends only on the leaf (task_b) — test_task has
    # something downstream of it, so it isn't a leaf.
    assert dag["tasks"]["__finalize__"]["depends_on"] == ["task_b"]
    assert dag["tasks"]["__finalize__"]["trigger_rule"] == "all_done"
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

    assert dag["tasks"]["__finalize__"]["depends_on"] == ["branch_a", "branch_b"]


def test_generate_pipeline_dag_maps_run_condition_onto_airflow_trigger_rules(
    pg_conn, cfg_pipeline, cfg_task
):
    # E2-41's stated reason for putting RUN_CONDITION on CFG_TASKS rather than
    # making it an OR-group on CFG_TASK_DEPENDENCY: "these should be resolved
    # while dag chain generation itself". This is that resolution.
    second_upstream = pg_conn.execute(
        text(
            "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
            "VALUES ('upstream_b', 'ETL', :pipeline_id, 'SQL') RETURNING TASK_ID"
        ),
        {"pipeline_id": cfg_pipeline},
    ).scalar_one()
    any_task = pg_conn.execute(
        text(
            "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER, RUN_CONDITION) "
            "VALUES ('any_task', 'ETL', :pipeline_id, 'SQL', 'ANY') RETURNING TASK_ID"
        ),
        {"pipeline_id": cfg_pipeline},
    ).scalar_one()
    for upstream in (cfg_task, second_upstream):
        pg_conn.execute(
            text(
                "INSERT INTO CFG_TASK_DEPENDENCY (PIPELINE_ID, TASK_ID, DEPENDS_ON_PIPELINE_ID, "
                "DEPENDS_ON_TASK_ID, DEPENDENCY_TYPE) "
                "VALUES (:pipeline_id, :task, :pipeline_id, :upstream, 'SUCCESS')"
            ),
            {"pipeline_id": cfg_pipeline, "task": any_task, "upstream": upstream},
        )

    dag = generate_pipeline_dag(pg_conn, make_config(), "TEST_PL")

    # ANY + SUCCESS -> one_success, on every one of the task's edges. A
    # default (NULL) RUN_CONDITION on the same edge type stays all_success.
    assert dag["tasks"]["any_task"]["depends_on"] == ["test_task", "upstream_b"]
    assert dag["tasks"]["any_task"]["trigger_rule"] == "one_success"


def test_generate_pipeline_dag_emits_one_trigger_rule_per_task(pg_conn, cfg_pipeline, cfg_task):
    # E2-46 regression. Airflow's trigger_rule is a property of the TASK — one
    # value applied to all its upstreams — but this module emitted it per
    # depends_on edge, which only works while every edge of a task shares one
    # DEPENDENCY_TYPE. Nothing requires that: DEPENDENCY_TYPE is a per-row
    # value on CFG_TASK_DEPENDENCY. A SUCCESS edge plus an ALWAYS edge emitted
    # two conflicting rules and handed a loader a choice it cannot make — the
    # very failure that emitting trigger_rule instead of dependency_type was
    # meant to prevent.
    second = pg_conn.execute(
        text(
            "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
            "VALUES ('mixed_up_b', 'ETL', :pipeline_id, 'SQL') RETURNING TASK_ID"
        ),
        {"pipeline_id": cfg_pipeline},
    ).scalar_one()
    mixed = pg_conn.execute(
        text(
            "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
            "VALUES ('mixed_task', 'ETL', :pipeline_id, 'SQL') RETURNING TASK_ID"
        ),
        {"pipeline_id": cfg_pipeline},
    ).scalar_one()
    for upstream, dep_type in ((cfg_task, "SUCCESS"), (second, "ALWAYS")):
        pg_conn.execute(
            text(
                "INSERT INTO CFG_TASK_DEPENDENCY (PIPELINE_ID, TASK_ID, DEPENDS_ON_PIPELINE_ID, "
                "DEPENDS_ON_TASK_ID, DEPENDENCY_TYPE) "
                "VALUES (:pipeline_id, :task, :pipeline_id, :upstream, :dep_type)"
            ),
            {
                "pipeline_id": cfg_pipeline,
                "task": mixed,
                "upstream": upstream,
                "dep_type": dep_type,
            },
        )

    dag = generate_pipeline_dag(pg_conn, make_config(), "TEST_PL")

    task = dag["tasks"]["mixed_task"]
    assert task["depends_on"] == ["test_task", "mixed_up_b"]
    # One rule, and the permissive one — Airflow starts it, the engine's own
    # per-edge gate decides. Same documented fail-safe as N and HAS_DATA.
    assert task["trigger_rule"] == "all_done"


def test_generate_pipeline_dag_with_no_tasks_still_has_init(pg_conn, cfg_pipeline):
    dag = generate_pipeline_dag(pg_conn, make_config(), "TEST_PL")
    assert set(dag["tasks"]) == {"__init__", "__finalize__"}
    # No real tasks at all -> __finalize__ falls back to depending on
    # __init__ directly, same as any real task with no dependencies would.
    assert dag["tasks"]["__finalize__"]["depends_on"] == ["__init__"]


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
        "depends_on": ["TEST_GLOBALDAG_UP"],
        "trigger_rule": "all_success",
    }
    # The upstream pipeline is included too (nothing it depends on itself),
    # since something else depending on it still needs a node to trigger.
    assert dag["pipelines"]["TEST_GLOBALDAG_UP"] == {
        "trigger_dag_id": "TEST_GLOBALDAG_UP",
        "depends_on": [],
        "trigger_rule": "all_success",
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
    assert check_pipeline_dependencies(postgres_engine, downstream_id).satisfied


def test_check_pipeline_dependencies_not_satisfied_with_no_upstream_run(
    postgres_engine, two_committed_pipelines
):
    downstream_id, upstream_id = two_committed_pipelines
    insert_committed_pipeline_dependency(postgres_engine, downstream_id, upstream_id, "SUCCESS")

    reason = check_pipeline_dependencies(postgres_engine, downstream_id).reason

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

    assert check_pipeline_dependencies(postgres_engine, downstream_id).satisfied


def test_check_pipeline_dependencies_failure_type_satisfied_by_failed_run(
    postgres_engine, two_committed_pipelines
):
    downstream_id, upstream_id = two_committed_pipelines
    insert_committed_pipeline_dependency(postgres_engine, downstream_id, upstream_id, "FAILURE")
    insert_committed_pipeline_run(
        postgres_engine, upstream_id, "SUCCESS", end_date=datetime.now(UTC)
    )

    assert not check_pipeline_dependencies(postgres_engine, downstream_id).satisfied

    insert_committed_pipeline_run(
        postgres_engine, upstream_id, "FAILED", end_date=datetime.now(UTC)
    )

    assert check_pipeline_dependencies(postgres_engine, downstream_id).satisfied


def test_check_pipeline_dependencies_always_type_satisfied_by_any_terminal_status(
    postgres_engine, two_committed_pipelines
):
    downstream_id, upstream_id = two_committed_pipelines
    insert_committed_pipeline_dependency(postgres_engine, downstream_id, upstream_id, "ALWAYS")
    insert_committed_pipeline_run(
        postgres_engine, upstream_id, "SKIPPED", end_date=datetime.now(UTC)
    )

    assert check_pipeline_dependencies(postgres_engine, downstream_id).satisfied


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

    assert not check_pipeline_dependencies(postgres_engine, downstream_id).satisfied

    insert_committed_task_run(postgres_engine, upstream_task_id, run_id, "SUCCESS", target_count=5)

    assert check_pipeline_dependencies(postgres_engine, downstream_id).satisfied


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
    assert not check_pipeline_dependencies(postgres_engine, downstream_id).satisfied


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
        postgres_engine,
        upstream_id,
        sleep=fake_sleep,
        now=lambda: datetime.now(UTC),
        budget=PollBudget.start(lambda: datetime.now(UTC)),
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
        postgres_engine,
        upstream_id,
        sleep=clock.sleep,
        now=clock.now,
        budget=PollBudget.start(clock.now),
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
        postgres_engine,
        upstream_id,
        sleep=jump_sleep,
        now=clock.now,
        budget=PollBudget.start(clock.now),
    )

    assert len(clock.sleeps) == 1


def test_check_task_cross_pipeline_dependencies_satisfied_when_no_edges(
    postgres_engine, two_committed_pipelines
):
    downstream_id, _ = two_committed_pipelines
    downstream_task_id = insert_committed_task(postgres_engine, downstream_id, "task_a")
    check = check_task_cross_pipeline_dependencies(postgres_engine, downstream_task_id)
    assert (check.total, check.satisfied_count, check.reasons) == (0, 0, ())


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

    check = check_task_cross_pipeline_dependencies(postgres_engine, downstream_task_id)
    assert (check.total, check.satisfied_count) == (1, 0)
    assert check.reasons

    insert_committed_task_run(postgres_engine, upstream_task_id, run_id, "SUCCESS")

    check = check_task_cross_pipeline_dependencies(postgres_engine, downstream_task_id)
    assert (check.total, check.satisfied_count, check.reasons) == (1, 1, ())


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

    assert check_task_cross_pipeline_dependencies(postgres_engine, downstream_task_id).reasons

    # A second, later run of the upstream task that genuinely reported data
    # — ux_task_run_one_per_pipeline_run means one row per (task, run), so
    # this needs its own pipeline run, same as a real second execution would.
    second_run_id = insert_committed_pipeline_run(
        postgres_engine, upstream_id, "SUCCESS", end_date=datetime.now(UTC)
    )
    insert_committed_task_run(
        postgres_engine, upstream_task_id, second_run_id, "SUCCESS", target_count=5
    )

    assert check_task_cross_pipeline_dependencies(postgres_engine, downstream_task_id).reasons == ()


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
        postgres_engine,
        upstream_task_id,
        sleep=fake_sleep,
        now=lambda: datetime.now(UTC),
        budget=PollBudget.start(lambda: datetime.now(UTC)),
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
        postgres_engine,
        upstream_task_id,
        sleep=jump_sleep,
        now=clock.now,
        budget=PollBudget.start(clock.now),
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
    duckdb_warehouse: str = "",
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
    if duckdb_warehouse:
        # [DEVIATION, 2026-09-20] Was `clickhouse_warehouse`. The gap this
        # exists to close is unchanged and still the important one: every
        # other SQL-action test points [Warehouse] at the same Postgres, so
        # without this the whole action vocabulary is exercised against
        # exactly one dialect. DuckDB is the second supported warehouse now,
        # and being embedded it needs no container.
        warehouse_section = ConnectionSection(
            active_profile="dev",
            profiles={
                "dev": ConnectionProfile(
                    section="WAREHOUSE",
                    name="dev",
                    jdbc_url=f"jdbc:duckdb:{duckdb_warehouse}",
                    user="",
                    auth_mode="none",
                )
            },
        )
    elif warehouse:
        # Same test Postgres, standing in as the warehouse — same pattern
        # test_build_warehouse_engine_connects_for_real above uses. Needs
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
    # binding, then fail on dispatch (HANDLER=SQL needs a warehouse to run
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


def test_run_task_times_out_a_wedged_handler_instead_of_hanging(
    postgres_engine, committed_pipeline, monkeypatch
):
    # E2-17. An unbounded join() on a hung handler left the row stuck
    # IN-PROGRESS forever -- and IN-PROGRESS is in resolver.NOT_RETRYABLE, so
    # that task became permanently un-retryable without someone editing
    # AUD_TASK_RUN_LOG by hand. The pipeline could never recover.
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "wedged_task")
    insert_committed_task_parameters(postgres_engine, task_id, {"TASK_TIMEOUT_SECONDS": "1"})
    seed_active_run(postgres_engine, committed_pipeline)

    def _hang(engine, ctx):
        time.sleep(60)

    monkeypatch.setattr("etl_craft.runner.dispatch", _hang)

    outcome = run_task(postgres_engine, make_config(), "TEST_CONCURRENT_PL", "wedged_task")

    assert outcome.status == "FAILED"
    assert "timeout" in outcome.message
    row = _task_run_row(postgres_engine, task_id)
    assert row.status == "FAILED"


def test_run_task_counts_attempts_and_resets_the_row_on_a_retry(
    postgres_engine, committed_pipeline, monkeypatch
):
    # E2-21. One row per task per run is load-bearing, so attempts are counted
    # within the row. The reset matters too: START_DATE previously spanned
    # from the first attempt, and that duration feeds the cross-pipeline poll
    # cadence.
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "retried_task")
    seed_active_run(postgres_engine, committed_pipeline)

    def _fail(engine, ctx):
        raise HandlerError("first attempt blew up")

    monkeypatch.setattr("etl_craft.runner.dispatch", _fail)
    assert (
        run_task(postgres_engine, make_config(), "TEST_CONCURRENT_PL", "retried_task").status
        == "FAILED"
    )

    monkeypatch.setattr(
        "etl_craft.runner.dispatch", lambda engine, ctx: HandlerResult(target_count=5)
    )
    assert (
        run_task(postgres_engine, make_config(), "TEST_CONCURRENT_PL", "retried_task").status
        == "SUCCESS"
    )

    with postgres_engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT ATTEMPT_COUNT AS attempt_count, STATUS AS status, "
                "ERROR_MESSAGE AS error_message, TARGET_COUNT AS target_count "
                "FROM AUD_TASK_RUN_LOG WHERE TASK_ID = :id"
            ),
            {"id": task_id},
        ).one()
    assert row.attempt_count == 2
    assert row.status == "SUCCESS"
    # The failed attempt's error is gone, not carried into the successful one.
    assert row.error_message is None
    assert row.target_count == 5


def test_validate_flags_a_has_data_edge_on_a_handler_with_no_row_count(
    postgres_engine, committed_pipeline, craft_connector_on_disk, capsys
):
    # E2-59. HAS_DATA means "upstream SUCCESS and TARGET_COUNT > 0", and
    # BUSINESS_RULES reports no row count -- so the edge can never be
    # satisfied. E2-01's unsatisfiable() made that *worse*: the downstream task
    # is now silently recorded SKIPPED and the run finalizes SUCCESS, where
    # before it at least showed up as stuck.
    br_id = insert_committed_task(
        postgres_engine, committed_pipeline, "hd_rules", handler="BUSINESS_RULES"
    )
    downstream = insert_committed_task(postgres_engine, committed_pipeline, "hd_after")
    insert_committed_dependency(
        postgres_engine, committed_pipeline, downstream, br_id, dependency_type="HAS_DATA"
    )

    assert cli_main(["validate"]) == 1

    out = capsys.readouterr().out
    assert "[dependency]" in out
    assert "never reports a TARGET_COUNT" in out


def test_validate_requires_an_email_alert_to_wait_on_every_leaf(
    postgres_engine, committed_pipeline, craft_connector_on_disk, capsys
):
    # E2-60. EMAIL_ALERT is a pipeline-level completion alert (E2-43) whose
    # flavour is computed from every task's status -- but nothing made it run
    # last. Gated on one task rather than every leaf, it runs mid-flight, sees
    # unsettled tasks, and sends the amber "something went wrong" email for a
    # run that goes on to finish cleanly.
    first = insert_committed_task(postgres_engine, committed_pipeline, "leaf_one")
    second = insert_committed_task(postgres_engine, committed_pipeline, "leaf_two")
    alert = insert_committed_task(
        postgres_engine, committed_pipeline, "leaf_alert", handler="EMAIL_ALERT"
    )
    # Waits on leaf_one only; leaf_two is a leaf it ignores.
    insert_committed_dependency(
        postgres_engine, committed_pipeline, alert, first, dependency_type="ALWAYS"
    )
    insert_committed_dependency(
        postgres_engine, committed_pipeline, second, first, dependency_type="SUCCESS"
    )

    assert cli_main(["validate"]) == 1

    out = capsys.readouterr().out
    assert "[alert_ordering]" in out
    assert "leaf_two" in out


def test_validate_flags_an_incomplete_sql_task(
    postgres_engine, committed_pipeline, craft_connector_on_disk, capsys
):
    # E2-25. These are conventions the execution code already depends on;
    # checking them here means a config mistake surfaces from `validate`
    # rather than from a task failing at 3 a.m. halfway through a run.
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "incomplete")
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {
            "SOURCE_OBJECT": "public.src",
            "TARGET_OBJECT": "public.tgt",
            "SQL_ACTION": "SCD1_MERGE",
            "SOURCE_SQL": "SELECT 1 AS id FROM public.src",
            # MERGE_KEY and MERGE_COMPARE_COLUMNS are missing, and this one is
            # a typo that would otherwise be silently ignored at runtime.
            "MEREG_KEY": "id",
        },
    )

    assert cli_main(["validate"]) == 1

    out = capsys.readouterr().out
    assert "requires a MERGE_KEY parameter" in out
    assert "MEREG_KEY" in out


def test_run_task_does_not_clobber_an_already_in_progress_row(postgres_engine, committed_pipeline):
    # E2-02 regression, reproduced against real Postgres during the iteration-1
    # review. resolver.ready() excludes an IN-PROGRESS task deliberately
    # ("never re-dispatch"), and run_task used to read that exclusion as
    # "dependencies not met" and call _bind_as_skipped — which overwrote the
    # *live* row of a task that was still executing. That lied in the audit
    # log, disarmed the original process's crash detection (it only writes
    # FAILED while the row still reads IN-PROGRESS), and made the final status
    # depend on which process happened to write last.
    #
    # Reachable by ordinary means: an Airflow retry firing while the first
    # attempt still runs, or a human running a task the local orchestrator
    # already spawned.
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "inflight_task")
    run_id = seed_active_run(postgres_engine, committed_pipeline)
    task_run_id = insert_committed_task_run(postgres_engine, task_id, run_id, "IN-PROGRESS")

    outcome = run_task(postgres_engine, make_config(), "TEST_CONCURRENT_PL", "inflight_task")

    assert outcome.status == "SKIPPED"
    assert "IN-PROGRESS" in outcome.message
    with postgres_engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT STATUS AS status, ERROR_MESSAGE AS error_message, END_DATE AS end_date "
                "FROM AUD_TASK_RUN_LOG WHERE TASK_RUN_ID = :id"
            ),
            {"id": task_run_id},
        ).one()
    # Untouched, not merely "not SKIPPED" — the run already under way owns it.
    assert row.status == "IN-PROGRESS"
    assert row.error_message is None
    assert row.end_date is None


def test_run_task_says_so_when_a_dependency_can_never_be_satisfied(
    postgres_engine, committed_pipeline
):
    # The single-task half of E2-01, and the case a generated Airflow DAG
    # actually hits: the DAG runs every task including the FAILURE-gated
    # alert, so run_task must record SKIPPED and exit 0 rather than treating
    # "correctly gated off by design" as a failure Airflow should retry.
    #
    # Also the reason the skip reason had to stop saying "dependencies not
    # met" unconditionally: that reads as "not yet", which is actively
    # misleading for a task that will never run under this pipeline_run_id.
    work_id = insert_committed_task(postgres_engine, committed_pipeline, "never_work")
    alert_id = insert_committed_task(
        postgres_engine, committed_pipeline, "never_alert", handler="EMAIL_ALERT"
    )
    insert_committed_dependency(
        postgres_engine, committed_pipeline, alert_id, work_id, dependency_type="FAILURE"
    )
    run_id = seed_active_run(postgres_engine, committed_pipeline)
    insert_committed_task_run(postgres_engine, work_id, run_id, "SUCCESS")

    outcome = run_task(postgres_engine, make_config(), "TEST_CONCURRENT_PL", "never_alert")

    assert outcome.status == "SKIPPED"
    assert "can never be satisfied" in outcome.message
    with postgres_engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT STATUS AS status, ERROR_MESSAGE AS error_message FROM AUD_TASK_RUN_LOG "
                "WHERE TASK_ID = :id AND PIPELINE_RUN_ID = :run_id"
            ),
            {"id": alert_id, "run_id": run_id},
        ).one()
    assert row.status == "SKIPPED"
    assert "can never be satisfied" in row.error_message


def test_run_task_writes_nothing_when_a_dependency_simply_has_not_run_yet(
    postgres_engine, committed_pipeline
):
    # task_b depends on task_a via SUCCESS; task_a hasn't been run at all.
    #
    # [DEVIATION, 2026-09-20, E2-47] Nothing is written now. This used to
    # record SKIPPED — which is terminal (NOT_RETRYABLE, SETTLED_STATUSES), so
    # it permanently disqualified task_b from the run: a later invocation,
    # even with task_a genuinely SUCCESS, still refused, and the pipeline
    # finalized SUCCESS with task_b never having run. CLAUDE.md explicitly
    # supports the paths that hit this ("a manual single-task run, a backfill,
    # a re-triggered task"), so the check must not be destructive.
    #
    # "Can never be satisfied" is a different case and still writes SKIPPED —
    # see test_run_task_says_so_when_a_dependency_can_never_be_satisfied.
    task_a = insert_committed_task(postgres_engine, committed_pipeline, "task_a")
    task_b = insert_committed_task(postgres_engine, committed_pipeline, "task_b")
    insert_committed_dependency(postgres_engine, committed_pipeline, task_b, task_a)
    run_id = seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(), "TEST_CONCURRENT_PL", "task_b")

    assert outcome.status == "SKIPPED"
    assert "not met yet" in outcome.message
    with postgres_engine.connect() as conn:
        rows = conn.execute(
            text("SELECT STATUS FROM AUD_TASK_RUN_LOG WHERE TASK_ID = :id"), {"id": task_b}
        ).all()
    assert rows == []

    # And the task is still runnable once its dependency really does succeed.
    insert_committed_task_run(postgres_engine, task_a, run_id, "SUCCESS")
    retry = run_task(postgres_engine, make_config(), "TEST_CONCURRENT_PL", "task_b")
    assert retry.status == "FAILED"  # got past the gate, hit the unconfigured handler


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


def test_run_task_any_condition_does_not_require_every_cross_pipeline_edge(
    postgres_engine, two_committed_pipelines
):
    # E2-45 regression. ready() applied RUN_CONDITION to same-pipeline edges
    # while run_task separately demanded that *every* cross-pipeline edge be
    # satisfied, so a task declared ANY was really gated ANY-and-ALL. Nothing
    # said so: not the schema comment, not the resolver docstring, and
    # generate-yml emitted one_success, a third semantic again.
    #
    # Also proves the upside: with the condition already met from the
    # same-pipeline side, the cross-pipeline edge is never polled at all —
    # an ANY task has no business blocking a worker slot for up to an hour on
    # an edge it does not need.
    downstream_id, upstream_id = two_committed_pipelines
    same_upstream = insert_committed_task(postgres_engine, downstream_id, "any_same_up")
    task_id = insert_committed_task(postgres_engine, downstream_id, "any_target")
    with postgres_engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_TASKS SET RUN_CONDITION = 'ANY' WHERE TASK_ID = :id"),
            {"id": task_id},
        )
    insert_committed_dependency(postgres_engine, downstream_id, task_id, same_upstream)
    far_task = insert_committed_task(postgres_engine, upstream_id, "far_up")
    insert_committed_cross_pipeline_task_dependency(
        postgres_engine, downstream_id, task_id, upstream_id, far_task, "SUCCESS"
    )
    run_id = seed_active_run(postgres_engine, downstream_id)
    insert_committed_task_run(postgres_engine, same_upstream, run_id, "SUCCESS")

    def never_sleep(_seconds):  # pragma: no cover - must never be reached
        raise AssertionError("polled a cross-pipeline edge the ANY condition did not need")

    outcome = run_task(
        postgres_engine, make_config(), "TEST_XPIPE_DOWN", "any_target", sleep=never_sleep
    )

    # Past the gate on the same-pipeline edge alone; fails on the
    # unconfigured handler, which is what proves the gate let it through.
    assert outcome.status == "FAILED"


def test_run_task_does_not_settle_an_any_task_whose_same_pipeline_upstream_has_not_run(
    postgres_engine, two_committed_pipelines
):
    # E2-82, the run_task half. The wave pre-filter now holds such a task
    # back, but a manual `run --task_code` never goes through it -- so
    # without this guard an operator running the task by hand, before its
    # same-pipeline upstream had run, would poll the cross-pipeline edge to
    # its budget and then record SKIPPED. SKIPPED is in SETTLED_STATUSES, so
    # the task would be permanently out of the run even though the upstream
    # succeeding moments later would have met its ANY condition. That is the
    # "not yet" recorded as "never" that E2-47 removed for the same-pipeline
    # half, reappearing through the cross-pipeline half.
    downstream_id, upstream_id = two_committed_pipelines
    same_upstream = insert_committed_task(postgres_engine, downstream_id, "pending_up")
    task_id = insert_committed_task(postgres_engine, downstream_id, "any_target")
    with postgres_engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_TASKS SET RUN_CONDITION = 'ANY' WHERE TASK_ID = :id"),
            {"id": task_id},
        )
    insert_committed_dependency(postgres_engine, downstream_id, task_id, same_upstream)
    far_task = insert_committed_task(postgres_engine, upstream_id, "far_up")
    insert_committed_cross_pipeline_task_dependency(
        postgres_engine, downstream_id, task_id, upstream_id, far_task, "SUCCESS"
    )
    run_id = seed_active_run(postgres_engine, downstream_id)

    outcome = run_task(
        postgres_engine, make_config(), "TEST_XPIPE_DOWN", "any_target", sleep=lambda _s: None
    )

    assert outcome.status == "SKIPPED"
    assert "nothing was recorded" in outcome.message
    # Nothing written, so a later invocation finds the task exactly as it
    # left it -- the shape E2-02/E2-47 established.
    with postgres_engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT STATUS FROM AUD_TASK_RUN_LOG " "WHERE TASK_ID = :t AND PIPELINE_RUN_ID = :r"
            ),
            {"t": task_id, "r": run_id},
        ).scalar_one_or_none()
    assert row is None


def test_cross_pipeline_poll_budget_is_shared_across_every_edge(
    postgres_engine, two_committed_pipelines
):
    # E2-11. CLAUDE.md specifies "a hard 1-hour timeout overall", but both the
    # deadline and MAX_POLLS were computed inside the per-edge wait helper,
    # which is called once per edge in a loop -- so a task with three
    # cross-pipeline edges could wait three hours and spend ninety polls.
    downstream_id, upstream_id = two_committed_pipelines
    task_id = insert_committed_task(postgres_engine, downstream_id, "budget_target")
    start = datetime.now(UTC)
    run_id = insert_committed_pipeline_run(
        postgres_engine, upstream_id, "IN-PROGRESS", start_date=start
    )
    # Two upstream tasks, both stuck IN-PROGRESS, so both edges want to poll.
    for name in ("budget_up_a", "budget_up_b"):
        upstream_task = insert_committed_task(postgres_engine, upstream_id, name)
        insert_committed_task_run(
            postgres_engine, upstream_task, run_id, "IN-PROGRESS", start_date=start
        )
        insert_committed_cross_pipeline_task_dependency(
            postgres_engine, downstream_id, task_id, upstream_id, upstream_task, "SUCCESS"
        )

    clock = _FakeClock(start)
    check = check_task_cross_pipeline_dependencies(
        postgres_engine, task_id, sleep=clock.sleep, now=clock.now
    )

    assert check.satisfied_count == 0
    # One budget across both edges, not one each: at most MAX_POLLS total, and
    # the wall clock never passes the single one-hour deadline.
    assert len(clock.sleeps) <= 30
    assert (clock.now() - start).total_seconds() < 3600


def test_check_task_cross_pipeline_dependencies_stops_once_enough_are_satisfied(
    postgres_engine, two_committed_pipelines
):
    # E2-45's second half: `needed` must stop the loop, not just cap the count.
    # An edge left unevaluated is also an edge left *unpolled*, which is the
    # whole point — polling can block for up to an hour per edge.
    downstream_id, upstream_id = two_committed_pipelines
    task_id = insert_committed_task(postgres_engine, downstream_id, "needed_target")
    run_id = insert_committed_pipeline_run(postgres_engine, upstream_id, "IN-PROGRESS")
    first = insert_committed_task(postgres_engine, upstream_id, "needed_up_a")
    second = insert_committed_task(postgres_engine, upstream_id, "needed_up_b")
    insert_committed_task_run(postgres_engine, first, run_id, "SUCCESS")
    # `second` never ran, so its edge is unsatisfied and would poll.
    for upstream in (first, second):
        insert_committed_cross_pipeline_task_dependency(
            postgres_engine, downstream_id, task_id, upstream_id, upstream, "SUCCESS"
        )

    def never_sleep(_seconds):  # pragma: no cover - must never be reached
        raise AssertionError("polled an edge beyond the number actually needed")

    check = check_task_cross_pipeline_dependencies(
        postgres_engine, task_id, needed=1, sleep=never_sleep
    )

    assert check.total == 2
    assert check.satisfied_count == 1
    assert check.reasons == ()


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

    # (engine, ctx), matching dispatch's real signature. It used to be
    # `_crash(handler)` -- a stale one-argument stub left over from an earlier
    # dispatch signature -- so the child actually died of a TypeError inside
    # runner, never reaching os._exit at all. The test passed anyway, because
    # any non-zero exit produced the same "died unexpectedly" row. E2-09's
    # BaseException handler exposed it: a TypeError now gets a real message,
    # which is exactly what this test asserts must NOT happen for a genuine
    # unannounced death.
    def _crash(engine, ctx):
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


def test_run_pipeline_runs_a_parallel_wave_against_a_duckdb_warehouse(
    duckdb_craft_connector_on_disk, postgres_engine, committed_pipeline
):
    # E2-61, and the test the review said would have caught it. Two
    # independent SQL tasks are one wave, so orchestrator._run_wave spawns two
    # real subprocesses at once -- and DuckDB admits exactly one writing OS
    # process, refusing the second with "IO Error: Could not set lock on
    # file". Max_parallel_tasks defaults to 8, so this was the default
    # behaviour on an embedded warehouse, not an edge case.
    #
    # Nothing caught it because every DuckDB test ran in a single process and
    # every subprocess-spawning orchestrator test pointed [Warehouse] at
    # Postgres. This is the combination.
    warehouse = duckdb_craft_connector_on_disk
    seed = create_engine(f"duckdb:///{warehouse}")
    try:
        with seed.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))
            conn.execute(text("CREATE TABLE staging.src AS SELECT 1 AS id"))
    finally:
        # Handed back before anything forks or spawns: an open handle here
        # would take the lock these subprocesses need.
        seed.dispose()

    for task_code, target in (("wave_a", "staging.out_a"), ("wave_b", "staging.out_b")):
        task_id = insert_committed_task(postgres_engine, committed_pipeline, task_code)
        insert_committed_task_parameters(
            postgres_engine,
            task_id,
            {
                "SQL_ACTION": "CREATE_TABLE",
                "TARGET_OBJECT": target,
                "SOURCE_SQL": "SELECT id FROM staging.src WHERE 1=1",
            },
        )

    outcome = run_pipeline(postgres_engine, load_config(), "TEST_CONCURRENT_PL")

    assert outcome.status == "SUCCESS", outcome.message
    with postgres_engine.connect() as conn:
        statuses = {
            row.status
            for row in conn.execute(
                text(
                    "SELECT STATUS AS status FROM AUD_TASK_RUN_LOG t "
                    "JOIN CFG_TASKS c ON c.TASK_ID = t.TASK_ID WHERE c.PIPELINE_ID = :pid"
                ),
                {"pid": committed_pipeline},
            )
        }
    # Both genuinely ran and wrote -- the wave queued rather than one of them
    # dying on the file lock.
    assert statuses == {"SUCCESS"}
    check = create_engine(f"duckdb:///{warehouse}")
    try:
        with check.connect() as conn:
            assert conn.execute(text("SELECT id FROM staging.out_a")).scalars().all() == [1]
            assert conn.execute(text("SELECT id FROM staging.out_b")).scalars().all() == [1]
    finally:
        check.dispose()


def test_run_pipeline_passes_config_to_every_task_subprocess(
    tmp_path, monkeypatch, postgres_engine, committed_pipeline
):
    # E2-78. E2-06 added a top-level --config PATH to every verb precisely
    # because a process's cwd is not always something the caller controls, and
    # _run_wave dropped it -- so the wave planner resolved one config while
    # every spawned task independently re-resolved a *different* one from its
    # inherited cwd. $ETL_CRAFT_CONFIG happened to survive, because Popen
    # inherits the environment; only the explicit flag was lost, which is what
    # made this look like it worked right up until someone used the flag.
    #
    # The cwd here has no craft-connector.yml anywhere above it, so a
    # subprocess that does not receive --config cannot resolve one at all and
    # exits before writing any AUD_TASK_RUN_LOG row.
    config_file = tmp_path / "elsewhere" / "craft-connector.yml"
    config_file.parent.mkdir()
    config_file.write_text(CRAFT_CONNECTOR_YAML, encoding="utf-8")
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    monkeypatch.setenv("ETL_CRAFT_POSTGRES_DEV_SECRET", "etl_craft")
    monkeypatch.delenv("ETL_CRAFT_CONFIG", raising=False)
    insert_committed_task(postgres_engine, committed_pipeline, "task_a")

    run_pipeline(postgres_engine, load_config(config_file), "TEST_CONCURRENT_PL")

    with postgres_engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT STATUS AS status FROM AUD_TASK_RUN_LOG t "
                "JOIN CFG_TASKS c ON c.TASK_ID = t.TASK_ID WHERE c.PIPELINE_ID = :pid"
            ),
            {"pid": committed_pipeline},
        ).all()
    # The row exists at all only if the subprocess resolved the config it was
    # given and reached the Engine DB. (It then fails on the stub handler,
    # which is beside the point here.)
    assert len(rows) == 1


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


def test_run_pipeline_succeeds_end_to_end_with_a_failure_gated_alert_task(
    postgres_engine, committed_pipeline
):
    # E2-01, local mode, spawning real subprocesses — the companion to the
    # --finalize-only version above. Before the fix, run_pipeline reported
    # FAILED and exited 1 on a run where the only work succeeded, because the
    # FAILURE-gated alert never became ready and so never got a row.
    work_id = insert_committed_task(postgres_engine, committed_pipeline, "e2e_work")
    alert_id = insert_committed_task(
        postgres_engine, committed_pipeline, "e2e_alert", handler="EMAIL_ALERT"
    )
    insert_committed_dependency(
        postgres_engine, committed_pipeline, alert_id, work_id, dependency_type="FAILURE"
    )
    run_id = seed_active_run(postgres_engine, committed_pipeline)
    # e2e_work pre-marked SUCCESS so the wave loop has no stub handler to hit
    # — the point under test is the alert's settlement, not handler dispatch.
    insert_committed_task_run(postgres_engine, work_id, run_id, "SUCCESS")

    outcome = run_pipeline(postgres_engine, make_config(), "TEST_CONCURRENT_PL")

    assert outcome.status == "SUCCESS"
    with postgres_engine.connect() as conn:
        alert_status = conn.execute(
            text(
                "SELECT STATUS FROM AUD_TASK_RUN_LOG "
                "WHERE TASK_ID = :task_id AND PIPELINE_RUN_ID = :run_id"
            ),
            {"task_id": alert_id, "run_id": run_id},
        ).scalar_one()
    assert alert_status == "SKIPPED"


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


def test_finalize_active_run_succeeds_with_a_failure_gated_alert_task(
    postgres_engine, committed_pipeline
):
    # E2-01 regression, reproduced against real Postgres during the iteration-1
    # review. An EMAIL_ALERT task gated on a FAILURE edge is the alerting
    # pattern CLAUDE.md's Handlers section explicitly recommends. When the task
    # it watches SUCCEEDS the alert correctly never becomes ready — but nothing
    # marked it terminal either, so it had no AUD_TASK_RUN_LOG row, so
    # _finalize_from_task_states counted it unsettled and wrote FAILED for a
    # run in which everything that should have happened did. Downstream
    # SUCCESS-typed CFG_PIPELINE_DEPENDENCY edges then never fired, so it
    # propagated silently.
    work_id = insert_committed_task(postgres_engine, committed_pipeline, "probe_work")
    alert_id = insert_committed_task(
        postgres_engine, committed_pipeline, "probe_alert", handler="EMAIL_ALERT"
    )
    insert_committed_dependency(
        postgres_engine, committed_pipeline, alert_id, work_id, dependency_type="FAILURE"
    )
    run_id = seed_active_run(postgres_engine, committed_pipeline)
    insert_committed_task_run(postgres_engine, work_id, run_id, "SUCCESS")

    outcome = finalize_active_run(postgres_engine, make_config(), "TEST_CONCURRENT_PL")

    assert outcome.status == "SUCCESS"
    with postgres_engine.connect() as conn:
        alert_status = conn.execute(
            text(
                "SELECT STATUS FROM AUD_TASK_RUN_LOG "
                "WHERE TASK_ID = :task_id AND PIPELINE_RUN_ID = :run_id"
            ),
            {"task_id": alert_id, "run_id": run_id},
        ).scalar_one()
    # Not merely "not counted" — the alert gets a real, honest audit row
    # saying it was gated off, which is what makes the run legible later.
    assert alert_status == "SKIPPED"


def test_finalize_active_run_skips_cascade_through_a_chain(postgres_engine, committed_pipeline):
    # The cascade half of E2-01: once the alert is SKIPPED, a task depending on
    # it via SUCCESS can never run either, while one depending via ALWAYS can.
    # This is why resolver.unsatisfiable() iterates to a fixpoint instead of
    # making a single pass.
    work_id = insert_committed_task(postgres_engine, committed_pipeline, "casc_work")
    alert_id = insert_committed_task(
        postgres_engine, committed_pipeline, "casc_alert", handler="EMAIL_ALERT"
    )
    after_id = insert_committed_task(postgres_engine, committed_pipeline, "casc_after")
    insert_committed_dependency(
        postgres_engine, committed_pipeline, alert_id, work_id, dependency_type="FAILURE"
    )
    insert_committed_dependency(
        postgres_engine, committed_pipeline, after_id, alert_id, dependency_type="SUCCESS"
    )
    run_id = seed_active_run(postgres_engine, committed_pipeline)
    insert_committed_task_run(postgres_engine, work_id, run_id, "SUCCESS")

    outcome = finalize_active_run(postgres_engine, make_config(), "TEST_CONCURRENT_PL")

    assert outcome.status == "SUCCESS"
    with postgres_engine.connect() as conn:
        statuses = dict(
            conn.execute(
                text(
                    "SELECT TASK_ID, STATUS FROM AUD_TASK_RUN_LOG WHERE PIPELINE_RUN_ID = :run_id"
                ),
                {"run_id": run_id},
            ).all()
        )
    assert statuses[alert_id] == "SKIPPED"
    assert statuses[after_id] == "SKIPPED"


def test_finalize_active_run_does_not_skip_a_task_whose_upstream_merely_failed(
    postgres_engine, committed_pipeline
):
    # The boundary E2-01's fix must not cross. A SUCCESS edge whose upstream is
    # FAILED is *not* permanently unsatisfiable — FAILED stays retry-eligible,
    # which is the whole point of "retry resumes". Converting that into SKIPPED
    # would report a broken run as SUCCESS, which is worse than the bug.
    work_id = insert_committed_task(postgres_engine, committed_pipeline, "keepfail_work")
    after_id = insert_committed_task(postgres_engine, committed_pipeline, "keepfail_after")
    insert_committed_dependency(
        postgres_engine, committed_pipeline, after_id, work_id, dependency_type="SUCCESS"
    )
    run_id = seed_active_run(postgres_engine, committed_pipeline)
    insert_committed_task_run(postgres_engine, work_id, run_id, "FAILED")

    outcome = finalize_active_run(postgres_engine, make_config(), "TEST_CONCURRENT_PL")

    assert outcome.status == "FAILED"
    with postgres_engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT STATUS FROM AUD_TASK_RUN_LOG "
                "WHERE TASK_ID = :task_id AND PIPELINE_RUN_ID = :run_id"
            ),
            {"task_id": after_id, "run_id": run_id},
        ).all()
    assert rows == []


def test_settle_unsatisfiable_tasks_leaves_a_concurrently_created_row_alone(
    postgres_engine, committed_pipeline, monkeypatch
):
    # E2-50 regression. settle reads run_state in one transaction and writes in
    # another. Between the two, a concurrent `run --task_code` can create the
    # row and start executing — and blindly updating it would overwrite a live
    # task with SKIPPED, which is E2-02's clobber reached from the other side.
    # The window is real: settle runs on every wave pass while subprocesses are
    # live. Simulated here by creating the row after the doomed set is computed.
    work_id = insert_committed_task(postgres_engine, committed_pipeline, "race_work")
    alert_id = insert_committed_task(
        postgres_engine, committed_pipeline, "race_alert", handler="EMAIL_ALERT"
    )
    insert_committed_dependency(
        postgres_engine, committed_pipeline, alert_id, work_id, dependency_type="FAILURE"
    )
    run_id = seed_active_run(postgres_engine, committed_pipeline)
    insert_committed_task_run(postgres_engine, work_id, run_id, "SUCCESS")

    with postgres_engine.connect() as conn:
        graph_data = fetch_pipeline_graph(conn, committed_pipeline)
    graph = build_graph(graph_data.tasks, graph_data.same_pipeline_edges)
    all_task_ids = [task.task_id for task in graph_data.tasks]

    # Drive the race window precisely: return the real state (in which the
    # alert has no row and so is genuinely doomed), then let the concurrent
    # invocation create the row and start executing — exactly the interleaving
    # that exists between settle's read transaction and its write transaction.
    # Seeding the row up front instead would prove nothing: unsatisfiable()
    # would exclude the task before the `created` guard was ever reached.
    real_fetch_run_state = orchestrator_module.fetch_run_state

    def fetch_then_race(conn, pipeline_run_id, task_ids):
        state = real_fetch_run_state(conn, pipeline_run_id, task_ids)
        insert_committed_task_run(postgres_engine, alert_id, run_id, "IN-PROGRESS")
        return state

    monkeypatch.setattr(orchestrator_module, "fetch_run_state", fetch_then_race)

    settled = settle_unsatisfiable_tasks(postgres_engine, graph, run_id, all_task_ids)

    assert settled == []
    with postgres_engine.connect() as conn:
        status = conn.execute(
            text(
                "SELECT STATUS FROM AUD_TASK_RUN_LOG "
                "WHERE TASK_ID = :task_id AND PIPELINE_RUN_ID = :run_id"
            ),
            {"task_id": alert_id, "run_id": run_id},
        ).scalar_one()
    assert status == "IN-PROGRESS"


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
        raise ValueError("warehouse unreachable")

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
    # HANDLER=SQL task correctly fails needing a warehouse it has none of.
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


def test_cli_graph_flags_tasks_that_can_start_before_their_wave(
    craft_connector_on_disk, postgres_engine, committed_pipeline, capsys
):
    # E2-51. waves() is the guaranteed-safe static order and deliberately
    # ignores RUN_CONDITION, so an ANY task is printed one wave later than it
    # can genuinely run. That divergence is fine, but it must not be silent —
    # a reader has no other way to tell the printed structure from the engine's
    # actual behaviour.
    task_a = insert_committed_task(postgres_engine, committed_pipeline, "wave_a")
    task_b = insert_committed_task(postgres_engine, committed_pipeline, "wave_any")
    insert_committed_dependency(postgres_engine, committed_pipeline, task_b, task_a)
    with postgres_engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_TASKS SET RUN_CONDITION = 'ANY' WHERE TASK_ID = :id"),
            {"id": task_b},
        )

    assert cli_main(["graph", "--name", "TEST_CONCURRENT_PL"]) == 0

    out = capsys.readouterr().out
    assert "may start earlier: wave_any" in out


def test_cli_reports_an_unreachable_engine_db_as_a_clean_error(
    craft_connector_on_disk, monkeypatch, capsys
):
    # E2-39. build_engine constructs a lazy Engine with a `creator`, so it
    # never connects — the first real connection happens inside each command,
    # which for `list` and `generate-docs` had no try/except at all. The most
    # common real-world failure there is (Postgres unreachable, or dropping
    # mid-command) printed a raw traceback.
    def refuse(*_args, **_kwargs):
        raise OperationalError("SELECT 1", {}, Exception("connection refused"))

    monkeypatch.setattr("etl_craft.cli.fetch_all_pipelines", refuse)

    assert cli_main(["list"]) == 2
    assert "error: Engine DB:" in capsys.readouterr().err


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
        # A complete SQL task, not just its lineage declarations: E2-25's
        # parameter checks now flag a HANDLER='SQL' task with no SQL_ACTION,
        # which this one genuinely was.
        {
            "SOURCE_OBJECT": "public.src",
            "TARGET_OBJECT": "public.tgt",
            "SQL_ACTION": "CREATE_TABLE",
            "SOURCE_SQL": "SELECT 1 AS id FROM public.src",
        },
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
        {
            "SOURCE_OBJECT": "public.src",
            "TARGET_OBJECT": "public.validate_cli_test_good",
            "SQL_ACTION": "CREATE_TABLE",
            "SOURCE_SQL": "SELECT 1 AS id FROM public.src",
        },
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


def test_cli_validate_reports_config_error_building_warehouse_engine(
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
# role) and the warehouse / [Warehouse] (make_config(warehouse=True) points
# both at the same instance) — the same "one dialect stands in for the
# generic mechanism" spirit as warehouse.py's own tests. Every test drives
# the real `run_task()` entry point end to end (CFG_ setup -> dispatch ->
# AUD_TASK_RUN_LOG), the same path a real deployment uses, rather than
# calling sql_actions.execute()/business_rules.execute() directly — that
# exercises handlers.py's own wiring (including its HandlerError-wrapping of
# ConfigError/SQLAlchemyError) for free, not just the leaf modules.
#
# warehouse_tables (conftest.py) tracks/drops every real table a test creates
# in the warehouse side of this same Postgres instance — separate from
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
    postgres_engine, committed_pipeline, warehouse_tables
):
    target = f"public.sqlx_create_{committed_pipeline}"
    warehouse_tables.append(target)
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
    postgres_engine, committed_pipeline, warehouse_tables
):
    target = f"public.sqlx_setup_scd2_{committed_pipeline}"
    warehouse_tables.append(target)
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
        # E2-54: the identity surrogate key, added after the CTAS. SCD2 is
        # exactly the case that motivated it -- several rows per merge key
        # by design, so the natural key can never be the primary key.
        "row_id",
    ]
    assert count == 0


def test_sql_setup_table_no_sibling_falls_back_to_no_audit_columns(
    postgres_engine, committed_pipeline, warehouse_tables
):
    target = f"public.sqlx_setup_solo_{committed_pipeline}"
    warehouse_tables.append(target)
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
    # row_id: every engine-created target now carries an identity surrogate
    # key (E2-54), which is what makes the single-column-PK convention
    # satisfiable on an SCD2 target too.
    assert cols == ["id", "name", "pipeline_run_id", "row_id"]


def test_sql_overwrite_table_truncates_and_reinserts(
    postgres_engine, committed_pipeline, warehouse_tables
):
    target = f"public.sqlx_over_{committed_pipeline}"
    src = f"sqlx_over_src_{committed_pipeline}"
    warehouse_tables.extend([target, src])
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


def test_sql_overwrite_table_creates_a_missing_target(
    postgres_engine, committed_pipeline, warehouse_tables
):
    # [DEVIATION, E2-42] This used to assert FAILED with "does not exist — run
    # a SETUP_TABLE or CREATE_TABLE task against it first". Per explicit
    # instruction every action except DROP_TABLE/DELETE_ROWS now bootstraps its
    # own target from the staged SELECT plus that action's own audit columns,
    # so a first run needs no separate setup task.
    target = f"public.sqlx_over_missing_{committed_pipeline}"
    warehouse_tables.append(target)
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "over")
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {
            "SQL_ACTION": "OVERWRITE_TABLE",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": "SELECT 1 AS id WHERE 1=1",
            "PRIMARY_KEY": "id",
        },
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "over")

    assert outcome.status == "SUCCESS"
    with postgres_engine.connect() as conn:
        rows = conn.execute(text(f"SELECT id FROM {target}")).all()
        # OVERWRITE_TABLE's own audit column is present on the created shape.
        columns = {
            row[0].lower()
            for row in conn.execute(
                text("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " "WHERE TABLE_NAME = :t"),
                {"t": target.split(".", 1)[1]},
            )
        }
    assert rows == [(1,)]
    assert {"id", "pipeline_run_id", "update_date"} <= columns


def _duckdb_sql_task(engine, pipeline_id, task_code, params):
    task_id = insert_committed_task(engine, pipeline_id, task_code)
    insert_committed_task_parameters(engine, task_id, params)
    return task_id


def test_sql_actions_run_end_to_end_against_real_duckdb(
    postgres_engine, committed_pipeline, tmp_path
):
    # [DEVIATION, 2026-09-20] Was a ClickHouse test. The gap it closes is
    # unchanged and is the one that mattered: every other SQL-action test
    # points [Warehouse] at the same Postgres, so without a second dialect the
    # whole action vocabulary is exercised against exactly one engine -- which
    # is how three ClickHouse DDL failures and a TIMESTAMP regression shipped.
    # DuckDB is the second supported warehouse now, and unlike ClickHouse it
    # is close enough to ANSI that the *full* vocabulary works, merges
    # included.
    #
    # Every warehouse engine here is disposed before any run_task call and
    # rebuilt after. DuckDB is embedded: if this process still holds the file
    # open when runner.py forks for crash detection, the forked child inherits
    # that in-memory database state and its writes are silently lost -- it
    # reports SUCCESS and the table is not there. Verified directly. The
    # engine itself is safe because run_task never opens the warehouse in the
    # parent (handlers.py builds it inside the child), but a test that seeds
    # fixture data has to hand the file back first.
    config = make_config(duckdb_warehouse=str(tmp_path / "warehouse.duckdb"))

    def with_warehouse(fn):
        engine = build_warehouse_engine(config)
        try:
            return fn(engine)
        finally:
            engine.dispose()

    def seed(engine):
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))
            conn.execute(text("CREATE TABLE staging.src AS SELECT 1 AS id, 'a' AS name"))

    with_warehouse(seed)

    _duckdb_sql_task(
        postgres_engine,
        committed_pipeline,
        "duck_create",
        {
            "SQL_ACTION": "CREATE_TABLE",
            "TARGET_OBJECT": "staging.customers",
            "SOURCE_SQL": "SELECT id, name FROM staging.src WHERE 1=1",
        },
    )
    merge_task = _duckdb_sql_task(
        postgres_engine,
        committed_pipeline,
        "duck_merge",
        {
            "SQL_ACTION": "SCD1_MERGE",
            "TARGET_OBJECT": "staging.dim",
            "SOURCE_SQL": "SELECT id, name FROM staging.src WHERE 1=1",
            "MERGE_KEY": "id",
            "MERGE_COMPARE_COLUMNS": "name",
        },
    )
    seed_active_run(postgres_engine, committed_pipeline)

    created = run_task(postgres_engine, config, "TEST_CONCURRENT_PL", "duck_create")
    assert created.status == "SUCCESS", created.message
    merged = run_task(postgres_engine, config, "TEST_CONCURRENT_PL", "duck_merge")
    assert merged.status == "SUCCESS", merged.message

    def check_first_run(engine):
        with engine.connect() as conn:
            assert conn.execute(text("SELECT id, name FROM staging.customers")).all() == [(1, "a")]
            return conn.execute(text("SELECT id, name, HASH_KEY, ROW_ID FROM staging.dim")).one()

    row = with_warehouse(check_first_run)
    assert (row[0], row[1]) == (1, "a")
    # The engine-managed columns really landed: a 32-hex hash, and the
    # surrogate identity key, which DuckDB needs a sequence for because it
    # rejects adding an identity column after table creation.
    assert len(row[2]) == 32
    assert row[3] == 1

    # A changed value exercises the correlated UPDATE leg -- the thing
    # ClickHouse could not do at all.
    def change_source(engine):
        with engine.begin() as conn:
            conn.execute(text("UPDATE staging.src SET name = 'b' WHERE id = 1"))

    with_warehouse(change_source)
    with postgres_engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_TASK_RUN_LOG SET STATUS = 'FAILED' WHERE TASK_ID = :id"),
            {"id": merge_task},
        )

    again = run_task(postgres_engine, config, "TEST_CONCURRENT_PL", "duck_merge")
    assert again.status == "SUCCESS", again.message

    def check_second_run(engine):
        with engine.connect() as conn:
            return conn.execute(text("SELECT name FROM staging.dim")).all()

    assert with_warehouse(check_second_run) == [("b",)]


def test_open_warehouse_times_out_with_a_clear_reason_when_the_warehouse_is_busy(
    postgres_engine, tmp_path
):
    # E2-61's failure path. Queueing has to be bounded, or one wedged holder
    # blocks every other task indefinitely -- and when the bound is hit the
    # message has to say why, because "Could not set lock on file" tells
    # someone nothing about what to do.
    config = make_config(duckdb_warehouse=str(tmp_path / "warehouse.duckdb"))
    holding = threading.Event()
    release = threading.Event()

    def hold_the_warehouse():
        with open_warehouse(config, postgres_engine):
            holding.set()
            release.wait(timeout=30)

    holder = threading.Thread(target=hold_the_warehouse)
    holder.start()
    try:
        assert holding.wait(timeout=10)
        with (
            pytest.raises(ConnectionError_, match="only one writing process at a time"),
            open_warehouse(config, postgres_engine, wait_seconds=1),
        ):
            pass  # pragma: no cover - the lock must not be granted
    finally:
        release.set()
        holder.join(timeout=10)


def test_single_writer_lock_also_queues_an_ingestion_task(
    postgres_engine, committed_pipeline, tmp_path
):
    # E2-81. E2-61 introduced open_warehouse as "the one way the engine reaches
    # the warehouse" and routed SQL and BUSINESS_RULES through it -- but not
    # PYTHON, which went straight to scripts.execute. PYTHON is the *ingestion*
    # handler: CLAUDE.md's own rule is that "the team's own script is
    # responsible for fetching and including pipeline_run_id in whatever it
    # inserts", so writing to the warehouse is its whole purpose. On a DuckDB
    # warehouse an ingestion task in the same wave as any SQL task therefore
    # raced for the file lock, and whichever lost failed with the raw
    # "Could not set lock on file" that E2-61 exists to prevent -- in the
    # handler most likely to be doing the writing.
    #
    # The engine cannot make a team's script take the lock, but it can hold it
    # around the script, which is the queueing asserted here.
    config = make_config(duckdb_warehouse=str(tmp_path / "warehouse.duckdb"))
    marker = tmp_path / "the-script-ran"
    script = tmp_path / "ingest.py"
    script.write_text(
        f"import pathlib; pathlib.Path({str(marker)!r}).write_text('ran', encoding='utf-8')\n"
        'print(\'{"INGESTION_COUNT": 1, "LATEST_OFFSET_UPDATE": "1|int"}\')\n',
        encoding="utf-8",
    )
    task_id = insert_committed_task(
        postgres_engine, committed_pipeline, "ingest_q", handler="PYTHON"
    )
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {
            "SCRIPT_NAME": str(script),
            "RETURN_VALUES": "INGESTION_COUNT|LATEST_OFFSET_UPDATE",
            "TASK_TIMEOUT_SECONDS": "1",
        },
    )
    seed_active_run(postgres_engine, committed_pipeline)

    holding = threading.Event()
    release = threading.Event()

    def hold_the_warehouse():
        with open_warehouse(config, postgres_engine):
            holding.set()
            release.wait(timeout=30)

    holder = threading.Thread(target=hold_the_warehouse)
    holder.start()
    try:
        assert holding.wait(timeout=10)
        outcome = run_task(postgres_engine, config, "TEST_CONCURRENT_PL", "ingest_q")
    finally:
        release.set()
        holder.join(timeout=10)

    # It queued behind the holder instead of running alongside it, which is
    # the whole point: before this, the script ran and raced for DuckDB's file
    # lock itself. Having queued past its own (deliberately tiny) budget, the
    # task then fails -- correctly, and without the script having touched the
    # warehouse.
    #
    # The message here is the fork watchdog's rather than the lock's, because
    # handlers.dispatch gives the lock the task's *own* timeout as its wait
    # bound, so the two expire together and the outer one reports first. Left
    # as-is deliberately: shortening the lock's budget to win the race would
    # make a team running a short TASK_TIMEOUT_SECONDS fail tasks that should
    # have queued, which is a worse trade than a blunter message.
    assert outcome.status == "FAILED"
    assert not marker.exists()


def _trino_config() -> ConnectorConfig:
    """Engine DB on real Postgres, warehouse on the real local Trino/Iceberg stack."""
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
        cloning=CloningConfig(),
        warehouse=ConnectionSection(
            active_profile="dev",
            profiles={
                "dev": ConnectionProfile(
                    section="WAREHOUSE",
                    name="dev",
                    # auth_mode=none: an unauthenticated local Trino. Not only
                    # embedded warehouses use it, which is what made the
                    # host-dropping bug in _none_creator visible.
                    jdbc_url="jdbc:trino://localhost:58080/iceberg/etltest",
                    user="etl",
                    auth_mode="none",
                )
            },
        ),
    )


def test_sql_actions_run_end_to_end_against_real_trino_iceberg(
    postgres_engine, trino_engine, committed_pipeline
):
    # The Iceberg execution path, against a real SQL engine over real Iceberg
    # tables in real object storage -- the gap that was previously stated as
    # unverified, since Databricks and Snowflake both need a cloud account.
    #
    # Four things only a real run can prove, each of which was genuinely
    # broken when this first ran:
    #   * Trino has no temporary tables, so the stage had to become an
    #     ordinary (still uniquely-named, still explicitly dropped) table.
    #   * Trino's md5() takes and returns varbinary, so HASH_KEY needed
    #     hex-encoding to be the 32 characters VARCHAR(32) expects.
    #   * Trino's UPDATE rejects a table alias, so correlated updates qualify
    #     by table name instead.
    #   * Iceberg has no identity columns, so ROW_ID is computed -- and has to
    #     stay stable for an updated row while a new row gets the next value.
    config = _trino_config()
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "ice_merge")
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {
            "SQL_ACTION": "SCD1_MERGE",
            "TARGET_OBJECT": "etltest.dim_trino",
            "SOURCE_SQL": "SELECT id, name FROM iceberg.etltest.src_trino WHERE 1=1",
            "MERGE_KEY": "id",
            "MERGE_COMPARE_COLUMNS": "name",
        },
    )
    seed_active_run(postgres_engine, committed_pipeline)
    with trino_engine.begin() as conn:
        for table in ("src_trino", "dim_trino"):
            conn.execute(text(f"DROP TABLE IF EXISTS iceberg.etltest.{table}"))
        conn.execute(text("CREATE TABLE iceberg.etltest.src_trino AS SELECT 1 AS id, 'a' AS name"))

    first = run_task(postgres_engine, config, "TEST_CONCURRENT_PL", "ice_merge")
    assert first.status == "SUCCESS", first.message

    with trino_engine.connect() as conn:
        rows = conn.execute(
            text("SELECT id, name, ROW_ID, length(HASH_KEY) FROM iceberg.etltest.dim_trino")
        ).all()
        # A genuine Iceberg table, not whatever the engine's default is.
        created = conn.execute(text("SHOW CREATE TABLE iceberg.etltest.dim_trino")).scalar_one()
    assert rows == [(1, "a", 1, 32)]
    assert "format = 'PARQUET'" in created

    # One changed row and one new row: the correlated UPDATE leg and the
    # computed-ROW_ID leg, in one pass.
    with trino_engine.begin() as conn:
        conn.execute(text("UPDATE iceberg.etltest.src_trino SET name = 'b' WHERE id = 1"))
        conn.execute(text("INSERT INTO iceberg.etltest.src_trino VALUES (2, 'c')"))
    with postgres_engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_TASK_RUN_LOG SET STATUS = 'FAILED' WHERE TASK_ID = :id"),
            {"id": task_id},
        )

    second = run_task(postgres_engine, config, "TEST_CONCURRENT_PL", "ice_merge")
    assert second.status == "SUCCESS", second.message

    with trino_engine.connect() as conn:
        rows = conn.execute(
            text("SELECT id, name, ROW_ID FROM iceberg.etltest.dim_trino ORDER BY id")
        ).all()
    # The updated row kept its ROW_ID; the new one took the next value.
    assert rows == [(1, "b", 1), (2, "c", 2)]


def _run_trino_task(postgres_engine, pipeline_id, code, params, config):
    task_id = insert_committed_task(postgres_engine, pipeline_id, code)
    insert_committed_task_parameters(postgres_engine, task_id, params)
    return task_id, run_task(postgres_engine, config, "TEST_CONCURRENT_PL", code)


def test_sql_hard_delete_rows_works_on_real_trino_iceberg(
    postgres_engine, trino_engine, committed_pipeline
):
    # E2-65. The table-alias problem was found and fixed for UPDATE, but the
    # HARD_DELETE branch one level up still emitted `DELETE FROM <target> t`,
    # which Trino rejects the same way -- so a whole action was unavailable on
    # the warehouse the architecture centres on. Nothing caught it because the
    # end-to-end Iceberg test covered SCD1_MERGE only.
    config = _trino_config()
    with trino_engine.begin() as conn:
        for table in ("del_src", "del_tgt"):
            conn.execute(text(f"DROP TABLE IF EXISTS iceberg.etltest.{table}"))
        conn.execute(
            text("CREATE TABLE iceberg.etltest.del_src AS SELECT 1 AS id UNION ALL SELECT 2")
        )
    seed_active_run(postgres_engine, committed_pipeline)

    _, created = _run_trino_task(
        postgres_engine,
        committed_pipeline,
        "ice_seed",
        {
            "SQL_ACTION": "CREATE_TABLE",
            "TARGET_OBJECT": "etltest.del_tgt",
            "SOURCE_SQL": ("SELECT id FROM (VALUES (1),(2),(3)) AS v(id) WHERE 1=1"),
        },
        config,
    )
    assert created.status == "SUCCESS", created.message

    _, deleted = _run_trino_task(
        postgres_engine,
        committed_pipeline,
        "ice_del",
        {
            "SQL_ACTION": "DELETE_ROWS",
            "TARGET_OBJECT": "etltest.del_tgt",
            "SOURCE_SQL": "SELECT id FROM iceberg.etltest.del_src WHERE 1=1",
            "MERGE_KEY": "id",
            "HARD_DELETE": "true",
        },
        config,
    )
    assert deleted.status == "SUCCESS", deleted.message

    with trino_engine.connect() as conn:
        remaining = conn.execute(text("SELECT id FROM iceberg.etltest.del_tgt")).scalars().all()
    assert remaining == [3]


def test_a_failed_action_leaves_no_stage_table_behind_on_trino(
    postgres_engine, trino_engine, committed_pipeline
):
    # E2-66. Trino has no temporary tables, so the stage is an ordinary table
    # -- and Trino does not roll back, so a failure between building the stage
    # and dropping it committed a real Iceberg table into the schema the
    # team's own data lives in. The name carries task_run_id, so a retry never
    # reused it: every failed attempt leaked another one, permanently, and
    # nothing looked for them.
    config = _trino_config()
    with trino_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS iceberg.etltest.leak_tgt"))
        # A target missing the audit columns OVERWRITE_TABLE requires: the
        # check fires *after* the stage is built, which is the window.
        conn.execute(text("CREATE TABLE iceberg.etltest.leak_tgt AS SELECT 1 AS id"))
    seed_active_run(postgres_engine, committed_pipeline)
    with trino_engine.connect() as conn:
        before = set(conn.execute(text("SHOW TABLES FROM iceberg.etltest")).scalars().all())

    _, outcome = _run_trino_task(
        postgres_engine,
        committed_pipeline,
        "ice_leak",
        {
            "SQL_ACTION": "OVERWRITE_TABLE",
            "TARGET_OBJECT": "etltest.leak_tgt",
            "SOURCE_SQL": "SELECT id FROM (VALUES (1)) AS v(id) WHERE 1=1",
        },
        config,
    )
    assert outcome.status == "FAILED", outcome.message

    with trino_engine.connect() as conn:
        after = set(conn.execute(text("SHOW TABLES FROM iceberg.etltest")).scalars().all())
    # Compared against a before-snapshot rather than asserting the schema holds
    # no stages at all: a leak from some *other* run is a real problem but not
    # this test's, and a shared schema would otherwise make this fail for
    # somebody else's reason.
    leaked = sorted(t for t in after - before if t.startswith("etl_stage_"))
    assert leaked == [], f"failed action leaked stage table(s): {leaked}"


def test_verify_iceberg_catalog_accepts_a_real_iceberg_catalog(trino_engine):
    # E2-69. "Is this warehouse Iceberg-backed?" was answered by dialect name
    # alone, which on Trino is an assumption: the format comes from the
    # CATALOG, and a deployment routinely has several. So verify it.
    assert verify_iceberg_catalog(_trino_config(), trino_engine) is None


def test_verify_iceberg_catalog_rejects_a_non_iceberg_catalog(trino_engine):
    # The failure this exists to catch: a valid [Warehouse] URL pointing at a
    # non-Iceberg catalog, which the engine would happily create tables in
    # while treating them as Iceberg -- everything "succeeds" and the
    # lakehouse invariant is silently false. `system` is a real Trino catalog
    # and is definitively not an Iceberg one.
    config = _trino_config()
    config.warehouse.profiles["dev"] = replace(
        config.warehouse.active, jdbc_url="jdbc:trino://localhost:58080/system/runtime"
    )
    problem = verify_iceberg_catalog(config, trino_engine)
    assert problem is not None
    assert "not an Iceberg catalog" in problem


def test_verify_iceberg_catalog_is_silent_where_the_question_does_not_apply(postgres_engine):
    # Postgres's storage is fixed by the connection, so there is nothing to
    # check and nothing to report.
    assert verify_iceberg_catalog(make_config(warehouse=True), postgres_engine) is None


def test_every_sql_action_runs_on_real_trino_iceberg(
    postgres_engine, trino_engine, committed_pipeline
):
    # Round 5's headline follow-up. The end-to-end Iceberg test covered
    # SCD1_MERGE only, and the single action probed outside it (DELETE_ROWS,
    # E2-65) turned out to be broken -- so the remaining actions were
    # unexercised on the warehouse the architecture centres on. This walks the
    # whole vocabulary there.
    config = _trino_config()
    seed_active_run(postgres_engine, committed_pipeline)
    src = "SELECT id, name FROM (VALUES (1,'a'),(2,'b')) AS v(id, name) WHERE 1=1"
    with trino_engine.begin() as conn:
        for table in ("all_create", "all_setup", "all_over", "all_scd2", "all_drop", "all_dedup"):
            conn.execute(text(f"DROP TABLE IF EXISTS iceberg.etltest.{table}"))

    for code, params in (
        ("a_create", {"SQL_ACTION": "CREATE_TABLE", "TARGET_OBJECT": "etltest.all_create"}),
        ("a_setup", {"SQL_ACTION": "SETUP_TABLE", "TARGET_OBJECT": "etltest.all_setup"}),
        ("a_over", {"SQL_ACTION": "OVERWRITE_TABLE", "TARGET_OBJECT": "etltest.all_over"}),
        (
            "a_scd2",
            {
                "SQL_ACTION": "SCD2_MERGE",
                "TARGET_OBJECT": "etltest.all_scd2",
                "MERGE_KEY": "id",
                "MERGE_COMPARE_COLUMNS": "name",
            },
        ),
        ("a_dropsrc", {"SQL_ACTION": "CREATE_TABLE", "TARGET_OBJECT": "etltest.all_drop"}),
    ):
        _, outcome = _run_trino_task(
            postgres_engine, committed_pipeline, code, {**params, "SOURCE_SQL": src}, config
        )
        assert outcome.status == "SUCCESS", f"{params['SQL_ACTION']}: {outcome.message}"

    # DROP_TABLE is gated on a CREATE_TABLE sibling for the same target having
    # already succeeded in this run, which a_dropsrc above is.
    _, dropped = _run_trino_task(
        postgres_engine,
        committed_pipeline,
        "a_drop",
        {"SQL_ACTION": "DROP_TABLE", "TARGET_OBJECT": "etltest.all_drop"},
        config,
    )
    assert dropped.status == "SUCCESS", dropped.message

    # E2-75. Duplicate source rows, so _dedupe_stage actually builds its
    # scratch table -- the one path the vocabulary walk above never reaches,
    # because its sources are duplicate-free and the dedupe returns early at
    # its `if not duplicates` guard. That is why a third direct
    # CREATE TEMPORARY TABLE survived two rounds of Trino work: the E2-04
    # guard, the only thing standing between duplicate source rows and a
    # permanently corrupted SCD target, was unreachable on Iceberg.
    _, deduped = _run_trino_task(
        postgres_engine,
        committed_pipeline,
        "a_dedup",
        {
            "SQL_ACTION": "SCD1_MERGE",
            "TARGET_OBJECT": "etltest.all_dedup",
            "SOURCE_SQL": (
                "SELECT id, name FROM (VALUES (1,'v1'),(1,'v2'),(2,'b')) AS v(id, name) WHERE 1=1"
            ),
            "MERGE_KEY": "id",
            "MERGE_COMPARE_COLUMNS": "name",
            "MERGE_DEDUPE_ORDER": "name DESC",
        },
        config,
    )
    assert deduped.status == "SUCCESS", deduped.message

    with trino_engine.connect() as conn:
        tables = set(conn.execute(text("SHOW TABLES FROM iceberg.etltest")).scalars().all())
        scd2 = conn.execute(
            text("SELECT id, ACTIVE_FLAG, ROW_ID FROM iceberg.etltest.all_scd2 ORDER BY id")
        ).all()
        dedup_rows = conn.execute(
            text("SELECT id, name FROM iceberg.etltest.all_dedup ORDER BY id")
        ).all()
    assert {"all_create", "all_setup", "all_over", "all_scd2", "all_dedup"} <= tables
    assert "all_drop" not in tables
    assert scd2 == [(1, "Y", 1), (2, "Y", 2)]
    # The declared ordering picked the winner; one row per key reached the target.
    assert dedup_rows == [(1, "v2"), (2, "b")]
    # Nothing leaked, across seven actions -- including the `_dedup` scratch
    # table, which _sweep_stage did not know about until E2-75.
    assert not [t for t in tables if t.startswith("etl_stage_")]


def _insert_task_parameters(conn, task_id: int, params: dict[str, str]) -> None:
    """Insert CFG_TASK_PARAMETERS rows through the rolled-back pg_conn fixture."""
    for name, value in params.items():
        conn.execute(
            text(
                "INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE) "
                "VALUES (:task_id, :name, :value)"
            ),
            {"task_id": task_id, "name": name, "value": value},
        )


def test_validate_reports_the_removed_primary_key_parameter(
    pg_conn, cfg_pipeline, cfg_task, postgres_engine
):
    # E2-71. PRIMARY_KEY was replaced by a generated ROW_ID in E2-54, but it
    # stayed in KNOWN_PARAMETERS, so validate's unrecognized-parameter check --
    # built precisely to catch "a typo will be ignored" -- stayed silent while
    # three separate documents told a reader to set it. Removing it from the
    # vocabulary is what makes the check speak.
    _insert_task_parameters(pg_conn, cfg_task, {"PRIMARY_KEY": "id", "SQL_ACTION": "CREATE_TABLE"})

    issues = validate_task_parameters(pg_conn)

    assert any("PRIMARY_KEY" in issue.message for issue in issues)


def test_validate_rejects_an_unsafe_pipeline_code(pg_conn, cfg_pipeline, cfg_task):
    # E2-84. TASK_CODE was checked with the right reason -- codes are
    # interpolated unquoted into SQL and into generate-yml's bash_command --
    # and PIPELINE_CODE, which gets the identical treatment in the identical
    # places, was checked by nothing: not here, and not by a CHECK in
    # schema.sql. docs_generator writes f"{pipeline_code}.html", so a code
    # containing `/` or `..` writes outside the output directory, and one
    # containing a space produces a file the generated href does not point at.
    _insert_task_parameters(
        pg_conn, cfg_task, {"SQL_ACTION": "CREATE_TABLE", "SOURCE_SQL": "SELECT 1 WHERE 1=1"}
    )
    pg_conn.execute(
        text("UPDATE CFG_PIPELINES SET PIPELINE_CODE = '../escape' WHERE PIPELINE_ID = :p"),
        {"p": cfg_pipeline},
    )

    issues = validate_task_parameters(pg_conn)

    assert any("PIPELINE_CODE" in issue.message for issue in issues)


def test_validate_rejects_a_merge_key_that_is_not_an_identifier(pg_conn, cfg_pipeline, cfg_task):
    # E2-85. _split_pipe_list only strips whitespace and the result is
    # interpolated unquoted into the merge SQL, so a typo like "customer id"
    # passed validate and failed the task at run time with a warehouse syntax
    # error naming neither the parameter nor the task.
    _insert_task_parameters(
        pg_conn,
        cfg_task,
        {
            "SQL_ACTION": "SCD1_MERGE",
            "TARGET_OBJECT": "public.t",
            "SOURCE_SQL": "SELECT 1 WHERE 1=1",
            "MERGE_KEY": "customer id",
            "MERGE_COMPARE_COLUMNS": "name",
        },
    )

    issues = validate_task_parameters(pg_conn)

    assert any("MERGE_KEY" in issue.message for issue in issues)


def test_validate_accepts_a_well_formed_merge_dedupe_order_and_rejects_a_strange_one(
    pg_conn, cfg_pipeline, cfg_task
):
    # E2-85's narrower half. MERGE_DEDUPE_ORDER is deliberately a SQL fragment,
    # so it gets a shape check rather than an identifier check -- the most that
    # fits without a parser.
    base = {
        "SQL_ACTION": "SCD1_MERGE",
        "TARGET_OBJECT": "public.t",
        "SOURCE_SQL": "SELECT 1 WHERE 1=1",
        "MERGE_KEY": "id",
        "MERGE_COMPARE_COLUMNS": "name",
    }
    _insert_task_parameters(
        pg_conn, cfg_task, {**base, "MERGE_DEDUPE_ORDER": "updated_at DESC, id ASC NULLS LAST"}
    )
    assert not [i for i in validate_task_parameters(pg_conn) if "MERGE_DEDUPE_ORDER" in i.message]

    pg_conn.execute(
        text(
            "UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE = 'updated_at; DROP TABLE t' "
            "WHERE TASK_ID = :t AND PARAMETER_NAME = 'MERGE_DEDUPE_ORDER'"
        ),
        {"t": cfg_task},
    )

    issues = validate_task_parameters(pg_conn)

    assert any("MERGE_DEDUPE_ORDER" in issue.message for issue in issues)


def test_validate_rejects_an_unrecognized_table_format(pg_conn, cfg_pipeline, cfg_task):
    # E2-73. The vocabulary was enforced only at execution, so a typo passed
    # validate and failed the task.
    _insert_task_parameters(
        pg_conn, cfg_task, {"SQL_ACTION": "CREATE_TABLE", "TABLE_FORMAT": "icberg"}
    )

    issues = validate_task_parameters(pg_conn)

    assert any("TABLE_FORMAT" in issue.message for issue in issues)


def test_requested_table_formats_unions_the_warehouse_default_with_task_overrides(
    pg_conn, cfg_pipeline, cfg_task
):
    # E2-72. The catalog guard was warehouse-level while the declaration is
    # task-level, so a native default plus one task overriding to iceberg
    # skipped the check for the task that needed it.
    _insert_task_parameters(
        pg_conn, cfg_task, {"SQL_ACTION": "CREATE_TABLE", "TABLE_FORMAT": "iceberg"}
    )
    native = replace(make_config(warehouse=True), warehouse_table_format="native")

    assert requested_table_formats(pg_conn, native) == {"native", "iceberg"}


def test_validate_flags_a_trino_catalog_that_cannot_serve_a_requested_iceberg_format(
    pg_conn, cfg_pipeline, cfg_task, trino_engine
):
    # The whole point of E2-72: a task that explicitly asked for Iceberg
    # against a non-Iceberg catalog would get Hive tables while everything
    # reported success.
    _insert_task_parameters(
        pg_conn, cfg_task, {"SQL_ACTION": "CREATE_TABLE", "TABLE_FORMAT": "iceberg"}
    )
    config = _trino_config()
    config = replace(config, warehouse_table_format="native")
    config.warehouse.profiles["dev"] = replace(
        config.warehouse.active, jdbc_url="jdbc:trino://localhost:58080/system/runtime"
    )

    issues = validate_warehouse_storage(pg_conn, config, trino_engine)

    assert any("not an Iceberg catalog" in issue.message for issue in issues)


def _cloud_config(profile, table_format: str) -> ConnectorConfig:
    """Engine DB on the test Postgres, warehouse on a real cloud warehouse."""
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
        cloning=CloningConfig(),
        warehouse=ConnectionSection(active_profile="dev", profiles={"dev": profile}),
        warehouse_table_format=table_format,
    )


def _run_cloud_vocabulary(postgres_engine, pipeline_id, config, schema, extra_params):
    """Walk the SQL action vocabulary against a real cloud warehouse."""
    seed_active_run(postgres_engine, pipeline_id)
    src = "SELECT 1 AS id, 'a' AS name"
    results = {}
    for code, params in (
        ("cw_create", {"SQL_ACTION": "CREATE_TABLE", "TARGET_OBJECT": f"{schema}.cw_create"}),
        ("cw_over", {"SQL_ACTION": "OVERWRITE_TABLE", "TARGET_OBJECT": f"{schema}.cw_over"}),
        (
            "cw_scd1",
            {
                "SQL_ACTION": "SCD1_MERGE",
                "TARGET_OBJECT": f"{schema}.cw_scd1",
                "MERGE_KEY": "id",
                "MERGE_COMPARE_COLUMNS": "name",
            },
        ),
    ):
        task_id = insert_committed_task(postgres_engine, pipeline_id, code)
        insert_committed_task_parameters(
            postgres_engine, task_id, {**params, "SOURCE_SQL": src, **extra_params}
        )
        results[params["SQL_ACTION"]] = run_task(
            postgres_engine, config, "TEST_CONCURRENT_PL", code
        )
    return results


def test_sql_actions_run_against_real_databricks(
    postgres_engine, databricks_profile, committed_pipeline
):
    # Skips unless ETL_CRAFT_TEST_DATABRICKS_* are set. This is the test that
    # turns "the Databricks path is unverified" into a one-command answer:
    # export the credentials (or put them in CI secrets) and run the suite.
    schema = os.environ.get("ETL_CRAFT_TEST_DATABRICKS_SCHEMA", "default")
    config = _cloud_config(databricks_profile, "iceberg")
    results = _run_cloud_vocabulary(postgres_engine, committed_pipeline, config, schema, {})
    for action, outcome in results.items():
        assert outcome.status == "SUCCESS", f"{action}: {outcome.message}"


def test_sql_actions_run_against_real_snowflake(
    postgres_engine, snowflake_profile, committed_pipeline
):
    # Snowflake Iceberg tables need an EXTERNAL VOLUME over real cloud storage,
    # which a trial account does not include -- so without it this runs the
    # `native` path instead of skipping outright. Connection, auth and the
    # whole action vocabulary are still exercised either way.
    schema = os.environ.get("ETL_CRAFT_TEST_SNOWFLAKE_SCHEMA", "PUBLIC")
    volume = os.environ.get("ETL_CRAFT_TEST_SNOWFLAKE_EXTERNAL_VOLUME", "")
    base = os.environ.get("ETL_CRAFT_TEST_SNOWFLAKE_BASE_LOCATION", "")
    if volume and base:
        table_format, extra = "iceberg", {"EXTERNAL_VOLUME": volume, "BASE_LOCATION": base}
    else:
        table_format, extra = "native", {}
    config = _cloud_config(snowflake_profile, table_format)
    results = _run_cloud_vocabulary(postgres_engine, committed_pipeline, config, schema, extra)
    for action, outcome in results.items():
        assert outcome.status == "SUCCESS", f"{action}: {outcome.message}"


def test_table_format_native_still_runs_end_to_end(
    postgres_engine, trino_engine, committed_pipeline
):
    # "support non iceberg as well" -- on Trino the catalog decides the format
    # either way, so what this actually proves is that asking for `native`
    # does not break the action path: no clause is emitted, nothing refuses,
    # and the table is still built and populated. The Databricks and Snowflake
    # clauses are unit-tested, since neither is reachable from here.
    config = replace(_trino_config(), warehouse_table_format="native")
    seed_active_run(postgres_engine, committed_pipeline)
    with trino_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS iceberg.etltest.native_tgt"))

    _, outcome = _run_trino_task(
        postgres_engine,
        committed_pipeline,
        "ice_native",
        {
            "SQL_ACTION": "CREATE_TABLE",
            "TARGET_OBJECT": "etltest.native_tgt",
            "SOURCE_SQL": "SELECT id FROM (VALUES (1),(2)) AS v(id) WHERE 1=1",
            "TABLE_FORMAT": "native",
        },
        config,
    )
    assert outcome.status == "SUCCESS", outcome.message
    with trino_engine.connect() as conn:
        assert conn.execute(
            text("SELECT id FROM iceberg.etltest.native_tgt ORDER BY id")
        ).scalars().all() == [1, 2]


def test_sql_actions_assign_row_ids_on_an_iceberg_backed_warehouse(
    postgres_engine, committed_pipeline, warehouse_tables, monkeypatch
):
    # Iceberg has no identity columns, no sequences and no enforced primary
    # keys, so ROW_ID has to be *computed* -- max already present, plus a row
    # number over the rows being added -- and every INSERT has to supply it,
    # where Postgres and DuckDB let the column fill itself.
    #
    # There is no Iceberg warehouse reachable from the test suite, so this
    # forces that code path on against real Postgres by taking Postgres out of
    # NATIVE_STORAGE_DIALECTS. What it proves is what most of the risk
    # actually is: the generated SQL is well-formed and the ROW_ID arithmetic
    # is right across runs. What it does NOT prove is that Databricks,
    # Snowflake or Trino accept these statements -- that needs a real
    # endpoint, and is stated as unverified rather than implied.
    monkeypatch.setattr(sql_actions_module, "NATIVE_STORAGE_DIALECTS", frozenset({"duckdb"}))
    target = f"public.iceberg_rows_{committed_pipeline}"
    warehouse_tables.append(target)

    task_id = insert_committed_task(postgres_engine, committed_pipeline, "ice")
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {
            "SQL_ACTION": "SCD1_MERGE",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": (
                "SELECT id, name FROM (VALUES (1,'a'),(2,'b')) AS v(id, name) WHERE 1=1"
            ),
            "MERGE_KEY": "id",
            "MERGE_COMPARE_COLUMNS": "name",
        },
    )
    seed_active_run(postgres_engine, committed_pipeline)
    config = make_config(warehouse=True)

    first = run_task(postgres_engine, config, "TEST_CONCURRENT_PL", "ice")
    assert first.status == "SUCCESS", first.message

    with postgres_engine.connect() as conn:
        rows = conn.execute(text(f"SELECT id, ROW_ID FROM {target} ORDER BY id")).all()
    assert rows == [(1, 1), (2, 2)]

    # A second run adding a row must continue the numbering rather than
    # restarting it -- the whole point of reading the current max first.
    with postgres_engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE = :v "
                "WHERE TASK_ID = :id AND PARAMETER_NAME = 'SOURCE_SQL'"
            ),
            {
                "id": task_id,
                "v": "SELECT id, name FROM (VALUES (1,'a'),(2,'b'),(3,'c')) "
                "AS v(id, name) WHERE 1=1",
            },
        )
        conn.execute(
            text("UPDATE AUD_TASK_RUN_LOG SET STATUS = 'FAILED' WHERE TASK_ID = :id"),
            {"id": task_id},
        )

    second = run_task(postgres_engine, config, "TEST_CONCURRENT_PL", "ice")
    assert second.status == "SUCCESS", second.message

    with postgres_engine.connect() as conn:
        rows = conn.execute(text(f"SELECT id, ROW_ID FROM {target} ORDER BY id")).all()
    assert rows == [(1, 1), (2, 2), (3, 3)]


def test_sql_same_table_name_in_two_schemas_on_duckdb(
    postgres_engine, committed_pipeline, tmp_path
):
    # E2-64, and the failure is worse than a shared name looks. The DuckDB
    # surrogate-key sequence was named from the *bare* table name and created
    # unqualified, so staging.orders and marts.orders -- two perfectly
    # ordinary targets -- shared one sequence.
    #
    # Probing it on a single connection makes it look self-limiting: DuckDB
    # refuses the DROP SEQUENCE with a dependency error, so nothing is
    # corrupted. But the engine runs every task in its own process with its
    # own connection, and there the DROP *succeeds silently*. The second
    # table's creation then resets the shared sequence to 1, and the next
    # insert into the first table -- which this run never touched -- collides
    # with a ROW_ID it already holds:
    #
    #   Constraint Error: Duplicate key "ROW_ID: 2" violates primary key
    #
    # Verified directly at both levels. So this asserts the *aftermath*, not
    # merely that both tables get built: a test that stops at creation passes
    # against the bug.
    config = make_config(duckdb_warehouse=str(tmp_path / "warehouse.duckdb"))

    def with_warehouse(fn):
        engine = build_warehouse_engine(config)
        try:
            return fn(engine)
        finally:
            engine.dispose()

    def seed(engine):
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS marts"))
            conn.execute(text("CREATE TABLE staging.src AS SELECT * FROM range(5) t(id)"))
            conn.execute(text("CREATE TABLE marts.src AS SELECT 1 AS id"))

    with_warehouse(seed)

    # Deliberately different row counts: with equal counts the shared counter
    # happens to land clear of the first table's keys and the bug hides.
    for task_code, target, source in (
        ("duck_stg", "staging.orders", "staging.src"),
        ("duck_mart", "marts.orders", "marts.src"),
    ):
        _duckdb_sql_task(
            postgres_engine,
            committed_pipeline,
            task_code,
            {
                "SQL_ACTION": "CREATE_TABLE",
                "TARGET_OBJECT": target,
                "SOURCE_SQL": f"SELECT id FROM {source} WHERE 1=1",
            },
        )
    seed_active_run(postgres_engine, committed_pipeline)

    first = run_task(postgres_engine, config, "TEST_CONCURRENT_PL", "duck_stg")
    assert first.status == "SUCCESS", first.message
    second = run_task(postgres_engine, config, "TEST_CONCURRENT_PL", "duck_mart")
    assert second.status == "SUCCESS", second.message

    def insert_into_first(engine):
        with engine.begin() as conn:
            conn.execute(text("INSERT INTO staging.orders (id) VALUES (99)"))
            return (
                conn.execute(text("SELECT ROW_ID FROM staging.orders ORDER BY ROW_ID"))
                .scalars()
                .all()
            )

    # Building marts.orders must leave staging.orders' own key allocation
    # untouched and still usable.
    assert with_warehouse(insert_into_first) == [1, 2, 3, 4, 5, 6]


def test_sql_schema_evolution_restores_the_surrogate_key_on_duckdb(
    postgres_engine, committed_pipeline, tmp_path
):
    # The evolution rebuild (CTAS -> drop -> rename) does not carry a primary
    # key or an identity across, so _restore_surrogate_key has to rebuild it
    # -- and on DuckDB that means repositioning the sequence past the values
    # already carried over. Nothing else in the suite reaches that branch:
    # every other evolution test runs against Postgres, where the identity is
    # restarted instead. An unrepositioned sequence hands the next insert
    # ROW_ID 1 again, colliding with the primary key it just re-added, so this
    # asserts the new row's own ROW_ID rather than merely that evolution
    # succeeded.
    config = make_config(duckdb_warehouse=str(tmp_path / "warehouse.duckdb"))

    def with_warehouse(fn):
        engine = build_warehouse_engine(config)
        try:
            return fn(engine)
        finally:
            engine.dispose()

    def seed(engine):
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))
            conn.execute(text("CREATE TABLE staging.src AS SELECT 1 AS id, 'a' AS name"))

    with_warehouse(seed)

    first = _duckdb_sql_task(
        postgres_engine,
        committed_pipeline,
        "duck_evo",
        {
            "SQL_ACTION": "SCD1_MERGE",
            "TARGET_OBJECT": "staging.evo",
            "SOURCE_SQL": "SELECT id, name FROM staging.src WHERE 1=1",
            "MERGE_KEY": "id",
            "MERGE_COMPARE_COLUMNS": "name",
        },
    )
    seed_active_run(postgres_engine, committed_pipeline)
    assert run_task(postgres_engine, config, "TEST_CONCURRENT_PL", "duck_evo").status == "SUCCESS"

    # A genuinely new column *and* a new row: the column forces the rebuild,
    # the row proves the restored sequence does not hand out a taken value.
    def widen_source(engine):
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE staging.src"))
            conn.execute(
                text(
                    "CREATE TABLE staging.src AS "
                    "SELECT 1 AS id, 'a' AS name, 'z' AS extra "
                    "UNION ALL SELECT 2, 'b', 'y'"
                )
            )

    with_warehouse(widen_source)
    with postgres_engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE = :v "
                "WHERE TASK_ID = :id AND PARAMETER_NAME = 'SOURCE_SQL'"
            ),
            {"id": first, "v": "SELECT id, name, extra FROM staging.src WHERE 1=1"},
        )
        conn.execute(
            text(
                "INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE) "
                "VALUES (:id, 'SCHEMA_EVOLUTION', 'true')"
            ),
            {"id": first},
        )
        conn.execute(
            text("UPDATE AUD_TASK_RUN_LOG SET STATUS = 'FAILED' WHERE TASK_ID = :id"),
            {"id": first},
        )

    outcome = run_task(postgres_engine, config, "TEST_CONCURRENT_PL", "duck_evo")
    assert outcome.status == "SUCCESS", outcome.message

    def check(engine):
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT id, name, extra, ROW_ID FROM staging.evo ORDER BY id")
            ).all()
            # Not Inspector.get_pk_constraint: duckdb_engine does not reflect
            # primary keys at all (the same gap validate._primary_key_columns
            # works around), so it would report none here whether the rebuild
            # restored one or not -- which is exactly the assertion this test
            # needs to be able to make.
            pk = conn.execute(
                text(
                    "SELECT constraint_column_names FROM duckdb_constraints() "
                    "WHERE table_name = 'evo' AND constraint_type = 'PRIMARY KEY'"
                )
            ).all()
            return rows, pk

    rows, pk = with_warehouse(check)
    # The pre-existing row kept its own ROW_ID; the new one got the next free
    # value, not a duplicate of it. Its `extra` is NULL rather than 'z' -- and
    # that is correct, not a gap in evolution: `extra` is not in
    # MERGE_COMPARE_COLUMNS, so the row's HASH_KEY did not change and SCD1
    # left it alone. An evolved-in column is backfilled NULL on existing rows
    # and stays that way until something the merge actually watches changes.
    assert rows == [(1, "a", None, 1), (2, "b", "y", 2)]
    assert [[c.lower() for c in row[0]] for row in pk] == [["row_id"]]


def test_sql_scd2_merge_keeps_history_with_a_surrogate_primary_key(
    postgres_engine, committed_pipeline, warehouse_tables
):
    # E2-54 regression, reproduced against real Postgres during round 3. The
    # earlier PRIMARY_KEY parameter named a *business* column, and an SCD2
    # target holds several rows per merge key by design -- so declaring the
    # natural key as the primary key worked for exactly one run and then
    # failed permanently with a unique violation, leaving the target holding
    # only the OLD version of every changed row. The history the merge exists
    # to record was never written.
    #
    # Per explicit correction -- "all primary keys are basically identity
    # columns. merge keys are natural keys" -- the engine generates ROW_ID
    # instead, so the natural key is free to repeat.
    target = f"public.sqlx_scd2pk_{committed_pipeline}"
    src = f"sqlx_scd2pk_src_{committed_pipeline}"
    warehouse_tables.extend([target, src])
    with postgres_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE {src} (id int, name varchar)"))
        conn.execute(text(f"INSERT INTO {src} VALUES (1, 'a')"))

    task_id = insert_committed_task(postgres_engine, committed_pipeline, "scd2pk")
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {
            "SQL_ACTION": "SCD2_MERGE",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": f"SELECT id, name FROM {src} WHERE 1=1",
            "MERGE_KEY": "id",
            "MERGE_COMPARE_COLUMNS": "name",
        },
    )
    seed_active_run(postgres_engine, committed_pipeline)

    assert (
        run_task(
            postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "scd2pk"
        ).status
        == "SUCCESS"
    )

    # A genuine value change: the merge must deactivate the old version and
    # insert a new one -- two rows sharing merge key 1.
    with postgres_engine.begin() as conn:
        conn.execute(text(f"UPDATE {src} SET name = 'b' WHERE id = 1"))
        conn.execute(
            text("UPDATE AUD_TASK_RUN_LOG SET STATUS = 'FAILED' WHERE TASK_ID = :id"),
            {"id": task_id},
        )

    assert (
        run_task(
            postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "scd2pk"
        ).status
        == "SUCCESS"
    )

    with postgres_engine.connect() as conn:
        rows = conn.execute(text(f"SELECT name, ACTIVE_FLAG FROM {target} ORDER BY name")).all()
        pk = inspect(postgres_engine).get_pk_constraint(target.split(".", 1)[1], schema="public")
        row_ids = conn.execute(text(f"SELECT COUNT(DISTINCT ROW_ID) FROM {target}")).scalar_one()
    # Both versions present: the history survived.
    assert rows == [("a", "N"), ("b", "Y")]
    # And the single-column PK convention holds, on the surrogate key.
    assert pk["constrained_columns"] == ["row_id"]
    assert row_ids == 2


def test_sql_create_table_target_passes_validates_own_primary_key_check(
    postgres_engine, committed_pipeline, warehouse_tables, pg_conn, cfg_pipeline, cfg_task
):
    # E2-03 regression, reproduced against real Postgres during the iteration-1
    # review. CLAUDE.md states "every target table is required to have a
    # single-column primary key — an enforced framework convention", checked by
    # validate through introspection. But CREATE_TABLE builds the target with
    # CREATE TABLE ... AS SELECT, which never creates one, so every table the
    # engine made failed the engine's own convention:
    #   "'public.probe_pk_…' must have exactly one primary key column, found []"
    target = f"public.sqlx_pk_{committed_pipeline}"
    warehouse_tables.append(target)
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "pk_create")
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {
            "SQL_ACTION": "CREATE_TABLE",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": "SELECT 1 AS id, 'x' AS val WHERE 1=1",
        },
    )
    seed_active_run(postgres_engine, committed_pipeline)

    assert (
        run_task(
            postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "pk_create"
        ).status
        == "SUCCESS"
    )

    # [DEVIATION, E2-54] The key is ROW_ID, generated by the engine, not a
    # business column the author named. "all primary keys are basically
    # identity columns. merge keys are natural keys" -- which is also what
    # makes this convention satisfiable on an SCD2 target.
    _insert_business_rule(pg_conn, cfg_pipeline, cfg_task, "pk_rule", target, "row_id")
    assert validate_business_rule_keys(pg_conn, postgres_engine) == []


def test_sql_action_rejects_a_target_object_with_no_schema(postgres_engine, committed_pipeline):
    # E2-25. A TARGET_OBJECT with no dot used to raise a bare
    # "ValueError: not enough values to unpack" from inside the crash-detection
    # fork, so the parent reported only its generic "died unexpectedly"
    # fallback — a traceback-shaped message about a config typo. qualify()
    # didn't validate either, so the creating actions silently emitted a
    # malformed two-part name instead of failing.
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "bad_target")
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {
            "SQL_ACTION": "CREATE_TABLE",
            "TARGET_OBJECT": "no_schema_here",
            "SOURCE_SQL": "SELECT 1 AS id WHERE 1=1",
        },
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(
        postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "bad_target"
    )

    assert outcome.status == "FAILED"
    assert "must be exactly 'schema.table'" in outcome.message
    assert "died unexpectedly" not in outcome.message


def test_sql_scd1_merge_rejects_duplicate_merge_keys_before_touching_the_target(
    postgres_engine, committed_pipeline, warehouse_tables
):
    # E2-04 regression, reproduced against real Postgres during the iteration-1
    # review. With two source rows sharing a MERGE_KEY: run 1 took the NOT
    # EXISTS insert leg and wrote *both*, leaving two "current" rows for one
    # key and reporting SUCCESS — silent corruption. Run 2, once any compared
    # value changed, died on the correlated `SET col = (SELECT ...)` with a
    # cardinality violation, and stayed dead: the duplicates were in the
    # target by then, so no retry could recover it without manual SQL.
    #
    # The guard runs before any merge statement, so the target is untouched.
    target = f"public.sqlx_dupe_{committed_pipeline}"
    src = f"sqlx_dupe_src_{committed_pipeline}"
    warehouse_tables.extend([target, src])
    with postgres_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE {src} (id int, name varchar)"))
        conn.execute(text(f"INSERT INTO {src} VALUES (1, 'a'), (1, 'b')"))

    task_id = insert_committed_task(postgres_engine, committed_pipeline, "dupe_merge")
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {
            "SQL_ACTION": "SCD1_MERGE",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": f"SELECT * FROM {src} WHERE 1=1",
            "MERGE_KEY": "id",
            "MERGE_COMPARE_COLUMNS": "name",
        },
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(
        postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "dupe_merge"
    )

    assert outcome.status == "FAILED"
    assert "more than one row for the same MERGE_KEY" in outcome.message
    assert "MERGE_DEDUPE_ORDER" in outcome.message
    with postgres_engine.connect() as conn:
        exists = conn.execute(text("SELECT to_regclass(:t)"), {"t": target}).scalar_one_or_none()
    # Nothing was created or written — the run failed before touching it.
    assert exists is None


def test_sql_scd1_merge_dedupes_by_declared_order_across_two_runs(
    postgres_engine, committed_pipeline, warehouse_tables
):
    # The other half of E2-04's decision: duplicates ARE allowed, but only when
    # the task says which row wins. Run twice, because run 1 exercises the
    # insert leg and run 2 the correlated-UPDATE leg — the statement that
    # actually died with a cardinality violation before this guard existed.
    target = f"public.sqlx_dedupe_{committed_pipeline}"
    src = f"sqlx_dedupe_src_{committed_pipeline}"
    warehouse_tables.extend([target, src])
    with postgres_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE {src} (id int, name varchar, seen int)"))
        conn.execute(text(f"INSERT INTO {src} VALUES (1, 'old', 1), (1, 'new', 2)"))

    task_id = insert_committed_task(postgres_engine, committed_pipeline, "dedupe_merge")
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {
            "SQL_ACTION": "SCD1_MERGE",
            "TARGET_OBJECT": target,
            "SOURCE_SQL": f"SELECT * FROM {src} WHERE 1=1",
            "MERGE_KEY": "id",
            "MERGE_COMPARE_COLUMNS": "name",
            "MERGE_DEDUPE_ORDER": "seen DESC",
            "PRIMARY_KEY": "id",
        },
    )
    seed_active_run(postgres_engine, committed_pipeline)
    assert (
        run_task(
            postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "dedupe_merge"
        ).status
        == "SUCCESS"
    )
    with postgres_engine.connect() as conn:
        assert conn.execute(text(f"SELECT id, name FROM {target}")).all() == [(1, "new")]

    # Run 2 with a changed winner — the correlated UPDATE leg.
    with postgres_engine.begin() as conn:
        conn.execute(text(f"UPDATE {src} SET name = 'newest' WHERE seen = 2"))
        conn.execute(
            text("UPDATE AUD_TASK_RUN_LOG SET STATUS = 'FAILED' WHERE TASK_ID = :id"),
            {"id": task_id},
        )
    assert (
        run_task(
            postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "dedupe_merge"
        ).status
        == "SUCCESS"
    )
    with postgres_engine.connect() as conn:
        assert conn.execute(text(f"SELECT id, name FROM {target}")).all() == [(1, "newest")]


def test_sql_scd1_merge_inserts_updates_and_skips_unchanged(
    postgres_engine, committed_pipeline, warehouse_tables
):
    target = f"public.sqlx_scd1_{committed_pipeline}"
    src = f"sqlx_scd1_src_{committed_pipeline}"
    warehouse_tables.extend([target, src])
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
    postgres_engine, committed_pipeline, warehouse_tables
):
    target = f"public.sqlx_scd2_{committed_pipeline}"
    src = f"sqlx_scd2_src_{committed_pipeline}"
    warehouse_tables.extend([target, src])
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


def test_sql_scd2_merge_converges_for_a_key_left_with_no_active_row(
    postgres_engine, committed_pipeline, warehouse_tables
):
    # E2-74. The two legs disagreed about what "already present" means:
    # changed_keys required an ACTIVE_FLAG = 'Y' row, while the new-row
    # NOT EXISTS looked at every row regardless of flag. A key holding rows
    # but no active one fell through *both* legs, forever -- the merge
    # reported SUCCESS on every retry and never wrote the current version.
    #
    # That state is exactly what a committed deactivate followed by a failed
    # INSERT leaves behind, which on Trino/Iceberg is an ordinary failure
    # mode rather than a hypothetical: this module promises idempotency
    # rather than atomicity there, and for SCD2_MERGE the promise was false.
    # Flipping ACTIVE_FLAG is what that half-written state looks like.
    target = f"public.sqlx_scd2conv_{committed_pipeline}"
    src = f"sqlx_scd2conv_src_{committed_pipeline}"
    warehouse_tables.extend([target, src])
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
    config = make_config(warehouse=True)
    for code in ("setup", "merge"):
        assert run_task(postgres_engine, config, "TEST_CONCURRENT_PL", code).status == "SUCCESS"

    # The half-written state: the deactivate committed for the x -> y change,
    # the INSERT of the new version did not.
    with postgres_engine.begin() as conn:
        finalize_pipeline_run_stub(conn, run1)
        conn.execute(text(f"UPDATE {src} SET name = 'y' WHERE id = 1"))
        conn.execute(text(f"UPDATE {target} SET active_flag = 'N' WHERE id = 1"))
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(postgres_engine, make_config(warehouse=True), "TEST_CONCURRENT_PL", "merge")

    assert outcome.status == "SUCCESS", outcome.message
    with postgres_engine.connect() as conn:
        rows = conn.execute(
            text(f"SELECT id, name, active_flag FROM {target} ORDER BY active_flag, name")
        ).all()
    # The retry converged: the current version is present and active again.
    assert rows == [(1, "x", "N"), (1, "y", "Y")]


def test_sql_scd_merge_missing_merge_key_fails(
    postgres_engine, committed_pipeline, warehouse_tables
):
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
    postgres_engine, committed_pipeline, warehouse_tables
):
    target = f"public.sqlx_drop_ok_{committed_pipeline}"
    warehouse_tables.append(target)
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
    postgres_engine, committed_pipeline, warehouse_tables
):
    target = f"public.sqlx_drop_refuse_{committed_pipeline}"
    bare_table = target.split(".", 1)[1]
    warehouse_tables.append(target)
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
    postgres_engine, committed_pipeline, warehouse_tables
):
    # A CREATE_TABLE sibling exists in CFG_ (the earlier test covers that
    # part) but hasn't actually executed under *this* run — "created by
    # this pipeline using create_table before this drop table step," per
    # explicit instruction, not just declared somewhere in config.
    target = f"public.sqlx_drop_not_run_{committed_pipeline}"
    warehouse_tables.append(target)
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


def test_sql_delete_rows_hard_and_soft(postgres_engine, committed_pipeline, warehouse_tables):
    target = f"public.sqlx_delete_{committed_pipeline}"
    warehouse_tables.append(target)
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
    postgres_engine, committed_pipeline, warehouse_tables
):
    target = f"public.sqlx_missing_col_{committed_pipeline}"
    warehouse_tables.append(target)
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
    postgres_engine, committed_pipeline, warehouse_tables
):
    target = f"public.sqlx_evolve_off_{committed_pipeline}"
    warehouse_tables.append(target)
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
    postgres_engine, committed_pipeline, warehouse_tables
):
    target = f"public.sqlx_evolve_on_{committed_pipeline}"
    warehouse_tables.extend([target, f"{target}__etl_evolve"])
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
    assert cols == ["id", "name", "extra", "pipeline_run_id", "update_date", "row_id"]
    assert data == [(1, "a", "z")]


def test_sql_overwrite_table_missing_audit_column_fails_clearly(
    postgres_engine, committed_pipeline, warehouse_tables
):
    # A target that predates this convention (or was hand-built) has its
    # business columns and PIPELINE_RUN_ID, but never got UPDATE_DATE — the
    # column OVERWRITE_TABLE itself needs to stamp. This must fail up front
    # with a clear message, not partway through the real UPDATE/INSERT with
    # a raw "column update_date does not exist".
    target = f"public.sqlx_over_missing_audit_{committed_pipeline}"
    warehouse_tables.append(target)
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
    postgres_engine, committed_pipeline, warehouse_tables
):
    # This check must fire regardless of SCHEMA_EVOLUTION: that flag only
    # ever governs new *business* columns the staged SELECT introduces, never
    # repairing a target's own missing engine-managed columns.
    target = f"public.sqlx_scd1_missing_audit_{committed_pipeline}"
    warehouse_tables.append(target)
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
    postgres_engine, committed_pipeline, warehouse_tables
):
    # DELETE_ROWS never goes through _check_or_evolve_schema (it only
    # matches on MERGE_KEY, no shape comparison) — its soft-delete path gets
    # its own DELETE_FLAG-presence check for the same reason.
    target = f"public.sqlx_delete_missing_flag_{committed_pipeline}"
    warehouse_tables.append(target)
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
    postgres_engine, committed_pipeline, warehouse_tables
):
    # HARD_DELETE=true issues a real DELETE and never touches DELETE_FLAG at
    # all -- unlike the soft-delete path above, a target that never had that
    # column must NOT be rejected by the audit-column check.
    target = f"public.sqlx_hard_delete_no_flag_{committed_pipeline}"
    warehouse_tables.append(target)
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
    postgres_engine, committed_pipeline, warehouse_tables
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
    postgres_engine, committed_pipeline, warehouse_tables
):
    target = f"public.brx_target_{committed_pipeline}"
    bare_table = target.split(".", 1)[1]
    warehouse_tables.append(target)
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


def test_business_rules_force_scans_all_data(postgres_engine, committed_pipeline, warehouse_tables):
    target = f"public.brx_force_{committed_pipeline}"
    bare_table = target.split(".", 1)[1]
    warehouse_tables.append(target)
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
    postgres_engine, committed_pipeline, warehouse_tables
):
    # "it sequence would be like a dense rank. run in waves. every rule
    # sharing same number for a task can run parallel" — two rules at the
    # same SEQUENCE_NUMBER exercise the ThreadPoolExecutor wave path
    # (single-rule waves take a separate, sequential fast path), and both
    # must still produce correct, independent results.
    target = f"public.brx_wave_{committed_pipeline}"
    bare_table = target.split(".", 1)[1]
    warehouse_tables.append(target)
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
    postgres_engine, committed_pipeline, warehouse_tables
):
    # "every rule sharing same number for a task can run parallel" — one
    # rule in the wave has malformed SQL; its wave-mate is independent and
    # must still run to completion and keep its own result, even though the
    # task as a whole still ends up FAILED because of the broken one.
    target = f"public.brx_wave_fail_{committed_pipeline}"
    bare_table = target.split(".", 1)[1]
    warehouse_tables.append(target)
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
    postgres_engine, committed_pipeline, warehouse_tables
):
    target = f"public.brx_bad_{committed_pipeline}"
    bare_table = target.split(".", 1)[1]
    warehouse_tables.append(target)
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
        def __init__(self, host, port, timeout=None):
            self.host = host
            self.port = port
            # E2-17: the real client is constructed with a timeout now. An
            # unreachable-but-accepting relay otherwise blocks on the default
            # socket timeout, which is None.
            self.timeout = timeout

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


def test_email_alert_picks_the_flavour_template_and_reports_the_run_status(
    postgres_engine, committed_pipeline, fake_smtp
):
    # E2-43. The alert is a pipeline-level completion alert now: it computes
    # the run's flavour from every active task's own status and picks the
    # matching template. Here one task FAILED, so the run is FAILED.
    work_id = insert_committed_task(postgres_engine, committed_pipeline, "flav_work")
    task_id = insert_committed_task(
        postgres_engine, committed_pipeline, "flav_alert", "EMAIL_ALERT"
    )
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {
            "EMAIL_TO": "ops@example.com",
            "EMAIL_SUBJECT": "generic subject",
            "EMAIL_SUBJECT_FAILED": "$$pipeline_code is $$status",
            "EMAIL_BODY_FAILED": "something broke",
        },
    )
    run_id = seed_active_run(postgres_engine, committed_pipeline)
    insert_committed_task_run(postgres_engine, work_id, run_id, "FAILED")

    outcome = run_task(postgres_engine, make_config(email=True), "TEST_CONCURRENT_PL", "flav_alert")

    assert outcome.status == "SUCCESS"
    sent = [e for e in _read_smtp_events(fake_smtp) if e["event"] == "sendmail"]
    assert len(sent) == 1
    # The FAILED-specific subject won over the generic one, and $$status
    # resolved to the computed flavour.
    assert "TEST_CONCURRENT_PL is FAILED" in sent[0]["message"]
    row = _task_run_row(postgres_engine, task_id)
    assert "RUN_STATUS = FAILED" in row.task_log


def test_email_alert_reports_completed_with_errors_when_a_task_was_skipped(
    postgres_engine, committed_pipeline, fake_smtp
):
    # The neutral middle flavour: nothing failed outright, but the run was not
    # clean — "if the pipeline is marked success with failure then a neutral
    # status like pipeline is COMPLETED with errors".
    ok_id = insert_committed_task(postgres_engine, committed_pipeline, "cwe_ok")
    skipped_id = insert_committed_task(postgres_engine, committed_pipeline, "cwe_skipped")
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "cwe_alert", "EMAIL_ALERT")
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {"EMAIL_TO": "ops@example.com", "EMAIL_SUBJECT": "$$status", "EMAIL_BODY": "b"},
    )
    run_id = seed_active_run(postgres_engine, committed_pipeline)
    insert_committed_task_run(postgres_engine, ok_id, run_id, "SUCCESS")
    insert_committed_task_run(postgres_engine, skipped_id, run_id, "SKIPPED")

    assert (
        run_task(postgres_engine, make_config(email=True), "TEST_CONCURRENT_PL", "cwe_alert").status
        == "SUCCESS"
    )

    sent = [e for e in _read_smtp_events(fake_smtp) if e["event"] == "sendmail"]
    assert "COMPLETED_WITH_ERRORS" in sent[0]["message"]


def test_email_alert_sends_nothing_when_the_flavour_is_not_in_email_on_status(
    postgres_engine, committed_pipeline, fake_smtp
):
    # E2-43. "if there are parameters saying which status to send, send only
    # on that condition." The task still records SUCCESS — it ran and
    # correctly decided not to act — which is also what keeps it out of the
    # unsettled set and so unable to re-create E2-01.
    work_id = insert_committed_task(postgres_engine, committed_pipeline, "onstat_work")
    task_id = insert_committed_task(
        postgres_engine, committed_pipeline, "onstat_alert", "EMAIL_ALERT"
    )
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {
            "EMAIL_TO": "ops@example.com",
            "EMAIL_SUBJECT": "s",
            "EMAIL_BODY": "b",
            "EMAIL_ON_STATUS": "FAILED",
        },
    )
    run_id = seed_active_run(postgres_engine, committed_pipeline)
    insert_committed_task_run(postgres_engine, work_id, run_id, "SUCCESS")

    outcome = run_task(
        postgres_engine, make_config(email=True), "TEST_CONCURRENT_PL", "onstat_alert"
    )

    assert outcome.status == "SUCCESS"
    assert [e for e in _read_smtp_events(fake_smtp) if e["event"] == "sendmail"] == []
    row = _task_run_row(postgres_engine, task_id)
    assert "EMAIL_SENT = false" in row.task_log
    assert "no email sent" in row.task_log


def test_email_alert_rejects_an_unknown_email_on_status_value(
    postgres_engine, committed_pipeline, fake_smtp
):
    task_id = insert_committed_task(
        postgres_engine, committed_pipeline, "badstat_alert", "EMAIL_ALERT"
    )
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {
            "EMAIL_TO": "ops@example.com",
            "EMAIL_SUBJECT": "s",
            "EMAIL_BODY": "b",
            "EMAIL_ON_STATUS": "PARTIAL",
        },
    )
    seed_active_run(postgres_engine, committed_pipeline)

    outcome = run_task(
        postgres_engine, make_config(email=True), "TEST_CONCURRENT_PL", "badstat_alert"
    )

    assert outcome.status == "FAILED"
    assert "unknown status" in outcome.message


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
    def _raise(host, port, timeout=None):
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
    (tmp_path / name).write_text(body, encoding="utf-8")


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
            # Scoped to this test's own two files by name, not LIKE '000%' —
            # sql/migrations/ now holds a real 0001_*.sql of its own (E2-41),
            # and SCHEMA_MIGRATIONS is shared across the whole test database.
            versions = (
                conn.execute(
                    text(
                        "SELECT VERSION FROM SCHEMA_MIGRATIONS "
                        "WHERE VERSION IN ('0001_create_table.sql', '0002_add_column.sql') "
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


def test_apply_pending_migrations_handles_a_colon_in_a_string_literal(
    postgres_engine, tmp_path, migrations_cleanup
):
    # E2-79. _split_statements is carefully quote-aware; text() then ran its
    # own, not quote-aware, :name bind-parameter scan over the same text, so a
    # migration containing an ordinary string literal with a colon in it --
    # seeding a CFG_TASK_PARAMETERS value, a COMMENT ON, a CHECK regex --
    # failed with a message about a bind parameter the author never wrote.
    # The three shipped migrations avoid it only by luck: SQLAlchemy's own
    # lookbehind protects a digit before a colon, so '12:30:00' is fine and
    # ':name' is not.
    _write_migration(
        tmp_path,
        "0001_colon_literal.sql",
        "CREATE TABLE migrate_colon_t (id int, note varchar); "
        "INSERT INTO migrate_colon_t (id, note) VALUES (1, 'see docs/#:ref for :name');",
    )
    migrations_cleanup.append("0001_colon_literal.sql")

    try:
        assert apply_pending_migrations(postgres_engine, tmp_path) == ["0001_colon_literal.sql"]
        with postgres_engine.connect() as conn:
            note = conn.execute(text("SELECT note FROM migrate_colon_t")).scalar_one()
        # The literal reached the database untouched.
        assert note == "see docs/#:ref for :name"
    finally:
        with postgres_engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS migrate_colon_t"))


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


def test_mark_packaged_migrations_applied_leaves_a_teams_own_migration_pending(
    postgres_engine, tmp_path, monkeypatch, migrations_cleanup
):
    # E2-83, and the part worth being careful about. schema.sql is the
    # authoritative definition of the *engine's* schema, so the engine's own
    # migrations are by definition already reflected in it -- but a team's
    # ./sql/migrations/ holds changes schema.sql knows nothing about.
    # Recording those unexecuted would silently skip a team's migration,
    # which is a worse bug than the one this fixes, so the marking is scoped
    # to the packaged directory rather than to whatever resolve_migrations_dir
    # happens to pick.
    team = tmp_path / "sql" / "migrations"
    team.mkdir(parents=True)
    # Deliberately *not* re-runnable — a plain CREATE TABLE, the common
    # spelling — so running it twice would fail outright.
    _write_migration(team, "0009_team_thing.sql", "CREATE TABLE team_thing (id int);")
    monkeypatch.chdir(tmp_path)
    migrations_cleanup.append("0009_team_thing.sql")

    try:
        recorded = mark_packaged_migrations_applied(postgres_engine)

        assert recorded == sorted(p.name for p in packaged_migrations_dir().glob("*.sql"))
        assert "0009_team_thing.sql" not in recorded
        # Still genuinely pending, so `migrate` runs it for real.
        assert apply_pending_migrations(postgres_engine, team) == ["0009_team_thing.sql"]
    finally:
        with postgres_engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS team_thing"))


def test_setup_brings_a_real_database_up_then_keeps_it_current(
    postgres_engine, tmp_path, monkeypatch
):
    # The dbt-route promise, end to end against real Postgres: run it once and
    # everything exists; run it again and it reports "already up to date"
    # rather than doing anything twice. Per explicit instruction: "one single
    # command with required files and it should itself up. so, everytime the
    # command is ran, it either set itself up, or updates the setup with
    # newest data."
    with postgres_engine.connect() as conn:
        url = conn.engine.url
    admin = create_engine(
        url.set(database="postgres").render_as_string(hide_password=False),
        isolation_level="AUTOCOMMIT",
    )
    db_name = "etl_craft_setup_test"
    try:
        with admin.connect() as conn:
            conn.execute(text(f"DROP DATABASE IF EXISTS {db_name}"))
            conn.execute(text(f"CREATE DATABASE {db_name}"))

        monkeypatch.chdir(tmp_path)
        (tmp_path / ".env").write_text(
            "ETL_CRAFT_MODE=local\n"
            "ETL_CRAFT_SOURCE_TYPE=file\n"
            "ETL_CRAFT_SOURCE_PATH=.env\n"
            "ETL_CRAFT_POSTGRES_PROFILE=dev\n"
            f"ETL_CRAFT_POSTGRES_JDBC_URL=jdbc:postgresql://{url.host}:{url.port}/{db_name}\n"
            f"ETL_CRAFT_POSTGRES_USER={url.username}\n"
            "ETL_CRAFT_POSTGRES_AUTH_MODE=password\n"
            f"ETL_CRAFT_POSTGRES_DEV_SECRET={url.password}\n"
        )

        first = run_setup(
            config_path=tmp_path / "craft-connector.yml",
            env_path=None,
            from_environment=False,
        )
        assert first.ok, first.problems
        assert "created" in first.config_action
        assert "schema created" in first.database_action
        # E2-83. Nothing was *executed*. schema.sql already contains
        # everything the packaged migrations add, and running all of them on
        # top of the schema it had just created worked only because all three
        # happen to be written re-runnably -- an invariant nothing enforces.
        # The first migration written as an ordinary ALTER TABLE ADD COLUMN
        # would have broken setup on every new environment, and the first one
        # carrying a data backfill would have applied it twice.
        assert first.applied_migrations == []

        second = run_setup(
            config_path=tmp_path / "craft-connector.yml",
            env_path=None,
            from_environment=False,
        )
        assert second.ok, second.problems
        assert "already current" in second.config_action
        assert second.database_action == "already up to date"

        target = create_engine(url.set(database=db_name).render_as_string(hide_password=False))
        try:
            with target.connect() as conn:
                # The tables the migrations add, not just schema.sql's own.
                assert (
                    conn.execute(
                        text(
                            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
                            "WHERE LOWER(TABLE_NAME) = 'aud_column_lineage'"
                        )
                    ).scalar_one()
                    == 1
                )
                # ...and they are recorded, so a later `migrate` does not
                # reach for them either.
                recorded = set(
                    conn.execute(text("SELECT VERSION FROM SCHEMA_MIGRATIONS")).scalars().all()
                )
            assert {p.name for p in packaged_migrations_dir().glob("*.sql")} <= recorded
        finally:
            target.dispose()
    finally:
        with admin.connect() as conn:
            conn.execute(text(f"DROP DATABASE IF EXISTS {db_name}"))
        admin.dispose()


def test_init_db_creates_the_schema_and_then_refuses(postgres_engine):
    # E2-13. There was previously no way to create the Engine DB from an
    # installed package at all: schema.sql is the single authoritative full
    # definition and it lived only in the git checkout. Run against a real,
    # genuinely empty throwaway database -- not the shared test one, which
    # already has the schema.
    with postgres_engine.connect() as conn:
        url = conn.engine.url
    # render_as_string(hide_password=False): str(url) masks the password.
    admin = create_engine(
        url.set(database="postgres").render_as_string(hide_password=False),
        isolation_level="AUTOCOMMIT",
    )
    db_name = "etl_craft_initdb_test"
    try:
        with admin.connect() as conn:
            conn.execute(text(f"DROP DATABASE IF EXISTS {db_name}"))
            conn.execute(text(f"CREATE DATABASE {db_name}"))
        target = create_engine(url.set(database=db_name).render_as_string(hide_password=False))
        try:
            count = init_db(target)
            assert count > 0
            with target.connect() as conn:
                assert conn.execute(text("SELECT COUNT(*) FROM CFG_PIPELINES")).scalar_one() == 0
                # The trigger functions survived statement splitting -- the
                # case a naive split(";") shreds (E2-05).
                assert (
                    conn.execute(
                        text(
                            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.ROUTINES "
                            "WHERE ROUTINE_NAME = 'trg_set_audit_columns'"
                        )
                    ).scalar_one()
                    == 1
                )

            # schema.sql is plain CREATE TABLE and deliberately not idempotent,
            # so a second run must refuse rather than fail half-applied.
            with pytest.raises(InitDbError, match="already has engine table"):
                init_db(target)
        finally:
            target.dispose()
    finally:
        with admin.connect() as conn:
            conn.execute(text(f"DROP DATABASE IF EXISTS {db_name}"))
        admin.dispose()


def test_doctor_reports_every_check_and_names_a_missing_secret(
    postgres_engine, monkeypatch, capsys
):
    # E2-16. `configure` writes a profile whose secret is looked up as
    # ETL_CRAFT_{SECTION}_{PROFILE}_SECRET and never mentions that name, so the
    # first sign of trouble was a later command failing. doctor names it.
    config = make_config()
    monkeypatch.delenv("ETL_CRAFT_POSTGRES_DEV_SECRET", raising=False)

    results = run_checks(config)

    by_name = {r.name: r for r in results}
    secret = by_name["Engine DB secret"]
    assert secret.ok is False
    assert "ETL_CRAFT_POSTGRES_DEV_SECRET" in secret.detail
    # Every check still reports -- one failure must not hide the rest.
    assert by_name["Execution mode"].ok is True
    assert "no [Warehouse] section" in by_name["Warehouse"].detail


def test_doctor_passes_against_a_real_engine_db(postgres_engine):
    results = run_checks(make_config())
    assert [r.name for r in results if not r.ok] == []
    assert any(r.name == "Engine DB connection" and r.ok for r in results)


def test_doctor_checks_a_configured_warehouse_and_email_relay(postgres_engine, monkeypatch, capsys):
    # The warehouse and email branches, which the no-section default skips.
    # Postgres stands in as the warehouse (as elsewhere in this suite), and the
    # relay is unreachable on purpose -- doctor must report it rather than
    # raise, and must still report every other check alongside it.
    monkeypatch.setenv("ETL_CRAFT_WAREHOUSE_DEV_SECRET", "etl_craft")
    config = make_config(warehouse=True, email=True)

    results = run_checks(config)
    by_name = {r.name: r for r in results}

    assert by_name["Warehouse secret"].ok is True
    assert by_name["Warehouse connection"].ok is True
    # make_config's email profile points at a port nothing is listening on.
    assert by_name["Email relay"].ok is False


def test_cli_doctor_reports_failures_with_exit_1(craft_connector_on_disk, monkeypatch, capsys):
    monkeypatch.delenv("ETL_CRAFT_POSTGRES_DEV_SECRET", raising=False)

    exit_code = cli_main(["doctor"])

    assert exit_code == 1
    out = capsys.readouterr()
    assert "ETL_CRAFT_POSTGRES_DEV_SECRET" in out.out
    assert "check(s) failed" in out.err


def test_cli_doctor_reports_a_missing_config_file_as_a_check(tmp_path, monkeypatch, capsys):
    # doctor runs before build_engine on purpose: its whole job is to diagnose
    # a configuration that does not work yet. The first version dispatched it
    # after the shared setup, so a missing secret exited 2 from build_engine
    # before doctor said a word.
    monkeypatch.delenv("ETL_CRAFT_CONFIG", raising=False)
    monkeypatch.chdir(tmp_path)

    exit_code = cli_main(["doctor"])

    assert exit_code == 1
    assert "[FAIL] Configuration:" in capsys.readouterr().err


def test_cli_doctor_passes_with_exit_0(craft_connector_on_disk, capsys):
    assert cli_main(["doctor"]) == 0
    assert "all checks passed" in capsys.readouterr().out


def test_cli_init_db_refuses_an_already_initialized_database(craft_connector_on_disk, capsys):
    # craft_connector_on_disk points at the shared test database, which already
    # has the schema -- exactly the case init-db must refuse rather than
    # half-apply, since schema.sql is deliberately not idempotent.
    exit_code = cli_main(["init-db"])

    assert exit_code == 1
    assert "already has engine table" in capsys.readouterr().err


def test_init_db_wraps_a_failure_with_the_file_it_was_applying(postgres_engine, monkeypatch):
    monkeypatch.setattr(
        "etl_craft.init_db.read_packaged_schema", lambda: "SELECT this_is_not_valid_sql("
    )
    with pytest.raises(InitDbError, match="failed applying schema.sql"):
        init_db(postgres_engine, force=True)


def test_consume_task_edges_records_the_run_the_gate_used_not_a_newer_one(
    postgres_engine, two_committed_pipelines
):
    # E2-12. consume_* used to re-evaluate satisfaction at finalize time and
    # record whatever qualified *then*. If the upstream completed a second
    # qualifying run while this one executed, the watermark jumped past it and
    # marked it consumed by a task that never read its data -- making the
    # tracker's own claim ("the run last consumed for each edge") false in
    # exactly the different-cadence case it exists for.
    downstream_id, upstream_id = two_committed_pipelines
    task_id = insert_committed_task(postgres_engine, downstream_id, "e212_target")
    upstream_task = insert_committed_task(postgres_engine, upstream_id, "e212_up")
    edge_id = insert_committed_cross_pipeline_task_dependency(
        postgres_engine, downstream_id, task_id, upstream_id, upstream_task, "SUCCESS"
    )

    first_run = insert_committed_pipeline_run(postgres_engine, upstream_id, "SUCCESS")
    first_task_run = insert_committed_task_run(postgres_engine, upstream_task, first_run, "SUCCESS")

    # The gate resolves against the run that exists now.
    check = check_task_cross_pipeline_dependencies(postgres_engine, task_id)
    assert check.satisfied_count == 1
    assert check.consumed == {edge_id: first_task_run}

    # While this task executes, the upstream completes another qualifying run.
    second_run = insert_committed_pipeline_run(postgres_engine, upstream_id, "SUCCESS")
    second_task_run = insert_committed_task_run(
        postgres_engine, upstream_task, second_run, "SUCCESS"
    )
    assert second_task_run > first_task_run

    consume_task_dependency_edges(postgres_engine, task_id, check.consumed)

    with postgres_engine.connect() as conn:
        recorded = conn.execute(
            text(
                "SELECT LAST_CONSUMED_TASK_RUN_ID FROM AUD_TASK_DEPENDENCY_TRACKER "
                "WHERE TASK_DEPENDENCY_ID = :edge_id"
            ),
            {"edge_id": edge_id},
        ).scalar_one()
    # The run the gate actually used, not the newer one nobody read.
    assert recorded == first_task_run


def test_validate_flags_source_sql_that_is_not_read_only(
    postgres_engine, committed_pipeline, craft_connector_on_disk, capsys
):
    # E2-07 at the level it is actually enforced. A data-modifying CTE is a
    # valid "SELECT" that writes, and nothing checked SOURCE_SQL at all.
    task_id = insert_committed_task(postgres_engine, committed_pipeline, "writer_task")
    insert_committed_task_parameters(
        postgres_engine,
        task_id,
        {
            "SOURCE_OBJECT": "public.src",
            "TARGET_OBJECT": "public.tgt",
            "SOURCE_SQL": "WITH x AS (DELETE FROM public.other RETURNING *) SELECT * FROM x",
        },
    )

    exit_code = cli_main(["validate"])

    assert exit_code == 1
    out = capsys.readouterr().out
    assert "[sql_read_only]" in out
    assert "writer_task" in out


def _documented_sql_task(engine, pipeline_id, task_code, source_sql, doc=None):
    task_id = insert_committed_task(engine, pipeline_id, task_code)
    params = {
        "SQL_ACTION": "CREATE_TABLE",
        "SOURCE_OBJECT": "raw.customers",
        "TARGET_OBJECT": f"public.{task_code}_out",
        "SOURCE_SQL": source_sql,
    }
    if doc is not None:
        params["DOCUMENTATION"] = doc
    insert_committed_task_parameters(engine, task_id, params)
    return task_id


def test_column_lineage_is_computed_then_served_from_the_cache(postgres_engine, committed_pipeline):
    # Per explicit instruction lineage is both computed and cached. The cache
    # key is a hash of everything the lineage was derived from, so an edit
    # invalidates it with nothing to remember -- the same reasoning as
    # HASH_KEY for SCD change detection.
    task_id = _documented_sql_task(
        postgres_engine,
        committed_pipeline,
        "cl_task",
        "SELECT c.id AS cust_id, UPPER(c.name) AS shouty FROM raw.customers c",
    )

    with postgres_engine.begin() as conn:
        first = lineage_for_tasks(conn)
    mine = [t for t in first if t.task_id == task_id]
    assert len(mine) == 1
    assert mine[0].cached is False
    by_column = {e.target_column: e for e in mine[0].edges}
    assert by_column["cust_id"].source_object == "raw.customers"
    assert "UPPER" in (by_column["shouty"].transformation or "")

    # Second read: same SQL, so the cached rows are used.
    with postgres_engine.begin() as conn:
        second = lineage_for_tasks(conn)
    assert [t for t in second if t.task_id == task_id][0].cached is True

    # Edit the SQL: the cache must miss, not serve a stale answer.
    with postgres_engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE = "
                "'SELECT c.email AS cust_email FROM raw.customers c' "
                "WHERE TASK_ID = :id AND PARAMETER_NAME = 'SOURCE_SQL'"
            ),
            {"id": task_id},
        )
    with postgres_engine.begin() as conn:
        third = lineage_for_tasks(conn)
    edited = [t for t in third if t.task_id == task_id][0]
    assert edited.cached is False
    assert [e.target_column for e in edited.edges] == ["cust_email"]


def test_column_lineage_cache_misses_when_only_the_target_object_changes(
    postgres_engine, committed_pipeline
):
    # E2-87. The key hashed SOURCE_SQL alone, with TARGET_OBJECT merely stored
    # on the cached row -- but lineage is a function of both, since the target
    # names the left-hand side of every edge. Renaming a task's TARGET_OBJECT
    # without touching its SOURCE_SQL is an ordinary thing to do, and
    # `lineage --column` then kept reporting the old target name indefinitely,
    # with nothing to invalidate it and no way to tell the answer was stale.
    task_id = _documented_sql_task(
        postgres_engine,
        committed_pipeline,
        "cl_rename",
        "SELECT c.id AS cust_id FROM raw.customers c",
    )
    with postgres_engine.begin() as conn:
        first = [t for t in lineage_for_tasks(conn) if t.task_id == task_id][0]
    assert first.cached is False
    assert first.edges[0].target_object == "public.cl_rename_out"

    with postgres_engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE = 'public.renamed_out' "
                "WHERE TASK_ID = :id AND PARAMETER_NAME = 'TARGET_OBJECT'"
            ),
            {"id": task_id},
        )

    with postgres_engine.begin() as conn:
        second = [t for t in lineage_for_tasks(conn) if t.task_id == task_id][0]

    assert second.cached is False
    assert second.edges[0].target_object == "public.renamed_out"


def test_generate_docs_writes_nothing_to_the_database(
    postgres_engine, committed_pipeline, tmp_path
):
    # E2-56 regression. generate-docs called a write path through a connection
    # the CLI never commits, so the lineage AND documentation-version writes
    # were both silently discarded -- and against a read-only replica or role,
    # a perfectly reasonable place to point a docs build, it would have failed
    # outright. Reproduced by running it exactly as the CLI does.
    _documented_sql_task(
        postgres_engine,
        committed_pipeline,
        "ro_task",
        "SELECT c.id AS cust_id FROM raw.customers c",
        doc="Read-only probe.",
    )
    with postgres_engine.begin() as conn:
        conn.execute(text("DELETE FROM AUD_COLUMN_LINEAGE"))
        conn.execute(text("DELETE FROM AUD_TASK_DOCUMENTATION"))

    with postgres_engine.connect() as conn:  # exactly what the CLI does
        generate_docs(conn, tmp_path)

    with postgres_engine.connect() as conn:
        lineage_rows = conn.execute(text("SELECT COUNT(*) FROM AUD_COLUMN_LINEAGE")).scalar_one()
        doc_rows = conn.execute(text("SELECT COUNT(*) FROM AUD_TASK_DOCUMENTATION")).scalar_one()
    assert (lineage_rows, doc_rows) == (0, 0)
    # And it still renders the documentation and lineage it computed.
    page = (tmp_path / "TEST_CONCURRENT_PL.html").read_text()
    assert "Read-only probe." in page
    assert "raw.customers.id" in page


def test_cli_lineage_column_reports_producers_and_consumers(
    postgres_engine, committed_pipeline, craft_connector_on_disk, capsys
):
    _documented_sql_task(
        postgres_engine,
        committed_pipeline,
        "cl_writer",
        "SELECT c.id AS cust_id FROM raw.customers c",
    )

    assert cli_main(["lineage", "--column", "public.cl_writer_out.cust_id"]) == 0

    out = capsys.readouterr().out
    assert "is produced by" in out
    assert "raw.customers.id" in out


def test_cli_lineage_column_rejects_a_reference_that_is_not_a_column(
    craft_connector_on_disk, capsys
):
    assert cli_main(["lineage", "--column", "nodots"]) == 2
    assert "schema.table.column" in capsys.readouterr().err


def test_documentation_versions_bump_only_when_the_text_changes(
    postgres_engine, committed_pipeline
):
    # A hand-set version drifts out of sync the moment someone edits one and
    # not the other, which is why the version is derived from the text.
    task_id = _documented_sql_task(
        postgres_engine,
        committed_pipeline,
        "doc_task",
        "SELECT 1 AS x",
        doc="Builds the customer dimension.",
    )

    with postgres_engine.begin() as conn:
        assert refresh_task_documentation(conn, task_id, "Builds the customer dimension.") == 1
        # Same text again -- not a new version.
        assert refresh_task_documentation(conn, task_id, "Builds the customer dimension.") == 1
        # Re-indented only -- still not a new version.
        assert refresh_task_documentation(conn, task_id, "  Builds the customer dimension. ") == 1
        # Genuinely different.
        assert refresh_task_documentation(conn, task_id, "Builds it, and dedupes.") == 2

    with postgres_engine.connect() as conn:
        history = fetch_history(conn, task_id)
    assert [v for v, _, _ in history] == [2, 1]


def test_cli_docs_version_refreshes_and_shows_history(
    postgres_engine, committed_pipeline, craft_connector_on_disk, capsys
):
    _documented_sql_task(
        postgres_engine,
        committed_pipeline,
        "dv_task",
        "SELECT 1 AS x",
        doc="First description.",
    )

    assert cli_main(["docs-version"]) == 0
    out = capsys.readouterr().out
    assert "dv_task\tv1\tupdated" in out

    # Idempotent: nothing changed, so nothing bumps.
    assert cli_main(["docs-version"]) == 0
    assert "unchanged" in capsys.readouterr().out

    assert (
        cli_main(
            ["docs-version", "--pipeline_code", "TEST_CONCURRENT_PL", "--task_code", "dv_task"]
        )
        == 0
    )
    assert "First description." in capsys.readouterr().out


def test_unknown_pipeline_code_suggests_a_near_miss(postgres_engine, committed_pipeline):
    # The fuzzy half that applies to the CLI. difflib, not a dependency: three
    # candidates for an error message is a different problem from ranking
    # hundreds of entries as someone types.
    with postgres_engine.connect() as conn, pytest.raises(CfgError) as excinfo:
        resolve_pipeline_id(conn, "TEST_CONCURRENT_P")
    assert "did you mean" in str(excinfo.value)
    assert "TEST_CONCURRENT_PL" in str(excinfo.value)


def test_generated_docs_include_documentation_and_column_lineage(
    postgres_engine, committed_pipeline, tmp_path
):
    _documented_sql_task(
        postgres_engine,
        committed_pipeline,
        "docs_task",
        "SELECT c.id AS cust_id FROM raw.customers c",
        doc="Loads customers from the raw layer.",
    )

    with postgres_engine.begin() as conn:
        generate_docs(conn, tmp_path)

    page = (tmp_path / "TEST_CONCURRENT_PL.html").read_text()
    assert "Loads customers from the raw layer." in page
    # [DEVIATION, E2-56] generate-docs is read-only now, so it shows the
    # current DOCUMENTATION text with "unversioned" until `docs-version` has
    # recorded one. Showing the recorded *text* instead would make an edit
    # invisible until someone remembered to run that command.
    assert "unversioned" in page
    assert "Column lineage" in page
    assert "raw.customers.id" in page
    # Fuse is vendored into the output, not loaded from a CDN -- this site
    # gets published to networks with no outbound access.
    assert (tmp_path / "fuse.min.js").is_file()
    assert "Fuse.js" in (tmp_path / "fuse.min.js").read_text()
    assert "new Fuse(" in (tmp_path / "search.js").read_text()
    # Once a version has been recorded, the badge shows it.
    with postgres_engine.begin() as conn:
        refresh_all(conn)
    with postgres_engine.connect() as conn:
        generate_docs(conn, tmp_path)
    assert "docs v1" in (tmp_path / "TEST_CONCURRENT_PL.html").read_text()

    index_entry = json.loads((tmp_path / "search-index.json").read_text())
    documented = [e for e in index_entry if e.get("task_code") == "docs_task"]
    assert documented and "Loads customers" in documented[0]["documentation"]


def test_cli_migrate_reports_a_missing_migrations_directory(
    craft_connector_on_disk, tmp_path, capsys
):
    # E2-05's headline failure: the old package-relative default resolved to a
    # path that does not exist once installed, and Path.glob on a missing
    # directory yields nothing without error -- so migrate printed "already up
    # to date" and silently skipped a team's migrations.
    missing = tmp_path / "nope"

    exit_code = cli_main(["migrate", "--migrations-dir", str(missing)])

    assert exit_code == 1
    err = capsys.readouterr().err
    assert "does not exist" in err
    assert "up to date" not in err


def test_migrate_creates_schema_migrations_when_the_table_is_absent(
    postgres_engine, tmp_path, migrations_cleanup
):
    # The other half of E2-05: SELECT VERSION FROM SCHEMA_MIGRATIONS raised a
    # raw ProgrammingError from outside the try against any database predating
    # that table, and nothing could bootstrap it.
    with postgres_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS SCHEMA_MIGRATIONS"))
    (tmp_path / "0001_bootstrap_probe.sql").write_text("SELECT 1")
    migrations_cleanup.append("0001_bootstrap_probe.sql")

    applied = apply_pending_migrations(postgres_engine, tmp_path)

    # With no ledger, no package migration has a recorded checksum or source.
    # The runner therefore applies its ENGINE stream before the explicitly
    # supplied PROJECT file. The packaged migrations are written to tolerate a
    # schema that already has their changes, which is the realistic recovery
    # path after an operator has deleted only the bookkeeping table.
    expected = [
        *(path.name for path in sorted(packaged_migrations_dir().glob("*.sql"))),
        "0001_bootstrap_probe.sql",
    ]
    assert applied == expected


def test_cli_migrate_applies_the_real_migrations_directory_and_is_idempotent(
    craft_connector_on_disk, postgres_engine, capsys
):
    # Exercises the genuine default directory (sql/migrations/), not a
    # test-scoped tmp_path like every other migrate test here. Until E2-41
    # this directory was empty and the assertion was simply "up to date" —
    # now it holds 0001_add_run_condition.sql, so this is the only test that
    # proves the runner actually applies a real, shipped migration file
    # against real Postgres.
    #
    # Run twice on purpose: sql/migrations/README.md requires every migration
    # to be safe against a database already carrying it, and the second call
    # must report up to date off SCHEMA_MIGRATIONS rather than re-running.
    first = cli_main(["migrate"])
    assert first == 0

    second = cli_main(["migrate"])
    assert second == 0
    assert "up to date" in capsys.readouterr().out

    # The columns the migration exists to add are really there.
    with postgres_engine.connect() as conn:
        columns = {
            row[0].lower()
            for row in conn.execute(
                text(
                    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                    "WHERE TABLE_NAME = 'cfg_tasks'"
                )
            )
        }
    assert {"run_condition", "run_condition_count"} <= columns


def test_cli_migrate_reports_applied_files(craft_connector_on_disk, monkeypatch, capsys):
    # Real success-with-results and failure paths both mock
    # apply_pending_migrations directly (same spirit as the runner.dispatch
    # monkeypatch elsewhere) — the function itself is already proven for
    # real against Postgres above; this just proves the CLI wires its
    # result/exception into the right message and exit code.
    monkeypatch.setattr(
        "etl_craft.cli.apply_pending_migrations",
        lambda engine, migrations_dir=None: ["0001_x.sql", "0002_y.sql"],
    )

    exit_code = cli_main(["migrate"])

    assert exit_code == 0
    out = capsys.readouterr().out
    assert "applied 0001_x.sql" in out
    assert "applied 0002_y.sql" in out


def test_cli_migrate_reports_error_on_failed_migration(
    craft_connector_on_disk, monkeypatch, capsys
):
    def _raise(engine, migrations_dir=None):
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
