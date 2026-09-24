"""The SQLite Engine DB — the default since 2026-09-24.

These need no Docker and never skip: the Engine DB is a file under tmp_path.
The whole integration suite additionally runs against a SQLite Engine DB with
ETL_CRAFT_TEST_ENGINE=sqlite (`make test-sqlite-engine`); this file is what
keeps the default path covered in the plain `pytest` run too.
"""

from __future__ import annotations

import re
import sqlite3
import threading
import time
from pathlib import Path

import pytest
import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

from etl_craft.cli import main
from etl_craft.config import ConfigError, load_config
from etl_craft.configure import configure_from_env
from etl_craft.db import ConnectionError_, build_engine, resolve_sqlite_path
from etl_craft.doctor import run_checks
from etl_craft.locks import LockTimeout, engine_lock
from etl_craft.migrate import split_sqlite_statements
from etl_craft.orchestrator import run_pipeline
from etl_craft.runlog import find_or_create_active_run
from etl_craft.setup_command import run_setup

ENGINE_ENV_VARS = (
    "ENGINE_JDBC_URL",
    "ENGINE_USER",
    "ENGINE_AUTH_MODE",
    "ETL_CRAFT_MODE",
    "ETL_CRAFT_SOURCE_TYPE",
    "ETL_CRAFT_ENGINE_PROFILE",
    "WAREHOUSE_JDBC_URL",
)


@pytest.fixture
def clean_env(monkeypatch, tmp_path):
    """An environment with none of setup's inputs set, and cwd in tmp_path."""
    for name in ENGINE_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)
    return tmp_path


@pytest.fixture
def sqlite_deployment(clean_env):
    """A deployment built by `setup` with nothing configured: the SQLite default."""
    config_path = clean_env / "craft-connector.yml"
    report = run_setup(config_path=config_path, env_path=None, from_environment=False)
    assert report.ok, report.problems
    config = load_config(config_path)
    engine = build_engine(config)
    yield config, engine
    engine.dispose()


def _pipeline(engine, code: str = "PL") -> int:
    with engine.begin() as conn:
        return conn.execute(
            text(
                "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
                "VALUES (:code, 'Pipeline', 'FULL') RETURNING PIPELINE_ID"
            ),
            {"code": code},
        ).scalar_one()


def _task(engine, pipeline_id: int, code: str, handler: str, params: dict[str, str]) -> int:
    with engine.begin() as conn:
        task_id = conn.execute(
            text(
                "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
                "VALUES (:code, 'ETL', :pid, :handler) RETURNING TASK_ID"
            ),
            {"code": code, "pid": pipeline_id, "handler": handler},
        ).scalar_one()
        for name, value in params.items():
            conn.execute(
                text(
                    "INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE) "
                    "VALUES (:task_id, :name, :value)"
                ),
                {"task_id": task_id, "name": name, "value": value},
            )
    return task_id


# --- setup and config -------------------------------------------------------


def test_setup_with_nothing_configured_creates_a_working_sqlite_deployment(sqlite_deployment):
    config, engine = sqlite_deployment
    raw = yaml.safe_load(Path(config.config_path).read_text(encoding="utf-8"))
    assert raw["Engine"] == {"Profile": "dev", "Jdbc_url": "jdbc:sqlite:etl-craft-engine.db"}
    assert raw["Orchestration"]["Mode"] == "local"
    # Resolved next to the config, not the cwd.
    assert Path(engine.url.database) == config.config_path.parent / "etl-craft-engine.db"
    with engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM CFG_PIPELINES")).scalar_one() == 0
    checks = {check.name: check for check in run_checks(config)}
    assert all(check.ok for check in checks.values()), checks
    assert "Engine DB secret" not in checks
    assert "use PostgreSQL for production" in checks["Engine DB kind"].detail


def test_setup_is_idempotent_on_sqlite(sqlite_deployment):
    config, _ = sqlite_deployment
    report = run_setup(config_path=config.config_path, env_path=None, from_environment=False)
    assert report.ok, report.problems
    assert report.database_action == "already up to date"
    assert report.config_action.endswith("already current")


def test_setup_never_swaps_an_existing_postgres_engine_for_the_sqlite_default(clean_env):
    config_path = clean_env / "craft-connector.yml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "Orchestration": {"Mode": "local"},
                "Secrets": {"Source_type": "environment"},
                "Engine": {"Profile": "dev", "Variables": {"jdbc_url": "ENGINE_JDBC_URL"}},
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match="ENGINE_JDBC_URL is required"):
        configure_from_env(None, config_path)


def test_setup_accepts_an_explicit_sqlite_url_and_rejects_credentials_for_it(
    clean_env, monkeypatch
):
    config_path = clean_env / "craft-connector.yml"
    monkeypatch.setenv("ENGINE_JDBC_URL", "jdbc:sqlite:state/engine.db")
    configure_from_env(None, config_path)
    assert load_config(config_path).postgres.active.jdbc_url == "jdbc:sqlite:state/engine.db"

    monkeypatch.setenv("ENGINE_AUTH_MODE", "password")
    with pytest.raises(ConfigError, match="must be 'none'"):
        configure_from_env(None, config_path)


@pytest.mark.parametrize(
    ("engine_block", "message"),
    [
        (
            {"Profile": "dev", "Jdbc_url": "jdbc:postgresql://db/etl"},
            "may only name a SQLite Engine DB",
        ),
    ],
)
def test_a_literal_engine_url_is_only_accepted_for_sqlite(tmp_path, engine_block, message):
    path = tmp_path / "craft-connector.yml"
    path.write_text(
        yaml.safe_dump(
            {
                "Orchestration": {"Mode": "local"},
                "Secrets": {"Source_type": "environment"},
                "Engine": engine_block,
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match=message):
        load_config(path)


@pytest.mark.parametrize(
    ("jdbc_url", "auth_mode", "message"),
    [
        ("jdbc:sqlite:engine.db", "password", "auth_mode must be 'none'"),
        ("jdbc:postgresql://db/etl", "none", "only valid for a SQLite Engine DB"),
    ],
)
def test_engine_url_and_auth_mode_must_agree_about_sqlite(tmp_path, jdbc_url, auth_mode, message):
    path = tmp_path / "craft-connector.yml"
    path.write_text(
        yaml.safe_dump(
            {
                "Execution": {"Mode": "local"},
                "Source": {"Type": "environment"},
                "Postgres": {
                    "Active_profile": "dev",
                    "Profiles": {
                        "dev": {"jdbc_url": jdbc_url, "user": "u", "auth_mode": auth_mode}
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match=message):
        load_config(path)


def test_an_in_memory_sqlite_engine_db_is_refused():
    with pytest.raises(ConnectionError_, match="in-memory"):
        resolve_sqlite_path("jdbc:sqlite::memory:")


# --- schema -----------------------------------------------------------------


def test_sqlite_schema_keeps_postgres_guarantees(sqlite_deployment):
    _, engine = sqlite_deployment
    pipeline_id = _pipeline(engine)
    first = _task(engine, pipeline_id, "first", "SQL", {})
    second = _task(engine, pipeline_id, "second", "SQL", {})

    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO CFG_TASK_DEPENDENCY (PIPELINE_ID, TASK_ID, DEPENDS_ON_TASK_ID, "
                "DEPENDENCY_TYPE) VALUES (:p, :t, :d, 'SUCCESS')"
            ),
            {"p": pipeline_id, "t": second, "d": first},
        )
        # trg_default_depends_on_pipeline's SQLite counterpart.
        assert (
            conn.execute(
                text("SELECT DEPENDS_ON_PIPELINE_ID FROM CFG_TASK_DEPENDENCY")
            ).scalar_one()
            == pipeline_id
        )

    rejected = [
        # Self-dependency, caught once the trigger fills DEPENDS_ON_PIPELINE_ID in.
        "INSERT INTO CFG_TASK_DEPENDENCY (PIPELINE_ID, TASK_ID, DEPENDS_ON_TASK_ID, "
        f"DEPENDENCY_TYPE) VALUES ({pipeline_id}, {first}, {first}, 'SUCCESS')",
        # Foreign keys are on.
        "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
        "VALUES ('orphan', 'ETL', 999999, 'SQL')",
        # CHECK constraints, including the IS DISTINCT FROM translation.
        "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER, RUN_CONDITION) "
        f"VALUES ('n', 'ETL', {pipeline_id}, 'SQL', 'N')",
        # A second active pipeline with the same code.
        "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
        "VALUES ('PL', 'dup', 'FULL')",
    ]
    for statement in rejected:
        with pytest.raises(IntegrityError), engine.begin() as conn:
            conn.execute(text(statement))

    # The partial unique index that makes run-id minting race-safe.
    with engine.begin() as conn:
        run_id = find_or_create_active_run(conn, pipeline_id)
    with engine.begin() as conn:
        assert find_or_create_active_run(conn, pipeline_id) == run_id
    with pytest.raises(IntegrityError), engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS) VALUES (:p, 'IN-PROGRESS')"
            ),
            {"p": pipeline_id},
        )


def test_sqlite_audit_columns_are_stamped_and_creation_is_immutable(sqlite_deployment):
    _, engine = sqlite_deployment
    pipeline_id = _pipeline(engine)
    with engine.connect() as conn:
        before = conn.execute(
            text("SELECT CREATED_BY, CREATE_DATE, UPDATED_DATE FROM CFG_PIPELINES")
        ).one()
    assert before.CREATED_BY == "etl-craft"
    assert before.CREATE_DATE.tzinfo is not None
    time.sleep(0.01)
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE CFG_PIPELINES SET CREATED_BY = 'someone', DESCRIPTION = 'x' "
                "WHERE PIPELINE_ID = :p"
            ),
            {"p": pipeline_id},
        )
    with engine.connect() as conn:
        after = conn.execute(
            text("SELECT CREATED_BY, CREATE_DATE, UPDATED_DATE FROM CFG_PIPELINES")
        ).one()
    assert after.CREATED_BY == "etl-craft"
    assert after.CREATE_DATE == before.CREATE_DATE
    assert after.UPDATED_DATE > before.UPDATED_DATE


def test_sqlite_statement_splitter_keeps_trigger_bodies_whole():
    statements = split_sqlite_statements(
        "CREATE TABLE t (a INT); -- a; comment\n"
        "CREATE TRIGGER tr AFTER INSERT ON t BEGIN UPDATE t SET a = 1; DELETE FROM t; END;\n"
        "INSERT INTO t VALUES (';');"
    )
    assert len(statements) == 3
    assert statements[1].endswith("END")


# --- locks ------------------------------------------------------------------


def test_file_lock_serializes_and_times_out(sqlite_deployment):
    _, engine = sqlite_deployment
    holding = threading.Event()
    release = threading.Event()

    def hold() -> None:
        with engine_lock(engine, 1, "unit"):
            holding.set()
            release.wait(5)

    holder = threading.Thread(target=hold)
    holder.start()
    assert holding.wait(5)
    try:
        with pytest.raises(LockTimeout), engine_lock(engine, 1, "unit", wait_seconds=1):
            pass
    finally:
        release.set()
        holder.join()
    with engine_lock(engine, 1, "unit", wait_seconds=1):
        pass


# --- execution --------------------------------------------------------------


def test_a_parallel_pipeline_runs_end_to_end_on_a_sqlite_engine_db(sqlite_deployment, monkeypatch):
    # Real task subprocesses, all writing AUD_ rows to one SQLite file at once,
    # plus a DuckDB warehouse whose single-writer queue now takes a file lock
    # beside the SQLite Engine DB instead of a Postgres advisory lock.
    config, engine = sqlite_deployment
    warehouse = config.config_path.parent / "warehouse.duckdb"
    raw = yaml.safe_load(config.config_path.read_text(encoding="utf-8"))
    raw["Warehouse"] = {
        "Profile": "dev",
        "Table_format": "native",
        "Variables": {"jdbc_url": "WAREHOUSE_JDBC_URL", "auth_mode": "WAREHOUSE_AUTH_MODE"},
    }
    # Cloning reads every engine table out of SQLite at finalize time -- and
    # its same-database guard used to crash on a jdbc:sqlite: URL.
    raw["Cloning"] = {"Enabled": True, "Scope": "all"}
    config.config_path.write_text(yaml.safe_dump(raw), encoding="utf-8")
    monkeypatch.setenv("WAREHOUSE_JDBC_URL", f"jdbc:duckdb:{warehouse}")
    monkeypatch.setenv("WAREHOUSE_AUTH_MODE", "none")

    seed = create_engine(f"duckdb:///{warehouse}")
    with seed.begin() as conn:
        conn.execute(text("CREATE SCHEMA staging"))
        conn.execute(text("CREATE TABLE staging.src AS SELECT 1 AS id UNION ALL SELECT 2"))
    seed.dispose()

    pipeline_id = _pipeline(engine, "SQLITE_E2E")
    for code in ("load_a", "load_b", "load_c"):
        _task(
            engine,
            pipeline_id,
            code,
            "SQL",
            {
                "SQL_ACTION": "CREATE_TABLE",
                "TARGET_OBJECT": f"staging.{code}",
                "SOURCE_SQL": "SELECT id FROM staging.src WHERE 1=1",
                "SOURCE_OBJECT": "staging.src",
            },
        )

    outcome = run_pipeline(engine, load_config(config.config_path), "SQLITE_E2E")
    assert outcome.status == "SUCCESS", outcome.message

    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT STATUS, TARGET_COUNT, START_DATE, END_DATE FROM AUD_TASK_RUN_LOG")
        ).all()
        run_status = conn.execute(text("SELECT STATUS FROM AUD_PIPELINES_RUN_LOG")).scalar_one()
    assert run_status == "SUCCESS"
    assert [(r.STATUS, r.TARGET_COUNT) for r in rows] == [("SUCCESS", 2)] * 3
    assert all(r.END_DATE >= r.START_DATE for r in rows)

    mirror = create_engine(f"duckdb:///{warehouse}")
    try:
        with mirror.connect() as conn:
            assert conn.execute(text("SELECT PIPELINE_CODE FROM CFG_PIPELINES")).scalar_one() == (
                "SQLITE_E2E"
            )
            cloned = conn.execute(text("SELECT STATUS, START_DATE FROM AUD_TASK_RUN_LOG")).all()
    finally:
        mirror.dispose()
    assert {row.STATUS for row in cloned} == {"SUCCESS"}
    assert all(row.START_DATE is not None for row in cloned)

    # The read-only verbs over the same file.
    assert (
        main(["--config", str(config.config_path), "history", "--pipeline_code", "SQLITE_E2E"]) == 0
    )
    assert (
        main(["--config", str(config.config_path), "steps", "--pipeline_code", "SQLITE_E2E"]) == 0
    )
    assert (
        main(["--config", str(config.config_path), "generate-yml", "--pipeline_code", "SQLITE_E2E"])
        == 0
    )
    assert main(["--config", str(config.config_path), "validate"]) == 0
    docs = config.config_path.parent / "docs"
    assert main(["--config", str(config.config_path), "docs-version"]) == 0
    assert main(["--config", str(config.config_path), "generate-docs", "--output", str(docs)]) == 0
    assert (docs / "SQLITE_E2E.html").is_file()


def test_first_pipeline_walkthrough_runs_twice_on_a_sqlite_engine_db(
    sqlite_deployment, monkeypatch
):
    # Executes docs/first-pipeline.md's own SQL blocks, so the walkthrough
    # cannot drift from what works. It had drifted: it paired CREATE_TABLE with
    # SCD1_MERGE, which the merge refuses, and used Postgres-only syntax. It
    # also covers the engine-side lookups a merge makes against SQLite.
    config, engine = sqlite_deployment
    warehouse = config.config_path.parent / "warehouse.duckdb"
    raw = yaml.safe_load(config.config_path.read_text(encoding="utf-8"))
    raw["Warehouse"] = {
        "Profile": "dev",
        "Table_format": "native",
        "Variables": {"jdbc_url": "WAREHOUSE_JDBC_URL", "auth_mode": "WAREHOUSE_AUTH_MODE"},
    }
    config.config_path.write_text(yaml.safe_dump(raw), encoding="utf-8")
    monkeypatch.setenv("WAREHOUSE_JDBC_URL", f"jdbc:duckdb:{warehouse}")
    monkeypatch.setenv("WAREHOUSE_AUTH_MODE", "none")

    doc = (Path(__file__).parents[1] / "docs" / "first-pipeline.md").read_text(encoding="utf-8")
    blocks = re.findall(r"```sql\n(.*?)```", doc, re.S)

    def warehouse_sql(statement: str) -> list:
        duck = create_engine(f"duckdb:///{warehouse}")
        try:
            with duck.begin() as conn:
                result = conn.exec_driver_sql(statement)
                return result.all() if result.returns_rows else []
        finally:
            duck.dispose()

    warehouse_sql(blocks[0])
    # Steps 2-4, run the way the walkthrough says to: a plain sqlite3 script.
    raw_conn = sqlite3.connect(engine.url.database)
    try:
        raw_conn.executescript("\n".join(blocks[1:5]))
    finally:
        raw_conn.close()

    loaded = load_config(config.config_path)
    assert main(["--config", str(config.config_path), "validate"]) == 0
    assert run_pipeline(engine, loaded, "CUSTOMERS").status == "SUCCESS"
    first = dict(warehouse_sql("SELECT id, ROW_ID FROM marts.customers"))

    warehouse_sql("UPDATE staging.customers_raw SET name = 'Ada L' WHERE id = 1")
    outcome = run_pipeline(engine, loaded, "CUSTOMERS")
    assert outcome.status == "SUCCESS", outcome.message

    rows = warehouse_sql("SELECT id, name, ROW_ID FROM marts.customers ORDER BY id")
    assert [(r.id, r.name) for r in rows] == [(1, "Ada L"), (2, "Grace")]
    assert {r.id: r.ROW_ID for r in rows} == first  # updated in place, identity kept
    with engine.connect() as conn:
        counts = conn.execute(
            text(
                "SELECT l.INSERT_COUNT, l.UPDATE_COUNT FROM AUD_TASK_RUN_LOG l "
                "JOIN CFG_TASKS t ON t.TASK_ID = l.TASK_ID "
                "WHERE t.TASK_CODE = 'merge_customers' ORDER BY l.PIPELINE_RUN_ID DESC LIMIT 1"
            )
        ).one()
    assert tuple(counts) == (0, 1)
