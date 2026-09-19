"""Shared fixtures for the optional real-Postgres integration suite.

The default `pytest -q` run needs nothing but the in-memory SQLite
stand-in in test_unit.py — CI runs that on every push with no setup. The
fixtures here back the separate, opt-in test_integration.py suite that
runs against a real Postgres, which is the only way to actually exercise
the partial unique index's concurrency guarantee (no amount of
single-connection mocking can prove that) or a real subprocess-spawning
orchestration run. See docker-compose.yml / Makefile (`make test`) to
bring one up.
"""

import os

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from etl_craft.runlog import find_or_create_active_run

TEST_DATABASE_URL_VAR = "ETL_CRAFT_TEST_DATABASE_URL"
DEFAULT_TEST_DATABASE_URL = "postgresql+psycopg://etl_craft:etl_craft@localhost:55432/etl_craft"


def _reachable(url: str) -> bool:
    try:
        probe = create_engine(url)
        with probe.connect() as conn:
            conn.execute(text("SELECT 1"))
        probe.dispose()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def postgres_engine() -> Engine:
    """Build a real Postgres engine with sql/schema.sql applied — skips if unreachable."""
    url = os.environ.get(TEST_DATABASE_URL_VAR, DEFAULT_TEST_DATABASE_URL)
    if not _reachable(url):
        pytest.skip(
            f"no reachable Postgres at {url!r} — run `make db-up` "
            f"(see docker-compose.yml) or set {TEST_DATABASE_URL_VAR}"
        )
    engine = create_engine(url)
    yield engine
    engine.dispose()


@pytest.fixture
def pg_conn(postgres_engine: Engine):
    """Yield a connection wrapped in a transaction that's always rolled back after the test."""
    conn = postgres_engine.connect()
    trans = conn.begin()
    try:
        yield conn
    finally:
        trans.rollback()
        conn.close()


@pytest.fixture
def cfg_pipeline(pg_conn) -> int:
    """Insert one CFG_PIPELINES row (rolled back with pg_conn) and return its id."""
    return pg_conn.execute(
        text(
            "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
            "VALUES ('TEST_PL', 'Test Pipeline', 'INCREMENTAL') RETURNING PIPELINE_ID"
        )
    ).scalar_one()


@pytest.fixture
def cfg_task(pg_conn, cfg_pipeline: int) -> int:
    """Insert one CFG_TASKS row under `cfg_pipeline` and return its id."""
    return pg_conn.execute(
        text(
            "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
            "VALUES ('test_task', 'ETL', :pipeline_id, 'SQL') RETURNING TASK_ID"
        ),
        {"pipeline_id": cfg_pipeline},
    ).scalar_one()


@pytest.fixture
def committed_pipeline(postgres_engine: Engine):
    """Yield a genuinely committed CFG_PIPELINES row's id."""
    # Needed whenever code-under-test opens its own connections — cross-
    # connection concurrency, or runner.run_task — which never see pg_conn's
    # uncommitted work. Teardown cascades through everything a test might
    # have hung off this pipeline_id, in FK order.
    with postgres_engine.begin() as conn:
        pipeline_id = conn.execute(
            text(
                "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
                "VALUES ('TEST_CONCURRENT_PL', 'Concurrent Test Pipeline', 'INCREMENTAL') "
                "RETURNING PIPELINE_ID"
            )
        ).scalar_one()
    yield pipeline_id
    with postgres_engine.begin() as conn:
        conn.execute(
            text(
                "DELETE FROM AUD_TASK_RUN_LOG WHERE TASK_ID IN "
                "(SELECT TASK_ID FROM CFG_TASKS WHERE PIPELINE_ID = :id)"
            ),
            {"id": pipeline_id},
        )
        conn.execute(
            text("DELETE FROM CFG_TASK_DEPENDENCY WHERE PIPELINE_ID = :id"), {"id": pipeline_id}
        )
        conn.execute(text("DELETE FROM CFG_TASKS WHERE PIPELINE_ID = :id"), {"id": pipeline_id})
        conn.execute(
            text("DELETE FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_ID = :id"), {"id": pipeline_id}
        )
        conn.execute(text("DELETE FROM CFG_PIPELINES WHERE PIPELINE_ID = :id"), {"id": pipeline_id})


def insert_committed_task(
    engine: Engine, pipeline_id: int, task_code: str, handler: str = "SQL"
) -> int:
    """Insert and commit one CFG_TASKS row — for code-under-test that opens its own connections."""
    with engine.begin() as conn:
        return conn.execute(
            text(
                "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
                "VALUES (:task_code, 'ETL', :pipeline_id, :handler) RETURNING TASK_ID"
            ),
            {"task_code": task_code, "pipeline_id": pipeline_id, "handler": handler},
        ).scalar_one()


def seed_active_run(engine: Engine, pipeline_id: int) -> int:
    """Mint an IN-PROGRESS run for `pipeline_id`, as a real orchestrator would before spawning."""
    # runner.run_task's resolve_run_for_task deliberately never mints a
    # fresh run itself (see runlog.py) — only find_or_create_active_run
    # does, played by the local orchestrator or Airflow's synthetic first
    # step. Tests exercising run_task on a freshly-created pipeline need to
    # seed that first, or they're testing an invocation path ("run a single
    # task against a pipeline that's never run at all") nothing produces.
    with engine.begin() as conn:
        return find_or_create_active_run(conn, pipeline_id)


def insert_committed_dependency(
    engine: Engine,
    pipeline_id: int,
    task_id: int,
    depends_on_task_id: int,
    dependency_type: str = "SUCCESS",
) -> None:
    """Insert and commit one same-pipeline CFG_TASK_DEPENDENCY row."""
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO CFG_TASK_DEPENDENCY (PIPELINE_ID, TASK_ID, DEPENDS_ON_PIPELINE_ID, "
                "DEPENDS_ON_TASK_ID, DEPENDENCY_TYPE) "
                "VALUES (:pipeline_id, :task_id, :pipeline_id, :depends_on_task_id, "
                ":dependency_type)"
            ),
            {
                "pipeline_id": pipeline_id,
                "task_id": task_id,
                "depends_on_task_id": depends_on_task_id,
                "dependency_type": dependency_type,
            },
        )


CRAFT_CONNECTOR_YAML = """
Execution:
  Mode: local

Source:
  Type: environment

Postgres:
  Active_profile: dev
  Profiles:
    dev:
      jdbc_url: jdbc:postgresql://localhost:55432/etl_craft
      user: etl_craft
      auth_mode: password

Cloning:
  Enabled: false
"""


@pytest.fixture
def craft_connector_on_disk(tmp_path, monkeypatch):
    """Write a real craft-connector.yml pointing at the test Postgres, and chdir into it."""
    # Needed by anything that spawns a real `python -m etl_craft` subprocess
    # (orchestrator.run_pipeline, and cli.main indirectly through it) — the
    # child process resolves its own config from cwd, same as a real
    # deployment, so it can't reuse the parent test's in-memory config/engine.
    (tmp_path / "craft-connector.yml").write_text(CRAFT_CONNECTOR_YAML)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ETL_CRAFT_POSTGRES_DEV_SECRET", "etl_craft")
