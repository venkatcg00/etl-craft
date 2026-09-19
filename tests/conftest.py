"""Shared fixtures for the optional real-Postgres integration suite.

The default `pytest -q` run needs nothing but the in-memory SQLite
stand-ins used by test_runlog.py/test_db.py/test_config.py — CI runs that
on every push with no setup. The fixtures here back a separate, opt-in
suite (test_runlog_postgres.py) that runs against a real Postgres, which
is the only way to actually exercise the partial unique index's
concurrency guarantee; no amount of single-connection mocking can prove
that. See docker-compose.yml / Makefile (`make test`) to bring one up.
"""

import os

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

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
    """Yield a CFG_PIPELINES row genuinely committed (needed for cross-connection races)."""
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
            text("DELETE FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_ID = :id"), {"id": pipeline_id}
        )
        conn.execute(text("DELETE FROM CFG_PIPELINES WHERE PIPELINE_ID = :id"), {"id": pipeline_id})
