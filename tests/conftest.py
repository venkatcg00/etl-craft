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

# Stands in as a genuinely different SQLAlchemy dialect for warehouse.py's
# tests — Postgres alone (the Engine DB) can't prove the generic,
# dialect-agnostic connect mechanism works against anything but Postgres.
# Needs the optional `clickhouse` extra installed (see pyproject.toml); if
# it isn't, _reachable's broad except below just reports "not reachable",
# same as Docker being down — a coarser message, but a clean skip either way.
TEST_CLICKHOUSE_URL_VAR = "ETL_CRAFT_TEST_CLICKHOUSE_URL"
DEFAULT_TEST_CLICKHOUSE_URL = "clickhouse://etl_craft:etl_craft@localhost:58123/etl_craft"


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
    # runner.py's crash-detection fork rebuilds its own Engine via
    # build_engine(config) rather than reusing whatever Engine a test passed
    # in — deliberately, to avoid sharing DB connections across fork (see
    # runner.py's own [CHOICE] comment). That means it needs a real,
    # resolvable secret even in tests that otherwise bypass config/secret
    # resolution entirely by injecting postgres_engine directly. setdefault
    # so a real developer override (if any) is never clobbered.
    os.environ.setdefault("ETL_CRAFT_POSTGRES_DEV_SECRET", "etl_craft")
    # Same reasoning, for handlers.py's own fresh-Data-DB-engine-per-dispatch
    # (sql_actions.py/business_rules.py tests configure [Warehouse] pointing
    # at this same Postgres, standing in as the Data DB — see
    # test_integration.py's make_config(warehouse=True)).
    os.environ.setdefault("ETL_CRAFT_WAREHOUSE_DEV_SECRET", "etl_craft")
    engine = create_engine(url)
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def clickhouse_engine() -> Engine:
    """Build a real ClickHouse engine — skips if unreachable or the extra isn't installed."""
    url = os.environ.get(TEST_CLICKHOUSE_URL_VAR, DEFAULT_TEST_CLICKHOUSE_URL)
    if not _reachable(url):
        pytest.skip(
            f"no reachable ClickHouse at {url!r} — run `make db-up` (see docker-compose.yml), "
            f"install the `clickhouse` extra, or set {TEST_CLICKHOUSE_URL_VAR}"
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
def data_db_tables(postgres_engine: Engine):
    """Yield a list; any table name a test appends is dropped after the test.

    For sql_actions.py/business_rules.py tests, which point [Warehouse] at
    this same Postgres (make_config(warehouse=True)) and create/drop real
    tables there as a side effect of running a SQL_ACTION — this is separate
    cleanup from committed_pipeline's own (which only ever touches CFG_/AUD_
    rows, never anything in the Data DB "warehouse" side of the same
    physical database).
    """
    tables: list[str] = []
    yield tables
    with postgres_engine.begin() as conn:
        for table in tables:
            conn.execute(text(f"DROP TABLE IF EXISTS {table}"))


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
        # AUD_BUSINESS_RULES_RESULTS/_RUN_LOG (business_rules.py) and
        # AUD_TASK_OFFSET_TRACKER (scripts.py) all FK onto CFG_TASKS/
        # CFG_BUSINESS_RULES/AUD_TASK_RUN_LOG — deleted before those, same
        # FK-order discipline as the rest of this teardown.
        conn.execute(
            text(
                "DELETE FROM AUD_BUSINESS_RULES_RESULTS WHERE BUSINESS_RULE_ID IN "
                "(SELECT BUSINESS_RULE_ID FROM CFG_BUSINESS_RULES WHERE PIPELINE_ID = :id)"
            ),
            {"id": pipeline_id},
        )
        conn.execute(
            text(
                "DELETE FROM AUD_BUSINESS_RULES_RUN_LOG WHERE BUSINESS_RULE_ID IN "
                "(SELECT BUSINESS_RULE_ID FROM CFG_BUSINESS_RULES WHERE PIPELINE_ID = :id)"
            ),
            {"id": pipeline_id},
        )
        conn.execute(
            text(
                "DELETE FROM AUD_TASK_OFFSET_TRACKER WHERE TASK_ID IN "
                "(SELECT TASK_ID FROM CFG_TASKS WHERE PIPELINE_ID = :id)"
            ),
            {"id": pipeline_id},
        )
        conn.execute(
            text(
                "DELETE FROM AUD_TASK_RUN_LOG WHERE TASK_ID IN "
                "(SELECT TASK_ID FROM CFG_TASKS WHERE PIPELINE_ID = :id)"
            ),
            {"id": pipeline_id},
        )
        conn.execute(
            text("DELETE FROM CFG_BUSINESS_RULES WHERE PIPELINE_ID = :id"), {"id": pipeline_id}
        )
        conn.execute(
            text(
                "DELETE FROM CFG_TASK_PARAMETERS WHERE TASK_ID IN "
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
    engine: Engine,
    pipeline_id: int,
    task_code: str,
    handler: str = "SQL",
    *,
    schema_evolution: bool = False,
    script_name: str | None = None,
    return_values: str | None = None,
) -> int:
    """Insert and commit one CFG_TASKS row — for code-under-test that opens its own connections.

    [DEVIATION, post-signoff 2026-09-20] SCHEMA_EVOLUTION/SCRIPT_NAME/
    RETURN_VALUES are no longer CFG_TASKS columns (see that table's own
    comment in schema.sql) — this helper keeps the same convenience kwargs
    for existing call sites, but now writes them as CFG_TASK_PARAMETERS rows
    instead. `schema_evolution=False` (the default) writes nothing, matching
    the "absent means false" convention sql_actions.py itself uses.
    """
    with engine.begin() as conn:
        task_id = conn.execute(
            text(
                "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
                "VALUES (:task_code, 'ETL', :pipeline_id, :handler) RETURNING TASK_ID"
            ),
            {"task_code": task_code, "pipeline_id": pipeline_id, "handler": handler},
        ).scalar_one()
    params = {}
    if schema_evolution:
        params["SCHEMA_EVOLUTION"] = "true"
    if script_name is not None:
        params["SCRIPT_NAME"] = script_name
    if return_values is not None:
        params["RETURN_VALUES"] = return_values
    if params:
        insert_committed_task_parameters(engine, task_id, params)
    return task_id


def insert_committed_task_parameters(engine: Engine, task_id: int, params: dict[str, str]) -> None:
    """Insert and commit CFG_TASK_PARAMETERS rows for `task_id` — for sql_actions.py tests."""
    with engine.begin() as conn:
        for name, value in params.items():
            conn.execute(
                text(
                    "INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE) "
                    "VALUES (:task_id, :name, :value)"
                ),
                {"task_id": task_id, "name": name, "value": value},
            )


def insert_committed_business_rule(
    engine: Engine,
    pipeline_id: int,
    task_id: int,
    business_rule_name: str,
    target_table: str,
    key_column: str,
    *,
    business_rule_sql: str = "SELECT 1",
    business_rule_type: str = "REJECT",
    sequence_number: int = 1,
) -> int:
    """Insert and commit one CFG_BUSINESS_RULES row — for validate.py's/business_rules.py tests."""
    with engine.begin() as conn:
        return conn.execute(
            text(
                "INSERT INTO CFG_BUSINESS_RULES (BUSINESS_RULE_NAME, PIPELINE_ID, TASK_ID, "
                "BUSINESS_RULE_SQL, BUSINESS_RULE_TYPE, BUSINESS_RULE_KEY_COLUMN, TARGET_TABLE, "
                "SEQUENCE_NUMBER) VALUES (:name, :pipeline_id, :task_id, :business_rule_sql, "
                ":business_rule_type, :key_column, :target_table, :sequence_number) "
                "RETURNING BUSINESS_RULE_ID"
            ),
            {
                "name": business_rule_name,
                "pipeline_id": pipeline_id,
                "task_id": task_id,
                "key_column": key_column,
                "target_table": target_table,
                "business_rule_sql": business_rule_sql,
                "business_rule_type": business_rule_type,
                "sequence_number": sequence_number,
            },
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


@pytest.fixture
def two_committed_pipelines(postgres_engine: Engine):
    """Yield (downstream_pipeline_id, upstream_pipeline_id), both genuinely committed."""
    # crosspipe.py's functions each open their own connections (see its own
    # module docstring on why — never holding one open across a poll's real
    # sleep), so — unlike pg_conn-based cfg.py/validate.py tests — every row
    # a crosspipe test sets up must be genuinely committed, not just held in
    # an uncommitted pg_conn transaction. Two pipelines, since almost every
    # cross-pipeline test needs a downstream subject and an upstream target.
    with postgres_engine.begin() as conn:
        downstream_id = conn.execute(
            text(
                "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
                "VALUES ('TEST_XPIPE_DOWN', 'Downstream', 'INCREMENTAL') RETURNING PIPELINE_ID"
            )
        ).scalar_one()
        upstream_id = conn.execute(
            text(
                "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
                "VALUES ('TEST_XPIPE_UP', 'Upstream', 'INCREMENTAL') RETURNING PIPELINE_ID"
            )
        ).scalar_one()
    yield downstream_id, upstream_id
    with postgres_engine.begin() as conn:
        ids = {"down": downstream_id, "up": upstream_id}
        # Tracker rows reference both sides (PIPELINE_ID and
        # DEPENDS_ON_PIPELINE_ID/DEPENDS_ON_TASK_ID), so either pipeline
        # appearing on either side must be checked before either CFG_
        # pipeline/task row can be deleted.
        conn.execute(
            text(
                "DELETE FROM AUD_TASK_DEPENDENCY_TRACKER WHERE PIPELINE_ID IN (:down, :up) "
                "OR DEPENDS_ON_PIPELINE_ID IN (:down, :up)"
            ),
            ids,
        )
        conn.execute(
            text(
                "DELETE FROM AUD_PIPELINE_DEPENDENCY_TRACKER WHERE PIPELINE_ID IN (:down, :up) "
                "OR DEPENDS_ON_PIPELINE_ID IN (:down, :up)"
            ),
            ids,
        )
        conn.execute(
            text(
                "DELETE FROM AUD_TASK_RUN_LOG WHERE PIPELINE_RUN_ID IN "
                "(SELECT PIPELINE_RUN_ID FROM AUD_PIPELINES_RUN_LOG "
                "WHERE PIPELINE_ID IN (:down, :up))"
            ),
            ids,
        )
        conn.execute(text("DELETE FROM CFG_TASK_DEPENDENCY WHERE PIPELINE_ID IN (:down, :up)"), ids)
        conn.execute(
            text("DELETE FROM CFG_PIPELINE_DEPENDENCY WHERE PIPELINE_ID IN (:down, :up)"), ids
        )
        conn.execute(
            text("DELETE FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_ID IN (:down, :up)"), ids
        )
        conn.execute(text("DELETE FROM CFG_TASKS WHERE PIPELINE_ID IN (:down, :up)"), ids)
        conn.execute(text("DELETE FROM CFG_PIPELINES WHERE PIPELINE_ID IN (:down, :up)"), ids)


def insert_committed_pipeline_run(
    engine: Engine,
    pipeline_id: int,
    status: str,
    *,
    start_date: object = None,
    end_date: object = None,
) -> int:
    """Insert and commit one AUD_PIPELINES_RUN_LOG row — for crosspipe.py's tests."""
    with engine.begin() as conn:
        return conn.execute(
            text(
                "INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS, START_DATE, END_DATE) "
                "VALUES (:pipeline_id, :status, COALESCE(:start_date, now()), :end_date) "
                "RETURNING PIPELINE_RUN_ID"
            ),
            {
                "pipeline_id": pipeline_id,
                "status": status,
                "start_date": start_date,
                "end_date": end_date,
            },
        ).scalar_one()


def insert_committed_task_run(
    engine: Engine,
    task_id: int,
    pipeline_run_id: int,
    status: str,
    *,
    start_date: object = None,
    target_count: int | None = None,
) -> int:
    """Insert and commit one AUD_TASK_RUN_LOG row — for crosspipe.py's tests."""
    with engine.begin() as conn:
        return conn.execute(
            text(
                "INSERT INTO AUD_TASK_RUN_LOG (TASK_ID, PIPELINE_RUN_ID, STATUS, START_DATE, "
                "TARGET_COUNT) VALUES (:task_id, :pipeline_run_id, :status, "
                "COALESCE(:start_date, now()), :target_count) RETURNING TASK_RUN_ID"
            ),
            {
                "task_id": task_id,
                "pipeline_run_id": pipeline_run_id,
                "status": status,
                "start_date": start_date,
                "target_count": target_count,
            },
        ).scalar_one()


def insert_committed_pipeline_dependency(
    engine: Engine, pipeline_id: int, depends_on_pipeline_id: int, dependency_type: str = "SUCCESS"
) -> int:
    """Insert and commit one CFG_PIPELINE_DEPENDENCY row — for crosspipe.py's tests."""
    with engine.begin() as conn:
        return conn.execute(
            text(
                "INSERT INTO CFG_PIPELINE_DEPENDENCY (PIPELINE_ID, DEPENDS_ON_PIPELINE_ID, "
                "DEPENDENCY_TYPE) VALUES (:pipeline_id, :depends_on_pipeline_id, :dependency_type) "
                "RETURNING PIPELINE_DEPENDENCY_ID"
            ),
            {
                "pipeline_id": pipeline_id,
                "depends_on_pipeline_id": depends_on_pipeline_id,
                "dependency_type": dependency_type,
            },
        ).scalar_one()


def insert_committed_cross_pipeline_task_dependency(
    engine: Engine,
    pipeline_id: int,
    task_id: int,
    depends_on_pipeline_id: int,
    depends_on_task_id: int,
    dependency_type: str = "SUCCESS",
) -> int:
    """Insert and commit one cross-pipeline CFG_TASK_DEPENDENCY row — for crosspipe.py's tests."""
    with engine.begin() as conn:
        return conn.execute(
            text(
                "INSERT INTO CFG_TASK_DEPENDENCY (PIPELINE_ID, TASK_ID, DEPENDS_ON_PIPELINE_ID, "
                "DEPENDS_ON_TASK_ID, DEPENDENCY_TYPE) VALUES (:pipeline_id, :task_id, "
                ":depends_on_pipeline_id, :depends_on_task_id, :dependency_type) "
                "RETURNING TASK_DEPENDENCY_ID"
            ),
            {
                "pipeline_id": pipeline_id,
                "task_id": task_id,
                "depends_on_pipeline_id": depends_on_pipeline_id,
                "depends_on_task_id": depends_on_task_id,
                "dependency_type": dependency_type,
            },
        ).scalar_one()


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
def craft_connector_on_disk(tmp_path, monkeypatch, postgres_engine):
    """Write a real craft-connector.yml pointing at the test Postgres, and chdir into it."""
    # Needed by anything that spawns a real `python -m etl_craft` subprocess
    # (orchestrator.run_pipeline, and cli.main indirectly through it) — the
    # child process resolves its own config from cwd, same as a real
    # deployment, so it can't reuse the parent test's in-memory config/engine.
    #
    # Depending on postgres_engine here — even though its value is unused —
    # is load-bearing, not incidental: it's what runs the skip-if-unreachable
    # check before any test that uses only this fixture (several CLI tests
    # never touch postgres_engine directly) tries to connect for real. Found
    # via a genuine failure: with Docker down, four CLI tests errored with a
    # raw connection-refused traceback instead of skipping, contradicting
    # this file's own module docstring that plain `pytest -q` never needs it.
    (tmp_path / "craft-connector.yml").write_text(CRAFT_CONNECTOR_YAML)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ETL_CRAFT_POSTGRES_DEV_SECRET", "etl_craft")
