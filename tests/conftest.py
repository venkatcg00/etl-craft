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

import atexit
import os
import shutil
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qsl, urlencode

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from etl_craft.config import ConnectionProfile
from etl_craft.runlog import find_or_create_active_run
from etl_craft.warehouse import (
    PREFERRED_CONNECTION_FIELDS,
    preferred_connection_url,
    translate_jdbc_url,
)

TEST_DATABASE_URL_VAR = "ETL_CRAFT_TEST_DATABASE_URL"
DEFAULT_TEST_DATABASE_URL = "postgresql+psycopg://etl_craft:etl_craft@localhost:55432/etl_craft"

# [ADDITION, 2026-09-24] Which Engine DB the suite's own fixtures use. SQLite
# became the default Engine DB, so the same suite runs against it with
# ETL_CRAFT_TEST_ENGINE=sqlite (`make test-sqlite-engine`): the Engine DB
# becomes a throwaway SQLite file, while every [Warehouse] a test configures
# stays exactly where that test put it.
ENGINE_KIND = os.environ.get("ETL_CRAFT_TEST_ENGINE", "postgres").lower()
SQLITE_ENGINE = ENGINE_KIND == "sqlite"
if SQLITE_ENGINE:
    import etl_craft.db as _engine_db_module

    # In-process only: a test that holds an uncommitted pg_conn write while the
    # code under test writes on another connection is a harness shape SQLite
    # can only answer by waiting. Fail it in seconds rather than a minute.
    # Spawned task subprocesses keep the real 60s, where waiting is correct.
    _engine_db_module.SQLITE_BUSY_TIMEOUT_MS = 3_000
    _sqlite_engine_dir = tempfile.mkdtemp(prefix="etl-craft-engine-")
    atexit.register(shutil.rmtree, _sqlite_engine_dir, ignore_errors=True)
    SQLITE_ENGINE_PATH = Path(_sqlite_engine_dir) / "engine.db"
    ENGINE_JDBC_URL = f"jdbc:sqlite:{SQLITE_ENGINE_PATH}"
    ENGINE_USER = ""
    ENGINE_AUTH_MODE = "none"
else:
    ENGINE_JDBC_URL = "jdbc:postgresql://localhost:55432/etl_craft"
    ENGINE_USER = "etl_craft"
    ENGINE_AUTH_MODE = "password"

# [ADDITION, 2026-09-24] Tests that exercise the *Postgres* Engine DB specifically,
# skipped under ETL_CRAFT_TEST_ENGINE=sqlite. Each was checked by hand: none is an engine
# behaviour SQLite gets wrong, each is a test built on a Postgres-only premise.
_WAREHOUSE = (
    "uses the Engine DB fixture as its Postgres warehouse "
    "(creates or reads warehouse tables through it)"
)
_MIGRATIONS = "drives Postgres migration files, Postgres DDL, or a disposable Postgres database"
_ROLE = "asserts CREATED_BY is the connected Postgres role; SQLite has no users"
_SECRET = "asserts the Engine DB secret check, which a SQLite Engine DB has no secret for"
_CLONING = "clones between two Postgres databases; the engine-is-warehouse guard is Postgres-shaped"
_TRINO_FORK = (
    "Trino-warehouse tests fork from the pytest process, which deadlocks intermittently here; "
    "covered by the Postgres-engine run"
)
POSTGRES_ENGINE_ONLY: dict[str, str] = {
    "test_apply_pending_migrations_applies_in_order_and_records_them": _MIGRATIONS,
    "test_apply_pending_migrations_handles_a_colon_in_a_string_literal": _MIGRATIONS,
    "test_apply_pending_migrations_stops_and_does_not_record_a_failed_file": _MIGRATIONS,
    "test_changed_applied_file_fails_before_later_project_migrations_run": _MIGRATIONS,
    "test_cli_migrate_applies_the_real_migrations_directory_and_is_idempotent": _MIGRATIONS,
    "test_init_db_creates_the_schema_and_then_refuses": _MIGRATIONS,
    "test_legacy_engine_records_are_adopted_without_reapplying_them": _MIGRATIONS,
    "test_mark_packaged_migrations_applied_leaves_a_teams_own_migration_pending": _MIGRATIONS,
    "test_migrate_creates_schema_migrations_when_the_table_is_absent": _MIGRATIONS,
    "test_project_stream_does_not_mask_packaged_stream_or_same_filename": _MIGRATIONS,
    "test_setup_brings_a_real_database_up_then_keeps_it_current": _MIGRATIONS,
    "test_business_rules_flags_deactivates_and_skips_rerun": _WAREHOUSE,
    "test_business_rules_force_scans_all_data": _WAREHOUSE,
    "test_business_rules_one_bad_rule_in_a_wave_does_not_block_its_wave_mate": _WAREHOUSE,
    "test_business_rules_same_sequence_number_rules_run_as_one_wave": _WAREHOUSE,
    "test_cli_validate_ok_with_warehouse_configured_and_matching_pk": _WAREHOUSE,
    "test_fetch_columns_accepts_catalog_qualified_stage": _WAREHOUSE,
    "test_hash_expression_yields_the_same_32_hex_chars_on_both_warehouses": _WAREHOUSE,
    "test_sql_actions_assign_row_ids_on_an_iceberg_backed_warehouse": _WAREHOUSE,
    "test_sql_create_table_stamps_pipeline_run_id_and_counts": _WAREHOUSE,
    "test_sql_create_table_target_passes_validates_own_primary_key_check": _WAREHOUSE,
    "test_sql_delete_rows_hard_and_soft": _WAREHOUSE,
    "test_sql_delete_rows_hard_delete_ignores_missing_delete_flag": _WAREHOUSE,
    "test_sql_delete_rows_soft_delete_missing_delete_flag_fails_clearly": _WAREHOUSE,
    "test_sql_drop_table_refused_without_create_table_sibling": _WAREHOUSE,
    "test_sql_drop_table_succeeds_with_create_table_sibling": _WAREHOUSE,
    "test_sql_overwrite_table_creates_a_missing_target": _WAREHOUSE,
    "test_sql_overwrite_table_missing_audit_column_fails_clearly": _WAREHOUSE,
    "test_sql_overwrite_table_truncates_and_reinserts": _WAREHOUSE,
    "test_sql_scd1_merge_dedupes_by_declared_order_across_two_runs": _WAREHOUSE,
    "test_sql_scd1_merge_inserts_updates_and_skips_unchanged": _WAREHOUSE,
    "test_sql_scd1_merge_missing_audit_column_fails_even_with_schema_evolution": _WAREHOUSE,
    "test_sql_scd1_merge_rejects_duplicate_merge_keys_before_touching_the_target": _WAREHOUSE,
    "test_sql_scd1_preserve_target_nulls_and_hashes": _WAREHOUSE,
    "test_sql_scd2_merge_converges_for_a_key_left_with_no_active_row": _WAREHOUSE,
    "test_sql_scd2_merge_deactivates_and_inserts_new_version": _WAREHOUSE,
    "test_sql_scd2_merge_keeps_history_with_a_surrogate_primary_key": _WAREHOUSE,
    "test_sql_schema_evolution_enabled_adds_column_at_right_position": _WAREHOUSE,
    "test_sql_setup_table_infers_audit_columns_from_scd2_sibling": _WAREHOUSE,
    "test_sql_setup_table_no_sibling_falls_back_to_no_audit_columns": _WAREHOUSE,
    "test_validate_business_rule_key_must_exist_on_the_target": _WAREHOUSE,
    "test_validate_business_rule_key_need_not_be_the_primary_key": _WAREHOUSE,
    "test_validate_business_rule_keys_composite_pk_reported": _WAREHOUSE,
    "test_validate_business_rule_keys_matching_single_column_pk_is_ok": _WAREHOUSE,
    "test_validate_business_rule_keys_no_pk_reported": _WAREHOUSE,
    "test_validate_business_rule_keys_table_does_not_exist": _WAREHOUSE,
    "test_cli_doctor_reports_failures_with_exit_1": _SECRET,
    "test_doctor_reports_every_check_and_names_a_missing_secret": _SECRET,
    "test_fetch_pipeline_detail": _ROLE,
    "test_fetch_pipeline_detail_nullable_fields_default_none": _ROLE,
    "test_generate_pipeline_dag_falls_back_to_global_orchestrator_config": _ROLE,
    "test_generate_pipeline_dag_linear_chain": _ROLE,
    "test_generate_pipeline_dag_pipeline_level_override_wins": _ROLE,
    "test_run_cloning_creates_generic_target_table_on_a_different_postgres_database": _CLONING,
    "test_run_cloning_refuses_when_warehouse_is_the_same_database_as_engine": _CLONING,
}


def pytest_collection_modifyitems(config, items):
    """Skip the Postgres-premised tests when the Engine DB fixture is SQLite."""
    if not SQLITE_ENGINE:
        return
    for item in items:
        if "trino_engine" in getattr(item, "fixturenames", ()):
            # Open item, not a verdict: forking a task child from this long-
            # lived, heavily threaded pytest process intermittently deadlocks
            # the child inside the Trino HTTP client under this run, and the
            # orphaned child then blocks interpreter exit. The CLI forks from a
            # fresh single-threaded process, and these tests pass alone on both
            # Engine DBs and in the Postgres-engine run, which covers them.
            item.add_marker(pytest.mark.skip(reason=f"SQLite Engine DB run: {_TRINO_FORK}"))
            continue
        reason = POSTGRES_ENGINE_ONLY.get(item.originalname)
        if reason:
            item.add_marker(pytest.mark.skip(reason=f"SQLite Engine DB run: {reason}"))


def engine_profile() -> ConnectionProfile:
    """Return the Engine DB profile every test config should use."""
    return ConnectionProfile(
        section="POSTGRES",
        name="dev",
        jdbc_url=ENGINE_JDBC_URL,
        user=ENGINE_USER,
        auth_mode=ENGINE_AUTH_MODE,
    )


# [DEVIATION, 2026-09-20] Replaces the ClickHouse fixture. DuckDB is the
# second supported warehouse now, and being embedded it needs no container at
# all — a tmp_path file per test, which is both faster and one less thing that
# can be "not reachable". `make db-up` is still needed for the Engine DB.


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
    """Build a real Postgres engine with sql/schema.sql applied — skips if unreachable.

    Under ETL_CRAFT_TEST_ENGINE=sqlite this is the SQLite Engine DB instead,
    built the way `etl-craft init-db` builds one.
    """
    os.environ.setdefault("ETL_CRAFT_POSTGRES_DEV_SECRET", "etl_craft")
    os.environ.setdefault("ETL_CRAFT_WAREHOUSE_DEV_SECRET", "etl_craft")
    if SQLITE_ENGINE:
        yield _sqlite_engine_db()
        return
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
    # at this same Postgres, standing in as the warehouse — see
    # test_integration.py's make_config(warehouse=True)).
    os.environ.setdefault("ETL_CRAFT_WAREHOUSE_DEV_SECRET", "etl_craft")
    engine = create_engine(url)
    yield engine
    engine.dispose()


def _sqlite_engine_db() -> Engine:
    from etl_craft.config import (
        CloningConfig,
        ConnectionSection,
        ConnectorConfig,
        SourceConfig,
    )
    from etl_craft.db import build_engine
    from etl_craft.init_db import existing_engine_tables, init_db

    config = ConnectorConfig(
        mode="local",
        source=SourceConfig(type="environment", path=None),
        postgres=ConnectionSection(active_profile="dev", profiles={"dev": engine_profile()}),
        cloning=CloningConfig(enabled=False, scope="cfg"),
    )
    engine = build_engine(config)
    if not existing_engine_tables(engine):
        init_db(engine)
        # A fresh file restarts every id at 1 each run, while the persistent
        # warehouses keep tables named after task_run_ids (etl_stage_<id>).
        # Start from the clock so ids only grow across runs, as they do in the
        # long-lived Postgres test database.
        with engine.begin() as conn:
            conn.execute(
                text("INSERT INTO sqlite_sequence (name, seq) VALUES ('AUD_TASK_RUN_LOG', :seq)"),
                {"seq": int(time.time() * 1000)},
            )
    return engine


# [ADDITION, 2026-09-22] Databricks and Snowflake, gated on credentials being
# present exactly as the container fixtures are gated on a container running.
#
# Neither can be stood up locally -- Databricks needs a workspace and Snowflake
# needs cloud object storage for an Iceberg external volume -- so these skip by
# default and there is no way around that. What this removes is the *other*
# blocker: with the tests written and waiting, verifying either one is now
# "export these variables and run the suite", locally or from CI secrets,
# rather than "someone writes the tests first".
#
# [DEVIATION, 2026-09-23] TESTED AND RECOMMENDED: the preferred-connection
# shape (warehouse.PREFERRED_CONNECTION_FIELDS), separate fields rather than
# one JDBC URL with everything packed into its query string. This is the
# branch _cloud_warehouse_profile takes when ETL_CRAFT_TEST_DATABRICKS_CATALOG
# / ETL_CRAFT_TEST_SNOWFLAKE_ACCOUNT is set. Verified live 2026-09-23 for both:
# Databricks (native and iceberg/UniForm both pass the full SQL action
# vocabulary) and Snowflake (native and iceberg both pass the full
# vocabulary too — Iceberg tables default to Snowflake's own internal
# storage, EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED', so no cloud bucket has to
# exist first; see sql_actions.SNOWFLAKE_MANAGED_VOLUME). A Programmatic
# Access Token additionally needs a network policy assigned to the account
# or user first — see docs/craft-connector.variables.env.
#
# Set, for Databricks:
#   ETL_CRAFT_TEST_DATABRICKS_JDBC_URL   jdbc:databricks://<host>:443/default;
#                                        httpPath=<path>   (no ConnCatalog here
#                                        — that is the CATALOG field below)
#   ETL_CRAFT_TEST_DATABRICKS_CATALOG    the catalog half of catalog.schema.table
#   ETL_CRAFT_TEST_DATABRICKS_SCHEMA     a schema the token may create in
#   ETL_CRAFT_TEST_DATABRICKS_TOKEN      a personal access token
#
# ...and for Snowflake:
#   ETL_CRAFT_TEST_SNOWFLAKE_USER
#   ETL_CRAFT_TEST_SNOWFLAKE_ACCOUNT     <org>-<account>, not a hostname
#   ETL_CRAFT_TEST_SNOWFLAKE_DATABASE
#   ETL_CRAFT_TEST_SNOWFLAKE_SCHEMA      a schema the user may create in
#   ETL_CRAFT_TEST_SNOWFLAKE_WAREHOUSE
#   ETL_CRAFT_TEST_SNOWFLAKE_ROLE
#   ETL_CRAFT_TEST_SNOWFLAKE_TOKEN       a Programmatic Access Token (PAT)
#   ETL_CRAFT_TEST_SNOWFLAKE_EXTERNAL_VOLUME / _BASE_LOCATION
#                                        optional; opts the Iceberg test into
#                                        a real customer-owned volume instead
#                                        of Snowflake's own managed storage
#
# The older single-JDBC-URL shape (ETL_CRAFT_TEST_SNOWFLAKE_JDBC_URL / _USER /
# _SECRET / _KEY_FILE, key-pair or password auth) is still supported as a
# fallback — see _cloud_warehouse_profile's second branch below — for a team
# standardising on RSA key-pair auth instead of PATs.
def _cloud_warehouse_profile(prefix: str) -> ConnectionProfile | None:
    """Build a [Warehouse] profile from ETL_CRAFT_TEST_<PREFIX>_* , or None if unset."""
    preferred = (
        os.environ.get(f"ETL_CRAFT_TEST_{prefix}_ACCOUNT")
        if prefix == "SNOWFLAKE"
        else os.environ.get(f"ETL_CRAFT_TEST_{prefix}_CATALOG")
    )
    if preferred:
        fields = {
            key: os.environ.get(f"ETL_CRAFT_TEST_{prefix}_{key.upper()}", "")
            for key in PREFERRED_CONNECTION_FIELDS[prefix.lower()]
            if key != "token"
        }
        return ConnectionProfile(
            section="WAREHOUSE",
            name="dev",
            jdbc_url=preferred_connection_url(prefix.lower(), fields),
            user=fields.get("user", ""),
            auth_mode="token",
            extra={"secret_var": f"ETL_CRAFT_TEST_{prefix}_TOKEN"},
        )
    jdbc_url = os.environ.get(f"ETL_CRAFT_TEST_{prefix}_JDBC_URL")
    if not jdbc_url:
        return None
    secret_env = f"ETL_CRAFT_TEST_{prefix}_SECRET"
    url_password = None
    url_user = None
    if prefix == "SNOWFLAKE":
        # JDBC query values can contain a literal '#'; do not parse them as
        # a web URL fragment. Move credentials out of the logged engine URL.
        base, _, query = jdbc_url.partition("?")
        params = dict(parse_qsl(query))
        url_password = params.pop("password", None)
        url_user = params.pop("user", None)
        jdbc_url = base + "?" + urlencode(params)
        if url_password is not None:
            os.environ[secret_env] = url_password
    user = url_user or os.environ.get(f"ETL_CRAFT_TEST_{prefix}_USER", "")
    if prefix == "DATABRICKS":
        secret_env = f"ETL_CRAFT_TEST_{prefix}_TOKEN"
        auth_mode, user = "token", ""
    elif translate_jdbc_url(jdbc_url)[1]["query"].get("authenticator") == "externalbrowser":
        auth_mode = "none"
    elif url_password is not None:
        auth_mode = "password"
    elif os.environ.get(f"ETL_CRAFT_TEST_{prefix}_KEY_FILE"):
        auth_mode = "key_file"
    else:
        auth_mode = "password"
    # Keep cloud credentials independent of the local Postgres fixture and
    # of each other, using the profile's existing variable-name indirection.
    extra = {"secret_var": secret_env}
    key_file = os.environ.get(f"ETL_CRAFT_TEST_{prefix}_KEY_FILE")
    if key_file:
        extra["key_file"] = key_file
    return ConnectionProfile(
        section="WAREHOUSE",
        name="dev",
        jdbc_url=jdbc_url,
        user=user,
        auth_mode=auth_mode,
        extra=extra,
    )


@pytest.fixture(scope="session")
def databricks_profile() -> ConnectionProfile:
    """Build a real Databricks [Warehouse] profile, or skip."""
    profile = _cloud_warehouse_profile("DATABRICKS")
    if profile is None:
        pytest.skip(
            "Databricks not configured — set ETL_CRAFT_TEST_DATABRICKS_JDBC_URL and "
            "ETL_CRAFT_TEST_DATABRICKS_TOKEN to run this against a real workspace"
        )
    return profile


@pytest.fixture(scope="session")
def snowflake_profile() -> ConnectionProfile:
    """Build a real Snowflake [Warehouse] profile, or skip."""
    profile = _cloud_warehouse_profile("SNOWFLAKE")
    if profile is None:
        pytest.skip(
            "Snowflake not configured — set ETL_CRAFT_TEST_SNOWFLAKE_JDBC_URL, _USER and "
            "_SECRET (plus _KEY_FILE for key-pair auth) to run this against a real account"
        )
    return profile


TRINO_URL = os.environ.get("ETL_CRAFT_TEST_TRINO_URL", "trino://etl@localhost:58080/iceberg")


@pytest.fixture(scope="session")
def trino_engine() -> Engine:
    """Build a real Trino engine over the local Iceberg REST catalog + MinIO.

    [ADDITION, 2026-09-22] This is the warehouse shape the project supports
    for everything except Postgres -- a SQL engine over Iceberg -- and until
    this existed there was no reachable one, so the Iceberg code path was
    tested only for producing well-formed SQL. Databricks and Snowflake both
    need a cloud account, and Snowflake cannot use a local MinIO at all (its
    external volumes are read by Snowflake's own cloud service). Trino + REST
    + MinIO runs locally and in CI and exercises the same code path.

    Skips, like the Postgres fixture, rather than failing when the stack is
    not up: `pytest -q` alone never requires Docker.
    """
    if not _reachable(TRINO_URL):
        pytest.skip(f"Trino not reachable at {TRINO_URL} — run `make db-up`")
    engine = create_engine(TRINO_URL)
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS iceberg.etltest"))
    yield engine
    engine.dispose()


@pytest.fixture
def duckdb_engine(tmp_path) -> Engine:
    """Build a real DuckDB engine on a throwaway file — the second supported warehouse.

    Deliberately not session-scoped, unlike the Postgres fixture: a DuckDB
    database is a file, so each test gets its own and nothing leaks between
    them. No container, so this never skips.
    """
    engine = create_engine(f"duckdb:///{tmp_path / 'warehouse.duckdb'}")
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
def warehouse_tables(postgres_engine: Engine):
    """Yield a list; any table name a test appends is dropped after the test.

    For sql_actions.py/business_rules.py tests, which point [Warehouse] at
    this same Postgres (make_config(warehouse=True)) and create/drop real
    tables there as a side effect of running a SQL_ACTION — this is separate
    cleanup from committed_pipeline's own (which only ever touches CFG_/AUD_
    rows, never anything in the warehouse "warehouse" side of the same
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
                # [ADDITION, 2026-09-20] The two tables added with column
                # lineage and documentation versioning. Both reference
                # CFG_TASKS, so leaving them out makes the whole FK-ordered
                # teardown fail -- and then the *next* run collides on
                # PIPELINE_CODE, which is a confusing way to learn about it.
                "DELETE FROM AUD_COLUMN_LINEAGE WHERE TASK_ID IN "
                "(SELECT TASK_ID FROM CFG_TASKS WHERE PIPELINE_ID = :id)"
            ),
            {"id": pipeline_id},
        )
        conn.execute(
            text(
                "DELETE FROM AUD_TASK_DOCUMENTATION WHERE TASK_ID IN "
                "(SELECT TASK_ID FROM CFG_TASKS WHERE PIPELINE_ID = :id)"
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
        # [ADDITION, 2026-09-23, E3-03] Everything else that FKs onto
        # CFG_TASKS/CFG_BUSINESS_RULES — the same tables committed_pipeline's
        # own teardown already covers, and the same reason: leaving one out
        # makes the CFG_TASKS delete below fail, and the *next* test then
        # collides on PIPELINE_CODE instead of surfacing this cleanly.
        conn.execute(
            text(
                "DELETE FROM AUD_BUSINESS_RULES_RESULTS WHERE BUSINESS_RULE_ID IN "
                "(SELECT BUSINESS_RULE_ID FROM CFG_BUSINESS_RULES "
                "WHERE PIPELINE_ID IN (:down, :up))"
            ),
            ids,
        )
        conn.execute(
            text(
                "DELETE FROM AUD_BUSINESS_RULES_RUN_LOG WHERE BUSINESS_RULE_ID IN "
                "(SELECT BUSINESS_RULE_ID FROM CFG_BUSINESS_RULES "
                "WHERE PIPELINE_ID IN (:down, :up))"
            ),
            ids,
        )
        conn.execute(
            text(
                "DELETE FROM AUD_COLUMN_LINEAGE WHERE TASK_ID IN "
                "(SELECT TASK_ID FROM CFG_TASKS WHERE PIPELINE_ID IN (:down, :up))"
            ),
            ids,
        )
        conn.execute(
            text(
                "DELETE FROM AUD_TASK_DOCUMENTATION WHERE TASK_ID IN "
                "(SELECT TASK_ID FROM CFG_TASKS WHERE PIPELINE_ID IN (:down, :up))"
            ),
            ids,
        )
        conn.execute(
            text(
                "DELETE FROM AUD_TASK_OFFSET_TRACKER WHERE TASK_ID IN "
                "(SELECT TASK_ID FROM CFG_TASKS WHERE PIPELINE_ID IN (:down, :up))"
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
        conn.execute(text("DELETE FROM CFG_BUSINESS_RULES WHERE PIPELINE_ID IN (:down, :up)"), ids)
        conn.execute(
            text(
                "DELETE FROM CFG_TASK_PARAMETERS WHERE TASK_ID IN "
                "(SELECT TASK_ID FROM CFG_TASKS WHERE PIPELINE_ID IN (:down, :up))"
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
                "VALUES (:pipeline_id, :status, :start_date, :end_date) "
                "RETURNING PIPELINE_RUN_ID"
            ),
            {
                "pipeline_id": pipeline_id,
                "status": status,
                # Bound, not SQL now(): portable to a SQLite Engine DB.
                "start_date": start_date or datetime.now(UTC),
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
                ":start_date, :target_count) RETURNING TASK_RUN_ID"
            ),
            {
                "task_id": task_id,
                "pipeline_run_id": pipeline_run_id,
                "status": status,
                "start_date": start_date or datetime.now(UTC),
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


CRAFT_CONNECTOR_YAML = f"""
Execution:
  Mode: local

Source:
  Type: environment

Postgres:
  Active_profile: dev
  Profiles:
    dev:
      jdbc_url: {ENGINE_JDBC_URL}
      user: {ENGINE_USER}
      auth_mode: {ENGINE_AUTH_MODE}

Cloning:
  Enabled: false
"""


@pytest.fixture
def duckdb_craft_connector_on_disk(tmp_path, monkeypatch, postgres_engine):
    """craft-connector.yml with a real DuckDB [Warehouse], for subprocess-spawning tests.

    [ADDITION, 2026-09-21, E2-61] Exists because every other DuckDB test runs
    in one process with its own tmp_path file, and every orchestrator test
    that spawns real subprocesses points [Warehouse] at Postgres -- so nothing
    exercised the combination that actually breaks: two task subprocesses,
    one embedded warehouse. Same structural blind spot E2-53 identified, one
    level up.
    """
    warehouse = tmp_path / "warehouse.duckdb"
    (tmp_path / "craft-connector.yml").write_text(CRAFT_CONNECTOR_YAML + f"""
Warehouse:
  Active_profile: dev
  Profiles:
    dev:
      jdbc_url: jdbc:duckdb:{warehouse}
      auth_mode: none
""")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ETL_CRAFT_POSTGRES_DEV_SECRET", "etl_craft")
    return warehouse


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
