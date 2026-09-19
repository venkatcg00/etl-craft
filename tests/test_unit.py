"""The default, no-Docker-needed test suite: pure logic plus SQLite stand-ins.

Everything here runs with plain `pytest -q` — no live Postgres required.
Real-Postgres integration tests (which need Docker; see the Makefile) live
in test_integration.py instead. Organized by source module, one section
per module, since combining them loses nothing (no fixture/helper name
collisions) and keeps file count down.
"""

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from etl_craft.config import ConfigError, ConnectionProfile, load_config, resolve_secret
from etl_craft.db import AUTH_REGISTRY, ConnectionError_, parse_jdbc_postgres
from etl_craft.resolver import (
    CycleError,
    ResolverError,
    SelfDependencyError,
    TaskEdge,
    TaskNode,
    TaskRunState,
    UnknownTaskError,
    build_graph,
)
from etl_craft.runlog import (
    RunLogError,
    find_or_create_active_run,
    find_or_create_task_run,
    resolve_run_for_task,
    update_task_run,
)

# ==============================================================================
# resolver.py — pure dependency-graph logic, no DB at all
# ==============================================================================


def nodes(*ids: int) -> list[TaskNode]:
    return [TaskNode(task_id=i) for i in ids]


def edge(task_id: int, depends_on_task_id: int, dependency_type: str = "SUCCESS") -> TaskEdge:
    return TaskEdge(
        task_id=task_id, depends_on_task_id=depends_on_task_id, dependency_type=dependency_type
    )


def test_no_dependencies_is_a_single_wave():
    graph = build_graph(nodes(1, 2, 3), [])
    assert graph.waves() == [[1, 2, 3]]


def test_linear_chain_waves():
    # 1 -> 2 -> 3  (2 depends on 1, 3 depends on 2)
    graph = build_graph(nodes(1, 2, 3), [edge(2, 1), edge(3, 2)])
    assert graph.waves() == [[1], [2], [3]]


def test_diamond_shape_waves():
    #     1
    #    / \
    #   2   3
    #    \ /
    #     4
    graph = build_graph(nodes(1, 2, 3, 4), [edge(2, 1), edge(3, 1), edge(4, 2), edge(4, 3)])
    assert graph.waves() == [[1], [2, 3], [4]]


def test_self_dependency_rejected():
    with pytest.raises(SelfDependencyError):
        build_graph(nodes(1), [edge(1, 1)])


def test_direct_cycle_rejected():
    with pytest.raises(CycleError):
        build_graph(nodes(1, 2), [edge(1, 2), edge(2, 1)])


def test_indirect_cycle_rejected():
    with pytest.raises(CycleError):
        build_graph(nodes(1, 2, 3), [edge(1, 2), edge(2, 3), edge(3, 1)])


def test_unknown_task_id_in_edge_rejected():
    with pytest.raises(UnknownTaskError):
        build_graph(nodes(1, 2), [edge(1, 99)])


def test_unknown_dependency_type_rejected():
    with pytest.raises(ResolverError):
        build_graph(nodes(1, 2), [edge(1, 2, dependency_type="BOGUS")])


def test_ready_with_no_edges_all_unstarted_tasks_ready():
    graph = build_graph(nodes(1, 2), [])
    assert graph.ready({}) == [1, 2]


def test_ready_excludes_terminal_tasks():
    graph = build_graph(nodes(1, 2), [])
    state = {1: TaskRunState(status="SUCCESS")}
    assert graph.ready(state) == [2]


def test_ready_success_edge_blocks_until_upstream_succeeds():
    # task 1 has no deps of its own, so its own retry-eligibility (tested
    # separately) is irrelevant here — only whether 2's edge is satisfied.
    graph = build_graph(nodes(1, 2), [edge(2, 1)])
    assert 2 not in graph.ready({})
    assert 2 not in graph.ready({1: TaskRunState(status="IN-PROGRESS")})
    assert 2 not in graph.ready({1: TaskRunState(status="FAILED")})
    assert graph.ready({1: TaskRunState(status="SUCCESS")}) == [2]


def test_ready_failure_edge_only_satisfied_by_failed_upstream():
    graph = build_graph(nodes(1, 2), [edge(2, 1, dependency_type="FAILURE")])
    assert 2 not in graph.ready({1: TaskRunState(status="SUCCESS")})
    assert 2 in graph.ready({1: TaskRunState(status="FAILED")})


@pytest.mark.parametrize("upstream_status", ["SUCCESS", "FAILED", "SKIPPED"])
def test_ready_always_edge_satisfied_by_any_terminal_status(upstream_status):
    graph = build_graph(nodes(1, 2), [edge(2, 1, dependency_type="ALWAYS")])
    assert 2 in graph.ready({1: TaskRunState(status=upstream_status)})


def test_ready_always_edge_not_satisfied_while_upstream_in_progress():
    graph = build_graph(nodes(1, 2), [edge(2, 1, dependency_type="ALWAYS")])
    assert 2 not in graph.ready({1: TaskRunState(status="IN-PROGRESS")})


def test_ready_has_data_edge_requires_success_and_positive_target_count():
    graph = build_graph(nodes(1, 2), [edge(2, 1, dependency_type="HAS_DATA")])
    assert 2 not in graph.ready({1: TaskRunState(status="SUCCESS", target_count=0)})
    assert 2 not in graph.ready({1: TaskRunState(status="SUCCESS", target_count=None)})
    assert graph.ready({1: TaskRunState(status="SUCCESS", target_count=5)}) == [2]
    assert 2 not in graph.ready({1: TaskRunState(status="FAILED", target_count=5)})


def test_ready_task_with_multiple_edges_needs_all_satisfied():
    graph = build_graph(nodes(1, 2, 3), [edge(3, 1), edge(3, 2)])
    # task 1 already SUCCESS (terminal, excluded); task 2 has no deps of its
    # own, so it's ready; task 3 still waits on task 2.
    assert graph.ready({1: TaskRunState(status="SUCCESS")}) == [2]
    assert graph.ready({1: TaskRunState(status="SUCCESS"), 2: TaskRunState(status="SUCCESS")}) == [
        3
    ]


def test_ready_reattempts_failed_task_itself():
    graph = build_graph(nodes(1), [])
    assert graph.ready({1: TaskRunState(status="FAILED")}) == [1]


def test_ready_excludes_in_progress_task_itself():
    graph = build_graph(nodes(1), [])
    assert graph.ready({1: TaskRunState(status="IN-PROGRESS")}) == []


def test_skipped_upstream_never_satisfies_success_edge():
    graph = build_graph(nodes(1, 2), [edge(2, 1)])
    assert graph.ready({1: TaskRunState(status="SKIPPED")}) == []


# ==============================================================================
# config.py — craft-connector.yml loading, no DB at all
# ==============================================================================

VALID_YAML = """
Execution:
  Mode: local

Source:
  Type: environment

Postgres:
  Active_profile: dev
  Profiles:
    dev:
      jdbc_url: jdbc:postgresql://localhost:5432/etl_craft
      user: etl_engine
      auth_mode: password
    prod:
      jdbc_url: jdbc:postgresql://prod-host:5432/etl_craft
      user: etl_engine
      auth_mode: key_file
      key_file: /etc/etl-craft/prod.key

Cloning:
  Enabled: true
  Scope: cfg
"""


def write_config(tmp_path, contents: str):
    path = tmp_path / "craft-connector.yml"
    path.write_text(contents)
    return path


def test_load_valid_config(tmp_path):
    config = load_config(write_config(tmp_path, VALID_YAML))
    assert config.mode == "local"
    assert config.source.type == "environment"
    assert config.postgres.active_profile == "dev"
    assert config.postgres.active.jdbc_url == "jdbc:postgresql://localhost:5432/etl_craft"
    assert config.postgres.active.auth_mode == "password"
    assert config.postgres.profiles["prod"].extra["key_file"] == "/etc/etl-craft/prod.key"
    assert config.cloning.enabled is True
    assert config.cloning.scope == "cfg"


def test_missing_file_raises(tmp_path):
    with pytest.raises(ConfigError):
        load_config(tmp_path / "does-not-exist.yml")


def test_invalid_mode_rejected(tmp_path):
    bad = VALID_YAML.replace("Mode: local", "Mode: bogus")
    with pytest.raises(ConfigError):
        load_config(write_config(tmp_path, bad))


def test_active_profile_must_exist_in_profiles(tmp_path):
    bad = VALID_YAML.replace("Active_profile: dev", "Active_profile: staging")
    with pytest.raises(ConfigError):
        load_config(write_config(tmp_path, bad))


def test_invalid_auth_mode_rejected(tmp_path):
    bad = VALID_YAML.replace("auth_mode: password", "auth_mode: bogus")
    with pytest.raises(ConfigError):
        load_config(write_config(tmp_path, bad))


def test_file_source_requires_path(tmp_path):
    bad = VALID_YAML.replace("Type: environment", "Type: file")
    with pytest.raises(ConfigError):
        load_config(write_config(tmp_path, bad))


def test_cloning_defaults_when_section_absent(tmp_path):
    no_cloning = VALID_YAML.replace("Cloning:\n  Enabled: true\n  Scope: cfg\n", "")
    config = load_config(write_config(tmp_path, no_cloning))
    assert config.cloning.enabled is False
    assert config.cloning.scope == "cfg"


def test_resolve_secret_from_environment(tmp_path, monkeypatch):
    config = load_config(write_config(tmp_path, VALID_YAML))
    monkeypatch.setenv("ETL_CRAFT_POSTGRES_DEV_SECRET", "s3cr3t")
    assert resolve_secret(config, config.postgres.active) == "s3cr3t"


def test_resolve_secret_missing_raises(tmp_path, monkeypatch):
    config = load_config(write_config(tmp_path, VALID_YAML))
    monkeypatch.delenv("ETL_CRAFT_POSTGRES_DEV_SECRET", raising=False)
    with pytest.raises(ConfigError):
        resolve_secret(config, config.postgres.active)


def test_resolve_secret_explicit_var_override(tmp_path, monkeypatch):
    overridden = VALID_YAML.replace(
        "auth_mode: password\n", "auth_mode: password\n      secret_var: MY_CUSTOM_SECRET\n"
    )
    config = load_config(write_config(tmp_path, overridden))
    monkeypatch.setenv("MY_CUSTOM_SECRET", "hunter2")
    assert resolve_secret(config, config.postgres.active) == "hunter2"


def test_resolve_secret_from_file_source(tmp_path):
    env_file = tmp_path / "secrets.env"
    env_file.write_text("ETL_CRAFT_POSTGRES_DEV_SECRET=filesecret\n# comment\n\nOTHER=1\n")
    file_source_yaml = VALID_YAML.replace(
        "Source:\n  Type: environment\n",
        f"Source:\n  Type: file\n  Path: {env_file}\n",
    )
    config = load_config(write_config(tmp_path, file_source_yaml))
    assert resolve_secret(config, config.postgres.active) == "filesecret"


# ==============================================================================
# db.py — JDBC parsing and auth_mode wiring, no live DB required
# ==============================================================================


def test_parse_jdbc_postgres_with_explicit_port():
    parts = parse_jdbc_postgres("jdbc:postgresql://myhost:6543/mydb")
    assert parts == {"host": "myhost", "port": 6543, "database": "mydb"}


def test_parse_jdbc_postgres_default_port():
    parts = parse_jdbc_postgres("jdbc:postgresql://myhost/mydb")
    assert parts == {"host": "myhost", "port": 5432, "database": "mydb"}


def test_parse_jdbc_postgres_rejects_non_jdbc_url():
    with pytest.raises(ConnectionError_):
        parse_jdbc_postgres("postgresql://myhost:5432/mydb")


def profile(auth_mode: str, **extra) -> ConnectionProfile:
    return ConnectionProfile(
        section="POSTGRES",
        name="dev",
        jdbc_url="jdbc:postgresql://localhost:5432/etl_craft",
        user="etl_engine",
        auth_mode=auth_mode,
        extra=extra,
    )


def test_password_creator_returns_callable_that_calls_psycopg_connect(monkeypatch):
    calls = {}

    class FakeConnection:
        pass

    def fake_connect(**kwargs):
        calls.update(kwargs)
        return FakeConnection()

    import psycopg

    monkeypatch.setattr(psycopg, "connect", fake_connect)

    creator = AUTH_REGISTRY["password"](profile("password"), "s3cr3t")
    conn = creator()
    assert isinstance(conn, FakeConnection)
    assert calls == {
        "host": "localhost",
        "port": 5432,
        "dbname": "etl_craft",
        "user": "etl_engine",
        "password": "s3cr3t",
    }


def test_key_file_creator_requires_key_file_in_extra():
    with pytest.raises(ConnectionError_):
        AUTH_REGISTRY["key_file"](profile("key_file"), "passphrase")


def test_key_file_creator_returns_callable_using_sslkey(monkeypatch):
    calls = {}

    class FakeConnection:
        pass

    def fake_connect(**kwargs):
        calls.update(kwargs)
        return FakeConnection()

    import psycopg

    monkeypatch.setattr(psycopg, "connect", fake_connect)

    creator = AUTH_REGISTRY["key_file"](profile("key_file", key_file="/etc/key.pem"), "passphrase")
    creator()
    assert calls["sslkey"] == "/etc/key.pem"
    assert calls["sslpassword"] == b"passphrase"


def test_token_and_sso_creators_are_not_implemented():
    with pytest.raises(NotImplementedError):
        AUTH_REGISTRY["token"](profile("token"), "unused")
    with pytest.raises(NotImplementedError):
        AUTH_REGISTRY["sso"](profile("sso"), "unused")


# ==============================================================================
# runlog.py — against an in-memory SQLite stand-in
# ==============================================================================
#
# The real Engine DB is always Postgres (per CLAUDE.md), and its own
# behavioral guarantees — the partial unique indexes in particular — are
# covered by sql/schema_test.sql against a real Postgres instance, plus the
# concurrency test in test_integration.py. This section instead exercises
# runlog.py's own control flow (find-or-create races, short-circuiting,
# update-in-place) against a lightweight SQLite schema that reproduces just
# the constraints runlog.py depends on.

RUNLOG_SCHEMA = """
CREATE TABLE AUD_PIPELINES_RUN_LOG (
    PIPELINE_RUN_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    PIPELINE_ID INTEGER NOT NULL,
    START_DATE TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    END_DATE TIMESTAMP,
    STATUS TEXT NOT NULL
);
CREATE UNIQUE INDEX ux_pipeline_run_one_active
    ON AUD_PIPELINES_RUN_LOG (PIPELINE_ID) WHERE STATUS = 'IN-PROGRESS';

CREATE TABLE AUD_TASK_RUN_LOG (
    TASK_RUN_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    TASK_ID INTEGER NOT NULL,
    PIPELINE_RUN_ID INTEGER NOT NULL,
    START_DATE TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    END_DATE TIMESTAMP,
    STATUS TEXT NOT NULL,
    SOURCE_COUNT INTEGER,
    TARGET_COUNT INTEGER,
    INSERT_COUNT INTEGER,
    UPDATE_COUNT INTEGER,
    DELETE_COUNT INTEGER,
    ERROR_MESSAGE TEXT,
    TASK_LOG TEXT
);
CREATE UNIQUE INDEX ux_task_run_one_per_pipeline_run
    ON AUD_TASK_RUN_LOG (TASK_ID, PIPELINE_RUN_ID);
"""


@pytest.fixture
def runlog_engine() -> Engine:
    """An in-memory SQLite engine with the audit tables runlog.py touches."""
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        for statement in RUNLOG_SCHEMA.strip().split(";"):
            if statement.strip():
                conn.execute(text(statement))
    return engine


def test_find_or_create_active_run_mints_new_run_when_none_exists(runlog_engine):
    with runlog_engine.begin() as conn:
        run_id = find_or_create_active_run(conn, pipeline_id=1)
    with runlog_engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT PIPELINE_ID, STATUS FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID = :id"
            ),
            {"id": run_id},
        ).one()
    assert row.PIPELINE_ID == 1
    assert row.STATUS == "IN-PROGRESS"


def test_find_or_create_active_run_reuses_existing_in_progress_run(runlog_engine):
    with runlog_engine.begin() as conn:
        first = find_or_create_active_run(conn, pipeline_id=1)
    with runlog_engine.begin() as conn:
        second = find_or_create_active_run(conn, pipeline_id=1)
    assert first == second


def test_find_or_create_active_run_is_independent_per_pipeline(runlog_engine):
    with runlog_engine.begin() as conn:
        run_for_1 = find_or_create_active_run(conn, pipeline_id=1)
        run_for_2 = find_or_create_active_run(conn, pipeline_id=2)
    assert run_for_1 != run_for_2


def test_find_or_create_active_run_mints_a_fresh_run_after_the_last_one_finished(runlog_engine):
    with runlog_engine.begin() as conn:
        first = find_or_create_active_run(conn, pipeline_id=1)
        conn.execute(
            text("UPDATE AUD_PIPELINES_RUN_LOG SET STATUS = 'SUCCESS' WHERE PIPELINE_RUN_ID = :id"),
            {"id": first},
        )
    with runlog_engine.begin() as conn:
        second = find_or_create_active_run(conn, pipeline_id=1)
    assert second != first


def test_resolve_run_for_task_binds_to_active_run(runlog_engine):
    with runlog_engine.begin() as conn:
        active = find_or_create_active_run(conn, pipeline_id=1)
    with runlog_engine.begin() as conn:
        resolved = resolve_run_for_task(conn, pipeline_id=1)
    assert resolved == active


def test_resolve_run_for_task_falls_back_to_latest_logged_run(runlog_engine):
    with runlog_engine.begin() as conn:
        run_id = find_or_create_active_run(conn, pipeline_id=1)
        conn.execute(
            text("UPDATE AUD_PIPELINES_RUN_LOG SET STATUS = 'SUCCESS' WHERE PIPELINE_RUN_ID = :id"),
            {"id": run_id},
        )
    with runlog_engine.begin() as conn:
        resolved = resolve_run_for_task(conn, pipeline_id=1)
    assert resolved == run_id
    with runlog_engine.connect() as conn:
        row = conn.execute(
            text("SELECT STATUS, END_DATE FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID = :id"),
            {"id": run_id},
        ).one()
    assert row.STATUS == "SUCCESS"  # status is left alone, not reopened
    assert row.END_DATE is not None  # but the date is touched


def test_resolve_run_for_task_raises_when_pipeline_never_ran(runlog_engine):
    with runlog_engine.begin() as conn, pytest.raises(RunLogError):
        resolve_run_for_task(conn, pipeline_id=999)


def test_find_or_create_task_run_creates_then_reuses_binding(runlog_engine):
    with runlog_engine.begin() as conn:
        run_id = find_or_create_active_run(conn, pipeline_id=1)
        first = find_or_create_task_run(conn, task_id=10, pipeline_run_id=run_id)
    assert first.status == "IN-PROGRESS"
    with runlog_engine.begin() as conn:
        second = find_or_create_task_run(conn, task_id=10, pipeline_run_id=run_id)
    assert second.task_run_id == first.task_run_id
    assert second.status == "IN-PROGRESS"


def test_find_or_create_task_run_reflects_updated_status(runlog_engine):
    with runlog_engine.begin() as conn:
        run_id = find_or_create_active_run(conn, pipeline_id=1)
        binding = find_or_create_task_run(conn, task_id=10, pipeline_run_id=run_id)
        update_task_run(conn, binding.task_run_id, status="SUCCESS", target_count=42)
    with runlog_engine.begin() as conn:
        rebound = find_or_create_task_run(conn, task_id=10, pipeline_run_id=run_id)
    assert rebound.task_run_id == binding.task_run_id
    assert rebound.status == "SUCCESS"
    with runlog_engine.connect() as conn:
        row = conn.execute(
            text("SELECT TARGET_COUNT, END_DATE FROM AUD_TASK_RUN_LOG WHERE TASK_RUN_ID = :id"),
            {"id": binding.task_run_id},
        ).one()
    assert row.TARGET_COUNT == 42
    assert row.END_DATE is not None


def test_update_task_run_leaves_unspecified_counts_untouched(runlog_engine):
    with runlog_engine.begin() as conn:
        run_id = find_or_create_active_run(conn, pipeline_id=1)
        binding = find_or_create_task_run(conn, task_id=10, pipeline_run_id=run_id)
        update_task_run(conn, binding.task_run_id, status="IN-PROGRESS", source_count=100)
        update_task_run(conn, binding.task_run_id, status="SUCCESS", target_count=99)
    with runlog_engine.connect() as conn:
        row = conn.execute(
            text("SELECT SOURCE_COUNT, TARGET_COUNT FROM AUD_TASK_RUN_LOG WHERE TASK_RUN_ID = :id"),
            {"id": binding.task_run_id},
        ).one()
    assert row.SOURCE_COUNT == 100  # untouched by the second call
    assert row.TARGET_COUNT == 99


def test_find_or_create_task_run_is_independent_per_task(runlog_engine):
    with runlog_engine.begin() as conn:
        run_id = find_or_create_active_run(conn, pipeline_id=1)
        a = find_or_create_task_run(conn, task_id=10, pipeline_run_id=run_id)
        b = find_or_create_task_run(conn, task_id=11, pipeline_run_id=run_id)
    assert a.task_run_id != b.task_run_id
