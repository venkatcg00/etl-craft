"""The default, no-Docker-needed test suite: pure logic plus SQLite stand-ins.

Everything here runs with plain `pytest -q` — no live Postgres required.
Real-Postgres integration tests (which need Docker; see the Makefile) live
in test_integration.py instead. Organized by source module, one section
per module, since combining them loses nothing (no fixture/helper name
collisions) and keeps file count down.
"""

import contextlib
import runpy
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

import etl_craft
import etl_craft.sql_actions as sql_actions
import etl_craft.warehouse as warehouse_module
from etl_craft.cfg import (
    CrossPipelineTaskEdge,
    PipelineDependencyEdge,
    PipelineStep,
    PipelineSummary,
    TaskStatusEntry,
    suggest,
)
from etl_craft.cli import main as cli_main
from etl_craft.cloning import AUD_TABLES, CFG_TABLES, tables_for_scope
from etl_craft.cloning import _same_database as same_database
from etl_craft.column_lineage import extract_column_lineage, source_sql_hash
from etl_craft.config import (
    CONFIG_PATH_ENV_VAR,
    DEFAULT_MAX_PARALLEL_TASKS,
    DEFAULT_TASK_TIMEOUT_SECONDS,
    CloningConfig,
    ConfigError,
    ConnectionProfile,
    ConnectionSection,
    ConnectorConfig,
    ExecutionLimits,
    SourceConfig,
    _load_dotenv_file,
    load_config,
    resolve_config_path,
    resolve_secret,
)
from etl_craft.configure import (
    _report_required_secrets,
    _required_secret_vars,
    configure_from_env,
    set_execution_mode,
)
from etl_craft.crosspipe import MIN_POLL_INTERVAL_SECONDS, _default_now, _next_poll_delay
from etl_craft.db import AUTH_REGISTRY, ConnectionError_, build_engine, parse_jdbc_postgres
from etl_craft.docs_generator import (
    PipelineDocData,
    build_search_index,
)
from etl_craft.docs_generator import _render_index_html as render_docs_index_html
from etl_craft.docs_generator import _render_pipeline_html as render_docs_pipeline_html
from etl_craft.documentation import documentation_hash
from etl_craft.email_alert import _PipelineDigestEntry, run_flavour
from etl_craft.email_alert import _render_digest_html as render_email_digest_html
from etl_craft.email_alert import _resolve_target_pipeline_codes as resolve_email_pipeline_codes
from etl_craft.email_alert import _substitute as substitute_email_tokens
from etl_craft.handlers import HandlerError, TaskExecutionContext, dispatch
from etl_craft.limits import task_timeout_seconds
from etl_craft.migrate import (
    MIGRATIONS_DIR_ENV_VAR,
    _split_statements,
    resolve_migrations_dir,
    split_statements,
)
from etl_craft.packaged_sql import (
    packaged_migrations_dir,
    packaged_schema_path,
    read_packaged_schema,
)
from etl_craft.resolver import (
    CycleError,
    DependencyGraph,
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
    begin_attempt,
    fetch_run_state,
    find_or_create_active_run,
    find_or_create_task_run,
    resolve_run_for_task,
    update_task_run,
)
from etl_craft.scripts import _parse_trailing_json
from etl_craft.scripts import execute as execute_python_script
from etl_craft.sql_actions import active_database, qualify, substitute_pipeline_id
from etl_craft.validate import looks_read_only
from etl_craft.warehouse import (
    WAREHOUSE_AUTH_REGISTRY,
    build_data_engine,
    translate_jdbc_url,
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


def test_build_graph_rejects_duplicate_task_ids():
    with pytest.raises(ResolverError):
        build_graph(nodes(1, 1), [])


def test_build_graph_rejects_edge_with_unknown_task_id():
    # Distinct from test_unknown_task_id_in_edge_rejected: that one has an
    # unknown depends_on_task_id with a valid task_id; this is the other
    # side — task_id itself doesn't exist in the supplied task set.
    with pytest.raises(UnknownTaskError):
        build_graph(nodes(1, 2), [edge(99, 1)])


# ------------------------------------------------------------------------------
# E2-41 — RUN_CONDITION (ALL / ANY / N) cardinality on a task's own edges
# ------------------------------------------------------------------------------


def test_run_condition_defaults_to_all_when_unset():
    # Every CFG_TASKS row predating E2-41 has RUN_CONDITION NULL, and must
    # behave exactly as it did before: all edges, or nothing runs.
    graph = build_graph(nodes(1, 2, 3), [edge(3, 1), edge(3, 2)])
    assert graph.required_edge_count(3) == 2
    assert 3 not in graph.ready({1: TaskRunState(status="SUCCESS")})
    assert 3 in graph.ready({1: TaskRunState(status="SUCCESS"), 2: TaskRunState(status="SUCCESS")})


def test_run_condition_any_runs_once_a_single_edge_is_satisfied():
    # The literal case that motivated E2-41: "a task is dependent on 10 tasks
    # but it can run at least one meets the condition".
    tasks = [TaskNode(task_id=i) for i in (1, 2, 3)] + [TaskNode(task_id=4, run_condition="ANY")]
    graph = build_graph(tasks, [edge(4, 1), edge(4, 2), edge(4, 3)])
    assert graph.required_edge_count(4) == 1
    assert 4 in graph.ready({1: TaskRunState(status="SUCCESS")})


def test_run_condition_n_requires_that_many_edges():
    tasks = [TaskNode(task_id=i) for i in (1, 2, 3)] + [
        TaskNode(task_id=4, run_condition="N", run_condition_count=2)
    ]
    graph = build_graph(tasks, [edge(4, 1), edge(4, 2), edge(4, 3)])
    assert graph.required_edge_count(4) == 2
    assert 4 not in graph.ready({1: TaskRunState(status="SUCCESS")})
    assert 4 in graph.ready({1: TaskRunState(status="SUCCESS"), 2: TaskRunState(status="SUCCESS")})


def test_build_graph_rejects_an_n_count_larger_than_the_edge_count():
    # Beyond what any CHECK constraint can express — it compares a CFG_TASKS
    # column against a count of CFG_TASK_DEPENDENCY rows. `validate` surfaces
    # it through validate_graphs, which is where a two-table config error
    # belongs.
    tasks = [TaskNode(task_id=1), TaskNode(task_id=2, run_condition="N", run_condition_count=3)]
    with pytest.raises(ResolverError, match="could never run"):
        build_graph(tasks, [edge(2, 1)])


@pytest.mark.parametrize(
    "condition, count",
    [("BOGUS", None), ("N", None), ("N", 0), ("ALL", 2), (None, 2)],
)
def test_build_graph_rejects_malformed_run_conditions(condition, count):
    tasks = [
        TaskNode(task_id=1),
        TaskNode(task_id=2, run_condition=condition, run_condition_count=count),
    ]
    with pytest.raises(ResolverError):
        build_graph(tasks, [edge(2, 1)])


def test_run_condition_counts_cross_pipeline_edges_too():
    # E2-44 regression. fetch_pipeline_graph deliberately keeps cross-pipeline
    # edges out of the graph, so the N guard used to compare the declared count
    # against the same-pipeline edges alone and reject a valid config outright
    # -- "requires 2 satisfied dependencies but only has 1". RUN_CONDITION
    # ranges over all of a task's dependencies.
    tasks = [
        TaskNode(task_id=1),
        TaskNode(task_id=2, run_condition="N", run_condition_count=2, cross_pipeline_edge_count=1),
    ]
    graph = build_graph(tasks, [edge(2, 1)])
    assert graph.total_edge_count(2) == 2
    assert graph.required_edge_count(2) == 2


def test_run_condition_any_is_satisfied_by_a_same_pipeline_edge_alone():
    # E2-45 regression, the pure half. ready() applied the cardinality to
    # same-pipeline edges while run_task separately required *every*
    # cross-pipeline edge, so 'ANY' was really 'ANY and ALL'. One satisfied
    # edge is now enough whichever half it comes from.
    tasks = [
        TaskNode(task_id=1),
        TaskNode(task_id=2, run_condition="ANY", cross_pipeline_edge_count=1),
    ]
    graph = build_graph(tasks, [edge(2, 1)])
    state = {1: TaskRunState(status="SUCCESS")}
    # cross_pipeline_satisfied={2: 0} -- not one cross-pipeline edge satisfied.
    assert graph.satisfied_edge_count(2, state, 0) == 1
    assert 2 in graph.ready(state, {2: 0})


def test_ready_treats_unevaluated_cross_pipeline_edges_optimistically():
    # The orchestrator's wave pre-filter passes no cross-pipeline counts: the
    # real gate runs inside the spawned `run --task_code` subprocess. Treating
    # them pessimistically here would deadlock -- the task would never be
    # spawned, so the check that settles it would never run.
    tasks = [TaskNode(task_id=1), TaskNode(task_id=2, cross_pipeline_edge_count=1)]
    graph = build_graph(tasks, [edge(2, 1)])
    state = {1: TaskRunState(status="SUCCESS")}
    assert 2 in graph.ready(state)
    assert 2 not in graph.ready(state, {2: 0})


def test_unsatisfiable_counts_an_unevaluated_cross_pipeline_edge_as_still_possible():
    # This module cannot see AUD_*_DEPENDENCY_TRACKER, so a cross-pipeline edge
    # must always count as "could still be satisfied". Under ANY that is enough
    # to keep the task alive even though its one same-pipeline edge is doomed.
    tasks = [
        TaskNode(task_id=1),
        TaskNode(task_id=2, run_condition="ANY", cross_pipeline_edge_count=1),
    ]
    graph = build_graph(tasks, [edge(2, 1, dependency_type="FAILURE")])
    assert graph.unsatisfiable({1: TaskRunState(status="SUCCESS")}) == []


def test_unsatisfiable_still_dooms_an_all_task_whose_same_pipeline_edge_is_hopeless():
    # The other side of the same boundary: under ALL every edge is required, so
    # one hopeless same-pipeline edge dooms the task no matter what the
    # cross-pipeline half might eventually do.
    tasks = [TaskNode(task_id=1), TaskNode(task_id=2, cross_pipeline_edge_count=1)]
    graph = build_graph(tasks, [edge(2, 1, dependency_type="FAILURE")])
    assert graph.unsatisfiable({1: TaskRunState(status="SUCCESS")}) == [2]


# ------------------------------------------------------------------------------
# E2-01 — unsatisfiable(): tasks that can never become ready under this run
# ------------------------------------------------------------------------------


def test_unsatisfiable_flags_a_failure_edge_whose_upstream_succeeded():
    # The recommended EMAIL_ALERT pattern, on a run where nothing failed.
    graph = build_graph(nodes(1, 2), [edge(2, 1, dependency_type="FAILURE")])
    assert graph.unsatisfiable({1: TaskRunState(status="SUCCESS")}) == [2]


def test_unsatisfiable_ignores_an_upstream_that_merely_failed():
    # FAILED stays retry-eligible — "retry resumes" — so a SUCCESS edge on it
    # is "not yet", not "never". Converting this into a skip would report a
    # broken run as SUCCESS.
    graph = build_graph(nodes(1, 2), [edge(2, 1)])
    assert graph.unsatisfiable({1: TaskRunState(status="FAILED")}) == []


def test_unsatisfiable_ignores_an_upstream_still_in_progress():
    graph = build_graph(nodes(1, 2), [edge(2, 1)])
    assert graph.unsatisfiable({1: TaskRunState(status="IN-PROGRESS")}) == []


def test_unsatisfiable_flags_a_has_data_edge_whose_upstream_wrote_nothing():
    graph = build_graph(nodes(1, 2), [edge(2, 1, dependency_type="HAS_DATA")])
    state = {1: TaskRunState(status="SUCCESS", target_count=0)}
    assert graph.unsatisfiable(state) == [2]


def test_unsatisfiable_cascades_to_a_fixpoint():
    # 1 SUCCESS -> 2 (FAILURE) can never run -> 3 (SUCCESS on 2) can never
    # run either, once 2 is settled SKIPPED. One pass would only find 2.
    graph = build_graph(nodes(1, 2, 3), [edge(2, 1, dependency_type="FAILURE"), edge(3, 2)])
    assert graph.unsatisfiable({1: TaskRunState(status="SUCCESS")}) == [2, 3]


def test_unsatisfiable_does_not_cascade_through_an_always_edge():
    # An ALWAYS edge is satisfied by the SKIPPED that 2 is about to get, so 3
    # is genuinely still runnable — the cascade must not over-reach.
    graph = build_graph(
        nodes(1, 2, 3),
        [edge(2, 1, dependency_type="FAILURE"), edge(3, 2, dependency_type="ALWAYS")],
    )
    assert graph.unsatisfiable({1: TaskRunState(status="SUCCESS")}) == [2]


def test_unsatisfiable_under_any_needs_every_edge_to_be_hopeless():
    tasks = [TaskNode(task_id=1), TaskNode(task_id=2)] + [TaskNode(task_id=3, run_condition="ANY")]
    graph = build_graph(
        tasks,
        [edge(3, 1, dependency_type="FAILURE"), edge(3, 2, dependency_type="FAILURE")],
    )
    # One upstream succeeded (that edge is hopeless), the other hasn't run.
    assert graph.unsatisfiable({1: TaskRunState(status="SUCCESS")}) == []
    assert graph.unsatisfiable(
        {1: TaskRunState(status="SUCCESS"), 2: TaskRunState(status="SUCCESS")}
    ) == [3]


def test_unsatisfiable_leaves_tasks_that_already_have_a_row_alone():
    # Only never-run tasks are reported. A task that already ran owns its own
    # status, whatever it is.
    graph = build_graph(nodes(1, 2), [edge(2, 1, dependency_type="FAILURE")])
    state = {1: TaskRunState(status="SUCCESS"), 2: TaskRunState(status="FAILED")}
    assert graph.unsatisfiable(state) == []


def test_check_acyclic_handles_a_chain_deeper_than_the_recursion_limit():
    # E2-37: the recursive version raised RecursionError — not ResolverError —
    # turning a config problem into what reads like an engine bug.
    depth = sys.getrecursionlimit() + 500
    ids = list(range(1, depth + 1))
    chain = [edge(i + 1, i) for i in range(1, depth)]
    graph = build_graph(nodes(*ids), chain)
    assert len(graph.waves()) == depth


def test_check_acyclic_skips_a_root_already_visited_as_a_descendant():
    # Task ids carry no ordering guarantee relative to the dependency
    # direction — 1 depending on 2 is perfectly ordinary. Walking roots in id
    # order then reaches 2 as a descendant of 1 first, so the outer loop must
    # skip it rather than restart the traversal.
    graph = build_graph(nodes(1, 2), [edge(1, 2)])
    assert graph.waves() == [[2], [1]]


def test_check_acyclic_still_finds_a_cycle_at_depth():
    depth = 2000
    ids = list(range(1, depth + 1))
    chain = [edge(i + 1, i) for i in range(1, depth)]
    chain.append(edge(1, depth))
    with pytest.raises(CycleError):
        build_graph(nodes(*ids), chain)


def test_edge_satisfied_rejects_unknown_dependency_type():
    # build_graph already validates dependency_type before a DependencyGraph
    # is ever constructed, so this path is unreachable through the public
    # API — exercised directly against the underlying building blocks instead.
    with pytest.raises(ResolverError):
        DependencyGraph._edge_satisfied(edge(2, 1, dependency_type="BOGUS"), TaskRunState())


# ------------------------------------------------------------------------------
# sql_actions.py — dialect portability (E2-31)
# ------------------------------------------------------------------------------


def test_hash_expression_uses_ansi_cast_not_postgres_shorthand():
    # E2-31. `::text` is Postgres-only shorthand; this module uses ANSI CAST
    # everywhere else and explains each exception it makes.
    expr = sql_actions._hash_expression(["a", "b"], "s", "postgresql")
    assert "::text" not in expr
    assert "COALESCE(CAST(s.a AS VARCHAR), '')" in expr
    assert expr.startswith("MD5(")


def test_hash_expression_hexes_the_result_on_clickhouse():
    # E2-31. Verified directly against the local ClickHouse before writing
    # this: its MD5() returns FixedString(16) — raw bytes — where Postgres's
    # returns 32 hex characters, so HASH_KEY VARCHAR(32) was simply wrong
    # there. hex() brings it back to the shape every other dialect produces.
    expr = sql_actions._hash_expression(["a"], "s", "clickhouse")
    assert expr.startswith("lower(hex(MD5(")
    # And a different cast target: CAST(col AS VARCHAR) raises
    # CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN on a nullable ClickHouse column the
    # moment any value is NULL, with the COALESCE unable to rescue it.
    assert "CAST(s.a AS Nullable(String))" in expr


def test_apply_primary_key_is_a_no_op_on_clickhouse():
    # ClickHouse has no ALTER TABLE ... ADD PRIMARY KEY — ordering is a
    # table-engine property fixed at CREATE time. Skipped rather than failed:
    # the convention exists so `validate` can introspect it, and validate
    # reports what it actually finds.
    class _Conn:
        dialect = SimpleNamespace(name="clickhouse")

        def execute(self, *_args, **_kwargs):  # pragma: no cover - must not run
            raise AssertionError("issued DDL ClickHouse cannot accept")

    sql_actions._apply_primary_key(_Conn(), "public.t", "db", ["id"])


def test_apply_primary_key_does_nothing_when_none_is_declared():
    class _Conn:
        dialect = SimpleNamespace(name="postgresql")

        def execute(self, *_args, **_kwargs):  # pragma: no cover - must not run
            raise AssertionError("issued DDL for a target with no PRIMARY_KEY")

    sql_actions._apply_primary_key(_Conn(), "public.t", "db", [])


# ------------------------------------------------------------------------------
# column_lineage.py — sqlglot-parsed column lineage
# ------------------------------------------------------------------------------


def _edges(sql, target="public.t"):
    result = extract_column_lineage(sql, target)
    assert result.ok, result.error
    return {e.target_column: e for e in result.edges}


def test_column_lineage_resolves_aliased_join_columns():
    edges = _edges(
        "SELECT a.id AS cust_id, b.name AS cust_name "
        "FROM raw.customers a JOIN raw.names b ON a.id = b.id"
    )
    assert (edges["cust_id"].source_object, edges["cust_id"].source_column) == (
        "raw.customers",
        "id",
    )
    assert (edges["cust_name"].source_object, edges["cust_name"].source_column) == (
        "raw.names",
        "name",
    )
    # A plain pass-through records no transformation -- that field is for
    # telling "this column IS that column" from "this column is derived".
    assert edges["cust_id"].transformation is None


def test_column_lineage_records_the_expression_for_a_derived_column():
    edges = _edges("SELECT UPPER(name) AS shouty FROM raw.customers")
    assert edges["shouty"].source_object == "raw.customers"
    assert edges["shouty"].source_column == "name"
    assert "UPPER" in (edges["shouty"].transformation or "")


def test_column_lineage_leaves_a_literal_unattributed():
    # Naming a source for a constant would be fabrication; the expression is
    # the honest answer.
    edges = _edges("SELECT 1 AS flag FROM raw.customers")
    assert edges["flag"].source_object is None
    assert edges["flag"].source_column is None
    assert edges["flag"].transformation == "1"


def test_column_lineage_resolves_through_a_cte():
    # Lineage that stops at the CTE name is much less useful than lineage that
    # reaches the real table, and CTEs are everywhere in ETL SQL.
    edges = _edges(
        "WITH recent AS (SELECT id, updated_at FROM raw.orders) "
        "SELECT r.id AS order_id FROM recent r"
    )
    assert edges["order_id"].source_object == "raw.orders"


def test_column_lineage_resolves_through_chained_ctes():
    edges = _edges(
        "WITH a AS (SELECT id FROM raw.src), b AS (SELECT id FROM a) SELECT b.id AS x FROM b"
    )
    assert edges["x"].source_object == "raw.src"


def test_column_lineage_does_not_guess_when_a_cte_reads_two_tables():
    # Two candidate sources and no basis to choose: it stops at the CTE rather
    # than naming one of them.
    edges = _edges(
        "WITH many AS (SELECT p.id FROM raw.one p JOIN raw.two q ON p.id = q.id) "
        "SELECT m.id AS y FROM many m"
    )
    assert edges["y"].source_object == "many"


def test_column_lineage_reports_a_parse_failure_instead_of_raising():
    # One unparseable task should cost that task's column lineage, not the
    # whole `lineage` command or a whole documentation build.
    result = extract_column_lineage("this is not sql at all ((", "public.t")
    assert not result.ok
    assert "could not parse" in (result.error or "")
    assert result.edges == []


def test_column_lineage_skips_an_unexpanded_star():
    # Without a schema sqlglot cannot expand `*`, and inventing column names
    # would be fabrication. Reported as no edges, not as a wrong answer.
    result = extract_column_lineage("SELECT * FROM raw.customers", "public.t")
    assert result.ok
    assert result.edges == []


def test_column_lineage_leaves_a_multi_column_expression_unattributed():
    # Two candidate sources for one target column: naming one would be a
    # guess, so the expression is kept instead.
    edges = _edges("SELECT a || b AS joined FROM raw.t")
    assert edges["joined"].source_object is None
    assert edges["joined"].source_column is None
    assert edges["joined"].transformation is not None


def test_column_lineage_rejects_a_statement_that_is_not_a_select():
    result = extract_column_lineage("DELETE FROM raw.customers", "public.t")
    assert not result.ok
    assert "not a SELECT" in (result.error or "")


def test_column_lineage_attributes_an_unqualified_column_in_a_single_table_query():
    edges = _edges("SELECT id, name FROM raw.customers")
    assert edges["id"].source_object == "raw.customers"


def test_column_lineage_handles_a_table_with_no_schema_prefix():
    edges = _edges("SELECT t.id AS x FROM customers t")
    assert edges["x"].source_object == "customers"


def test_source_sql_hash_changes_with_the_sql():
    # The cache key. A cached row is reused only when it came from byte-for-byte
    # the SQL in CFG_TASK_PARAMETERS right now, so an edit invalidates it.
    assert source_sql_hash("SELECT 1") == source_sql_hash("SELECT 1")
    assert source_sql_hash("SELECT 1") != source_sql_hash("SELECT 2")


# ------------------------------------------------------------------------------
# documentation.py — content-hash versioning
# ------------------------------------------------------------------------------


def test_documentation_hash_ignores_surrounding_whitespace():
    # Re-indenting a docstring is not a new version.
    assert documentation_hash("  text  ") == documentation_hash("text")
    assert documentation_hash("text") != documentation_hash("other text")


# ------------------------------------------------------------------------------
# cfg.suggest — "did you mean" for an unknown code
# ------------------------------------------------------------------------------


def test_suggest_finds_a_near_miss_and_a_prefix():
    assert suggest("CUSTOMER", ["CUSTOMERS", "CUSTOMERS_DAILY", "ORDERS"]) == [
        "CUSTOMERS",
        "CUSTOMERS_DAILY",
    ]
    assert suggest("custmers", ["CUSTOMERS", "ORDERS"]) == ["CUSTOMERS"]


def test_suggest_is_case_insensitive_but_returns_the_real_code():
    # CODE-style identifiers get typed in the wrong case routinely.
    assert suggest("customers", ["CUSTOMERS"]) == ["CUSTOMERS"]


def test_suggest_says_nothing_rather_than_guessing_wildly():
    assert suggest("zzzzzz", ["CUSTOMERS", "ORDERS"]) == []


# ------------------------------------------------------------------------------
# E2-17 / E2-19 / E2-34 — limits, and stricter config parsing
# ------------------------------------------------------------------------------


_MINIMAL_YAML = """
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


def _config_with(tmp_path, execution_extra=""):
    path = tmp_path / "craft-connector.yml"
    path.write_text(_MINIMAL_YAML.replace("  Mode: local", "  Mode: local" + execution_extra))
    return load_config(path)


def _ctx_with(params, limits=None):
    return SimpleNamespace(
        task_params=params,
        config=SimpleNamespace(limits=limits or ExecutionLimits()),
    )


def test_task_timeout_prefers_the_task_parameter_then_the_global_default():
    assert task_timeout_seconds(_ctx_with({})) == DEFAULT_TASK_TIMEOUT_SECONDS
    assert task_timeout_seconds(_ctx_with({}, ExecutionLimits(task_timeout_seconds=120))) == 120
    assert task_timeout_seconds(_ctx_with({"TASK_TIMEOUT_SECONDS": "45"})) == 45


def test_task_timeout_zero_disables_it():
    # For a task that legitimately runs longer than any sensible global bound.
    assert task_timeout_seconds(_ctx_with({"TASK_TIMEOUT_SECONDS": "0"})) == 0


@pytest.mark.parametrize("bad", ["soon", "-5"])
def test_task_timeout_rejects_a_value_that_is_not_a_non_negative_number(bad):
    with pytest.raises(HandlerError):
        task_timeout_seconds(_ctx_with({"TASK_TIMEOUT_SECONDS": bad}))


def test_execution_limits_have_real_defaults(tmp_path):
    # A limit nobody sets is a limit nobody benefits from, so these default to
    # real values rather than None.
    config = _config_with(tmp_path)
    assert config.limits.task_timeout_seconds == DEFAULT_TASK_TIMEOUT_SECONDS
    assert config.limits.max_parallel_tasks == DEFAULT_MAX_PARALLEL_TASKS
    assert config.limits.enforce_sla is False


def test_execution_limits_are_read_from_the_config(tmp_path):
    config = _config_with(tmp_path, "\n  Task_timeout_seconds: 900\n  Max_parallel_tasks: 3")
    assert config.limits.task_timeout_seconds == 900
    assert config.limits.max_parallel_tasks == 3


@pytest.mark.parametrize("bad", ['"three"', "-1"])
def test_execution_limits_reject_a_non_numeric_value(tmp_path, bad):
    # E2-34: [Execution]/[Orchestrator] scalars were taken straight from
    # raw.get(...) with no type check, so `Retries: "three"` flowed unexamined
    # into the generated YAML.
    with pytest.raises(ConfigError):
        _config_with(tmp_path, f"\n  Max_parallel_tasks: {bad}")


# ------------------------------------------------------------------------------
# validate.py — the read-only SQL lint (E2-07)
# ------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT a, b FROM t WHERE 1=1",
        "  select 1 ",
        "WITH x AS (SELECT 1) SELECT * FROM x",
        # A column named like a keyword, and a keyword inside a literal, must
        # not trip it -- literals and comments are stripped before matching.
        "SELECT update_date, created_by FROM t",
        "SELECT 'DROP TABLE t' AS warning FROM t",
        "SELECT a FROM t -- DELETE this later",
    ],
)
def test_looks_read_only_accepts_genuine_selects(sql):
    assert looks_read_only(sql) is None


@pytest.mark.parametrize(
    "sql, fragment",
    [
        # The case CLAUDE.md's "a step cannot touch the warehouse outside its
        # declared action" is meant to prevent, and which nothing checked: a
        # data-modifying CTE is a perfectly valid "SELECT" that writes.
        ("WITH x AS (DELETE FROM other RETURNING *) SELECT * FROM x", "DELETE"),
        ("INSERT INTO t VALUES (1)", "not SELECT/WITH"),
        ("SELECT 1; DROP TABLE t", "DROP"),
        ("TRUNCATE TABLE t", "not SELECT/WITH"),
        ("   ", "is empty"),
        ("-- only a comment", "is empty"),
    ],
)
def test_looks_read_only_rejects_statements_that_write(sql, fragment):
    reason = looks_read_only(sql)
    assert reason is not None
    assert fragment in reason


# ------------------------------------------------------------------------------
# migrate.py / packaged_sql.py / config.py — the install path (E2-05, E2-06, E2-13)
# ------------------------------------------------------------------------------


def test_split_statements_keeps_dollar_quoted_bodies_whole():
    # E2-05. This used to be sql_text.split(";"), which shredded exactly the
    # thing anyone would migrate first: schema.sql's own trigger functions are
    # CREATE FUNCTION ... $$ ... ; ... $$ bodies.
    sql = (
        "CREATE FUNCTION f() RETURNS TRIGGER AS $$ BEGIN "
        "NEW.a := 1; NEW.b := 2; RETURN NEW; END; $$ LANGUAGE plpgsql;\n"
        "SELECT 1;"
    )
    statements = split_statements(sql)
    assert len(statements) == 2
    assert statements[0].startswith("CREATE FUNCTION")
    assert statements[0].count(";") == 4  # every semicolon inside the body survived
    assert statements[1] == "SELECT 1"


def test_split_statements_ignores_semicolons_in_literals_and_comments():
    sql = (
        "INSERT INTO t VALUES ('a;b', 'it''s; fine');\n"
        "-- a trailing comment; with a semicolon\n"
        "/* and a block; comment */\n"
        "SELECT 2;"
    )
    statements = split_statements(sql)
    assert len(statements) == 2
    assert "'a;b'" in statements[0]
    assert statements[1].endswith("SELECT 2")


def test_split_statements_handles_tagged_dollar_quotes():
    sql = "DO $fn$ BEGIN RAISE NOTICE 'x;y'; END $fn$;\nSELECT 3;"
    assert len(split_statements(sql)) == 2


def test_the_real_packaged_schema_splits_into_whole_statements():
    # The case init-db actually depends on, against the file that really ships.
    statements = split_statements(read_packaged_schema())
    functions = [s for s in statements if "CREATE OR REPLACE FUNCTION" in s]
    assert functions, "schema.sql should define trigger functions"
    assert all(s.rstrip().endswith("plpgsql") for s in functions)


def test_packaged_sql_files_are_readable_from_the_installed_package():
    # E2-13. The wheel used to ship 24 .py files and nothing else, so there was
    # no way to create the Engine DB from an installed package at all.
    assert packaged_schema_path().is_file()
    assert packaged_migrations_dir().is_dir()
    assert "CREATE TABLE CFG_PIPELINES" in read_packaged_schema()


def test_resolve_migrations_dir_precedence(tmp_path, monkeypatch):
    explicit = tmp_path / "explicit"
    from_env = tmp_path / "from_env"
    local = tmp_path / "project" / "sql" / "migrations"
    local.mkdir(parents=True)

    monkeypatch.setenv(MIGRATIONS_DIR_ENV_VAR, str(from_env))
    assert resolve_migrations_dir(explicit) == explicit
    assert resolve_migrations_dir() == from_env

    monkeypatch.delenv(MIGRATIONS_DIR_ENV_VAR)
    monkeypatch.chdir(tmp_path / "project")
    assert resolve_migrations_dir() == local

    # Nothing local either: fall back to the copy inside the package, which is
    # what makes `migrate` work from an installed wheel at all.
    monkeypatch.chdir(tmp_path)
    assert resolve_migrations_dir() == packaged_migrations_dir()


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("SELECT $", 1),  # a lone $ is not a dollar quote
        ("SELECT $1 + $2", 1),  # $1 is a placeholder, not a tag
        ("SELECT $a-b$ x $a-b$", 1),  # not a valid tag either
    ],
)
def test_split_statements_does_not_mistake_other_dollars_for_quotes(sql, expected):
    assert len(split_statements(sql)) == expected


def test_required_secret_vars_names_each_profiles_variable():
    # E2-16. The whole point: the name is derivable from what the user just
    # entered, so there is no reason to make them discover it from a failure.
    raw = {
        "Postgres": {"Active_profile": "dev", "Profiles": {"dev": {"auth_mode": "password"}}},
        "Warehouse": {
            "Active_profile": "prod",
            "Profiles": {"prod": {"auth_mode": "password", "secret_var": "MY_OWN_VAR"}},
        },
        # auth_mode none needs no secret, so it must not be listed.
        "Email": {"Active_profile": "dev", "Profiles": {"dev": {"auth_mode": "none"}}},
    }
    assert _required_secret_vars(raw) == [
        ("Postgres.dev", "ETL_CRAFT_POSTGRES_DEV_SECRET"),
        ("Warehouse.prod", "MY_OWN_VAR"),
    ]


def test_required_secret_vars_ignores_malformed_sections():
    assert _required_secret_vars({"Postgres": "not-a-dict"}) == []
    assert _required_secret_vars({"Postgres": {"Active_profile": "dev", "Profiles": {}}}) == []


def test_report_required_secrets_points_at_the_env_file_when_source_is_a_file():
    printed: list[str] = []
    raw = {
        "Source": {"Type": "file", "Path": "/etc/secrets.env"},
        "Postgres": {"Active_profile": "dev", "Profiles": {"dev": {"auth_mode": "password"}}},
    }
    _report_required_secrets(raw, printed.append)
    out = "\n".join(printed)
    assert "ETL_CRAFT_POSTGRES_DEV_SECRET" in out
    assert "/etc/secrets.env" in out
    assert "doctor" in out


def test_report_required_secrets_says_nothing_when_nothing_is_needed():
    printed: list[str] = []
    _report_required_secrets({"Source": {"Type": "environment"}}, printed.append)
    assert printed == []


def test_resolve_config_path_precedence(tmp_path, monkeypatch):
    # E2-06. There was no --config and no env var, so every command had to run
    # with cwd set to the directory holding the file -- a poor fit for an
    # Airflow BashOperator, whose cwd a DAG author does not control.
    explicit = tmp_path / "explicit.yml"
    from_env = tmp_path / "env.yml"
    project = tmp_path / "project"
    nested = project / "a" / "b"
    nested.mkdir(parents=True)
    (project / "craft-connector.yml").write_text("Execution:\n  Mode: local\n")

    monkeypatch.setenv(CONFIG_PATH_ENV_VAR, str(from_env))
    assert resolve_config_path(explicit) == explicit
    assert resolve_config_path() == from_env

    monkeypatch.delenv(CONFIG_PATH_ENV_VAR)
    # Upward search from a subdirectory, the pyproject.toml/.git pattern.
    monkeypatch.chdir(nested)
    assert resolve_config_path() == project / "craft-connector.yml"


def test_resolve_config_path_falls_back_to_the_plain_relative_name(tmp_path, monkeypatch):
    # Nothing found anywhere: the "not found" error should still name something
    # a reader recognizes, not an absolute path from a failed search.
    monkeypatch.delenv(CONFIG_PATH_ENV_VAR, raising=False)
    monkeypatch.chdir(tmp_path)
    assert resolve_config_path() == Path("craft-connector.yml")


# ==============================================================================
# crosspipe.py — pure poll-interval math, no DB at all
# ==============================================================================


def test_next_poll_delay_targets_the_fraction_of_average_duration():
    # avg=300s, fraction=0.70 (first poll) -> target 210s; nothing elapsed
    # yet, so the full 210s is owed (well under the huge remaining deadline).
    delay = _next_poll_delay(300.0, 0.0, 0.70, remaining_deadline=3600.0)
    assert delay == 210.0


def test_next_poll_delay_accounts_for_already_elapsed_time():
    # Same target (210s) but 150s has already passed -> only 60s left to wait.
    delay = _next_poll_delay(300.0, 150.0, 0.70, remaining_deadline=3600.0)
    assert delay == 60.0


def test_next_poll_delay_floors_at_minimum_when_already_overdue():
    # Already past the target fraction entirely -> don't return 0 or
    # negative (which would hot-loop with no real delay between checks).
    delay = _next_poll_delay(300.0, 1000.0, 0.70, remaining_deadline=3600.0)
    assert delay == MIN_POLL_INTERVAL_SECONDS


def test_next_poll_delay_never_exceeds_remaining_deadline():
    # Target says wait 210s, but only 5s is left before the 1-hour cap.
    delay = _next_poll_delay(300.0, 0.0, 0.70, remaining_deadline=5.0)
    assert delay == 5.0


def test_next_poll_delay_never_negative_when_deadline_already_passed():
    delay = _next_poll_delay(300.0, 0.0, 0.70, remaining_deadline=-10.0)
    assert delay == 0.0


def test_default_now_returns_a_timezone_aware_datetime():
    # The real default for check_*_dependencies' `now` kwarg — everything
    # else in this test suite injects a fake one, so this is its only
    # direct exercise. datetime.now(UTC) is trivial but was worth pinning
    # down as tz-aware specifically, since the poll loop subtracts it from
    # a Postgres TIMESTAMPTZ (also tz-aware) and mixing aware/naive would
    # raise at runtime, not silently misbehave.
    before = datetime.now(UTC)
    result = _default_now()
    after = datetime.now(UTC)
    assert result.tzinfo is not None
    assert before <= result <= after


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


def test_malformed_yaml_rejected(tmp_path):
    with pytest.raises(ConfigError):
        load_config(write_config(tmp_path, "Execution: [unterminated"))


def test_missing_section_rejected(tmp_path):
    no_postgres = VALID_YAML.replace("Postgres:", "NotPostgres:")
    with pytest.raises(ConfigError):
        load_config(write_config(tmp_path, no_postgres))


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


def test_invalid_source_type_rejected(tmp_path):
    bad = VALID_YAML.replace("Type: environment", "Type: bogus")
    with pytest.raises(ConfigError):
        load_config(write_config(tmp_path, bad))


def test_connection_section_requires_active_profile_and_profiles(tmp_path):
    # Distinct from test_active_profile_must_exist_in_profiles: that one has
    # a well-formed section whose Active_profile just doesn't match any
    # entry; this is the section itself missing Active_profile outright.
    bad = VALID_YAML.replace("  Active_profile: dev\n", "")
    with pytest.raises(ConfigError):
        load_config(write_config(tmp_path, bad))


def test_invalid_cloning_scope_rejected(tmp_path):
    bad = VALID_YAML.replace("Scope: cfg", "Scope: bogus")
    with pytest.raises(ConfigError):
        load_config(write_config(tmp_path, bad))


def test_cloning_defaults_when_section_absent(tmp_path):
    no_cloning = VALID_YAML.replace("Cloning:\n  Enabled: true\n  Scope: cfg\n", "")
    config = load_config(write_config(tmp_path, no_cloning))
    assert config.cloning.enabled is False
    assert config.cloning.scope == "cfg"


def test_warehouse_is_none_when_section_absent(tmp_path):
    config = load_config(write_config(tmp_path, VALID_YAML))
    assert config.warehouse is None


def test_warehouse_parsed_when_present(tmp_path):
    with_warehouse = VALID_YAML + (
        "\nWarehouse:\n"
        "  Active_profile: dev\n"
        "  Profiles:\n"
        "    dev:\n"
        "      jdbc_url: jdbc:postgresql://warehouse-host:5432/analytics\n"
        "      user: etl_engine\n"
        "      auth_mode: password\n"
    )
    config = load_config(write_config(tmp_path, with_warehouse))
    assert config.warehouse.active_profile == "dev"
    assert config.warehouse.active.jdbc_url == "jdbc:postgresql://warehouse-host:5432/analytics"
    assert config.warehouse.active.secret_var == "ETL_CRAFT_WAREHOUSE_DEV_SECRET"


def test_warehouse_section_must_be_a_mapping_if_present(tmp_path):
    bad = VALID_YAML + "\nWarehouse: not-a-mapping\n"
    with pytest.raises(ConfigError):
        load_config(write_config(tmp_path, bad))


def test_email_is_none_when_section_absent(tmp_path):
    config = load_config(write_config(tmp_path, VALID_YAML))
    assert config.email is None


def test_email_parsed_when_present(tmp_path):
    with_email = VALID_YAML + (
        "\nEmail:\n"
        "  Active_profile: dev\n"
        "  Profiles:\n"
        "    dev:\n"
        "      host: smtp.example.com\n"
        "      port: 587\n"
        "      from_address: etl-craft@example.com\n"
        "      auth_mode: password\n"
        "      user: alerts@example.com\n"
    )
    config = load_config(write_config(tmp_path, with_email))
    assert config.email.active_profile == "dev"
    assert config.email.active.host == "smtp.example.com"
    assert config.email.active.port == 587
    assert config.email.active.use_tls is True
    assert config.email.active.secret_var == "ETL_CRAFT_EMAIL_DEV_SECRET"


def test_email_defaults_auth_mode_to_none_and_needs_no_user(tmp_path):
    with_email = VALID_YAML + (
        "\nEmail:\n"
        "  Active_profile: dev\n"
        "  Profiles:\n"
        "    dev:\n"
        "      host: smtp.example.com\n"
        "      port: 25\n"
        "      from_address: etl-craft@example.com\n"
    )
    config = load_config(write_config(tmp_path, with_email))
    assert config.email.active.auth_mode == "none"
    assert config.email.active.user is None


def test_email_password_auth_mode_requires_user(tmp_path):
    with_email = VALID_YAML + (
        "\nEmail:\n"
        "  Active_profile: dev\n"
        "  Profiles:\n"
        "    dev:\n"
        "      host: smtp.example.com\n"
        "      port: 587\n"
        "      from_address: etl-craft@example.com\n"
        "      auth_mode: password\n"
    )
    with pytest.raises(ConfigError, match="needs user"):
        load_config(write_config(tmp_path, with_email))


def test_email_invalid_auth_mode_rejected(tmp_path):
    with_email = VALID_YAML + (
        "\nEmail:\n"
        "  Active_profile: dev\n"
        "  Profiles:\n"
        "    dev:\n"
        "      host: smtp.example.com\n"
        "      port: 587\n"
        "      from_address: etl-craft@example.com\n"
        "      auth_mode: bogus\n"
    )
    with pytest.raises(ConfigError):
        load_config(write_config(tmp_path, with_email))


def test_email_section_must_be_a_mapping_if_present(tmp_path):
    bad = VALID_YAML + "\nEmail: not-a-mapping\n"
    with pytest.raises(ConfigError):
        load_config(write_config(tmp_path, bad))


def test_email_requires_active_profile_and_profiles(tmp_path):
    bad = VALID_YAML + "\nEmail:\n  Profiles:\n    dev:\n      host: h\n"
    with pytest.raises(ConfigError, match="needs Active_profile"):
        load_config(write_config(tmp_path, bad))


def test_email_active_profile_must_exist_in_profiles(tmp_path):
    bad = VALID_YAML + (
        "\nEmail:\n"
        "  Active_profile: staging\n"
        "  Profiles:\n"
        "    dev:\n"
        "      host: smtp.example.com\n"
        "      port: 587\n"
        "      from_address: etl-craft@example.com\n"
    )
    with pytest.raises(ConfigError, match="has no matching entry"):
        load_config(write_config(tmp_path, bad))


def test_email_profile_requires_host_port_and_from_address(tmp_path):
    bad = VALID_YAML + (
        "\nEmail:\n  Active_profile: dev\n  Profiles:\n    dev:\n      host: smtp.example.com\n"
    )
    with pytest.raises(ConfigError, match="needs host, port, and from_address"):
        load_config(write_config(tmp_path, bad))


def test_email_secret_var_override(tmp_path):
    with_email = VALID_YAML + (
        "\nEmail:\n"
        "  Active_profile: dev\n"
        "  Profiles:\n"
        "    dev:\n"
        "      host: smtp.example.com\n"
        "      port: 587\n"
        "      from_address: etl-craft@example.com\n"
        "      auth_mode: password\n"
        "      user: alerts@example.com\n"
        "      secret_var: MY_CUSTOM_SMTP_SECRET\n"
    )
    config = load_config(write_config(tmp_path, with_email))
    assert config.email.active.secret_var == "MY_CUSTOM_SMTP_SECRET"


def test_orchestrator_defaults_when_section_absent(tmp_path):
    config = load_config(write_config(tmp_path, VALID_YAML))
    orch = config.orchestrator
    assert orch.global_dag is False
    assert orch.catchup is None
    assert orch.tags is None
    assert orch.retries is None
    assert orch.retry_delay_minutes is None
    assert orch.depends_on_past is None
    assert orch.email_on_failure is None
    assert orch.email_recipients is None


def test_orchestrator_parsed_when_present(tmp_path):
    with_orchestrator = VALID_YAML + (
        "\nOrchestrator:\n"
        "  Global_dag: true\n"
        "  Catchup: true\n"
        "  Tags: [team-a, nightly]\n"
        "  Retries: 3\n"
        "  Retry_delay_minutes: 15\n"
        "  Depends_on_past: true\n"
        "  Email_on_failure: true\n"
        "  Email_recipients: [oncall@example.com]\n"
    )
    config = load_config(write_config(tmp_path, with_orchestrator))
    orch = config.orchestrator
    assert orch.global_dag is True
    assert orch.catchup is True
    assert orch.tags == ["team-a", "nightly"]
    assert orch.retries == 3
    assert orch.retry_delay_minutes == 15
    assert orch.depends_on_past is True
    assert orch.email_on_failure is True
    assert orch.email_recipients == ["oncall@example.com"]


def test_orchestrator_tags_must_be_a_list_if_present(tmp_path):
    bad = VALID_YAML + "\nOrchestrator:\n  Tags: not-a-list\n"
    with pytest.raises(ConfigError):
        load_config(write_config(tmp_path, bad))


def test_orchestrator_email_recipients_must_be_a_list_if_present(tmp_path):
    bad = VALID_YAML + "\nOrchestrator:\n  Email_recipients: not-a-list\n"
    with pytest.raises(ConfigError):
        load_config(write_config(tmp_path, bad))


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


def test_load_dotenv_file_requires_a_path():
    # Reached in practice only via resolve_secret(Source.Type='file'), but
    # _parse_source already refuses to load a config with Type=file and no
    # Path at all — so this defensive check is unreachable through the
    # public API. Exercised directly instead.
    with pytest.raises(ConfigError):
        _load_dotenv_file(None)


# ==============================================================================
# db.py — JDBC parsing and auth_mode wiring, no live DB required
# ==============================================================================


def test_parse_jdbc_postgres_with_explicit_port():
    parts = parse_jdbc_postgres("jdbc:postgresql://myhost:6543/mydb")
    assert parts == {"host": "myhost", "port": 6543, "database": "mydb", "query": {}}


def test_parse_jdbc_postgres_default_port():
    parts = parse_jdbc_postgres("jdbc:postgresql://myhost/mydb")
    assert parts == {"host": "myhost", "port": 5432, "database": "mydb", "query": {}}


def test_parse_jdbc_postgres_keeps_the_query_string():
    # E2-10, security-relevant: the pattern had no `query` group and stopped
    # the database capture at "?", so jdbc:postgresql://host/db?sslmode=require
    # connected *without* TLS and said nothing. warehouse.py forwarded query
    # parameters all along, which made the Engine DB -- always Postgres,
    # always required -- the weaker of the two.
    parts = parse_jdbc_postgres(
        "jdbc:postgresql://myhost/mydb?sslmode=require&application_name=etl"
    )
    assert parts["database"] == "mydb"
    assert parts["query"] == {"sslmode": "require", "application_name": "etl"}


def test_parse_jdbc_postgres_rejects_non_jdbc_url():
    with pytest.raises(ConnectionError_):
        parse_jdbc_postgres("postgresql://myhost:5432/mydb")


def profile(auth_mode: str, *, jdbc_url: str | None = None, **extra) -> ConnectionProfile:
    return ConnectionProfile(
        section="POSTGRES",
        name="dev",
        jdbc_url=jdbc_url or "jdbc:postgresql://localhost:5432/etl_craft",
        user="etl_engine",
        auth_mode=auth_mode,
        extra=extra,
    )


def test_password_creator_forwards_query_parameters_to_the_driver(monkeypatch):
    calls: dict = {}

    class FakeConnection:
        pass

    def fake_connect(**kwargs):
        calls.update(kwargs)
        return FakeConnection()

    import psycopg

    monkeypatch.setattr(psycopg, "connect", fake_connect)
    creator = AUTH_REGISTRY["password"](
        profile("password", jdbc_url="jdbc:postgresql://h/d?sslmode=require"), "pw"
    )
    creator()

    assert calls["sslmode"] == "require"


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
    # str, not bytes: psycopg's own signature is str | int | None and libpq
    # treats it as text. Encoding it happened to work but was wrong by the
    # driver's contract — caught by mypy's first pass (E2-29).
    assert calls["sslpassword"] == "passphrase"


def test_token_and_sso_creators_are_not_implemented():
    with pytest.raises(NotImplementedError):
        AUTH_REGISTRY["token"](profile("token"), "unused")
    with pytest.raises(NotImplementedError):
        AUTH_REGISTRY["sso"](profile("sso"), "unused")


def test_build_engine_rejects_unknown_auth_mode():
    # config.py's own validation already rejects an auth_mode outside
    # VALID_AUTH_MODES at load time, so this is unreachable via a normally-
    # loaded config — exercised by constructing a ConnectionProfile directly
    # instead. build_engine checks auth_mode before ever touching `config`
    # when `profile` is passed explicitly, so `config=None` is fine here.
    with pytest.raises(ConnectionError_):
        build_engine(None, profile("bogus"))


# ==============================================================================
# warehouse.py — JDBC/dialect translation and auth wiring, no live DB required
# ==============================================================================


def test_translate_jdbc_url_maps_known_scheme_and_defaults_port():
    dialect, parts = translate_jdbc_url("jdbc:postgresql://myhost/mydb")
    assert dialect == "postgresql+psycopg"
    assert parts == {"host": "myhost", "port": None, "database": "mydb", "query": {}}


def test_translate_jdbc_url_explicit_port_and_query():
    dialect, parts = translate_jdbc_url("jdbc:mysql://myhost:3306/mydb?useSSL=true")
    assert dialect == "mysql+pymysql"
    assert parts["port"] == 3306
    assert parts["query"] == {"useSSL": "true"}


def test_translate_jdbc_url_unmapped_scheme_passes_through():
    # Not in JDBC_SCHEME_TO_SQLALCHEMY_DIALECT — deliberately, per this
    # module's own [ADDITION] comment: an unrecognized scheme is used
    # verbatim as the SQLAlchemy dialect name rather than rejected.
    dialect, _ = translate_jdbc_url("jdbc:oracle://myhost:1521/mydb")
    assert dialect == "oracle"


def test_translate_jdbc_url_rejects_malformed_url():
    with pytest.raises(ConnectionError_):
        translate_jdbc_url("not-a-jdbc-url")


def warehouse_profile(
    auth_mode: str, jdbc_url: str = "jdbc:postgresql://localhost/etl_craft", **extra
):
    return ConnectionProfile(
        section="WAREHOUSE",
        name="dev",
        jdbc_url=jdbc_url,
        user="etl_engine",
        auth_mode=auth_mode,
        extra=extra,
    )


def test_password_creator_builds_url_and_uses_generic_dbapi_connect(monkeypatch):
    captured = {}

    def fake_dbapi_connect(url):
        captured["url"] = url
        return "fake-connection"

    monkeypatch.setattr(warehouse_module, "_dbapi_connect", fake_dbapi_connect)

    creator = WAREHOUSE_AUTH_REGISTRY["password"](
        warehouse_profile("password", "jdbc:mysql://myhost:3306/mydb"), "s3cr3t"
    )
    conn = creator()

    assert conn == "fake-connection"
    url = captured["url"]
    assert url.drivername == "mysql+pymysql"
    assert url.username == "etl_engine"
    assert url.password == "s3cr3t"
    assert url.host == "myhost"
    assert url.port == 3306
    assert url.database == "mydb"


def test_warehouse_key_file_token_sso_creators_are_not_implemented():
    with pytest.raises(NotImplementedError):
        WAREHOUSE_AUTH_REGISTRY["key_file"](warehouse_profile("key_file"), "unused")
    with pytest.raises(NotImplementedError):
        WAREHOUSE_AUTH_REGISTRY["token"](warehouse_profile("token"), "unused")
    with pytest.raises(NotImplementedError):
        WAREHOUSE_AUTH_REGISTRY["sso"](warehouse_profile("sso"), "unused")


def test_build_data_engine_raises_when_no_warehouse_configured():
    config = ConnectorConfig(
        mode="local",
        source=SourceConfig(type="environment"),
        postgres=ConnectionSection(active_profile="dev", profiles={"dev": profile("password")}),
        cloning=CloningConfig(),
        warehouse=None,
    )
    with pytest.raises(ConnectionError_):
        build_data_engine(config)


def test_build_data_engine_rejects_unknown_auth_mode():
    # Mirrors test_build_engine_rejects_unknown_auth_mode above: passing a
    # profile directly skips the config.warehouse lookup entirely, so
    # config=None is fine here too.
    with pytest.raises(ConnectionError_):
        build_data_engine(None, warehouse_profile("bogus"))


# ==============================================================================
# handlers.py — the dispatch() seam itself; sql_actions.py/business_rules.py/
# scripts.py each get their own dedicated real-Postgres test sections in
# test_integration.py, since none of them can do anything meaningful without
# a live DB connection.
# ==============================================================================


def _dummy_ctx(handler: str) -> TaskExecutionContext:
    return TaskExecutionContext(
        config=None,
        pipeline_id=1,
        pipeline_code="P",
        task_id=1,
        task_code="T",
        task_run_id=1,
        pipeline_run_id=1,
        handler=handler,
        refresh_type="FULL",
        task_params={},
        force=False,
    )


def test_dispatch_unknown_handler_rejected():
    # CFG_TASKS.HANDLER has a DB-level CHECK constraint restricting it to the
    # four known values, so this is unreachable via a real task row — still
    # worth guarding directly since dispatch() takes a bare ctx.handler
    # string. The unknown-handler check happens before any DB access, so a
    # dummy ctx/engine (never touched) is enough.
    with pytest.raises(HandlerError):
        dispatch(None, _dummy_ctx("BOGUS"))


# ------------------------------------------------------------------------------
# email_alert.py — the pure substitution logic (no DB/SMTP); the real send
# path is covered in test_integration.py (against real Postgres, SMTP
# mocked — no real mail server is part of this project's test infra).


def test_email_substitute_replaces_every_known_token():
    ctx = _dummy_ctx("EMAIL_ALERT")
    rendered = substitute_email_tokens(
        "run $$pipeline_id of $$pipeline_code failed at $$task_code: $$error_message",
        ctx,
        "disk full",
    )
    assert rendered == "run 1 of P failed at T: disk full"


def test_email_substitute_leaves_unknown_tokens_untouched():
    ctx = _dummy_ctx("EMAIL_ALERT")
    rendered = substitute_email_tokens("see $$nonsense for details", ctx, "")
    assert rendered == "see $$nonsense for details"


def test_resolve_email_pipeline_codes_single():
    assert resolve_email_pipeline_codes(None, "PIPE_A") == ["PIPE_A"]


def test_resolve_email_pipeline_codes_some_pipe_separated():
    assert resolve_email_pipeline_codes(None, "PIPE_A|PIPE_B") == ["PIPE_A", "PIPE_B"]


def _status(task_id, status, error=None):
    return TaskStatusEntry(task_id, f"t{task_id}", status, error)


def test_run_flavour_success_when_every_task_succeeded_cleanly():
    statuses = [_status(1, "SUCCESS"), _status(2, "SUCCESS"), _status(99, "IN-PROGRESS")]
    # 99 is the alerting task itself — necessarily IN-PROGRESS while it runs,
    # so counting it would make every run look unfinished.
    assert run_flavour(statuses, exclude_task_id=99) == "SUCCESS"


def test_run_flavour_failed_when_any_task_failed():
    statuses = [_status(1, "SUCCESS"), _status(2, "FAILED")]
    assert run_flavour(statuses, exclude_task_id=99) == "FAILED"


@pytest.mark.parametrize(
    "statuses",
    [
        # A skipped task: nothing failed outright, but the run was not clean.
        [_status(1, "SUCCESS"), _status(2, "SKIPPED")],
        # Succeeded while still carrying an error — "success with failure",
        # the literal case the neutral flavour exists for.
        [_status(1, "SUCCESS", "a transient blip")],
        # Not settled yet. Reporting plain SUCCESS for a run that has not
        # finished would be the one genuinely misleading answer of the three.
        [_status(1, "SUCCESS"), _status(2, "PENDING")],
    ],
)
def test_run_flavour_completed_with_errors(statuses):
    assert run_flavour(statuses, exclude_task_id=99) == "COMPLETED_WITH_ERRORS"


def test_run_flavour_of_a_pipeline_whose_only_task_is_the_alert_itself():
    assert run_flavour([_status(99, "IN-PROGRESS")], exclude_task_id=99) == "SUCCESS"


def test_render_email_digest_html_color_codes_status_and_escapes_content():
    entries = [
        _PipelineDigestEntry(
            "PIPE_A",
            "SUCCESS",
            1,
            None,
            None,
            [TaskStatusEntry(1, "t1", "SUCCESS", None)],
        ),
        _PipelineDigestEntry(
            "PIPE_B",
            "FAILED",
            2,
            None,
            None,
            [TaskStatusEntry(2, "t2", "FAILED", "<boom> & broke")],
        ),
    ]
    rendered = render_email_digest_html(entries)
    assert "#1a7f37" in rendered  # SUCCESS color
    assert "#cf222e" in rendered  # FAILED color
    assert "<details>" in rendered and "<summary>" in rendered
    # error message content is HTML-escaped, not injected raw
    assert "&lt;boom&gt; &amp; broke" in rendered
    assert "<boom>" not in rendered


def test_render_email_digest_html_no_tasks_shows_placeholder():
    entries = [_PipelineDigestEntry("PIPE_A", "NEVER_RUN", None, None, None, [])]
    rendered = render_email_digest_html(entries)
    assert "(no active tasks)" in rendered


# ------------------------------------------------------------------------------
# cloning.py — the pure table-list logic (no DB); the actual copy mechanism
# is covered in test_integration.py, against real Postgres standing in as
# both the Engine DB and the Data DB (same spirit as sql_actions.py's own
# tests).


def test_tables_for_scope_cfg():
    assert tables_for_scope("cfg") == CFG_TABLES


def test_tables_for_scope_aud():
    assert tables_for_scope("aud") == AUD_TABLES


def test_tables_for_scope_all_is_cfg_plus_aud_with_no_overlap():
    all_tables = tables_for_scope("all")
    assert set(all_tables) == set(CFG_TABLES) | set(AUD_TABLES)
    assert len(all_tables) == len(CFG_TABLES) + len(AUD_TABLES)


def _cloning_config(postgres_jdbc: str, warehouse_jdbc: str) -> ConnectorConfig:
    return ConnectorConfig(
        mode="local",
        source=SourceConfig(type="environment"),
        postgres=ConnectionSection(
            active_profile="dev",
            profiles={
                "dev": ConnectionProfile(
                    section="POSTGRES",
                    name="dev",
                    jdbc_url=postgres_jdbc,
                    user="u",
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
                    jdbc_url=warehouse_jdbc,
                    user="u",
                    auth_mode="password",
                )
            },
        ),
    )


def test_same_database_true_for_identical_host_port_database():
    config = _cloning_config(
        "jdbc:postgresql://localhost:5432/etl_craft", "jdbc:postgresql://localhost:5432/etl_craft"
    )
    assert same_database(config) is True


def test_same_database_false_for_different_database_name():
    config = _cloning_config(
        "jdbc:postgresql://localhost:5432/etl_craft", "jdbc:postgresql://localhost:5432/analytics"
    )
    assert same_database(config) is False


def test_same_database_false_for_different_dialect_even_if_host_port_match():
    config = _cloning_config(
        "jdbc:postgresql://localhost:5432/etl_craft", "jdbc:clickhouse://localhost:5432/etl_craft"
    )
    assert same_database(config) is False


# ------------------------------------------------------------------------------
# docs_generator.py — the pure rendering pieces (no DB); the real read layer
# (collect_docs) is covered in test_integration.py against real Postgres.


def _sample_doc() -> tuple[PipelineSummary, PipelineDocData]:
    summary = PipelineSummary(pipeline_code="PIPE_A", pipeline_name="Pipe A", refresh_type="FULL")
    data = PipelineDocData(
        waves=[["t1"], ["t2"]],
        steps=[
            PipelineStep(task_code="t1", handler="SQL", parameters={"SQL_ACTION": "CREATE_TABLE"}),
            PipelineStep(task_code="t2", handler="PYTHON", parameters={}),
        ],
        pipeline_dependencies=[
            PipelineDependencyEdge(depends_on_pipeline_code="PIPE_UP", dependency_type="SUCCESS")
        ],
        cross_task_dependencies=[
            CrossPipelineTaskEdge(
                task_code="t1",
                depends_on_pipeline_code="PIPE_UP",
                depends_on_task_code="up_task",
                dependency_type="SUCCESS",
            )
        ],
    )
    return summary, data


def test_build_search_index_has_one_pipeline_entry_and_one_per_task():
    docs = [_sample_doc()]
    index = build_search_index(docs)
    types = [entry["type"] for entry in index]
    assert types == ["pipeline", "task", "task"]
    assert index[0]["url"] == "PIPE_A.html"
    assert index[1]["url"] == "PIPE_A.html#t1"


def test_build_search_index_task_text_includes_parameters():
    docs = [_sample_doc()]
    index = build_search_index(docs)
    task_entry = next(e for e in index if e.get("task_code") == "t1")
    assert "SQL_ACTION=CREATE_TABLE" in task_entry["text"]


def test_render_docs_index_html_lists_pipeline_and_has_search_box():
    rendered = render_docs_index_html([_sample_doc()])
    assert "PIPE_A" in rendered
    assert 'id="search-box"' in rendered
    assert "search.js" in rendered


def test_render_docs_pipeline_html_includes_waves_steps_and_dependencies():
    summary, data = _sample_doc()
    rendered = render_docs_pipeline_html(summary, data)
    assert "Wave 1: t1" in rendered
    assert "Wave 2: t2" in rendered
    assert 'id="t1"' in rendered
    assert "PIPE_UP" in rendered
    assert "up_task" in rendered


def test_render_docs_pipeline_html_escapes_content():
    summary = PipelineSummary(pipeline_code="P", pipeline_name="<script>", refresh_type="FULL")
    data = PipelineDocData(waves=[], steps=[], pipeline_dependencies=[], cross_task_dependencies=[])
    rendered = render_docs_pipeline_html(summary, data)
    assert "<script>" not in rendered
    assert "&lt;script&gt;" in rendered


# ------------------------------------------------------------------------------
# sql_actions.py — the pure pieces (no DB); real-Postgres behavior is covered
# in test_integration.py's own sql_actions.py section.
# ------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("refresh_type", "force_all", "expected"),
    [
        ("INCREMENTAL", False, "pipeline_run_id = 42"),
        ("FULL", False, "1=1"),
        # A manual/force invocation scans everything regardless of
        # refresh_type — the same override business_rules.py's own
        # scope predicate uses for --force.
        ("INCREMENTAL", True, "1=1"),
    ],
)
def test_substitute_pipeline_id(refresh_type, force_all, expected):
    # $$pipeline_id substitutes to the bare condition alone — the author's
    # own SQL text supplies the surrounding "WHERE", same as real usage.
    result = substitute_pipeline_id(
        "SELECT 1 WHERE $$pipeline_id",
        refresh_type=refresh_type,
        pipeline_run_id=42,
        force_all=force_all,
    )
    assert result == f"SELECT 1 WHERE {expected}"


def test_substitute_pipeline_id_leaves_a_tokenless_query_with_no_where_untouched():
    # No $$pipeline_id token, and no WHERE clause at all -> left completely
    # untouched. An earlier version auto-appended a WHERE here; reverted
    # per explicit instruction (see the [DEVIATION] note on the function
    # itself) — a missing token on a genuinely incremental source is a
    # pipeline-definition mistake for review to catch, not something the
    # engine silently rescues by guessing at scoping.
    sql = "SELECT * FROM some_table"
    assert substitute_pipeline_id(sql, refresh_type="INCREMENTAL", pipeline_run_id=42) == sql


def test_substitute_pipeline_id_leaves_an_unrelated_where_clause_alone():
    # No $$pipeline_id token, but a real WHERE clause already exists -> also
    # left completely untouched. A query against a small reference table
    # with its own filter and no PIPELINE_RUN_ID column must never get one
    # silently AND'd on.
    sql = "SELECT * FROM reference_table WHERE active = true"
    assert substitute_pipeline_id(sql, refresh_type="INCREMENTAL", pipeline_run_id=42) == sql


def test_qualify_prepends_the_active_profiles_database():
    assert qualify("public.some_table", "etl_craft") == "etl_craft.public.some_table"


def test_active_database_requires_a_warehouse_section():
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
    )
    with pytest.raises(HandlerError, match=r"\[Warehouse\]"):
        active_database(config)


def test_active_database_resolves_from_jdbc_url():
    warehouse_profile = ConnectionProfile(
        section="WAREHOUSE",
        name="dev",
        jdbc_url="jdbc:postgresql://localhost:55432/some_warehouse_db",
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
        warehouse=ConnectionSection(active_profile="dev", profiles={"dev": warehouse_profile}),
    )
    assert active_database(config) == "some_warehouse_db"


# ------------------------------------------------------------------------------
# scripts.py — HANDLER=PYTHON's SCRIPT_NAME precondition
# ------------------------------------------------------------------------------


def test_parse_trailing_json_blank_stdout_returns_empty_dict():
    # A script that prints nothing at all (no counts to report) — distinct
    # from printing plain log lines with no trailing JSON, already covered
    # in test_integration.py's test_python_handler_no_trailing_json_is_fine.
    assert _parse_trailing_json("") == {}
    assert _parse_trailing_json("   \n\n  ") == {}


def test_python_handler_missing_script_name_rejected_before_any_db_access():
    # CFG_TASKS.ck_tasks_script_required already blocks inserting a real
    # HANDLER=PYTHON row with no SCRIPT_NAME — this guard is unreachable via
    # a real task, same class as db.build_engine's bad-auth_mode check.
    # Worth keeping anyway: it's the one thing standing between a
    # hand-constructed ctx (or a future caller that skips the CFG_ layer)
    # and a crash. cfg_conn is never touched before this check fires.
    with pytest.raises(HandlerError, match="SCRIPT_NAME"):
        execute_python_script(None, _dummy_ctx("PYTHON"))


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
    TASK_LOG TEXT,
    ATTEMPT_COUNT INTEGER NOT NULL DEFAULT 1
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
    with runlog_engine.begin() as conn, pytest.raises(RunLogError, match="already SUCCESS"):
        resolve_run_for_task(conn, pipeline_id=1)
    with runlog_engine.begin() as conn:
        resolved = resolve_run_for_task(conn, pipeline_id=1, force=True)
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


def test_update_task_run_writes_exactly_what_this_attempt_measured(runlog_engine):
    # [DEVIATION, 2026-09-20, E2-21] This used to assert the opposite: that an
    # unspecified count was COALESCE'd forward from the previous call. That
    # meant one row could hold numbers from two different attempts, with
    # nothing saying which was which. Each write now says exactly what it
    # measured, and begin_attempt resets the row on the way into a retry.
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
    assert row.SOURCE_COUNT is None
    assert row.TARGET_COUNT == 99


def test_begin_attempt_counts_retries_and_resets_the_rows_state(runlog_engine):
    # E2-21. The one-row-per-task-per-run rule is load-bearing, so attempts are
    # counted *within* the row. START_DATE is reset too: it previously kept the
    # first attempt's timestamp, so a task retried an hour later reported a
    # duration spanning the gap -- and that duration feeds the cross-pipeline
    # poll cadence.
    with runlog_engine.begin() as conn:
        run_id = find_or_create_active_run(conn, pipeline_id=1)
        binding = find_or_create_task_run(conn, task_id=10, pipeline_run_id=run_id)
        assert binding.created is True
        update_task_run(
            conn,
            binding.task_run_id,
            status="FAILED",
            error_message="first attempt blew up",
            source_count=7,
        )

        assert begin_attempt(conn, binding.task_run_id) == 2

    with runlog_engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT ATTEMPT_COUNT, STATUS, ERROR_MESSAGE, SOURCE_COUNT, END_DATE "
                "FROM AUD_TASK_RUN_LOG WHERE TASK_RUN_ID = :id"
            ),
            {"id": binding.task_run_id},
        ).one()
    assert row.ATTEMPT_COUNT == 2
    assert row.STATUS == "IN-PROGRESS"
    # The previous attempt's outcome is gone, not carried into this one.
    assert row.ERROR_MESSAGE is None
    assert row.SOURCE_COUNT is None
    assert row.END_DATE is None


def test_find_or_create_task_run_is_independent_per_task(runlog_engine):
    with runlog_engine.begin() as conn:
        run_id = find_or_create_active_run(conn, pipeline_id=1)
        a = find_or_create_task_run(conn, task_id=10, pipeline_run_id=run_id)
        b = find_or_create_task_run(conn, task_id=11, pipeline_run_id=run_id)
    assert a.task_run_id != b.task_run_id


def test_fetch_run_state_with_no_task_ids_returns_empty_without_touching_the_connection():
    # Guards the early return: with an empty task_ids list there's nothing
    # to query, so this never even touches `conn` — passing None proves it.
    assert fetch_run_state(None, pipeline_run_id=1, task_ids=[]) == {}


class _FakeResult:
    """A minimal stand-in for a SQLAlchemy CursorResult, just enough for runlog.py's own calls."""

    def __init__(self, value):
        self._value = value

    def scalar_one_or_none(self):
        return self._value

    def scalar_one(self):
        return self._value

    def one_or_none(self):
        return self._value


class _RaceThenVanishConnection:
    """Simulates: insert hits a unique violation, then the immediate re-SELECT finds nothing.

    Against a real Postgres this can't happen — a unique_violation on
    ux_pipeline_run_one_active/ux_task_run_one_per_pipeline_run means a
    conflicting row exists by definition, so the re-SELECT right after
    losing the race is guaranteed to find the winner. runlog.py's own
    defensive RunLogError for that "impossible" case can only be exercised
    by simulating it directly like this — see
    tests/test_integration.py's deterministic race tests for the real,
    non-simulated version of this same race.
    """

    def __init__(self):
        self._call_count = 0

    def begin_nested(self):
        return contextlib.nullcontext()

    def execute(self, *args, **kwargs):
        self._call_count += 1
        if self._call_count == 2:
            raise IntegrityError("INSERT", {}, Exception("unique_violation"))
        return _FakeResult(None)


def test_find_or_create_active_run_raises_if_winner_vanishes_after_losing_race():
    with pytest.raises(RunLogError):
        find_or_create_active_run(_RaceThenVanishConnection(), pipeline_id=1)


def test_find_or_create_task_run_raises_if_winner_vanishes_after_losing_race():
    with pytest.raises(RunLogError):
        find_or_create_task_run(_RaceThenVanishConnection(), task_id=1, pipeline_run_id=1)


# ==============================================================================
# cli.py — set-execution-mode / configure, the two commands that never need
# a live Postgres connection (both are handled before load_config/
# build_engine in main() — see cli.py's own module comment)
# ==============================================================================


def test_cli_set_execution_mode(tmp_path, monkeypatch, capsys):
    write_config(tmp_path, VALID_YAML)
    monkeypatch.chdir(tmp_path)

    exit_code = cli_main(["set-execution-mode", "orchestrator"])

    assert exit_code == 0
    assert "orchestrator" in capsys.readouterr().out
    assert load_config().mode == "orchestrator"


def test_cli_set_execution_mode_invalid_choice_is_an_argparse_error(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(SystemExit) as exc_info:
        cli_main(["set-execution-mode", "bogus"])
    assert exc_info.value.code == 2


def test_cli_set_execution_mode_missing_file_reports_clean_error(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)

    exit_code = cli_main(["set-execution-mode", "local"])

    assert exit_code == 2
    assert "error:" in capsys.readouterr().err


def test_cli_setup_creates_config_and_reports_what_it_did(tmp_path, monkeypatch, capsys):
    # [DEVIATION, 2026-09-20] `configure` is gone. Per explicit instruction
    # ("remove the ineractive setup, lets go in dbt route. one single command
    # with required files and it should itself up"), `setup` is idempotent:
    # first run sets up, later runs update. The Engine DB is unreachable here,
    # which must be *reported*, not raised -- writing the config is useful on
    # its own, and is often the step that fixes the connection.
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(
        "ETL_CRAFT_MODE=local\n"
        "ETL_CRAFT_SOURCE_TYPE=environment\n"
        "ETL_CRAFT_POSTGRES_PROFILE=dev\n"
        "ETL_CRAFT_POSTGRES_JDBC_URL=jdbc:postgresql://127.0.0.1:1/nope\n"
        "ETL_CRAFT_POSTGRES_USER=u\n"
        "ETL_CRAFT_POSTGRES_AUTH_MODE=password\n"
    )
    monkeypatch.setenv("ETL_CRAFT_POSTGRES_DEV_SECRET", "s")

    exit_code = cli_main(["setup"])

    out = capsys.readouterr()
    assert "created" in out.out
    assert "ETL_CRAFT_POSTGRES_DEV_SECRET" in out.out
    assert (tmp_path / "craft-connector.yml").is_file()
    # Unreachable database -> reported as a problem, exit 1, config still written.
    assert exit_code == 1
    assert "not reachable" in out.out


def test_cli_setup_is_idempotent(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(
        "ETL_CRAFT_MODE=local\n"
        "ETL_CRAFT_SOURCE_TYPE=environment\n"
        "ETL_CRAFT_POSTGRES_PROFILE=dev\n"
        "ETL_CRAFT_POSTGRES_JDBC_URL=jdbc:postgresql://127.0.0.1:1/nope\n"
        "ETL_CRAFT_POSTGRES_USER=u\n"
        "ETL_CRAFT_POSTGRES_AUTH_MODE=password\n"
    )
    cli_main(["setup"])
    capsys.readouterr()

    cli_main(["setup"])

    assert "already current" in capsys.readouterr().out


def test_cli_setup_reads_the_environment_when_asked(tmp_path, monkeypatch, capsys):
    # Per explicit instruction: "the craft connector can take values from .env
    # or the environment itself based on the options". A container or CI
    # runner already has these exported; writing a file first would be a step
    # backwards.
    monkeypatch.chdir(tmp_path)
    for key, value in {
        "ETL_CRAFT_MODE": "local",
        "ETL_CRAFT_SOURCE_TYPE": "environment",
        "ETL_CRAFT_POSTGRES_PROFILE": "prod",
        "ETL_CRAFT_POSTGRES_JDBC_URL": "jdbc:postgresql://127.0.0.1:1/nope",
        "ETL_CRAFT_POSTGRES_USER": "u",
        "ETL_CRAFT_POSTGRES_AUTH_MODE": "password",
    }.items():
        monkeypatch.setenv(key, value)

    cli_main(["setup", "--from-environment"])

    written = yaml.safe_load((tmp_path / "craft-connector.yml").read_text())
    assert written["Postgres"]["Active_profile"] == "prod"
    assert "ETL_CRAFT_POSTGRES_PROD_SECRET" in capsys.readouterr().out


def test_cli_setup_without_any_settings_source_says_so(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)

    exit_code = cli_main(["setup"])

    assert exit_code == 2
    err = capsys.readouterr().err
    assert "no settings file" in err
    assert "--from-environment" in err


def test_configure_from_env_reports_a_missing_file(tmp_path):
    with pytest.raises(ConfigError, match="env file not found"):
        configure_from_env(tmp_path / "absent.env", tmp_path / "craft-connector.yml")


# ==============================================================================
# __init__.py / __main__.py — the two console-script entry points
# ==============================================================================
#
# Both are pure delegation to cli.main(); the fastest way to exercise them
# for real (not just by inspection) without needing a working
# craft-connector.yml is to trigger argparse's own "no command given"
# SystemExit(2), which fires before any config loading happens.


def test_init_main_delegates_to_cli_main(monkeypatch):
    monkeypatch.setattr("sys.argv", ["etl-craft"])
    with pytest.raises(SystemExit) as exc_info:
        etl_craft.main()
    assert exc_info.value.code == 2


def test_dunder_main_runs_cli_when_invoked_as_a_module(monkeypatch):
    monkeypatch.setattr("sys.argv", ["etl-craft"])
    with pytest.raises(SystemExit) as exc_info:
        runpy.run_module("etl_craft", run_name="__main__")
    assert exc_info.value.code == 2


# ==============================================================================
# configure.py — set_execution_mode / configure_from_env, no DB at all
# ==============================================================================
#
# Both write craft-connector.yml directly (no engine, no secret resolution),
# so these — and the CLI paths that dispatch to them — never need a live
# Postgres connection, unlike every other command.


def test_set_execution_mode_updates_only_mode(tmp_path):
    path = write_config(tmp_path, VALID_YAML)

    set_execution_mode("orchestrator", path)

    reloaded = load_config(path)
    assert reloaded.mode == "orchestrator"
    assert reloaded.postgres.active.jdbc_url == "jdbc:postgresql://localhost:5432/etl_craft"
    assert reloaded.cloning.enabled is True


def test_set_execution_mode_rejects_invalid_mode(tmp_path):
    path = write_config(tmp_path, VALID_YAML)
    with pytest.raises(ConfigError):
        set_execution_mode("bogus", path)


def test_set_execution_mode_requires_existing_file(tmp_path):
    with pytest.raises(ConfigError):
        set_execution_mode("local", tmp_path / "does-not-exist.yml")


def test_set_execution_mode_requires_execution_section(tmp_path):
    path = tmp_path / "craft-connector.yml"
    path.write_text("Postgres:\n  Active_profile: dev\n  Profiles: {}\n")
    with pytest.raises(ConfigError):
        set_execution_mode("local", path)


def test_set_execution_mode_rejects_malformed_yaml(tmp_path):
    path = tmp_path / "craft-connector.yml"
    path.write_text("Execution: [unterminated")
    with pytest.raises(ConfigError):
        set_execution_mode("local", path)


def _write_env(tmp_path, contents: str, name: str = "config.env"):
    path = tmp_path / name
    path.write_text(contents)
    return path


VALID_ENV = """
ETL_CRAFT_MODE=local
ETL_CRAFT_SOURCE_TYPE=environment
ETL_CRAFT_POSTGRES_PROFILE=dev
ETL_CRAFT_POSTGRES_JDBC_URL=jdbc:postgresql://localhost:5432/etl_craft
ETL_CRAFT_POSTGRES_USER=etl_engine
ETL_CRAFT_POSTGRES_AUTH_MODE=password
"""


def test_configure_from_env_creates_valid_config(tmp_path):
    env_path = _write_env(tmp_path, VALID_ENV)
    output_path = tmp_path / "craft-connector.yml"

    configure_from_env(env_path, output_path)

    config = load_config(output_path)
    assert config.mode == "local"
    assert config.source.type == "environment"
    assert config.postgres.active_profile == "dev"
    assert config.postgres.active.jdbc_url == "jdbc:postgresql://localhost:5432/etl_craft"
    assert config.postgres.active.user == "etl_engine"
    assert config.postgres.active.auth_mode == "password"
    assert config.cloning.enabled is False
    assert config.cloning.scope == "cfg"


def test_configure_from_env_missing_env_file_raises(tmp_path):
    with pytest.raises(ConfigError):
        configure_from_env(tmp_path / "no-such.env", tmp_path / "craft-connector.yml")


@pytest.mark.parametrize(
    "key",
    [
        "ETL_CRAFT_MODE",
        "ETL_CRAFT_SOURCE_TYPE",
        "ETL_CRAFT_POSTGRES_PROFILE",
        "ETL_CRAFT_POSTGRES_JDBC_URL",
        "ETL_CRAFT_POSTGRES_USER",
        "ETL_CRAFT_POSTGRES_AUTH_MODE",
    ],
)
def test_configure_from_env_requires_each_field(tmp_path, key):
    lines = [line for line in VALID_ENV.strip().splitlines() if not line.startswith(key)]
    env_path = _write_env(tmp_path, "\n".join(lines))
    with pytest.raises(ConfigError):
        configure_from_env(env_path, tmp_path / "craft-connector.yml")


def test_configure_from_env_rejects_invalid_mode(tmp_path):
    env_path = _write_env(
        tmp_path, VALID_ENV.replace("ETL_CRAFT_MODE=local", "ETL_CRAFT_MODE=bogus")
    )
    with pytest.raises(ConfigError):
        configure_from_env(env_path, tmp_path / "craft-connector.yml")


def test_configure_from_env_rejects_invalid_source_type(tmp_path):
    env_path = _write_env(
        tmp_path,
        VALID_ENV.replace("ETL_CRAFT_SOURCE_TYPE=environment", "ETL_CRAFT_SOURCE_TYPE=bogus"),
    )
    with pytest.raises(ConfigError):
        configure_from_env(env_path, tmp_path / "craft-connector.yml")


def test_configure_from_env_file_source_requires_path(tmp_path):
    env_path = _write_env(
        tmp_path,
        VALID_ENV.replace("ETL_CRAFT_SOURCE_TYPE=environment", "ETL_CRAFT_SOURCE_TYPE=file"),
    )
    with pytest.raises(ConfigError):
        configure_from_env(env_path, tmp_path / "craft-connector.yml")


def test_configure_from_env_file_source_with_path(tmp_path):
    secrets_path = tmp_path / "secrets.env"
    env = VALID_ENV.replace(
        "ETL_CRAFT_SOURCE_TYPE=environment",
        f"ETL_CRAFT_SOURCE_TYPE=file\nETL_CRAFT_SOURCE_PATH={secrets_path}",
    )
    env_path = _write_env(tmp_path, env)
    output_path = tmp_path / "craft-connector.yml"

    configure_from_env(env_path, output_path)

    config = load_config(output_path)
    assert config.source.type == "file"
    assert config.source.path == str(secrets_path)


def test_configure_from_env_rejects_invalid_auth_mode(tmp_path):
    env_path = _write_env(
        tmp_path,
        VALID_ENV.replace(
            "ETL_CRAFT_POSTGRES_AUTH_MODE=password", "ETL_CRAFT_POSTGRES_AUTH_MODE=bogus"
        ),
    )
    with pytest.raises(ConfigError):
        configure_from_env(env_path, tmp_path / "craft-connector.yml")


def test_configure_from_env_rejects_invalid_cloning_scope(tmp_path):
    env_path = _write_env(tmp_path, VALID_ENV + "ETL_CRAFT_CLONING_SCOPE=bogus\n")
    with pytest.raises(ConfigError):
        configure_from_env(env_path, tmp_path / "craft-connector.yml")


def test_configure_from_env_enables_cloning(tmp_path):
    env_path = _write_env(tmp_path, VALID_ENV + "ETL_CRAFT_CLONING_ENABLED=true\n")
    output_path = tmp_path / "craft-connector.yml"

    configure_from_env(env_path, output_path)

    assert load_config(output_path).cloning.enabled is True


def test_configure_from_env_includes_orchestrator_name(tmp_path):
    env_path = _write_env(tmp_path, VALID_ENV + "ETL_CRAFT_ORCHESTRATOR_NAME=Airflow\n")
    output_path = tmp_path / "craft-connector.yml"

    configure_from_env(env_path, output_path)

    raw = yaml.safe_load(output_path.read_text())
    assert raw["Execution"]["Orchestrator name"] == "Airflow"


def test_configure_from_env_merges_a_second_profile_without_losing_the_first(tmp_path):
    output_path = tmp_path / "craft-connector.yml"
    configure_from_env(_write_env(tmp_path, VALID_ENV, "dev.env"), output_path)

    uat_env = VALID_ENV.replace(
        "ETL_CRAFT_POSTGRES_PROFILE=dev", "ETL_CRAFT_POSTGRES_PROFILE=uat"
    ).replace(
        "jdbc:postgresql://localhost:5432/etl_craft", "jdbc:postgresql://uat-host:5432/etl_craft"
    )
    configure_from_env(_write_env(tmp_path, uat_env, "uat.env"), output_path)

    config = load_config(output_path)
    assert set(config.postgres.profiles) == {"dev", "uat"}
    assert config.postgres.active_profile == "uat"
    assert config.postgres.profiles["dev"].jdbc_url == "jdbc:postgresql://localhost:5432/etl_craft"


# ==============================================================================
# migrate.py — the pure statement-splitting piece; apply_pending_migrations
# itself needs a real Postgres (SCHEMA_MIGRATIONS table) — see
# test_integration.py.
# ==============================================================================


def test_split_statements_ignores_blank_and_whitespace_only_segments():
    sql_text = "ALTER TABLE t ADD COLUMN c int;  \n\n  UPDATE t SET c = 1; \n ;"
    assert _split_statements(sql_text) == ["ALTER TABLE t ADD COLUMN c int", "UPDATE t SET c = 1"]


def test_split_statements_empty_input_returns_empty_list():
    assert _split_statements("") == []
    assert _split_statements("   \n  ") == []
