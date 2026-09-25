"""The CFG_ readers and the run log, against a real Engine DB on SQLite and PostgreSQL."""

import threading
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from etl_craft.core.enums import Mode, RunStatus, SlaStatus
from etl_craft.core.errors import MetadataError, RunStateError
from etl_craft.core.graph import TaskEdge, TaskRunState, build_graph
from etl_craft.engine import runlog
from etl_craft.engine.repository import business_rules, dependencies, pipelines, runs, tasks


def insert(conn, sql, id_column, **params):
    return conn.execute(text(f"{sql} RETURNING {id_column}"), params).scalar_one()


def add_task(conn, pipeline_id, code, handler="SQL", *, active="Y", **params):
    task_id = insert(
        conn,
        "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER, ACTIVE_FLAG) "
        "VALUES (:code, 'ETL', :pipeline, :handler, :active)",
        "TASK_ID",
        code=code,
        pipeline=pipeline_id,
        handler=handler,
        active=active,
    )
    for name, value in params.items():
        conn.execute(
            text(
                "INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE) "
                "VALUES (:task, :name, :value)"
            ),
            {"task": task_id, "name": name, "value": value},
        )
    return task_id


def add_dependency(conn, pipeline_id, task_id, depends_on, dependency_type="SUCCESS", **extra):
    columns = "PIPELINE_ID, TASK_ID, DEPENDS_ON_TASK_ID, DEPENDENCY_TYPE"
    values = ":pipeline, :task, :depends_on, :type"
    params = {"pipeline": pipeline_id, "task": task_id, "depends_on": depends_on}
    if "depends_on_pipeline" in extra:
        columns += ", DEPENDS_ON_PIPELINE_ID"
        values += ", :depends_on_pipeline"
        params["depends_on_pipeline"] = extra["depends_on_pipeline"]
    return insert(
        conn,
        f"INSERT INTO CFG_TASK_DEPENDENCY ({columns}) VALUES ({values})",
        "TASK_DEPENDENCY_ID",
        type=dependency_type,
        **params,
    )


@pytest.fixture
def seeded(engine_db):
    """Pipeline PL_ALPHA with five tasks and every kind of dependency, and PL_BETA with one."""
    ids = {}
    with engine_db.engine.begin() as conn:
        ids["alpha"] = insert(
            conn,
            "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE, SLA_IN_HOURS, "
            "PIPELINE_PARAMETERS) VALUES ('PL_ALPHA', 'Alpha', 'INCREMENTAL', 2.5, :params)",
            "PIPELINE_ID",
            params='{"RETRIES": 3, "TAGS": ["daily"], "CATCHUP": false}',
        )
        ids["beta"] = insert(
            conn,
            "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
            "VALUES ('PL_BETA', 'Beta', 'FULL')",
            "PIPELINE_ID",
        )
        insert(
            conn,
            "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE, ACTIVE_FLAG) "
            "VALUES ('PL_RETIRED', 'Retired', 'FULL', 'N')",
            "PIPELINE_ID",
        )
        a = ids["alpha"]
        ids["extract"] = add_task(conn, a, "extract", "PYTHON", SCRIPT_NAME="extract.py")
        ids["setup"] = add_task(
            conn, a, "setup_dim", SQL_ACTION="SETUP_TABLE", TARGET_OBJECT="core.dim"
        )
        ids["load"] = add_task(
            conn, a, "load_dim", SQL_ACTION="SCD1_MERGE", TARGET_OBJECT="core.dim"
        )
        ids["rules"] = add_task(conn, a, "check_dim", "BUSINESS_RULES")
        ids["alert"] = add_task(conn, a, "alert", "EMAIL_ALERT")
        ids["retired"] = add_task(conn, a, "old_task", active="N")
        ids["upstream"] = add_task(conn, ids["beta"], "publish")
        conn.execute(
            text(
                "UPDATE CFG_TASK_PARAMETERS SET ACTIVE_FLAG = 'N' "
                "WHERE TASK_ID = :t AND PARAMETER_NAME = 'SCRIPT_NAME'"
            ),
            {"t": ids["extract"]},
        )
        conn.execute(
            text(
                "INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE) "
                "VALUES (:t, 'SCRIPT_NAME', 'extract_v2.py')"
            ),
            {"t": ids["extract"]},
        )
        add_dependency(conn, a, ids["load"], ids["extract"])
        add_dependency(conn, a, ids["load"], ids["setup"], "ALWAYS")
        add_dependency(conn, a, ids["rules"], ids["load"])
        add_dependency(conn, a, ids["alert"], ids["load"], "FAILURE")
        ids["cross"] = add_dependency(
            conn, a, ids["load"], ids["upstream"], "HAS_DATA", depends_on_pipeline=ids["beta"]
        )
        ids["pipeline_dependency"] = insert(
            conn,
            "INSERT INTO CFG_PIPELINE_DEPENDENCY (PIPELINE_ID, DEPENDS_ON_PIPELINE_ID, "
            "DEPENDENCY_TYPE) VALUES (:a, :b, 'SUCCESS')",
            "PIPELINE_DEPENDENCY_ID",
            a=a,
            b=ids["beta"],
        )
        for sequence, name in ((2, "second"), (1, "first")):
            conn.execute(
                text(
                    "INSERT INTO CFG_BUSINESS_RULES (BUSINESS_RULE_NAME, PIPELINE_ID, TASK_ID, "
                    "BUSINESS_RULE_SQL, BUSINESS_RULE_TYPE, BUSINESS_RULE_KEY_COLUMN, "
                    "TARGET_TABLE, SEQUENCE_NUMBER) "
                    "VALUES (:name, :p, :t, 'SELECT 1', 'REJECT', 'id', 'core.dim', :seq)"
                ),
                {"name": name, "p": a, "t": ids["rules"], "seq": sequence},
            )
    return engine_db.engine, ids


# Pipelines and tasks


def test_resolve_pipeline_id(seeded):
    engine, ids = seeded
    with engine.connect() as conn:
        assert pipelines.resolve_pipeline_id(conn, "PL_ALPHA") == ids["alpha"]
        with pytest.raises(MetadataError, match="PIPELINE_CODE='pl_alpa' — did you mean: PL_ALPHA"):
            pipelines.resolve_pipeline_id(conn, "pl_alpa")
        with pytest.raises(
            MetadataError, match="no active pipeline with PIPELINE_CODE='PL_RETIRED'"
        ):
            pipelines.resolve_pipeline_id(conn, "PL_RETIRED")


def test_resolve_task_id(seeded):
    engine, ids = seeded
    with engine.connect() as conn:
        assert tasks.resolve_task_id(conn, ids["alpha"], "load_dim") == ids["load"]
        with pytest.raises(MetadataError, match="did you mean: load_dim"):
            tasks.resolve_task_id(conn, ids["alpha"], "load_dm")
        with pytest.raises(MetadataError, match="TASK_CODE='old_task'"):
            tasks.resolve_task_id(conn, ids["alpha"], "old_task")
        # A code is scoped to its pipeline.
        with pytest.raises(MetadataError):
            tasks.resolve_task_id(conn, ids["beta"], "load_dim")


def test_task_details_and_parameters(seeded):
    engine, ids = seeded
    with engine.connect() as conn:
        codes = tasks.fetch_task_codes(conn, ids["alpha"])
        assert sorted(codes.values()) == ["alert", "check_dim", "extract", "load_dim", "setup_dim"]
        detail = tasks.fetch_task_execution_detail(conn, ids["load"])
        assert (detail.handler, detail.task_code, detail.pipeline_code) == (
            "SQL",
            "load_dim",
            "PL_ALPHA",
        )
        assert (detail.pipeline_id, detail.refresh_type) == (ids["alpha"], "INCREMENTAL")
        # Only active parameters, so a replaced value is the one read.
        assert tasks.fetch_task_parameters(conn, ids["extract"]) == {"SCRIPT_NAME": "extract_v2.py"}


def test_sibling_target_writer_ignores_setup_tasks(seeded):
    engine, ids = seeded
    with engine.connect() as conn:
        sibling = tasks.fetch_sibling_target_writer(conn, ids["alpha"], ids["setup"], "core.dim")
        assert sibling == tasks.SiblingTargetWriter(ids["load"], "SCD1_MERGE")
        assert (
            tasks.fetch_sibling_target_writer(conn, ids["alpha"], ids["load"], "core.dim") is None
        )
        assert tasks.fetch_sibling_target_writer(conn, ids["alpha"], ids["setup"], "x.y") is None


def test_pipeline_detail_reads_its_json_parameters(seeded):
    engine, ids = seeded
    with engine.connect() as conn:
        detail = pipelines.fetch_pipeline_detail(conn, ids["alpha"])
        assert (detail.pipeline_code, detail.sla_in_hours, detail.refresh_type) == (
            "PL_ALPHA",
            2.5,
            "INCREMENTAL",
        )
        assert (detail.retries, detail.tags, detail.catchup) == (3, ["daily"], False)
        assert detail.email_recipients is None
        assert pipelines.fetch_pipeline_detail(conn, ids["beta"]).retries is None
        assert pipelines.fetch_pipeline_handlers(conn, ids["alpha"]) == {
            "PYTHON",
            "SQL",
            "BUSINESS_RULES",
            "EMAIL_ALERT",
        }


# Dependencies


def test_the_pipeline_graph(seeded):
    engine, ids = seeded
    with engine.connect() as conn:
        data = dependencies.fetch_pipeline_graph(conn, ids["alpha"])
    assert sorted(node.task_id for node in data.tasks) == sorted(
        ids[name] for name in ("extract", "setup", "load", "rules", "alert")
    )
    assert TaskEdge(ids["load"], ids["setup"], "ALWAYS") in data.same_pipeline_edges
    assert len(data.same_pipeline_edges) == 4
    assert data.cross_pipeline_task_ids == {ids["load"]}
    load = next(node for node in data.tasks if node.task_id == ids["load"])
    assert load.cross_pipeline_edge_count == 1
    graph = build_graph(data.tasks, data.same_pipeline_edges)
    assert graph.waves()[0] == sorted([ids["extract"], ids["setup"]])


def test_inactive_tasks_in_the_pipeline_graph(seeded):
    engine, ids = seeded
    with engine.begin() as conn:
        # A dependency of an inactive task is left out: that task does not run.
        add_dependency(conn, ids["alpha"], ids["retired"], ids["load"])
        assert len(dependencies.fetch_pipeline_graph(conn, ids["alpha"]).same_pipeline_edges) == 4
        # An active task that waits on an inactive one could never run.
        add_dependency(conn, ids["alpha"], ids["alert"], ids["retired"], "ALWAYS")
        with pytest.raises(
            MetadataError,
            match=r"active tasks depend on inactive ones \(alert depends on old_task\)",
        ):
            dependencies.fetch_pipeline_graph(conn, ids["alpha"])


def test_dependency_edges_for_the_gates(seeded):
    engine, ids = seeded
    with engine.connect() as conn:
        assert dependencies.fetch_pipeline_dependency_edges(conn, ids["alpha"]) == [
            dependencies.PipelineDependencyEdge(
                ids["pipeline_dependency"], ids["beta"], "SUCCESS", "PL_BETA"
            )
        ]
        assert dependencies.fetch_cross_pipeline_task_edges(conn, ids["load"]) == [
            dependencies.CrossPipelineTaskEdge(
                ids["cross"],
                ids["alpha"],
                ids["beta"],
                ids["upstream"],
                "HAS_DATA",
                "PL_BETA.publish",
            )
        ]
        assert dependencies.fetch_cross_pipeline_task_edges(conn, ids["rules"]) == []


def test_business_rules(seeded):
    engine, ids = seeded
    with engine.connect() as conn:
        rules = business_rules.fetch_business_rules_for_task(conn, ids["rules"])
        assert [rule.business_rule_name for rule in rules] == ["first", "second"]
        assert rules[0].business_rule_key_column == "id"
        assert [t.business_rule_name for t in business_rules.fetch_business_rule_targets(conn)] == [
            "first",
            "second",
        ]


# The run log


def test_a_run_is_found_or_started(seeded):
    engine, ids = seeded
    with engine.begin() as conn:
        assert runlog.fetch_active_pipeline_run_id(conn, ids["alpha"]) is None
        run_id = runlog.find_or_create_active_run(conn, ids["alpha"])
    with engine.begin() as conn:
        assert runlog.find_or_create_active_run(conn, ids["alpha"]) == run_id
        assert runlog.fetch_pipeline_run_status(conn, run_id) == RunStatus.IN_PROGRESS
        assert runlog.resolve_run_for_task(conn, ids["alpha"]) == run_id


def test_concurrent_starts_share_one_run(seeded):
    engine, ids = seeded
    results, errors, barrier = [], [], threading.Barrier(4)

    def start():
        try:
            barrier.wait(10)
            with engine.begin() as conn:
                results.append(runlog.find_or_create_active_run(conn, ids["alpha"]))
        except Exception as error:  # pragma: no cover - surfaced by the assertion below
            errors.append(error)

    threads = [threading.Thread(target=start) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(60)
    assert errors == []
    assert len(set(results)) == 1


def test_a_single_task_needs_a_run_to_bind_to(seeded):
    engine, ids = seeded
    with engine.begin() as conn, pytest.raises(RunStateError, match="has no run to bind"):
        runlog.resolve_run_for_task(conn, ids["alpha"])


def test_a_finished_run_is_rebound_only_with_force(seeded):
    engine, ids = seeded
    with engine.begin() as conn:
        run_id = runlog.find_or_create_active_run(conn, ids["alpha"])
        runlog.finalize_pipeline_run(conn, run_id, RunStatus.SUCCESS)
    with engine.begin() as conn:
        with pytest.raises(RunStateError, match=r"already SUCCESS.*or pass --force"):
            runlog.resolve_run_for_task(conn, ids["alpha"])
        with pytest.raises(RunStateError, match="already SUCCESS") as error:
            runlog.resolve_run_for_task(conn, ids["alpha"], mode=Mode.REMOTE)
        assert "--force" not in str(error.value)
        assert runlog.resolve_run_for_task(conn, ids["alpha"], force=True) == run_id


def test_a_task_run_row_is_bound_once_and_retried_in_place(seeded):
    engine, ids = seeded
    with engine.begin() as conn:
        run_id = runlog.find_or_create_active_run(conn, ids["alpha"])
        first = runlog.find_or_create_task_run(conn, ids["extract"], run_id)
        assert first.created and first.status == RunStatus.IN_PROGRESS
        again = runlog.find_or_create_task_run(conn, ids["extract"], run_id)
        assert again == runlog.TaskRunBinding(first.task_run_id, RunStatus.IN_PROGRESS)
        runlog.finish_task_run(
            conn,
            first.task_run_id,
            status=RunStatus.FAILED,
            source_count=10,
            target_count=7,
            error_message="boom",
            task_log="tail",
        )
        assert runlog.fetch_task_run_result(conn, first.task_run_id) == runlog.TaskRunResult(
            RunStatus.FAILED, "boom", 1
        )
        assert runlog.fetch_task_run_status(conn, ids["extract"], run_id) == RunStatus.FAILED
        assert runlog.fetch_task_run_status(conn, ids["load"], run_id) is None
        assert runlog.begin_attempt(conn, first.task_run_id) == 2
        cleared = conn.execute(
            text(
                "SELECT STATUS AS status, SOURCE_COUNT AS source_count, "
                "ERROR_MESSAGE AS error_message, TASK_LOG AS task_log, END_DATE AS end_date "
                "FROM AUD_TASK_RUN_LOG WHERE TASK_RUN_ID = :id"
            ),
            {"id": first.task_run_id},
        ).one()
        assert tuple(cleared) == (RunStatus.IN_PROGRESS, None, None, None, None)


def test_concurrent_binds_share_one_row(seeded):
    engine, ids = seeded
    with engine.begin() as conn:
        run_id = runlog.find_or_create_active_run(conn, ids["alpha"])
    results, errors, barrier = [], [], threading.Barrier(4)

    def bind():
        try:
            barrier.wait(10)
            with engine.begin() as conn:
                results.append(runlog.find_or_create_task_run(conn, ids["load"], run_id))
        except Exception as error:  # pragma: no cover - surfaced by the assertion below
            errors.append(error)

    threads = [threading.Thread(target=bind) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(60)
    assert errors == []
    assert len({binding.task_run_id for binding in results}) == 1
    assert sum(binding.created for binding in results) == 1


def test_run_state_and_statuses(seeded):
    engine, ids = seeded
    with engine.begin() as conn:
        run_id = runlog.find_or_create_active_run(conn, ids["alpha"])
        binding = runlog.find_or_create_task_run(conn, ids["extract"], run_id)
        runlog.finish_task_run(conn, binding.task_run_id, status=RunStatus.SUCCESS, target_count=5)
        state = runlog.fetch_run_state(conn, run_id, [ids["extract"], ids["load"]])
        assert state == {ids["extract"]: TaskRunState(RunStatus.SUCCESS, 5)}
        assert runlog.fetch_run_state(conn, run_id, []) == {}
        statuses = {
            s.task_code: s for s in runs.fetch_task_statuses_for_run(conn, ids["alpha"], run_id)
        }
        assert statuses["extract"].status == RunStatus.SUCCESS
        assert statuses["load_dim"].status == runs.PENDING
        assert statuses["load_dim"].attempt_count == 1


def test_failure_watch_reads_the_latest_error(seeded):
    engine, ids = seeded
    with engine.begin() as conn:
        run_id = runlog.find_or_create_active_run(conn, ids["alpha"])
        binding = runlog.find_or_create_task_run(conn, ids["load"], run_id)
        runlog.finish_task_run(
            conn, binding.task_run_id, status=RunStatus.FAILED, error_message="merge failed"
        )
        assert tasks.fetch_failure_watch_messages(conn, ids["alert"]) == [
            tasks.FailureWatchMessage("load_dim", "merge failed")
        ]
        assert tasks.fetch_failure_watch_messages(conn, ids["extract"]) == []


@pytest.mark.parametrize(
    ("hours_ago", "sla", "expected"),
    [(1, 2.0, SlaStatus.MET), (3, 2.0, SlaStatus.BREACHED), (1, None, None)],
)
def test_finalizing_a_run_judges_its_sla(seeded, hours_ago, sla, expected):
    engine, ids = seeded
    with engine.begin() as conn:
        run_id = runlog.find_or_create_active_run(conn, ids["alpha"])
        conn.execute(
            text(
                "UPDATE AUD_PIPELINES_RUN_LOG SET START_DATE = :start WHERE PIPELINE_RUN_ID = :id"
            ),
            {"start": datetime.now(UTC) - timedelta(hours=hours_ago), "id": run_id},
        )
        result = runlog.finalize_pipeline_run(conn, run_id, RunStatus.SUCCESS, sla_in_hours=sla)
        row = conn.execute(
            text(
                "SELECT STATUS AS status, SLA_STATUS AS sla_status, END_DATE AS end_date "
                "FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID = :id"
            ),
            {"id": run_id},
        ).one()
    assert (row.status, row.sla_status) == (RunStatus.SUCCESS, expected)
    assert row.end_date is not None
    if expected is None:
        assert result is None
    else:
        assert result.status == expected
        assert result.elapsed_hours == pytest.approx(hours_ago, abs=0.01)
        assert result.describe().startswith(f"SLA of 2 h {expected}")


def test_losing_the_race_to_start_a_run_reads_back_the_winner(seeded, monkeypatch):
    engine, ids = seeded
    with engine.begin() as conn:
        winner = runlog.find_or_create_active_run(conn, ids["alpha"])
    real = runlog.fetch_active_pipeline_run_id
    calls = []

    def first_lookup_misses(conn, pipeline_id):
        calls.append(pipeline_id)
        return None if len(calls) == 1 else real(conn, pipeline_id)

    monkeypatch.setattr(runlog, "fetch_active_pipeline_run_id", first_lookup_misses)
    with engine.begin() as conn:
        assert runlog.find_or_create_active_run(conn, ids["alpha"]) == winner
    monkeypatch.setattr(runlog, "fetch_active_pipeline_run_id", lambda conn, pipeline_id: None)
    with engine.begin() as conn, pytest.raises(RunStateError, match="no IN-PROGRESS run exists"):
        runlog.find_or_create_active_run(conn, ids["alpha"])


def test_losing_the_race_to_bind_a_task_reads_back_the_winner(seeded, monkeypatch):
    engine, ids = seeded
    with engine.begin() as conn:
        run_id = runlog.find_or_create_active_run(conn, ids["alpha"])
        winner = runlog.find_or_create_task_run(conn, ids["load"], run_id)
    real_statement = runlog.statement
    seen = []

    def hide_first_lookup(conn, name):
        if name == "task_run":
            seen.append(name)
            if len(seen) == 1:
                return text(f"{real_statement(conn, 'task_run').text} AND 1 = 0")
        return real_statement(conn, name)

    monkeypatch.setattr(runlog, "statement", hide_first_lookup)
    with engine.begin() as conn:
        assert runlog.find_or_create_task_run(conn, ids["load"], run_id) == runlog.TaskRunBinding(
            winner.task_run_id, RunStatus.IN_PROGRESS
        )


@pytest.mark.unit
def test_elapsed_hours_reads_a_naive_start_as_utc():
    now = datetime(2026, 1, 1, 12, tzinfo=UTC)
    assert runlog.elapsed_hours(datetime(2026, 1, 1, 9), now) == 3
