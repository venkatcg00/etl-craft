"""Admitted definitions remain exact across edits, retries and independent controllers."""

from sqlalchemy import text

from etl_craft.core.definitions import Definition, using
from etl_craft.engine.repository.dependencies import fetch_pipeline_graph
from etl_craft.engine.repository.tasks import fetch_task_parameters
from fixtures.metadata import add_pipeline, add_task, start_run


def test_admission_records_an_immutable_definition(engine_db):
    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "T", "PYTHON", SCRIPT_FILE="before.py")
        run = start_run(conn, pipeline)
        stored = conn.execute(
            text(
                "SELECT METADATA_SNAPSHOT AS snapshot,METADATA_VERSION AS "
                "version FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID=:id"
            ),
            {"id": run},
        ).one()
        assert len(stored.version) == 64
        definition = Definition.read(stored.snapshot)
        conn.execute(
            text(
                "UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE='after.py' WHERE "
                "TASK_ID=:id AND PARAMETER_NAME='SCRIPT_FILE'"
            ),
            {"id": task},
        )
        add_task(conn, pipeline, "LATER", "PYTHON")
        with using(definition):
            assert fetch_task_parameters(conn, task)["SCRIPT_FILE"] == "before.py"
            assert [t.task_code for t in fetch_pipeline_graph(conn, pipeline).tasks] == ["T"]
        assert fetch_task_parameters(conn, task)["SCRIPT_FILE"] == "after.py"


def test_publication_pins_bytes_and_admission_refuses_an_empty_store(engine_db, tmp_path):
    import base64
    from dataclasses import replace

    import pytest

    from etl_craft.config.object_models import Artifacts
    from etl_craft.core.actor import SYSTEM_ACTOR
    from etl_craft.core.errors import ConfigurationError
    from etl_craft.engine import transitions
    from etl_craft.engine.bundles import Bundle
    from etl_craft.services.operations.bundles import PublishBundle, publish
    from etl_craft.services.operations.context import OperationContext
    from etl_craft.services.operations.snapshots import run_view

    config = replace(engine_db.config, artifacts=Artifacts((tmp_path / "store").as_uri()))
    engine_db.engine.update_execution_options(bundles_enabled=True)
    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        add_task(conn, pipeline, "T", "PYTHON", SCRIPT_FILE="python_scripts/a.py")
        with pytest.raises(ConfigurationError, match="bundle publish"):
            transitions.create_run(conn, pipeline, SYSTEM_ACTOR)
        assert (
            conn.execute(text("SELECT count(*) AS count FROM AUD_PIPELINES_RUN_LOG")).scalar_one()
            == 0
        )
    from etl_craft.core.enums import ApiRole

    ctx = OperationContext(engine_db.engine, config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
    files = {
        "craft-connector.yml": b"Secrets:\n  Source_type: ENVIRONMENT\nEngine:\n "
        b" dev:\n    Jdbc_url: jdbc:sqlite:engine.db\n    Schema: main\n",
        "python_scripts/a.py": b"version = 1\n",
    }
    first = publish(
        ctx,
        PublishBundle(
            files={name: base64.b64encode(data).decode() for name, data in files.items()}
        ),
    )
    with engine_db.engine.begin() as conn:
        run = start_run(conn, pipeline)
        view = run_view(conn, pipeline, run)
        assert view.bundle_id == first.bundle_id
        assert view.newer_metadata_available is False
        conn.execute(
            text("UPDATE CFG_TASKS SET ACTIVE_FLAG='N' WHERE PIPELINE_ID=:id"), {"id": pipeline}
        )
        assert run_view(conn, pipeline, run).newer_metadata_available is True
    files["python_scripts/a.py"] = b"version = 2\n"
    second = publish(
        ctx,
        PublishBundle(
            files={name: base64.b64encode(data).decode() for name, data in files.items()}
        ),
    )
    assert second.bundle_id != first.bundle_id
    with engine_db.engine.connect() as conn:
        pinned = Bundle.load(conn, run_view(conn, pipeline, run).bundle_id)
    assert pinned.read(config, "python_scripts/a.py") == b"version = 1\n"
    assert (
        pinned.materialize(config, cache=tmp_path / "cache")
        .joinpath("python_scripts/a.py")
        .read_bytes()
        == b"version = 1\n"
    )


def test_a_restart_reads_the_same_graph_and_pins_cannot_be_edited(engine_db):
    import pytest
    from sqlalchemy.exc import DBAPIError

    from etl_craft.core.actor import SYSTEM_ACTOR
    from etl_craft.engine import transitions
    from etl_craft.engine.run_definitions import for_run
    from fixtures.metadata import add_dependency

    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        first = add_task(conn, pipeline, "A", "PYTHON")
        second = add_task(conn, pipeline, "B", "PYTHON")
        add_dependency(conn, pipeline, second, first)
        run = start_run(conn, pipeline)
        conn.execute(text("UPDATE CFG_TASKS SET ACTIVE_FLAG='N' WHERE TASK_ID=:id"), {"id": second})
        conn.execute(
            text("UPDATE CFG_TASK_DEPENDENCY SET ACTIVE_FLAG='N' WHERE PIPELINE_ID=:id"),
            {"id": pipeline},
        )
        add_task(conn, pipeline, "C", "PYTHON")
    with for_run(engine_db.engine, run), engine_db.engine.connect() as conn:
        graph = fetch_pipeline_graph(conn, pipeline)
        assert {task.task_code for task in graph.tasks} == {"A", "B"}
        assert len(graph.same_pipeline_edges) == 1
    with pytest.raises(DBAPIError, match="immutable"), engine_db.engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE AUD_PIPELINES_RUN_LOG SET METADATA_VERSION='other' WHERE "
                "PIPELINE_RUN_ID=:id"
            ),
            {"id": run},
        )
    with engine_db.engine.begin() as conn:
        transitions.finish_run(conn, run, "SUCCESS", SYSTEM_ACTOR)
        next_run = start_run(conn, pipeline)
    with for_run(engine_db.engine, next_run), engine_db.engine.connect() as conn:
        assert {task.task_code for task in fetch_pipeline_graph(conn, pipeline).tasks} == {"A", "C"}


def test_admitted_target_keeps_live_cross_pipeline_protection(engine_db):
    from etl_craft.engine.queries import statement

    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(
            conn, pipeline, "merge", SQL_ACTION="SCD1_MERGE", TARGET_OBJECT="warehouse.main.shared"
        )
        run = start_run(conn, pipeline)
        snapshot = conn.execute(
            text(
                "SELECT METADATA_SNAPSHOT AS snapshot FROM AUD_PIPELINES_RUN_LOG "
                "WHERE PIPELINE_RUN_ID=:id"
            ),
            {"id": run},
        ).scalar_one()
        other = add_pipeline(conn, "OTHER")
        add_task(
            conn, other, "history", SQL_ACTION="SCD2_MERGE", TARGET_OBJECT="warehouse.main.shared"
        )
        with using(Definition.read(snapshot, run)):
            rows = conn.execute(
                statement(conn, "sql_task_state"),
                {
                    "task_id": task,
                    "pipeline_id": pipeline,
                    "pipeline_run_id": run,
                    "sql_action": "SCD1_MERGE",
                    "catalog": "warehouse",
                },
            ).all()
        assert rows[0].merge_conflict == "SCD2_MERGE"


def test_rule_reporting_uses_admitted_rules_after_they_are_disabled(engine_db):
    from etl_craft.core.enums import RunStatus
    from etl_craft.engine import transitions
    from etl_craft.engine.task_store import LocalTaskStore, RuleEnding, TaskIdentity
    from etl_craft.execution.context import build_task_context
    from etl_craft.handlers.resources import LocalLocker
    from fixtures.metadata import add_table

    engine, config = engine_db.engine, engine_db.config
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "rules", "BUSINESS_RULES")
        target = add_table(conn, "warehouse.main.original")
        rule = conn.execute(
            text(
                "INSERT INTO CFG_BUSINESS_RULES (BUSINESS_RULE_NAME,PIPELINE_ID,TASK_ID,"
                "BUSINESS_RULE_SQL,BUSINESS_RULE_TYPE,BUSINESS_RULE_KEY_COLUMN,TARGET_TABLE_ID,"
                "SEQUENCE_NUMBER) VALUES ('check',:pipeline,:task,'SELECT 7 AS id','REPORT',"
                "'id',:target,1) RETURNING BUSINESS_RULE_ID AS id"
            ),
            {"pipeline": pipeline, "task": task, "target": target},
        ).scalar_one()
        run = start_run(conn, pipeline)
        import json

        snapshot = json.loads(
            conn.execute(
                text(
                    "SELECT METADATA_SNAPSHOT AS snapshot FROM AUD_PIPELINES_RUN_LOG "
                    "WHERE PIPELINE_RUN_ID=:id"
                ),
                {"id": run},
            ).scalar_one()
        )
        for name, table in snapshot.items():
            for row in table["rows"]:
                assert set(row) == set(table["columns"]), (name, set(row), set(table["columns"]))
        binding = transitions.find_or_create_task_run(conn, task, run)
        conn.execute(text("UPDATE CFG_BUSINESS_RULES SET ACTIVE_FLAG='N'"))
        conn.execute(text("UPDATE CFG_TASKS SET ACTIVE_FLAG='N' WHERE TASK_ID=:id"), {"id": task})
        other = add_pipeline(conn, "OTHER")
        add_task(conn, other, "new", "EMAIL_ALERT")
        start_run(conn, other)
    context = build_task_context(engine, config, binding.task_run_id)
    store = LocalTaskStore(
        engine,
        config,
        TaskIdentity(pipeline, task, run, binding.task_run_id, None, None),
        LocalLocker(engine, config),
        definition=context._definition,
    )
    alert = store.alert("OTHER")
    assert [row.task_code for row in alert.statuses] == ["rules"]
    assert [row.task_code for row in alert.digests[0].tasks] == ["new"]
    recorded = store.begin_rule(rule)
    assert store.rule_keys(rule) == set()
    store.end_rule(RuleEnding(rule, recorded.business_rule_run_id, RunStatus.SUCCESS, ("7",)))
    assert store.rule_keys(rule) == {"7"}
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT TARGET_CATALOG_NAME AS catalog, "
                "TARGET_SCHEMA_NAME AS schema_name, TARGET_TABLE_NAME AS name "
                "FROM AUD_BUSINESS_RULES_RESULTS"
            )
        ).one()
    assert dict(result._mapping) == {
        "catalog": "warehouse",
        "schema_name": "main",
        "name": "original",
    }


def test_a_handler_edit_cannot_change_retry_kind_or_bypass_resource_claims(engine_db):
    from datetime import UTC, datetime, timedelta

    from etl_craft.core.actor import current_actor
    from etl_craft.core.enums import AttemptStatus, SlotKind
    from etl_craft.core.resources import MIB, TASK_COST
    from etl_craft.engine import transitions
    from etl_craft.engine.agent_transitions import claim_with_capacity
    from etl_craft.engine.run_definitions import for_run

    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "work", "PYTHON")
        run = start_run(conn, pipeline)
        binding = transitions.find_or_create_task_run(conn, task, run)
        with for_run(conn, run):
            first = transitions.queue_attempt(conn, binding.task_run_id, current_actor())
            transitions.claim_attempt(
                conn,
                first,
                current_actor(),
                owner="first",
                lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
            )
            transitions.finish_attempt(
                conn, first, AttemptStatus.FAILED, current_actor(), owner="first"
            )
        conn.execute(
            text("UPDATE CFG_TASKS SET HANDLER='EVENT_WAIT' WHERE TASK_ID=:id"), {"id": task}
        )
        with for_run(conn, run):
            retry = transitions.queue_attempt(conn, binding.task_run_id, current_actor())
        kind = conn.execute(
            text("SELECT SLOT_KIND AS kind FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"),
            {"id": retry},
        ).scalar_one()
        assert kind == SlotKind.PYTHON
        capacity = {
            "owner": "retry",
            "lease": datetime.now(UTC) + timedelta(seconds=60),
            "local_cpu": 0,
            "local_memory_mb": 0,
        }
        assert not claim_with_capacity(conn, retry, current_actor(), capacity, wait=True)
        capacity.update(local_cpu=8, local_memory_mb=8192)
        assert claim_with_capacity(conn, retry, current_actor(), capacity, wait=True)
        row = conn.execute(
            text(
                "SELECT RESERVED_CPU AS cpu, RESERVED_MEMORY_MB AS memory_mb "
                "FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"
            ),
            {"id": retry},
        ).one()
        assert row.cpu == TASK_COST[SlotKind.PYTHON][0]
        assert row.memory_mb == TASK_COST[SlotKind.PYTHON][1] // MIB


def test_moving_a_task_keeps_claims_and_wait_reasons_under_its_admitted_pipeline(engine_db):
    from datetime import UTC, datetime, timedelta

    from etl_craft.core.actor import current_actor
    from etl_craft.core.enums import AttemptStatus
    from etl_craft.engine import transitions
    from etl_craft.engine.agent_transitions import claim_with_capacity
    from etl_craft.engine.placement import record_wait_reasons
    from etl_craft.engine.run_definitions import for_run

    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        conn.execute(
            text("UPDATE CFG_PIPELINES SET MAX_PARALLEL_TASKS=1 WHERE PIPELINE_ID=:id"),
            {"id": pipeline},
        )
        moving = add_task(conn, pipeline, "moving", "PYTHON")
        resident = add_task(conn, pipeline, "resident", "PYTHON")
        run = start_run(conn, pipeline)
        with for_run(conn, run):
            resident_run = transitions.find_or_create_task_run(conn, resident, run)
            busy = transitions.queue_attempt(conn, resident_run.task_run_id, current_actor())
            transitions.claim_attempt(
                conn,
                busy,
                current_actor(),
                owner="resident",
                lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
            )
            moving_run = transitions.find_or_create_task_run(conn, moving, run)
            waiting = transitions.queue_attempt(conn, moving_run.task_run_id, current_actor())
        other = add_pipeline(conn, "OTHER")
        conn.execute(
            text("UPDATE CFG_TASKS SET PIPELINE_ID=:other WHERE TASK_ID=:id"),
            {"other": other, "id": moving},
        )
        capacity = {"owner": "moving", "lease": datetime.now(UTC) + timedelta(seconds=60)}
        assert not claim_with_capacity(conn, waiting, current_actor(), capacity, wait=True)
        record_wait_reasons(conn, None, attempt_id=waiting)
        reason = conn.execute(
            text("SELECT ERROR_MESSAGE AS reason FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"),
            {"id": waiting},
        ).scalar_one()
        assert reason == "pipeline P at MAX_PARALLEL_TASKS=1"
        transitions.finish_attempt(
            conn, busy, AttemptStatus.SUCCESS, current_actor(), owner="resident"
        )
        assert claim_with_capacity(conn, waiting, current_actor(), capacity, wait=True)
