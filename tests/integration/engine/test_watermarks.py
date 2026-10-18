"""SQL ranges survive failure; offsets, success and consumption still commit atomically."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from etl_craft.core.actor import current_actor
from etl_craft.core.errors import (
    InjectedFaultError,
    RunRefusedError,
    StaleTransitionError,
    UsageError,
)
from etl_craft.engine import transitions as tr
from etl_craft.engine.repository.offsets import StoredOffset, fetch_task_offset, save_task_offset
from etl_craft.engine.runlog import RunSelector
from etl_craft.execution.interventions import reset_watermark
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.status import explain_task
from fixtures.auth import service_token
from fixtures.metadata import add_pipeline, add_task


@pytest.fixture
def watermarked(engine_db):
    engine = engine_db.engine
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(
            conn,
            pipeline,
            "T",
            "SQL",
            SQL_ACTION="APPEND_TABLE",
            TARGET_OBJECT="s.events",
            SOURCE_SQL="SELECT id FROM raw.events WHERE id > $$watermark",
            WATERMARK_COLUMN="id",
            WATERMARK_TYPE="NUMBER",
            WATERMARK_START="0",
        )
        run = tr.create_run(conn, pipeline, current_actor())
        summary = tr.find_or_create_task_run(conn, task, run).task_run_id
        attempt = tr.queue_attempt(conn, summary, current_actor())
        tr.claim_attempt(
            conn,
            attempt,
            current_actor(),
            owner="worker",
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
        )
        tr.start_attempt(conn, attempt, current_actor(), owner="worker")
        save_task_offset(conn, task, StoredOffset("NUMBER", "2"))
    return engine, task, run, summary, attempt


def test_failed_ranges_and_success_offsets_are_atomic(engine_db, watermarked, monkeypatch):
    engine, task, run, summary, attempt = watermarked
    with engine.begin() as conn:
        tr.record_watermark_range(conn, summary, "2", "3", attempt_id=attempt, owner="worker")
        tr.finish_attempt(conn, attempt, "FAILED", current_actor(), owner="worker")
        assert fetch_task_offset(conn, task).offset_value == "2"
        retry = tr.queue_attempt(conn, summary, current_actor())
        tr.claim_attempt(
            conn,
            retry,
            current_actor(),
            owner="worker",
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
        )
        tr.start_attempt(conn, retry, current_actor(), owner="worker")
        tr.record_watermark_range(conn, summary, "2", "4", attempt_id=retry, owner="worker")
    monkeypatch.setenv("ETL_CRAFT_FAULT", "script.after_offset")
    with pytest.raises(InjectedFaultError), engine.begin() as conn:
        tr.finish_attempt(
            conn,
            retry,
            "SUCCESS",
            current_actor(),
            owner="worker",
            offset=StoredOffset("NUMBER", "4"),
        )
    with engine.connect() as conn:
        assert fetch_task_offset(conn, task).offset_value == "2"
        assert (
            conn.execute(
                text("SELECT STATUS FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"), {"id": retry}
            ).scalar_one()
            == "RUNNING"
        )
    monkeypatch.delenv("ETL_CRAFT_FAULT")
    with engine.begin() as conn:
        tr.finish_attempt(
            conn,
            retry,
            "SUCCESS",
            current_actor(),
            owner="worker",
            offset=StoredOffset("NUMBER", "4"),
        )
        assert fetch_task_offset(conn, task).offset_value == "4"
    explanation = explain_task(
        OperationContext(engine, engine_db.config, current_actor()),
        "P",
        "T",
        selector=RunSelector(run_id=run),
    )
    assert (explanation.task.watermark_from, explanation.task.watermark_to) == ("2", "4")
    assert [(a.watermark_from, a.watermark_to) for a in explanation.task.attempts] == [
        ("2", "3"),
        ("2", "4"),
    ]


def test_stale_owners_cannot_record_ranges(watermarked):
    engine, _, _, summary, attempt = watermarked
    with engine.begin() as conn:
        with pytest.raises(StaleTransitionError):
            tr.record_watermark_range(conn, summary, "2", "3", attempt_id=attempt, owner="wrong")
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET LEASE_EXPIRES_AT=:expired WHERE ATTEMPT_ID=:id"),
            {"expired": datetime.now(UTC) - timedelta(seconds=1), "id": attempt},
        )
        with pytest.raises(StaleTransitionError):
            tr.record_watermark_range(conn, summary, "2", "3", attempt_id=attempt, owner="worker")
        row = conn.execute(
            text("SELECT WATERMARK_FROM, WATERMARK_TO FROM AUD_TASK_RUN_LOG WHERE TASK_RUN_ID=:id"),
            {"id": summary},
        ).one()
        assert tuple(row) == (None, None)


def test_reset_refuses_active_attempts_and_records_old_and_new(engine_db, watermarked):
    engine, task, run, _, attempt = watermarked
    # Reset does not connect to the warehouse; its profile only resolves the target lock.
    from dataclasses import replace

    from etl_craft.config import ConnectionProfile

    config = replace(
        engine_db.config,
        warehouse=ConnectionProfile(
            "WAREHOUSE", "local", "jdbc:duckdb:warehouse.duckdb", "", "none"
        ),
    )
    selector = RunSelector(run_id=run)
    with pytest.raises(RunRefusedError, match="active attempt"):
        reset_watermark(engine, config, "P", "T", "1", "reload", selector=selector)
    with engine.begin() as conn:
        tr.finish_attempt(conn, attempt, "FAILED", current_actor(), owner="worker")
    done = reset_watermark(engine, config, "P", "T", "1", "reload", selector=selector)
    assert "2 -> 1 (NUMBER)" in done.message
    with engine.connect() as conn:
        assert fetch_task_offset(conn, task).offset_value == "1"
        record = conn.execute(
            text("SELECT PREVIOUS_MESSAGE, REASON FROM AUD_RUN_INTERVENTIONS WHERE TASK_ID=:id"),
            {"id": task},
        ).one()
        assert tuple(record) == (done.message, "reload")
    with pytest.raises(UsageError, match="reason"):
        reset_watermark(engine, config, "P", "T", "1", "", selector=selector)


def test_cli_watermark_reset_does_not_change_run_status(metadata_project, capsys, monkeypatch):
    monkeypatch.setenv("ETL_CRAFT_TOKEN", service_token(metadata_project.engine))
    from etl_craft.cli import main

    p = metadata_project
    with p.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(
            conn,
            pipeline,
            "T",
            "SQL",
            SQL_ACTION="APPEND_TABLE",
            TARGET_OBJECT="s.events",
            SOURCE_SQL="SELECT id FROM raw.events WHERE id > $$watermark",
            WATERMARK_COLUMN="id",
            WATERMARK_TYPE="NUMBER",
            WATERMARK_START="0",
        )
        run = tr.create_run(conn, pipeline, current_actor())
        save_task_offset(conn, task, StoredOffset("NUMBER", "4"))
        tr.finish_run(conn, run, "SUCCESS", current_actor())
    args = [
        "mark",
        "--config",
        str(p.root / "craft-connector.yml"),
        "--pipeline_code",
        "P",
        "--task_code",
        "T",
        "--run-id",
        str(run),
        "--watermark",
        "1",
        "--reason",
        "reload",
    ]
    assert main(args) == 0
    assert "4 -> 1 (NUMBER)" in capsys.readouterr().out
    with p.engine.connect() as conn:
        assert fetch_task_offset(conn, task).offset_value == "1"
        assert (
            conn.execute(
                text("SELECT STATUS FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID=:id"),
                {"id": run},
            ).scalar_one()
            == "SUCCESS"
        )
    with pytest.raises(SystemExit) as error:
        main([*args, "--status", "FAILED"])
    assert error.value.code == 2


def test_real_task_processes_commit_each_sql_watermark(metadata_project):
    from etl_craft.execution.pipeline import run_pipeline
    from etl_craft.services.operations.snapshots import task_run_view
    from etl_craft.warehouse.connection import open_warehouse
    from fixtures.metadata import add_dependency

    p = metadata_project
    with open_warehouse(p.config, p.engine) as warehouse, warehouse.begin() as conn:
        conn.execute(text("CREATE SCHEMA s"))
        conn.execute(text("CREATE TABLE s.feed(id BIGINT)"))
        conn.execute(text("INSERT INTO s.feed VALUES (1), (2)"))
    with p.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        setup = add_task(
            conn,
            pipeline,
            "setup",
            "SQL",
            SQL_ACTION="SETUP_TABLE",
            TARGET_OBJECT="s.events",
            SOURCE_SQL="SELECT id FROM s.feed",
            SETUP_FOR="APPEND_TABLE",
        )
        task = add_task(
            conn,
            pipeline,
            "load",
            "SQL",
            SQL_ACTION="APPEND_TABLE",
            TARGET_OBJECT="s.events",
            SOURCE_SQL="SELECT id FROM s.feed WHERE id > $$watermark",
            WATERMARK_COLUMN="id",
            WATERMARK_TYPE="NUMBER",
            WATERMARK_START="0",
        )
        add_dependency(conn, pipeline, task, setup)
    first = run_pipeline(p.engine, p.config, "P")
    assert first.status == "SUCCESS", first.message
    with open_warehouse(p.config, p.engine) as warehouse, warehouse.begin() as conn:
        conn.execute(text("INSERT INTO s.feed VALUES (3)"))
    second = run_pipeline(p.engine, p.config, "P")
    assert second.status == "SUCCESS", second.message
    with p.engine.connect() as conn:
        assert fetch_task_offset(conn, task).offset_value == "3"
        for run, expected in ((first, ("0", "2")), (second, ("2", "3"))):
            row = task_run_view(conn, pipeline, pipeline_run_id=run.pipeline_run_id, task_id=task)
            assert (row.watermark_from, row.watermark_to) == expected
            assert (row.attempts[0].watermark_from, row.attempts[0].watermark_to) == expected
    with open_warehouse(p.config, p.engine) as warehouse, warehouse.connect() as conn:
        assert list(conn.execute(text("SELECT id FROM s.events ORDER BY id"))) == [(1,), (2,), (3,)]
