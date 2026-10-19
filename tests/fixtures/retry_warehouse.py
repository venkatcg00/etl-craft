"""Real SQL attempts and connection interruptions shared by local and cloud retry tests."""

from datetime import UTC, datetime
from pathlib import Path

import yaml
from sqlalchemy import text

from etl_craft.config import load_config
from etl_craft.core.actor import current_actor
from etl_craft.core.enums import AttemptStatus, FailureClass, RetryOn, RunStatus
from etl_craft.engine import transitions
from etl_craft.execution.attempt_types import ChildOptions
from etl_craft.execution.runner import run_task
from etl_craft.warehouse.connection import warehouse_dialect


def save_project(world):
    """Make a fixture's connection profiles available to its real task process."""
    config = world.config

    def profile(value, schema):
        fields = {
            "jdbc_url": value.jdbc_url,
            "user": value.user,
            "auth_mode": str(value.auth_mode),
            "schema": schema,
            **value.extra,
        }
        if "secret_var" in fields:
            fields["secret"] = fields.pop("secret_var")
        return fields

    raw = {
        "Secrets": {"Source_type": "environment"},
        "Orchestration": {"Mode": "local", "Task_timeout_seconds": 60},
        "Engine": {"dev": profile(config.engine, "main")},
        "Warehouse": {
            "Table_format": str(config.warehouse_table_format),
            "dev": profile(config.warehouse, world.schema),
        },
    }
    if config.warehouse.jdbc_url.startswith("jdbc:duckdb:"):
        raw["Warehouse"]["Name"] = "DuckDB"
    config.config_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    world.config = load_config(config.config_path)


def attempt_rows(world, task_id):
    with world.engine_db.connect() as conn:
        return conn.execute(
            text(
                "SELECT a.STATUS AS status, a.FAILURE_CLASS AS failure_class, "
                "a.FAILURE_SOURCE AS failure_source, a.FAILURE_CODE AS failure_code, "
                "a.RETRYABLE AS retryable, a.RETRY_ON AS retry_on "
                "FROM AUD_TASK_ATTEMPTS a JOIN AUD_TASK_RUN_LOG t ON t.TASK_RUN_ID=a.TASK_RUN_ID "
                "WHERE t.TASK_ID=:task ORDER BY a.ATTEMPT_NUMBER"
            ),
            {"task": task_id},
        ).all()


def check_syntax_error(world):
    context = world.task(
        "bad_syntax",
        SQL_ACTION="CREATE_TABLE",
        TARGET_OBJECT="never_created",
        SOURCE_SQL="SELECT 1 +",
        RETRIES="3",
        RETRY_DELAY_SECONDS="0",
    )
    with world.engine_db.begin() as conn:
        transitions.queue_attempt(
            conn,
            context.task_run_id,
            current_actor(),
            not_before=datetime(2000, 1, 1, tzinfo=UTC),
        )
    outcome = run_task(world.engine_db, world.config, "P", "bad_syntax")
    assert outcome.status == RunStatus.FAILED, outcome.message
    rows = attempt_rows(world, context.task_id)
    assert len(rows) == 1
    assert rows[0].failure_class == FailureClass.PERMANENT
    assert not rows[0].retryable
    assert rows[0].failure_source not in {None, "TaskProcess", "HandlerError"}
    if warehouse_dialect(world.config).spec.sqlalchemy_name in {
        "postgresql",
        "snowflake",
        "databricks",
    }:
        assert rows[0].failure_code
    assert "never_created" not in world.tables()


def check_connection_retry(world, monkeypatch, policy=RetryOn.TRANSIENT):
    """Interrupt the first live SQL connection; the next real task process must succeed."""
    context = world.task(
        "disconnect",
        SQL_ACTION="CREATE_TABLE",
        TARGET_OBJECT="after_disconnect",
        SOURCE_SQL="SELECT 1 AS id",
        RETRIES="1",
        RETRY_ON=policy,
        RETRY_DELAY_SECONDS="0",
    )
    with world.engine_db.begin() as conn:
        queued = transitions.queue_attempt(
            conn,
            context.task_run_id,
            current_actor(),
            not_before=datetime(2000, 1, 1, tzinfo=UTC),
        )
        transitions.set_attempt_retry_policy(conn, queued, policy)
    monkeypatch.setenv("PYTHONPATH", str(Path(__file__).parents[1]))
    outcome = run_task(
        world.engine_db,
        world.config,
        "P",
        "disconnect",
        child=ChildOptions(module="fixtures.retry_child"),
    )
    rows = attempt_rows(world, context.task_id)
    assert rows[0].failure_class == FailureClass.TRANSIENT
    if policy == RetryOn.NONE:
        assert outcome.status == RunStatus.FAILED, outcome.message
        assert len(rows) == 1 and not rows[0].retryable
        return
    assert outcome.status == RunStatus.SUCCESS, outcome.message
    assert [row.status for row in rows] == [AttemptStatus.FAILED, AttemptStatus.SUCCESS]
    assert rows[0].retryable
    assert world.rows(f"SELECT id FROM {world.name('after_disconnect')}") == [(1,)]
