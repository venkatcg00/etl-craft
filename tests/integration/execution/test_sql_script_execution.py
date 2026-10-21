"""SQL scripts are ordinary dependent tasks with durable failure and statement logs."""

import logging

import pytest
import yaml
from sqlalchemy import text

from etl_craft.config import load_config
from etl_craft.core.enums import Handler, RunStatus
from etl_craft.execution.pipeline import run_pipeline
from etl_craft.services.catalog.collect import build_catalog
from etl_craft.services.doctor import Status
from etl_craft.services.lineage import collect
from etl_craft.services.validate import validate
from etl_craft.warehouse.connection import build_warehouse_engine
from fixtures.engine_db import profile_block
from fixtures.metadata import add_dependency, add_pipeline, add_task


@pytest.fixture
def project(engine_db, tmp_path):
    root = (engine_db.config.config_path or tmp_path / "craft-connector.yml").parent
    root.mkdir(exist_ok=True, parents=True)
    (root / "sql_scripts").mkdir(exist_ok=True)
    (root / "python_scripts").mkdir(exist_ok=True)
    block = profile_block(engine_db.config.engine, engine_db.config.engine.schema or "main")
    config_file = root / "craft-connector.yml"
    config_file.write_text(
        yaml.safe_dump(
            {
                "Secrets": {"Source_type": "ENVIRONMENT"},
                "Engine": {"dev": block},
                "Warehouse": {
                    "dev": {"Jdbc_url": "jdbc:duckdb:warehouse.duckdb", "Schema": "main"}
                },
            },
            sort_keys=False,
        ),
        "utf-8",
    )
    loggers = [logging.getLogger("etl_craft"), logging.getLogger()]
    saved = [(list(log.handlers), log.level) for log in loggers]
    yield engine_db.engine, load_config(config_file)
    for log, (handlers, level) in zip(loggers, saved, strict=True):
        log.handlers[:] = handlers
        log.setLevel(level)


@pytest.mark.parametrize("log_level", ["INFO", "ERROR"])
def test_python_managed_sql_and_unmanaged_post_step_follow_dependencies(project, log_level):
    engine, config = project
    (config.python_scripts_dir / "pre.py").write_text(
        """
from etl_craft.scripting import ScriptResult

def run(task):
    with task.warehouse() as engine, engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE main.source (id INTEGER)")
        conn.exec_driver_sql("INSERT INTO main.source VALUES (7)")
    return ScriptResult(row_count=1)
""",
        "utf-8",
    )
    (config.sql_scripts_dir / "post.sql").write_text(
        """
        CREATE TABLE main.receipt AS SELECT id FROM main.target;
        INSERT INTO main.receipt VALUES (:pipeline_id);
    """,
        "utf-8",
    )
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        pre = add_task(conn, pipeline, "pre", Handler.PYTHON, SCRIPT_FILE="python_scripts/pre.py")
        managed = add_task(
            conn,
            pipeline,
            "managed",
            Handler.SQL,
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT="main.target",
            SELECT_QUERY="SELECT id FROM main.source",
        )
        post = add_task(
            conn,
            pipeline,
            "post",
            Handler.SQL_SCRIPT,
            SCRIPT_FILE="sql_scripts/post.sql",
            RETRY_ON="NONE",
        )
        add_dependency(conn, pipeline, managed, pre)
        add_dependency(conn, pipeline, post, managed)
    from etl_craft.execution.attempt_types import ChildOptions

    outcome = run_pipeline(engine, config, "P", child=ChildOptions(log_level=log_level))
    assert outcome.status == RunStatus.SUCCESS, outcome.message
    warehouse = build_warehouse_engine(config)
    try:
        with warehouse.connect() as conn:
            rows = conn.execute(text("SELECT id FROM main.receipt ORDER BY id")).mappings().all()
            assert [row["id"] for row in rows] == sorted([7, pipeline])
    finally:
        warehouse.dispose()
    with engine.connect() as conn:
        attempt = conn.execute(
            text(
                "SELECT a.ATTEMPT_ID AS attempt_id, a.LOG_PATH AS log_path, a.TASK_LOG AS task_log "
                "FROM AUD_TASK_ATTEMPTS a JOIN AUD_TASK_RUN_LOG r ON r.TASK_RUN_ID=a.TASK_RUN_ID "
                "WHERE r.TASK_ID=:task"
            ),
            {"task": post},
        ).one()
    from pathlib import Path

    log = Path(attempt.log_path).read_text("utf-8")
    assert "statement 1:" in log and "statement 2:" in log
    assert "rows affected=" in log and "duration=" in log
    assert "STATEMENTS = 2" in attempt.task_log
    from etl_craft.core.actor import SYSTEM_ACTOR
    from etl_craft.core.enums import ActionName, ApiRole
    from etl_craft.services.operations import OperationContext
    from fixtures.actions import act

    ctx = OperationContext(engine, config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
    assert b"statement 1:" in b"".join(act(ctx, ActionName.LOGS, attempt_id=attempt.attempt_id))


@pytest.mark.parametrize(("retry_on", "count"), [("NONE", 1), ("TRANSIENT", 1), ("ANY", 2)])
def test_script_failure_uses_retry_policy_and_audits_the_failing_statement(
    project, retry_on, count
):
    engine, config = project
    (config.sql_scripts_dir / "bad.sql").write_text("SELECT * FROM main.absent; SELECT 1;", "utf-8")
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        add_task(
            conn,
            pipeline,
            "bad",
            Handler.SQL_SCRIPT,
            SCRIPT_FILE="sql_scripts/bad.sql",
            RETRY_ON=retry_on,
            RETRIES="1",
            RETRY_DELAY_SECONDS="0",
        )
    outcome = run_pipeline(engine, config, "P")
    assert outcome.status == RunStatus.FAILED
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT STATUS AS status, ERROR_MESSAGE AS error_message, "
                "FAILURE_CLASS AS failure_class FROM AUD_TASK_ATTEMPTS ORDER BY ATTEMPT_ID"
            )
        ).all()
    assert len(rows) == count
    for row in rows:
        assert row.status == RunStatus.FAILED
        assert row.failure_class == "PERMANENT"
        assert "bad.sql" in row.error_message and "statement 1" in row.error_message
        assert "absent" in row.error_message
    logs = list(config.log_dir.glob("P/run-*/bad.attempt-*.log"))
    assert len(logs) == count
    for log in logs:
        output = log.read_text("utf-8")
        assert "statement 1 failed" in output
        assert "statement 2 starting" not in output


def test_validate_overlap_and_unmanaged_catalog_and_lineage(project):
    engine, config = project
    (config.sql_scripts_dir / "custom.sql").write_text(
        """
        INSERT INTO main.target SELECT id FROM main.source;
        CREATE TABLE main.other AS SELECT id FROM main.target;
    """,
        "utf-8",
    )
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        add_task(
            conn,
            pipeline,
            "managed",
            Handler.SQL,
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT="main.target",
            SELECT_QUERY="SELECT 1 AS id",
        )
        add_task(conn, pipeline, "custom", Handler.SQL_SCRIPT, SCRIPT_FILE="sql_scripts/custom.sql")
    report = validate(engine, config)
    assert not report.failed, report.findings
    assert any(
        f.status == Status.WARN
        and f.where == "P.custom"
        and "P.managed" in f.message
        and "main.target" in f.message
        for f in report.findings
    )
    lineage = next(
        item for item in collect(engine, config, record=False) if item.task == "P.custom"
    )
    assert lineage.error == "unmanaged"
    assert set(lineage.targets) == {"main.target", "main.other"}
    assert any(
        edge.source_object == "main.source"
        and edge.target_object == "main.target"
        and edge.target_column == "id"
        for edge in lineage.edges
    )
    catalog = build_catalog(engine, config)
    assert catalog.tasks["P.custom"].lineage_error == "unmanaged"
    assert "P.custom" in catalog.tables["main.target"].writers
    assert "P.custom" in catalog.tables["main.other"].writers


@pytest.mark.chaos
def test_killed_script_keeps_committed_work_and_none_does_not_retry(project, monkeypatch):
    from etl_craft.execution.context import build_task_context
    from etl_craft.handlers import sql_script

    engine, config = project
    warehouse = build_warehouse_engine(config)
    with warehouse.begin() as conn:
        conn.execute(text("CREATE TABLE main.crash_rows (id INTEGER)"))
        conn.execute(text("INSERT INTO main.crash_rows VALUES (99)"))
    warehouse.dispose()
    (config.sql_scripts_dir / "crash.sql").write_text(
        "DELETE FROM main.crash_rows; INSERT INTO main.crash_rows VALUES (7);", "utf-8"
    )
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        add_task(
            conn,
            pipeline,
            "crash",
            Handler.SQL_SCRIPT,
            SCRIPT_FILE="sql_scripts/crash.sql",
            RETRY_ON="NONE",
            RETRIES="2",
        )
    monkeypatch.setenv("ETL_CRAFT_FAULT", "sql_script.after_statement:kill")
    result = run_pipeline(engine, config, "P")
    assert result.status == RunStatus.FAILED
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT TASK_RUN_ID AS task_run_id FROM AUD_TASK_ATTEMPTS")).all()
    assert len(rows) == 1
    warehouse = build_warehouse_engine(config)
    try:
        with warehouse.connect() as conn:
            assert conn.execute(text("SELECT COUNT(*) FROM main.crash_rows")).scalar_one() == 0
        monkeypatch.delenv("ETL_CRAFT_FAULT")
        sql_script.run(build_task_context(engine, config, rows[0].task_run_id), engine)
        with warehouse.connect() as conn:
            assert conn.execute(text("SELECT id FROM main.crash_rows")).scalar_one() == 7
    finally:
        warehouse.dispose()


def test_driver_failure_and_debug_logs_do_not_expose_bound_values(project):
    from etl_craft.execution.attempt_types import ChildOptions

    engine, config = project
    hidden = "never-print-this-bind-value"
    (config.sql_scripts_dir / "cast.sql").write_text("SELECT CAST(:value AS INTEGER);", "utf-8")
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        add_task(
            conn,
            pipeline,
            "cast",
            Handler.SQL_SCRIPT,
            SCRIPT_FILE="sql_scripts/cast.sql",
            INPUT_PARAMS='{"value": "' + hidden + '"}',
            RETRY_ON="NONE",
        )
    result = run_pipeline(engine, config, "P", child=ChildOptions(log_level="DEBUG"))
    assert result.status == RunStatus.FAILED
    with engine.connect() as conn:
        attempt = conn.execute(
            text(
                "SELECT ERROR_MESSAGE AS error_message, TASK_LOG AS task_log FROM AUD_TASK_ATTEMPTS"
            )
        ).one()
    assert hidden not in attempt.error_message
    assert hidden not in (attempt.task_log or "")
    log = next(config.log_dir.glob("P/run-*/cast.attempt-1.log")).read_text("utf-8")
    assert hidden not in log
    assert "[REDACTED]" in log


def test_missing_script_bind_is_permanent_and_has_no_warehouse_effect(project):
    engine, config = project
    (config.sql_scripts_dir / "missing.sql").write_text(
        "CREATE TABLE main.must_not_exist (id INTEGER); SELECT :missing;", "utf-8"
    )
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        add_task(
            conn,
            pipeline,
            "missing",
            Handler.SQL_SCRIPT,
            SCRIPT_FILE="sql_scripts/missing.sql",
            RETRIES="2",
            RETRY_DELAY_SECONDS="0",
        )
    result = run_pipeline(engine, config, "P")
    assert result.status == RunStatus.FAILED
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT FAILURE_CLASS AS failure_class, RETRYABLE AS retryable, "
                "ERROR_MESSAGE AS error_message FROM AUD_TASK_ATTEMPTS"
            )
        ).all()
    assert len(rows) == 1
    assert rows[0].failure_class == "PERMANENT" and not rows[0].retryable
    assert "statement 2" in rows[0].error_message and "missing" in rows[0].error_message
    warehouse = build_warehouse_engine(config)
    try:
        with warehouse.connect() as conn:
            assert (
                conn.execute(
                    text(
                        "SELECT COUNT(*) FROM information_schema.tables "
                        "WHERE table_name='must_not_exist'"
                    )
                ).scalar_one()
                == 0
            )
    finally:
        warehouse.dispose()
