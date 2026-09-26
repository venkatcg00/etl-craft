"""Ingestion scripts: the contract, offsets, INPUT_PARAMS, captured output and every mistake."""

import logging
import textwrap
from dataclasses import replace
from datetime import date

import pytest
import yaml
from sqlalchemy import text

from etl_craft.config import load_config
from etl_craft.core.enums import RunStatus
from etl_craft.core.errors import HandlerError, MetadataError
from etl_craft.engine import runlog
from etl_craft.execution.context import build_task_context
from etl_craft.execution.runner import run_task
from etl_craft.handlers import python_scripts
from etl_craft.warehouse.connection import build_warehouse_engine
from fixtures.metadata import add_pipeline, add_task, start_run

SCRIPT = """
import logging

from etl_craft.scripting import Offset, ScriptResult

log = logging.getLogger("my_ingestion")


def run(task):
    start = task.offset.value if task.offset else 0
    region, count = task.input_params["region"], task.input_params["count"]
    print(f"reading {region} after {start}")
    log.info("the source has %d new rows", count)
    task.logger.warning("through the task's logger")
    ids = list(range(start + 1, start + count + 1))
    with task.warehouse() as engine, engine.begin() as conn:
        conn.exec_driver_sql(
            "CREATE TABLE IF NOT EXISTS main.events (id INTEGER, region VARCHAR, run BIGINT)"
        )
        for i in ids:
            conn.exec_driver_sql(
                f"INSERT INTO main.events VALUES ({i}, '{region}', {task.pipeline_run_id})"
            )
    return ScriptResult(
        row_count=len(ids),
        offset=Offset.number(ids[-1]),
        variables={"REGION": region},
    )
"""


@pytest.fixture(autouse=True)
def restore_logging():
    loggers = [logging.getLogger("etl_craft"), logging.getLogger()]
    saved = [(list(logger.handlers), logger.level) for logger in loggers]
    yield
    for logger, (handlers, level) in zip(loggers, saved, strict=True):
        logger.handlers[:] = handlers
        logger.setLevel(level)


@pytest.fixture
def project(engine_db, tmp_path):
    """A project with a DuckDB warehouse, scripts in ingestion_scripts/, and task P.load."""
    profile = engine_db.config.engine.active
    schema = "public" if profile.jdbc_url.startswith("jdbc:postgresql") else "main"
    block = {"jdbc_url": profile.jdbc_url, "schema": schema}
    if profile.auth_mode != "none":
        block |= {
            "user": profile.user,
            "auth_mode": profile.auth_mode,
            "secret": profile.secret_var,
        }
    root = (engine_db.config.config_path or tmp_path / "craft-connector.yml").parent
    (root / "ingestion_scripts").mkdir()
    (root / "ingestion_scripts" / "load.py").write_text(SCRIPT, encoding="utf-8")
    raw = {
        "Secrets": {"Source_type": "environment"},
        "Orchestration": {"Mode": "local", "Task_timeout_seconds": 60},
        "Engine": {"dev": block},
        "Warehouse": {"dev": {"jdbc_url": "jdbc:duckdb:warehouse.duckdb", "schema": "main"}},
    }
    (root / "craft-connector.yml").write_text(yaml.safe_dump(raw, sort_keys=False), "utf-8")
    config = load_config(root / "craft-connector.yml")
    engine = engine_db.engine
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "load", "PYTHON", SCRIPT_NAME="load.py")
        conn.execute(
            text(
                "INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE) "
                "VALUES (:t, 'INPUT_PARAMS', :v)"
            ),
            {"t": task, "v": '{"region": "eu", "count": 2}'},
        )
        start_run(conn, pipeline)
    return engine, config, pipeline, task


def task_row(engine, task_run_id):
    with engine.connect() as conn:
        return conn.execute(
            text(
                "SELECT STATUS AS status, SOURCE_COUNT AS source, TARGET_COUNT AS target, "
                "INSERT_COUNT AS inserted, TASK_LOG AS task_log, ERROR_MESSAGE AS error "
                "FROM AUD_TASK_RUN_LOG WHERE TASK_RUN_ID = :id"
            ),
            {"id": task_run_id},
        ).one()


def test_a_script_runs_in_the_task_process_and_resumes_from_its_offset(project):
    engine, config, pipeline, _ = project
    first = run_task(engine, config, "P", "load")
    assert first.status == RunStatus.SUCCESS, first.message
    row = task_row(engine, first.task_run_id)
    assert (row.source, row.target, row.inserted) == (2, 2, 2)
    assert row.task_log.startswith(
        "SOURCE_COUNT = 2\nTARGET_COUNT = 2\nINSERT_COUNT = 2\nOFFSET = 2 (NUMBER)\nREGION = eu"
    )
    # What the script printed and logged is in the attempt's log, with the task's context.
    log = next(config.log_dir.glob("P/run-*/load.attempt-1.log")).read_text("utf-8")
    assert "reading eu after 0" in log
    assert "INFO my_ingestion [task_run_id=" in log and "the source has 2 new rows" in log
    assert "WARNING etl_craft_script.load [" in log
    assert "running load.py from offset none (first run) with input params count, region" in log

    # The next run starts where this one left off.
    assert "load.py wrote 2 row(s)" in log
    with engine.begin() as conn:
        runlog.finalize_pipeline_run(
            conn, runlog.fetch_active_pipeline_run_id(conn, pipeline), "SUCCESS"
        )
        start_run(conn, pipeline)
    second = run_task(engine, config, "P", "load")
    assert second.status == RunStatus.SUCCESS, second.message
    assert task_row(engine, second.task_run_id).inserted == 2
    warehouse = build_warehouse_engine(config)
    try:
        with warehouse.connect() as conn:
            ids = sorted(conn.exec_driver_sql("SELECT id FROM main.events").scalars())
    finally:
        warehouse.dispose()
    assert ids == [1, 2, 3, 4]


def run_in_process(project, script, **params):
    engine, config, _, task = project
    (config.ingestion_scripts_dir / "load.py").write_text(textwrap.dedent(script), "utf-8")
    with engine.begin() as conn:
        run_id = runlog.fetch_active_pipeline_run_id(conn, project[2])
        binding = runlog.find_or_create_task_run(conn, task, run_id)
    context = build_task_context(engine, config, binding.task_run_id, force=False)
    if params:
        context = replace(context, task_params={**context.task_params, **params})
    return python_scripts.run(context, engine)


RESULT = "from etl_craft.scripting import Offset, ScriptResult\n"


@pytest.mark.parametrize(
    ("script", "params", "message"),
    [
        ("x = 1\n", {}, r"SCRIPT_NAME='load.py' defines no run\(task\) function"),
        ("def run(task:\n", {}, r"importing it failed: SyntaxError"),
        ("def run(task):\n    raise KeyError('id')\n", {}, r"load.py failed: KeyError: 'id'"),
        ("import sys\ndef run(task):\n    sys.exit(3)\n", {}, r"load.py called sys.exit\(3\)"),
        ("def run(task):\n    return 5\n", {}, "load.py returned int, not a ScriptResult"),
        (
            RESULT + "def run(task):\n    return ScriptResult(True)\n",
            {},
            "load.py returned row_count=True; it must be a whole number, 0 or more",
        ),
        (
            RESULT + "def run(task):\n    return ScriptResult(-1)\n",
            {},
            "returned row_count=-1",
        ),
        ("def run(a, b):\n    pass\n", {}, "run takes 2 arguments; it takes the task, or nothing"),
        ("def run(task):\n    pass\n", {"INPUT_PARAMS": '["eu", 2]'}, "must be a JSON object"),
        ("def run(task):\n    pass\n", {"INPUT_PARAMS": "[1,"}, "INPUT_PARAMS is not valid JSON"),
        ("def run(task):\n    pass\n", {"SCRIPT_NAME": ""}, "SCRIPT_NAME is required"),
    ],
)
def test_script_mistakes_fail_with_what_is_wrong(project, script, params, message):
    with pytest.raises(HandlerError, match=message):
        run_in_process(project, script, **params)


def test_a_missing_script_is_named(project):
    with pytest.raises(MetadataError, match=r"SCRIPT_NAME='nope.py': no such file"):
        run_in_process(project, "def run(task):\n    pass\n", SCRIPT_NAME="nope.py")


def test_an_offset_keeps_its_type(project):
    number = RESULT + "def run(task):\n    return ScriptResult(0, Offset.number(7))\n"
    run_in_process(project, number)
    textual = RESULT + "def run(task):\n    return ScriptResult(0, Offset.text('x'))\n"
    with pytest.raises(HandlerError, match="returned a TEXT offset, but the stored one is NUMBER"):
        run_in_process(project, textual)
    kept = (
        RESULT + "def run(task):\n    assert task.offset == Offset.number(7)\n"
        "    return ScriptResult(0)\n"
    )
    assert run_in_process(project, kept).variables == {}


def test_a_script_that_needs_nothing_from_the_task(project):
    result = run_in_process(project, RESULT + "def run():\n    return ScriptResult(3)\n")
    assert (result.source_count, result.target_count, result.insert_count) == (3, 3, 3)


def test_a_backfill_run_gives_the_date_and_neither_reads_nor_stores_an_offset(project):
    run_in_process(
        project, RESULT + "def run(task):\n    return ScriptResult(0, Offset.number(7))\n"
    )
    engine, config, _, task = project
    script = RESULT + textwrap.dedent(
        """
        def run(task):
            assert task.backfill and task.offset is None, (task.backfill, task.offset)
            assert task.run_date.isoformat() == "2026-09-01", task.run_date
            return ScriptResult(0, Offset.number(99))
        """
    )
    (config.ingestion_scripts_dir / "load.py").write_text(script, "utf-8")
    with engine.begin() as conn:
        run_id = runlog.fetch_active_pipeline_run_id(conn, project[2])
        runlog.finalize_pipeline_run(conn, run_id, "SUCCESS")
        backfill_run = runlog.find_or_create_active_run(
            conn, project[2], run_date=date(2026, 9, 1), backfill=True
        )
        binding = runlog.find_or_create_task_run(conn, task, backfill_run)
    context = build_task_context(engine, config, binding.task_run_id)
    assert (context.run_date, context.backfill) == (date(2026, 9, 1), True)
    assert python_scripts.run(context, engine).variables == {}
    # The stored offset is the one the scheduled run left, not the backfill's.
    kept = RESULT + "def run(task):\n    assert task.offset == Offset.number(7)\n"
    kept += "    return ScriptResult(0)\n"
    with engine.begin() as conn:
        runlog.finalize_pipeline_run(conn, backfill_run, "SUCCESS")
        runlog.find_or_create_active_run(conn, project[2])
    run_in_process(project, kept)
