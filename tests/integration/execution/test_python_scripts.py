"""Ingestion scripts: the contract, offsets, INPUT_PARAMS, captured output and every mistake."""

import logging
import os
import signal
import subprocess
import sys
import textwrap
import threading
import time
from dataclasses import replace
from datetime import date
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
import yaml
from sqlalchemy import text

from etl_craft.config import load_config
from etl_craft.core.enums import RunStatus
from etl_craft.core.errors import HandlerError, MetadataError
from etl_craft.engine import runlog, transitions
from etl_craft.engine.repository.offsets import fetch_task_offset
from etl_craft.execution.context import build_task_context
from etl_craft.execution.runner import run_task
from etl_craft.handlers import python_scripts
from etl_craft.warehouse.connection import build_warehouse_engine
from fixtures.cli_project import CliProcess
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
            "CREATE TABLE IF NOT EXISTS main.events (id INTEGER, region VARCHAR, "
            "pipeline_run_id BIGINT, pipeline_id BIGINT, task_run_id BIGINT)"
        )
        for i in ids:
            conn.exec_driver_sql(
                f"INSERT INTO main.events VALUES ({i}, '{region}', {task.pipeline_run_id}, "
                f"{task.pipeline_id}, {task.task_run_id})"
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
        transitions.finalize_pipeline_run(
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
            identities = conn.exec_driver_sql(
                "SELECT DISTINCT pipeline_id, pipeline_run_id, task_run_id FROM main.events"
            ).fetchall()
    finally:
        warehouse.dispose()
    assert ids == [1, 2, 3, 4]
    assert {r[0] for r in identities} == {pipeline}
    assert {r[2] for r in identities} == {first.task_run_id, second.task_run_id}
    assert len({r[1] for r in identities}) == 2


def run_in_process(project, script, **params):
    engine, config, _, task = project
    (config.ingestion_scripts_dir / "load.py").write_text(textwrap.dedent(script), "utf-8")
    with engine.begin() as conn:
        run_id = runlog.fetch_active_pipeline_run_id(conn, project[2])
        binding = transitions.find_or_create_task_run(conn, task, run_id)
    context = build_task_context(engine, config, binding.task_run_id, force=False)
    if params:
        context = replace(context, task_params={**context.task_params, **params})
    result = python_scripts.run(context, engine)
    with engine.begin() as conn:
        transitions.finish_task_run(
            conn, binding.task_run_id, status="SUCCESS", offset=result.offset
        )
    return result


RESULT = "from etl_craft.scripting import Offset, ScriptResult\n"


@pytest.mark.parametrize(
    "script",
    [
        "from dataclasses import dataclass\n"
        "@dataclass\n"
        "class Row:\n"
        "    id: int\n"
        "def run(task):\n"
        "    assert Row.__annotations__['id'] is int\n"
        "    return ScriptResult(Row(3).id)\n",
        "from __future__ import annotations\n"
        "from dataclasses import dataclass, fields\n"
        "from typing import ClassVar\n"
        "@dataclass\n"
        "class Row:\n"
        "    kind: ClassVar[str] = 'row'\n"
        "    id: int\n"
        "def run(task):\n"
        "    assert [field.name for field in fields(Row)] == ['id']\n"
        "    return ScriptResult(Row(3).id)\n",
        "import pickle\n"
        "class Row:\n"
        "    id = 3\n"
        "def run(task):\n"
        "    return ScriptResult(pickle.loads(pickle.dumps(Row())).id)\n",
        "import pickle\n"
        "from enum import Enum\n"
        "class Count(Enum):\n"
        "    THREE = 3\n"
        "def run(task):\n"
        "    return ScriptResult(pickle.loads(pickle.dumps(Count.THREE)).value)\n",
        "from __future__ import annotations\n"
        "from typing import get_type_hints\n"
        "class Row:\n"
        "    pass\n"
        "class Holder:\n"
        "    row: Row\n"
        "def run(task):\n"
        "    assert get_type_hints(Holder)['row'] is Row\n"
        "    return ScriptResult(3)\n",
    ],
    ids=["dataclass", "future-dataclass", "pickle-class", "enum", "type-hints"],
)
def test_scripts_have_real_module_semantics(project, script):
    future, separator, body = script.partition("from __future__ import annotations\n")
    source = separator + RESULT + body if separator else RESULT + future
    assert run_in_process(project, source).insert_count == 3


def test_a_script_can_use_a_fork_process_pool(project):
    import multiprocessing

    if "fork" not in multiprocessing.get_all_start_methods():
        pytest.skip("the fork start method is unavailable")
    script = RESULT + textwrap.dedent(
        """
        import multiprocessing
        from concurrent.futures import ProcessPoolExecutor
        from dataclasses import dataclass

        @dataclass
        class Row:
            id: int

        def read_row(value):
            return Row(value)

        def run(task):
            with ProcessPoolExecutor(mp_context=multiprocessing.get_context("fork"),
                                     max_workers=1) as pool:
                row = pool.submit(read_row, 3).result(timeout=10)
            return ScriptResult(row.id)
        """
    )
    assert run_in_process(project, script).insert_count == 3


@pytest.mark.parametrize("variables", ["None", "42", "[('CUSTOM', 'value')]"])
@pytest.mark.parametrize("existing", [False, True])
def test_an_invalid_result_does_not_advance_the_offset(project, variables, existing):
    engine, _, _, task = project
    if existing:
        run_in_process(
            project, RESULT + "def run(task):\n    return ScriptResult(0, Offset.number(7))\n"
        )
    with engine.connect() as conn:
        before = fetch_task_offset(conn, task)
    script = RESULT + "def run(task):\n"
    script += f"    return ScriptResult(0, Offset.number(99), variables={variables})\n"
    with pytest.raises(HandlerError, match=r"load\.py returned variables=.*must be a mapping"):
        run_in_process(project, script)
    with engine.connect() as conn:
        assert fetch_task_offset(conn, task) == before


def test_an_invalid_result_is_recorded_as_a_failed_attempt(project):
    engine, config, _, task = project
    with_script(
        project,
        RESULT + "def run(task):\n    return ScriptResult(0, Offset.number(99), variables=None)\n",
    )
    outcome = run_task(engine, config, "P", "load")
    assert outcome.status == RunStatus.FAILED
    assert "must be a mapping" in task_row(engine, outcome.task_run_id).error
    assert "must be a mapping" in attempt_log(config)
    with engine.connect() as conn:
        assert fetch_task_offset(conn, task) is None


def test_a_timestamp_offset_keeps_microseconds_in_the_engine_db(project):
    script = RESULT + textwrap.dedent(
        """
        from datetime import UTC, datetime

        def run(task):
            value = datetime(2026, 1, 1, microsecond=123456, tzinfo=UTC)
            if task.offset is not None:
                assert task.offset.value == value
            return ScriptResult(0, Offset.timestamp(value))
        """
    )
    run_in_process(project, script)
    assert run_in_process(project, script).variables["OFFSET"] == (
        "2026-01-01T00:00:00.123456+00:00 (TIMESTAMP)"
    )


def test_result_variables_accept_a_mapping_besides_a_dictionary(project):
    script = RESULT + textwrap.dedent(
        """
        from collections import UserDict

        def run(task):
            return ScriptResult(0, Offset.number(7), variables=UserDict(REGION="eu"))
        """
    )
    assert run_in_process(project, script).variables == {"OFFSET": "7 (NUMBER)", "REGION": "eu"}


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
        transitions.finalize_pipeline_run(conn, run_id, "SUCCESS")
        backfill_run = transitions.find_or_create_active_run(
            conn, project[2], run_date=date(2026, 9, 1), backfill=True
        )
        binding = transitions.find_or_create_task_run(conn, task, backfill_run)
    context = build_task_context(engine, config, binding.task_run_id)
    assert (context.run_date, context.backfill) == (date(2026, 9, 1), True)
    assert python_scripts.run(context, engine).variables == {}
    # The stored offset is the one the scheduled run left, not the backfill's.
    kept = RESULT + "def run(task):\n    assert task.offset == Offset.number(7)\n"
    kept += "    return ScriptResult(0)\n"
    with engine.begin() as conn:
        transitions.finalize_pipeline_run(conn, backfill_run, "SUCCESS")
        transitions.find_or_create_active_run(conn, project[2])
    run_in_process(project, kept)


def with_script(project, script, **params):
    engine, config, _, task = project
    (config.ingestion_scripts_dir / "load.py").write_text(textwrap.dedent(script), "utf-8")
    with engine.begin() as conn:
        for name, value in params.items():
            conn.execute(
                text(
                    "INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE) "
                    "VALUES (:t, :n, :v)"
                ),
                {"t": task, "n": name, "v": value},
            )


def attempt_log(config):
    return next(config.log_dir.glob("P/run-*/load.attempt-1.log")).read_text("utf-8")


def test_the_task_process_exits_once_its_outcome_is_recorded(project):
    engine, config, _, _ = project
    with_script(
        project,
        RESULT + "import threading, time\n"
        "def run(task):\n"
        "    threading.Thread(target=time.sleep, args=(60,), name='pool-1').start()\n"
        "    return ScriptResult(1)\n",
    )
    started = time.monotonic()
    outcome = run_task(engine, config, "P", "load")
    assert outcome.status == RunStatus.SUCCESS, outcome.message
    assert time.monotonic() - started < 20
    assert "exiting with 1 thread(s) still running" in attempt_log(config)
    assert "pool-1" in attempt_log(config)


def test_what_a_script_printed_is_kept_when_it_is_stopped(project):
    engine, config, _, _ = project
    with_script(
        project,
        "import time\n"
        "def run(task):\n"
        "    for n in range(5):\n"
        "        print(f'line {n}')\n"
        "    time.sleep(60)\n",
        TASK_TIMEOUT_SECONDS="10",
    )
    outcome = run_task(engine, config, "P", "load")
    assert outcome.status == RunStatus.FAILED
    assert "timed out after 10s" in outcome.message
    log = attempt_log(config)
    assert all(f"line {n}" in log for n in range(5))


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGHUP])
@pytest.mark.parametrize("how", [["--task_code", "load"], ["--task_code", "load", "--force"], []])
def test_a_signal_to_run_stops_the_task_process_and_records_it(project, tmp_path, sig, how):
    engine, config, _, task = project
    pid_file = tmp_path / "script.pid"
    with_script(
        project,
        "import os, time\n"
        "def run(task):\n"
        f"    open({str(pid_file)!r}, 'w').write(str(os.getpid()))\n"
        "    time.sleep(60)\n",
    )
    parent_log = tmp_path / "parent.log"
    with parent_log.open("wb") as output:
        parent = subprocess.Popen(
            [sys.executable, "-m", "etl_craft", "run", "--pipeline_code", "P", *how],
            env={**os.environ, "ETL_CRAFT_CONFIG": str(config.config_path)},
            cwd=config.project_dir,
            stdout=output,
            stderr=subprocess.STDOUT,
        )
    command = CliProcess(parent, parent_log)
    try:
        deadline = time.monotonic() + 30
        while not pid_file.exists() or not pid_file.read_text():
            assert parent.poll() is None, command.output
            assert time.monotonic() < deadline, f"the script never started: {command.output}"
            time.sleep(0.2)
        child_pid = int(pid_file.read_text())
        command.signal(sig)
        command.wait(timeout=30)
    finally:
        command.close()
    with pytest.raises(ProcessLookupError):
        os.kill(child_pid, 0)
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT STATUS AS status, ERROR_MESSAGE AS error FROM AUD_TASK_RUN_LOG "
                "WHERE TASK_ID = :t"
            ),
            {"t": task},
        ).one()
    assert row.status == "FAILED"
    assert sig.name in row.error or "interrupted" in row.error, row.error


def test_http_request_logs_do_not_expose_query_keys_in_attempt_log(project):
    engine, config, _, _ = project
    requests = []

    class Endpoint(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, format, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Endpoint)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    url = f"http://127.0.0.1:{server.server_port}/?api_key=hidden-key"
    (config.ingestion_scripts_dir / "load.py").write_text(
        "import logging, requests\n"
        "from etl_craft.scripting import ScriptResult\n"
        "def run(task):\n"
        f"    response = requests.get({url!r}, timeout=5)\n"
        "    response.raise_for_status()\n"
        "    logging.getLogger('httpx').info('GET %s', response.url)\n"
        "    logging.getLogger('urllib3.connectionpool').info('GET %s', response.url)\n"
        "    task.logger.info('finished request')\n"
        "    return ScriptResult(row_count=0)\n",
        encoding="utf-8",
    )
    try:
        outcome = run_task(engine, config, "P", "load")
        assert outcome.status == RunStatus.SUCCESS, outcome.message
        assert requests == ["/?api_key=hidden-key"]
        contents = next(config.log_dir.glob("P/run-*/load.attempt-1.log")).read_text("utf-8")
        assert "hidden-key" not in contents
        assert "finished request" in contents
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=5)


def test_handler_returns_the_offset_without_persisting_it(project):
    from etl_craft.engine.repository.offsets import StoredOffset

    engine, config, pipeline, task = project
    (config.ingestion_scripts_dir / "load.py").write_text(
        RESULT + "def run(task):\n    return ScriptResult(1, Offset.number(9))\n"
    )
    with engine.begin() as conn:
        run_id = runlog.fetch_active_pipeline_run_id(conn, pipeline)
        binding = transitions.find_or_create_task_run(conn, task, run_id)
    result = python_scripts.run(build_task_context(engine, config, binding.task_run_id), engine)
    assert result.offset == StoredOffset("NUMBER", "9")
    with engine.connect() as conn:
        assert fetch_task_offset(conn, task) is None
    assert task_row(engine, binding.task_run_id).status == "IN-PROGRESS"
