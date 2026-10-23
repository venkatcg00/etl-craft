"""Python scripts: the contract, offsets, INPUT_PARAMS, captured output and every mistake."""

import logging
import os
import signal
import subprocess
import sys
import textwrap
import time
from dataclasses import replace
from datetime import date

import pytest
import yaml
from sqlalchemy import text

from etl_craft.config import load_config
from etl_craft.core.actor import current_actor
from etl_craft.core.enums import RunStatus
from etl_craft.core.errors import HandlerError, MetadataError
from etl_craft.engine import runlog, transitions
from etl_craft.engine.repository.offsets import fetch_task_offset
from etl_craft.execution.attempt_types import ChildOptions
from etl_craft.execution.context import build_task_context
from etl_craft.execution.runner import run_task
from etl_craft.handlers import python_scripts
from etl_craft.warehouse.connection import build_warehouse_engine
from fixtures.auth import service_token
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
    """A project with a DuckDB warehouse, scripts in python_scripts/, and task P.load."""
    profile = engine_db.config.engine
    schema = "public" if profile.jdbc_url.startswith("jdbc:postgresql") else "main"
    block = {"Jdbc_url": profile.jdbc_url, "Schema": schema}
    if profile.auth_mode != "NONE":
        block |= {
            "User": profile.user,
            "Auth_mode": profile.auth_mode,
            "Secret": profile.secret_var,
        }
    root = (engine_db.config.config_path or tmp_path / "craft-connector.yml").parent
    (root / "python_scripts").mkdir()
    (root / "python_scripts" / "load.py").write_text(SCRIPT, encoding="utf-8")
    raw = {
        "Secrets": {"Source_type": "ENVIRONMENT"},
        "Orchestration": {"Task_timeout_seconds": 60},
        "Engine": {"dev": block},
        "Warehouse": {"dev": {"Jdbc_url": "jdbc:duckdb:warehouse.duckdb", "Schema": "main"}},
    }
    (root / "craft-connector.yml").write_text(yaml.safe_dump(raw, sort_keys=False), "utf-8")
    config = load_config(root / "craft-connector.yml")
    engine = engine_db.engine
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "load", "PYTHON", SCRIPT_FILE="python_scripts/load.py")
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
    assert (
        "running python_scripts/load.py from offset none (first run) with input params "
        "count, region" in log
    )

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
    (config.python_scripts_dir / "load.py").write_text(textwrap.dedent(script), "utf-8")
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
        ("x = 1\n", {}, r"SCRIPT_FILE='python_scripts/load.py' defines no run\(task\) function"),
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
        ("def run(task):\n    pass\n", {"SCRIPT_FILE": ""}, "SCRIPT_FILE is required"),
    ],
)
def test_script_mistakes_fail_with_what_is_wrong(project, script, params, message):
    with pytest.raises(HandlerError, match=message):
        run_in_process(project, script, **params)


def test_a_missing_script_is_named(project):
    with pytest.raises(MetadataError, match=r"SCRIPT_FILE='python_scripts/nope.py': no such file"):
        run_in_process(project, "def run(task):\n    pass\n", SCRIPT_FILE="python_scripts/nope.py")


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
    (config.python_scripts_dir / "load.py").write_text(script, "utf-8")
    with engine.begin() as conn:
        run_id = runlog.fetch_active_pipeline_run_id(conn, project[2])
        transitions.finalize_pipeline_run(conn, run_id, "SUCCESS")
        backfill_run = start_run(conn, project[2], run_date=date(2026, 9, 1), backfill=True)
        binding = transitions.find_or_create_task_run(conn, task, backfill_run)
    context = build_task_context(engine, config, binding.task_run_id)
    assert (context.run_date, context.backfill) == (date(2026, 9, 1), True)
    assert python_scripts.run(context, engine).variables == {}
    # The stored offset is the one the scheduled run left, not the backfill's.
    kept = RESULT + "def run(task):\n    assert task.offset == Offset.number(7)\n"
    kept += "    return ScriptResult(0)\n"
    with engine.begin() as conn:
        transitions.finalize_pipeline_run(conn, backfill_run, "SUCCESS")
        start_run(conn, project[2])
    run_in_process(project, kept)


def with_script(project, script, **params):
    engine, config, _, task = project
    (config.python_scripts_dir / "load.py").write_text(textwrap.dedent(script), "utf-8")
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
@pytest.mark.parametrize("how", [["--task-code", "load"], ["--task-code", "load", "--force"], []])
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
            [sys.executable, "-m", "etl_craft", "run", "--pipeline-code", "P", *how],
            env={
                **os.environ,
                "ETL_CRAFT_CONFIG": str(config.config_path),
                "ETL_CRAFT_TOKEN": service_token(engine),
            },
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


@pytest.mark.parametrize(
    "library",
    [
        "urllib3",
        "httpx",
        "httpcore",
        "httpx2",
        "httpcore2",
        "requests",
        "botocore",
        "boto3",
        "s3transfer",
    ],
)
def test_http_request_logs_do_not_expose_query_keys_in_attempt_log(project, library):
    engine, config, _, _ = project
    (config.python_scripts_dir / "load.py").write_text(
        "import logging\n"
        "from etl_craft.scripting import ScriptResult\n"
        "def run(task):\n"
        "    assert task.logger.getEffectiveLevel() == logging.INFO\n"
        f"    logging.getLogger({library!r}).info('GET https://example.test/?api_key=hidden-key')\n"
        "    task.logger.info('finished request')\n"
        "    return ScriptResult(row_count=0)\n",
        encoding="utf-8",
    )
    outcome = run_task(engine, config, "P", "load", child=ChildOptions(log_level="INFO"))
    assert outcome.status == RunStatus.SUCCESS, outcome.message
    contents = next(config.log_dir.glob("P/run-*/load.attempt-1.log")).read_text("utf-8")
    assert "hidden-key" not in contents
    assert "GET https://example.test/" not in contents
    assert "finished request" in contents


def test_handler_returns_the_offset_without_persisting_it(project):
    from etl_craft.engine.repository.offsets import StoredOffset

    engine, config, pipeline, task = project
    (config.python_scripts_dir / "load.py").write_text(
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


def partitions_script(positions):
    body = f"    return ScriptResult(0, Offset.partitions({positions!r}))\n"
    return RESULT + "def run(task):\n" + body


def stored_offset(engine, task):
    with engine.connect() as conn:
        stored = fetch_task_offset(conn, task)
    return stored and (stored.offset_type, stored.offset_value)


def test_partition_offsets_move_forward_and_never_lose_a_partition(project, caplog):
    engine, config, _, task = project
    run_in_process(project, partitions_script({"1": 5, "0": 10}))
    assert stored_offset(engine, task) == ("PARTITIONS", '{"0":10,"1":5}')
    reader = RESULT + textwrap.dedent(
        """
        def run(task):
            assert dict(task.offset.value) == {"0": 10, "1": 5}, task.offset
            return ScriptResult(0, Offset.partitions({"0": 10, "1": 7, "2": 0}))
        """
    )
    with caplog.at_level(logging.INFO):
        run_in_process(project, reader)
    assert "new partition(s) 2 recorded" in caplog.text
    assert stored_offset(engine, task) == ("PARTITIONS", '{"0":10,"1":7,"2":0}')
    with pytest.raises(HandlerError, match=r"partition '1'.*behind its stored position 7"):
        run_in_process(project, partitions_script({"0": 11, "1": 6, "2": 0}))
    with pytest.raises(HandlerError, match=r"no position for partition '2'.*--offset"):
        run_in_process(project, partitions_script({"0": 11, "1": 7}))
    assert stored_offset(engine, task) == ("PARTITIONS", '{"0":10,"1":7,"2":0}')
    # In the task process, the refused offset fails the attempt and stores nothing.
    with engine.begin() as conn:
        run_id = runlog.fetch_active_pipeline_run_id(conn, project[2])
        transitions.finalize_pipeline_run(conn, run_id, "SUCCESS")
        start_run(conn, project[2])
    with_script(project, partitions_script({"0": 9, "1": 7, "2": 0}))
    outcome = run_task(engine, config, "P", "load")
    assert outcome.status == RunStatus.FAILED
    assert "behind its stored position 10" in task_row(engine, outcome.task_run_id).error
    assert stored_offset(engine, task) == ("PARTITIONS", '{"0":10,"1":7,"2":0}')


def test_an_offset_reset_sets_adds_and_removes_partitions_and_is_audited(project):
    from etl_craft.core.errors import RunRefusedError, UsageError
    from etl_craft.execution.interventions import OffsetReset, reset_offset

    engine, config, pipeline, task = project
    run_in_process(project, partitions_script({"0": 10, "1": 5}))

    def reset(value, reason="rebalance"):
        return reset_offset(engine, config, OffsetReset("P", "load", value, reason))

    done = reset('{"0": 3, "1": null, "9": 0}')
    assert done.message == (
        'P.load: offset {"0":10,"1":5} (PARTITIONS) -> {"0":3,"9":0} (PARTITIONS)'
    )
    assert stored_offset(engine, task) == ("PARTITIONS", '{"0":3,"9":0}')
    with engine.connect() as conn:
        audit = conn.execute(
            text(
                "SELECT PREVIOUS_MESSAGE AS message, REASON AS reason "
                "FROM AUD_RUN_INTERVENTIONS WHERE TASK_ID=:id"
            ),
            {"id": task},
        ).one()
    assert (audit.message, audit.reason) == (done.message, "rebalance")
    for value, message in (
        ('{"5": null}', "no stored partition '5'"),
        ('{"0": -1}', "partition"),
        ("12", "JSON object of partition positions"),
    ):
        with pytest.raises(UsageError, match=message):
            reset(value)
    with pytest.raises(UsageError, match="reason"):
        reset('{"0": 4}', reason="")
    with engine.begin() as conn:
        run_id = runlog.fetch_active_pipeline_run_id(conn, pipeline)
        binding = transitions.find_or_create_task_run(conn, task, run_id)
        transitions.queue_attempt(conn, binding.task_run_id, current_actor())
    with pytest.raises(RunRefusedError, match="active attempt"):
        reset('{"0": 4}')
    assert stored_offset(engine, task) == ("PARTITIONS", '{"0":3,"9":0}')


def test_an_offset_reset_refuses_a_sql_task(project):
    from etl_craft.core.errors import UsageError
    from etl_craft.execution.interventions import OffsetReset, reset_offset

    engine, config, pipeline, _ = project
    with engine.begin() as conn:
        add_task(
            conn,
            pipeline,
            "sql",
            "SQL",
            SQL_ACTION="APPEND_TABLE",
            TARGET_OBJECT="s.t",
            SELECT_QUERY="SELECT 1 AS a",
        )
    with pytest.raises(UsageError, match="HANDLER=SQL; `offset` needs a Python task"):
        reset_offset(engine, config, OffsetReset("P", "sql", '{"0": 1}', "why"))


def test_mark_offset_on_the_cli_moves_a_scalar_offset(project, capsys, monkeypatch):
    from etl_craft.cli import main

    engine, config, _, task = project
    number = RESULT + "def run(task):\n    return ScriptResult(0, Offset.number(7))\n"
    run_in_process(project, number)
    monkeypatch.setenv("ETL_CRAFT_TOKEN", service_token(engine))
    args = ["mark", "--config", str(config.config_path), "--pipeline-code", "P"]
    args += ["--task-code", "load", "--offset", "3", "--reason", "replay"]
    assert main(args) == 0
    assert "offset 7 (NUMBER) -> 3 (NUMBER)" in capsys.readouterr().out
    assert stored_offset(engine, task) == ("NUMBER", "3")


def test_script_warehouse_calls_share_one_task_session(project):
    engine, config, _, _ = project
    script = config.project_dir / "python_scripts" / "load.py"
    script.write_text("""from etl_craft.scripting import ScriptResult

def run(task):
    with task.warehouse() as first, first.begin() as conn:
        conn.exec_driver_sql("CREATE TEMPORARY TABLE task_session AS SELECT 1 AS id")
    with task.warehouse() as second, second.connect() as conn:
        assert first is second
        assert conn.exec_driver_sql("SELECT id FROM task_session").scalar_one() == 1
    return ScriptResult(row_count=1)
""")
    outcome = run_task(engine, config, "P", "load")
    assert outcome.status == RunStatus.SUCCESS, outcome.message


def test_fan_out_retry_keeps_item_rows_and_commits_the_offset_only_on_success(project):
    engine, config, _, task_id = project
    script = """
from pathlib import Path
from sqlalchemy import text
from etl_craft.scripting import ScriptResult, Offset

def run(task):
    failed_once = task.config.project_dir / "failed-once"
    def item(key):
        if key == "second" and not failed_once.exists():
            failed_once.touch()
            raise Exception("page second unavailable; expected source data; retry the task")
        with task.warehouse() as engine, engine.begin() as conn:
            conn.exec_driver_sql("CREATE TABLE IF NOT EXISTS item_landing "
                                 "(task_run BIGINT, item_key VARCHAR, value INTEGER)")
            conn.execute(text("DELETE FROM item_landing "
                              "WHERE task_run=:run AND item_key=:key"),
                         {"run": task.task_run_id, "key": key})
            conn.execute(text("INSERT INTO item_landing VALUES (:run, :key, :value)"),
                         {"run": task.task_run_id, "key": key,
                          "value": 1 if key == "first" else 2})
        print("wrote item", key)
        return ScriptResult(1)
    result = task.fan_out(["first", "second"], item, max_items=2, max_parallel=2)
    return ScriptResult(result.row_count, offset=Offset.number(2))
"""
    (config.python_scripts_dir / "load.py").write_text(script)
    first = run_task(engine, config, "P", "load")
    assert first.status == RunStatus.FAILED
    with engine.connect() as conn:
        assert fetch_task_offset(conn, task_id) is None
        rows = conn.execute(
            text(
                "SELECT ITEM_KEY AS key, STATUS AS status FROM AUD_TASK_ITEMS WHERE "
                "STATUS IN ('SUCCESS','FAILED') ORDER BY ITEM_KEY"
            )
        ).all()
        assert [(r.key, r.status) for r in rows] == [("first", "SUCCESS"), ("second", "FAILED")]
    second = run_task(engine, config, "P", "load")
    assert second.status == RunStatus.SUCCESS
    assert second.task_run_id == first.task_run_id
    with engine.connect() as conn:
        assert fetch_task_offset(conn, task_id).offset_value == "2"
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) AS n FROM AUD_TASK_ITEMS WHERE ITEM_KEY='first' "
                    "AND STATUS='RUNNING'"
                )
            ).scalar_one()
            == 1
        )
    with build_warehouse_engine(config).connect() as conn:
        landed = conn.execute(
            text(
                "SELECT task_run AS run, item_key AS key, value AS value FROM "
                "item_landing ORDER BY item_key"
            )
        ).all()
        assert [(r.run, r.key, r.value) for r in landed] == [
            (first.task_run_id, "first", 1),
            (first.task_run_id, "second", 2),
        ]


@pytest.mark.chaos
@pytest.mark.parametrize("point", ["item.before_result", "item.after_result"])
def test_item_commit_crash_replays_the_batch_without_doubling_rows(project, monkeypatch, point):
    engine, config, _, _ = project
    (config.python_scripts_dir / "load.py").write_text("""
from sqlalchemy import text
from etl_craft.scripting import ScriptResult

def run(task):
    def item(key):
        with task.warehouse() as engine,engine.begin() as conn:
            conn.exec_driver_sql("CREATE TABLE IF NOT EXISTS crash_items "
                                 "(task_run BIGINT, item_key VARCHAR)")
            conn.execute(text("DELETE FROM crash_items "
                              "WHERE task_run=:run AND item_key=:key"),
                         {"run":task.task_run_id,"key":key})
            conn.execute(text("INSERT INTO crash_items VALUES (:run,:key)"),
                         {"run":task.task_run_id,"key":key})
        return ScriptResult(1)
    return task.fan_out(["one"],item,max_items=1)
""")
    monkeypatch.setenv("ETL_CRAFT_FAULT", point + ":kill")
    first = run_task(engine, config, "P", "load")
    assert first.status == RunStatus.FAILED
    monkeypatch.delenv("ETL_CRAFT_FAULT")
    second = run_task(engine, config, "P", "load")
    assert second.status == RunStatus.SUCCESS
    assert first.task_run_id == second.task_run_id
    with build_warehouse_engine(config).connect() as conn:
        rows = conn.execute(text("SELECT task_run AS run,item_key AS key FROM crash_items")).all()
        assert [(r.run, r.key) for r in rows] == [(first.task_run_id, "one")]
    with engine.connect() as conn:
        starts = conn.execute(
            text("SELECT COUNT(*) AS n FROM AUD_TASK_ITEMS WHERE STATUS='RUNNING'")
        ).scalar_one()
        assert starts == (2 if point == "item.before_result" else 1)


def test_whole_task_cancel_stops_fan_out_and_recovery_retains_finished_items(project):
    from concurrent.futures import ThreadPoolExecutor
    from contextvars import copy_context

    from etl_craft.core.enums import ItemStatus
    from etl_craft.engine.items import latest
    from fixtures.cancellation import wait_for

    engine, config, _, _ = project
    (config.python_scripts_dir / "load.py").write_text("""
from pathlib import Path
import threading
from etl_craft.scripting import ScriptResult

def run(task):
    def item(key):
        if key == "hold" and not (task.config.project_dir / "release-items").exists():
            (task.config.project_dir / "item-ready").touch()
            threading.Event().wait(300)
        return ScriptResult(1)
    return task.fan_out(["quick","hold"],item,max_items=2,max_parallel=1)
""")
    from etl_craft.core.actor import SYSTEM_ACTOR
    from etl_craft.core.enums import ControlAction
    from etl_craft.services.operations.context import OperationContext
    from etl_craft.services.operations.controls import request_control
    from etl_craft.services.operations.requests import ControlRequest

    ctx = OperationContext(engine, config, SYSTEM_ACTOR)
    with engine.connect() as conn:
        run_id = conn.execute(
            text(
                "SELECT PIPELINE_RUN_ID AS run_id FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_ID=:id"
            ),
            {"id": project[2]},
        ).scalar_one()
    with ThreadPoolExecutor(1) as parent:
        future = parent.submit(
            copy_context().run,
            run_task,
            engine,
            config,
            "P",
            "load",
            child=ChildOptions(kill_grace_seconds=1, cancel_poll_seconds=0.1),
        )
        wait_for(lambda: (config.project_dir / "item-ready").exists())
        request_control(
            ctx,
            ControlRequest(
                action=ControlAction.STOP,
                pipeline_code="P",
                task_code="load",
                run_id=run_id,
                reason="stop item work",
            ),
        )
        first = future.result(20)
    assert first.status == RunStatus.CANCELLED
    with engine.connect() as conn:
        records = {r.key: r for r in latest(conn, first.task_run_id)}
        assert records["quick"].status == ItemStatus.SUCCESS
        assert records["hold"].status == ItemStatus.CANCELLED
    (config.project_dir / "release-items").touch()
    request_control(
        ctx,
        ControlRequest(
            action=ControlAction.RECOVER,
            pipeline_code="P",
            task_code="load",
            run_id=run_id,
            reason="resume unfinished items",
        ),
    )
    second = run_task(engine, config, "P", "load")
    assert second.status == RunStatus.SUCCESS
    assert second.task_run_id == first.task_run_id
    with engine.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) AS n FROM AUD_TASK_ITEMS "
                    "WHERE ITEM_KEY='quick' AND STATUS='RUNNING'"
                )
            ).scalar_one()
            == 1
        )
