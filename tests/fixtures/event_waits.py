"""Warehouse-row event probes and portable downstream SQL file tokens."""

import json
from dataclasses import replace
from datetime import UTC, datetime

from sqlalchemy import text

from etl_craft.execution.context import build_task_context
from etl_craft.execution.events import check_event
from etl_craft.handlers.sql.spec import read_sql_task
from fixtures.metadata import add_task


def check_event_queries(world, target):
    """An empty warehouse table waits; its first row releases and records the event."""
    world.setup(target, "SELECT CAST(1 AS INTEGER) AS ready WHERE 1=0", "APPEND_TABLE")
    with world.engine_db.begin() as conn:
        event = add_task(
            conn,
            world.pipeline_id,
            "wait",
            "EVENT_WAIT",
            EVENT_SQL=f"SELECT ready FROM {world.name(target)} WHERE ready=1",
            EVENT_TIMEOUT_MINUTES="NONE",
        )
    now = datetime.now(UTC)
    waiting = check_event(world.engine_db, world.config, world.pipeline_run_id, event, now)
    assert waiting.next_check_at
    world.run(
        "insert",
        SQL_ACTION="APPEND_TABLE",
        TARGET_OBJECT=target,
        SELECT_QUERY="SELECT CAST(1 AS INTEGER) AS ready",
    )
    result = check_event(
        world.engine_db, world.config, world.pipeline_run_id, event, waiting.next_check_at
    )
    assert result.status == "SUCCESS"
    with world.engine_db.connect() as conn:
        evidence = json.loads(
            conn.execute(
                text("SELECT EVIDENCE AS evidence FROM AUD_TASK_EVENTS WHERE TASK_RUN_ID=:id"),
                {"id": result.task_run_id},
            ).scalar_one()
        )
        assert {key.lower(): value for key, value in evidence["row"].items()} == {"ready": 1}
    context = build_task_context(world.engine_db, world.config, result.task_run_id)
    assert context.event_files == ()
    # The file token is portable SQL: one escaped string per file, usable in IN or array syntax.
    downstream = world.task(
        "files",
        SQL_ACTION="CREATE_TABLE",
        TARGET_OBJECT=target,
        SELECT_QUERY="SELECT $$event_files AS path",
    )
    downstream = replace(downstream, event_files=("/tmp/a\\b:batch'01.csv",))
    assert world.rows(read_sql_task(downstream).select_sql) == [("/tmp/a\\b:batch'01.csv",)]


SIGNAL_KEY = "object/a\\b:batch'01@version-1"


def signal_scene(world, target):
    """A team-owned signal table, read by one stable event task."""
    world.execute(
        f"CREATE TABLE {world.name(target)} (event_id VARCHAR(200), payload VARCHAR(200))"
    )
    quote = world.warehouse.dialect.identifier_preparer.quote_identifier
    with world.engine_db.begin() as conn:
        task = add_task(
            conn,
            world.pipeline_id,
            "signals_wait",
            "EVENT_WAIT",
            # Quoted the warehouse's own way, so the names stay lower case on Snowflake too.
            EVENT_SQL=(
                f"SELECT event_id AS {quote('event_id')}, payload AS {quote('payload')} "
                f"FROM {world.name(target)}"
            ),
            EVENT_KEY_COLUMN="event_id",
            EVENT_POLL_SECONDS="1",
            EVENT_TIMEOUT_MINUTES="NONE",
        )
    return task


def send_signal(world, target, key=SIGNAL_KEY, payload="first"):
    """Model an INSERT-only sender's idempotent insert without warehouse-specific upsert SQL."""
    world.execute(
        f"INSERT INTO {world.name(target)} (event_id,payload) "
        f"SELECT CAST(:key AS VARCHAR(200)), CAST(:payload AS VARCHAR(200)) WHERE NOT EXISTS "
        f"(SELECT 1 FROM {world.name(target)} WHERE event_id=CAST(:key AS VARCHAR(200)))",
        key=key,
        payload=payload,
    )
    world.warehouse.dispose()


def check_signal_queries(world, target):
    """Consume once, retain the exact run's payload and expose portable escaped key literals."""
    from etl_craft.core.enums import RunStatus
    from etl_craft.engine.repository.events import run_event_data
    from etl_craft.handlers.python_scripts import run as run_python
    from etl_craft.handlers.registry import TaskContext
    from etl_craft.services.operations.serialization import encode, from_json

    task = signal_scene(world, target)
    send_signal(world, target)
    run = world.pipeline_run_id
    now = datetime.now(UTC)
    result = check_event(world.engine_db, world.config, run, task, now)
    assert result.status == RunStatus.SUCCESS, result.message
    with world.engine_db.connect() as conn:
        consumed = conn.execute(
            text(
                "SELECT EVENT_KEY AS event_key,TASK_RUN_ID AS task_run_id,CREATED_BY AS created_by "
                "FROM AUD_EVENT_KEYS WHERE TASK_ID=:task"
            ),
            {"task": task},
        ).one()
        assert consumed.event_key == SIGNAL_KEY
        assert consumed.task_run_id == result.task_run_id
        assert consumed.created_by == "etl-craft"
        assert run_event_data(conn, run).keys == (SIGNAL_KEY,)
    context = build_task_context(world.engine_db, world.config, result.task_run_id)
    assert context.event_keys == (SIGNAL_KEY,)
    assert context.event_rows == ({"payload": "first"},)
    # The agent's serialized context carries the same immutable values.
    remote = from_json(TaskContext, encode(context))
    assert remote.event_keys == context.event_keys
    assert remote.event_rows == context.event_rows
    downstream = replace(
        world.task(
            "key_reader",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT=target + "_out",
            SELECT_QUERY="SELECT $$event_keys AS event_id",
        ),
        event_keys=context.event_keys,
    )
    assert world.rows(read_sql_task(downstream).select_sql) == [(SIGNAL_KEY,)]
    script = world.project / "python_scripts" / "signals.py"
    script.parent.mkdir(exist_ok=True)
    script.write_text(
        "from etl_craft.scripting import ScriptResult\n"
        "def run(task):\n"
        "    assert task.input_params['keys'] == list(task.event_keys)\n"
        "    assert task.event_rows == ({'payload': 'first'},)\n"
        "    return ScriptResult(row_count=len(task.event_keys))\n"
    )
    downstream = replace(
        downstream,
        task_params={
            "SCRIPT_FILE": "python_scripts/signals.py",
            "INPUT_PARAMS": '{"keys":"$$event_keys"}',
        },
        event_rows=context.event_rows,
    )
    assert run_python(downstream, world.engine_db).source_count == 1
    # A delivery during the active run is not added to an already completed wait.
    send_signal(world, target, "second", "later")
    assert (
        check_event(world.engine_db, world.config, run, task, now).task_run_id == result.task_run_id
    )
    assert build_task_context(world.engine_db, world.config, result.task_run_id).event_keys == (
        SIGNAL_KEY,
    )
    next_run = world.new_run()
    next_result = check_event(world.engine_db, world.config, next_run, task, now)
    assert next_result.status == RunStatus.SUCCESS
    with world.engine_db.connect() as conn:
        assert run_event_data(conn, run).rows == ({"payload": "first"},)
        assert run_event_data(conn, next_run).keys == ("second",)
    # Repeated delivery is a no-op and consumed rows never satisfy a new wait.
    send_signal(world, target)
    final_run = world.new_run()
    waiting = check_event(world.engine_db, world.config, final_run, task, now)
    assert waiting.status is None and waiting.next_check_at is not None
    with world.engine_db.connect() as conn:
        assert run_event_data(conn, final_run).keys == ()
        assert conn.execute(text("SELECT count(*) AS n FROM AUD_EVENT_KEYS")).scalar_one() == 2
