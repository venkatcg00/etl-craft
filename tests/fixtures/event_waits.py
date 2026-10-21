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
