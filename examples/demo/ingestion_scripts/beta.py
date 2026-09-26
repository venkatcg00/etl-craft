"""Land Client Beta's support events: those after the last event time loaded, each run.

Client Beta sends events with its own names for things (``regarding`` for the support area,
``handle_time`` in seconds, ``score`` for the rating) and a timestamp per event, one hour
apart. The offset is the time of the last event loaded, a timestamp.
"""

from datetime import datetime, timedelta

from sqlalchemy import text

from etl_craft.scripting import Offset, ScriptResult, ScriptTask

START = datetime(2026, 9, 1, 8, 0)
AREAS = ("billing", "technical", "account")


def run(task: ScriptTask) -> ScriptResult:
    """Land the events after the stored event time."""
    last = task.offset.value if task.offset else START - timedelta(hours=1)
    assert isinstance(last, datetime)
    count = int(task.input_params.get("rows", 8))
    table = task.table("lnd.client_beta")
    first = int((last - START) / timedelta(hours=1)) + 1
    events = [
        {
            "support_identifier": 5000 + n,
            "agent": f"A0{n % 4 + 1}",
            "regarding": AREAS[n % 3],
            "event_time": START + timedelta(hours=n),
            "status": "closed" if n % 5 else "open",
            "handle_time": 45 * n % 700 + 60,
            "score": n % 5 + 1,
        }
        for n in range(first, first + count)
    ]
    with task.warehouse() as engine, engine.begin() as conn:
        conn.execute(
            text(
                f"CREATE TABLE IF NOT EXISTS {table} (support_identifier BIGINT, agent VARCHAR, "
                "regarding VARCHAR, event_time TIMESTAMP, status VARCHAR, handle_time BIGINT, "
                "score BIGINT, pipeline_run_id BIGINT)"
            )
        )
        for event in events:
            conn.execute(
                text(
                    f"INSERT INTO {table} VALUES (:support_identifier, :agent, :regarding, "
                    ":event_time, :status, :handle_time, :score, :run)"
                ),
                {**event, "run": task.pipeline_run_id},
            )
    newest = events[-1]["event_time"]
    return ScriptResult(row_count=len(events), offset=Offset.timestamp(newest))
