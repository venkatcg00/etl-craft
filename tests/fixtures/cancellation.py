"""Observe real warehouse statements without adding runtime instrumentation."""

import json
import os
import time
from pathlib import Path

import psutil
from sqlalchemy import event
from sqlalchemy.engine import Engine


def install_query_marker():
    """Mark entry to the long statement in a fresh task or probe interpreter."""
    path = os.environ.get("ETL_CRAFT_TEST_QUERY_STARTED")
    if not path:
        return

    def started(conn, cursor, sql, parameters, context, many):
        if "cancel_slow" in sql.lower():
            cpu = psutil.Process().cpu_times()
            pending = Path(path).with_suffix(".new")
            pending.write_text(
                json.dumps({"pid": os.getpid(), "cpu": cpu.user + cpu.system}), encoding="utf-8"
            )
            pending.replace(path)

    event.listen(Engine, "before_cursor_execute", started)


def wait_for(check, seconds=20):
    """Bound an observation; never use a delay to order the callers."""
    import threading

    deadline = time.monotonic() + seconds
    while True:
        if value := check():
            return value
        assert time.monotonic() < deadline, "expected cancellation observation did not arrive"
        threading.Event().wait(0.02)
