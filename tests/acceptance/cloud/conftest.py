"""Cloud statements have a wall-clock deadline, including native-driver SQL scripts."""

import functools
import importlib
import logging
import os
import signal
import threading
import time
from contextlib import contextmanager

import pytest

from etl_craft.handlers.sql_script import statement_text
from fixtures.round_trips import cloud_statement

STATEMENT_SECONDS = 180


@contextmanager
def statement_deadline(warehouse, sql):
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    started = time.monotonic()
    old_handler = signal.getsignal(signal.SIGALRM)
    old_timer = signal.getitimer(signal.ITIMER_REAL)

    def expired(signum, frame):
        pytest.fail(
            f"{warehouse}: statement exceeded {STATEMENT_SECONDS}s: {statement_text(sql)}; "
            "start the warehouse and inspect its query queue before running acceptance",
            pytrace=False,
        )

    signal.signal(signal.SIGALRM, expired)
    remaining = old_timer[0] if old_timer[0] else STATEMENT_SECONDS
    signal.setitimer(signal.ITIMER_REAL, min(STATEMENT_SECONDS, remaining))
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, old_handler)
        if old_timer[0]:
            signal.setitimer(
                signal.ITIMER_REAL,
                max(0.001, old_timer[0] - (time.monotonic() - started)),
                old_timer[1],
            )


@pytest.fixture(autouse=True)
def bound_cloud_statements(request, monkeypatch):
    drivers = [
        ("cloud_snowflake", "SNOWFLAKE", "snowflake.connector.cursor", "SnowflakeCursor"),
        ("cloud_databricks", "DATABRICKS", "databricks.sql.client", "Cursor"),
    ]
    for marker, vendor, module, name in drivers:
        if request.node.get_closest_marker(marker) and os.environ.get(
            f"ETL_CRAFT_TEST_{vendor}_TOKEN"
        ):
            if vendor == "DATABRICKS":
                monkeypatch.setattr(logging.getLogger("databricks.sql.unsafe"), "disabled", True)
            cursor = getattr(importlib.import_module(module), name)
            original = cursor.execute

            @functools.wraps(original)
            def execute(self, operation, *args, _original=original, _vendor=vendor, **kwargs):
                cloud_statement(operation)
                if _vendor == "SNOWFLAKE":
                    kwargs.setdefault("timeout", STATEMENT_SECONDS)
                with statement_deadline(_vendor, operation):
                    return _original(self, operation, *args, **kwargs)

            monkeypatch.setattr(cursor, "execute", execute)
