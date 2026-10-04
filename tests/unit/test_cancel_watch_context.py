"""Cancellation polling retains the task's logging context across the thread boundary."""

import logging

import pytest

from etl_craft.core.log import ContextFilter, log_context
from etl_craft.execution import runner

pytestmark = pytest.mark.unit


def test_watch_warning_carries_run_and_task_context(monkeypatch):
    records = []

    class Capture(logging.Handler):
        def emit(self, record):
            ContextFilter().filter(record)
            records.append(record)

    logger = logging.getLogger(runner.__name__)
    handler = Capture()
    logger.addHandler(handler)
    monkeypatch.setattr(runner, "run_cancelled", lambda engine, run: True)
    try:
        with (
            log_context(pipeline="P", task="T", pipeline_run_id=7),
            runner._CancelWatch(None, 7, runner.ChildOptions(cancel_poll_seconds=0.05)) as stop,
        ):
            assert stop.wait(5)
    finally:
        logger.removeHandler(handler)
    warning = next(record for record in records if "was cancelled" in record.getMessage())
    assert warning.pipeline == "P"
    assert warning.task == "T"
    assert warning.pipeline_run_id == 7
