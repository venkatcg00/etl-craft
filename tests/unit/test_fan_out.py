"""Bounded futures keep I/O fan-out inside its task's memory reservation."""

import logging
import threading
import tracemalloc
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from etl_craft.core.enums import ItemStatus
from etl_craft.core.errors import HandlerError
from etl_craft.core.items import ItemState
from etl_craft.scripting import ScriptResult, ScriptTask

pytestmark = pytest.mark.unit


class Store:
    """An in-memory persistence boundary; runtime orchestration remains the real helper."""

    def __init__(self, memory=512):
        self.records = {}
        self.memory = memory

    def items(self):
        return ItemState(tuple(self.records.values()), self.memory)

    def prepare_items(self, keys):
        from dataclasses import replace

        from etl_craft.core.items import ItemRecord

        for key in keys:
            old = self.records.get(key)
            if old is None:
                self.records[key] = ItemRecord(key, 0, ItemStatus.QUEUED)
            elif old.status in {ItemStatus.SUCCESS, ItemStatus.SKIPPED}:
                self.records[key] = replace(old, status=ItemStatus.SKIPPED)

    def item_event(self, event):
        self.records[event.key] = event
        return event


def task(memory=512):
    return ScriptTask(
        "P",
        "T",
        1,
        1,
        1,
        "FULL",
        None,
        {},
        {},
        False,
        SimpleNamespace(),
        logging.getLogger("etl_craft.items"),
        _items=Store(memory),
    )


def test_a_thousand_items_at_width_two_hundred_have_bounded_pending_work(monkeypatch):
    helper = task()
    barrier = threading.Barrier(201, timeout=15)
    release = threading.Event()
    lock = threading.Lock()
    issued = 0
    outstanding = 0
    peak = 0
    calls = []
    original = ThreadPoolExecutor.submit

    def submit(executor, *args, **kwargs):
        nonlocal outstanding, peak
        counted = executor._thread_name_prefix == "etl-item"
        if counted:
            with lock:
                outstanding += 1
                peak = max(peak, outstanding)
        future = original(executor, *args, **kwargs)
        if counted:

            def finished(_):
                nonlocal outstanding
                with lock:
                    outstanding -= 1

            future.add_done_callback(finished)
        return future

    def item(key):
        nonlocal issued
        with lock:
            issued += 1
            first = issued <= 200
            calls.append(key)
        if first:
            barrier.wait()
            assert release.wait(15)
        return ScriptResult(1)

    monkeypatch.setattr(ThreadPoolExecutor, "submit", submit)
    tracemalloc.start()
    try:
        with ThreadPoolExecutor(1) as parent:
            future = parent.submit(
                helper.fan_out,
                (str(i) for i in range(1000)),
                item,
                max_items=1000,
                max_parallel=200,
            )
            try:
                barrier.wait()
                assert peak <= 200
                assert tracemalloc.get_traced_memory()[1] < 32 * 1024 * 1024
            finally:
                release.set()
            assert future.result(20).row_count == 1000
        assert sorted(calls, key=int) == [str(i) for i in range(1000)]
        assert peak <= 200
    finally:
        tracemalloc.stop()


@pytest.mark.parametrize("value", [True, 0, -1, 1.5])
def test_an_invalid_parallel_width_executes_no_function(value):
    with pytest.raises(HandlerError, match="max_parallel"):
        task().fan_out(
            ["one"], lambda key: pytest.fail("bad width ran work"), max_items=1, max_parallel=value
        )


def test_a_process_pool_refuses_a_closure_before_any_item_record():
    helper = task()
    with pytest.raises(HandlerError, match=r"importable.*top-level"):
        helper.fan_out(["one"], lambda key: ScriptResult(1), max_items=1, processes=True)
    assert helper._items.records == {}


def test_a_bad_item_result_is_a_recorded_failure():
    helper = task()
    with pytest.raises(HandlerError, match=r"one.*ScriptResult"):
        helper.fan_out(["one"], lambda key: None, max_items=1)
    assert helper._items.records["one"].status == ItemStatus.FAILED


def test_an_explicit_item_rerun_does_not_execute_another_failed_item():
    from etl_craft.core.items import ItemRecord

    helper = task()
    helper._items.records = {
        "selected": ItemRecord("selected", 1, ItemStatus.QUEUED, error="operator rerun"),
        "other": ItemRecord("other", 1, ItemStatus.FAILED, error="still failed"),
    }
    ran = []
    with pytest.raises(HandlerError, match="other"):
        helper.fan_out(
            ["selected", "other"], lambda key: ran.append(key) or ScriptResult(1), max_items=2
        )
    assert ran == ["selected"]
    assert helper._items.records["other"].status == ItemStatus.FAILED


@pytest.mark.parametrize("format_", ["text", "json"])
def test_item_command_renders_the_service_document(monkeypatch, format_):
    import argparse
    import io
    import json

    from etl_craft.cli.commands import items
    from etl_craft.cli.output import Output
    from etl_craft.core.items import ItemRecord, progress
    from etl_craft.services.operations.items import ItemView

    records = (ItemRecord("one", 1, ItemStatus.SUCCESS, row_count=2),)
    view = ItemView(1, records, progress(records))
    monkeypatch.setattr(items, "call", lambda *args, **kwargs: view)
    parser = argparse.ArgumentParser()
    items.configure(parser)
    args = parser.parse_args(
        ["list", "--pipeline-code", "P", "--task-code", "T", "--run-id", "1", "--format", format_]
    )
    output = io.StringIO()
    assert items.run(args, Output(stdout=output)) == 0
    if format_ == "json":
        assert json.loads(output.getvalue())["progress"]["done"] == 1
    else:
        assert "items 1 / 1, 0 failed, 0 skipped" in output.getvalue()
        assert "one\tSUCCESS\t1\t2" in output.getvalue()


def test_a_permanent_failure_keeps_its_class_and_does_not_retry():
    from etl_craft.scripting import PermanentError

    calls = []

    def item(key):
        calls.append(key)
        raise PermanentError("page one: forbidden; expected source access; correct its grant")

    with pytest.raises(PermanentError, match="forbidden"):
        task().fan_out(["one"], item, max_items=1, retries=3)
    assert calls == ["one"]


def test_an_empty_fresh_listing_does_not_discard_retained_unfinished_work():
    from etl_craft.core.items import ItemRecord

    helper = task()
    helper._items.records = {"retained": ItemRecord("retained", 1, ItemStatus.FAILED)}
    ran = []
    assert (
        helper.fan_out([], lambda key: ran.append(key) or ScriptResult(3), max_items=1).row_count
        == 3
    )
    assert ran == ["retained"]


def test_an_item_scoped_rerun_refuses_new_discovery_before_any_work():
    from etl_craft.core.items import ItemRecord

    helper = task()
    helper._items.records = {
        "selected": ItemRecord("selected", 1, ItemStatus.QUEUED, error="operator rerun")
    }
    with pytest.raises(HandlerError, match="recorded work list"):
        helper.fan_out(["selected", "new"], lambda key: pytest.fail("scope changed"), max_items=2)
    assert set(helper._items.records) == {"selected"}


@pytest.mark.parametrize("selection", [["--item-key", "one"], ["--all-failed"]])
def test_item_reruns_use_the_run_verb_and_the_item_operation(monkeypatch, selection):
    import argparse
    import io

    from etl_craft.cli.commands import item_requests
    from etl_craft.cli.commands import run as run_command
    from etl_craft.cli.output import Output
    from etl_craft.core.enums import ActionName, ItemAction
    from etl_craft.core.items import ItemProgress
    from etl_craft.services.operations.items import ItemView

    calls = []

    def call(args, name, request, **kwargs):
        calls.append((name, request))
        return ItemView(1, (), ItemProgress(0, 0, 0, 0))

    monkeypatch.setattr(item_requests, "call", call)
    parser = argparse.ArgumentParser()
    run_command.COMMAND.configure(parser)
    args = parser.parse_args(
        [
            "--pipeline-code",
            "P",
            "--task-code",
            "T",
            "--run-id",
            "1",
            "--rerun",
            "--reason",
            "verified",
            *selection,
        ]
    )
    assert run_command.COMMAND.run(args, Output(stdout=io.StringIO())) == 0
    name, request = calls.pop()
    assert name == ActionName.ITEM_CONTROL and request.action == ItemAction.RERUN
    assert request.run_id == 1 and request.task_code == "T"
    assert request.item_key == (None if selection == ["--all-failed"] else "one")


def test_a_process_mode_string_is_not_silently_treated_as_true():
    with pytest.raises(HandlerError, match="boolean"):
        task().fan_out(["one"], lambda key: ScriptResult(1), max_items=1, processes="false")


@pytest.mark.parametrize(
    "given",
    [
        {"action": "UNKNOWN"},
        {"action": "MARK", "status": "UNKNOWN"},
        {"action": "CANCEL", "status": "SUCCESS"},
        {"action": "MARK", "status": "SUCCESS", "rows": -1},
        {"action": "CANCEL", "item_key": "bad\x00key"},
    ],
)
def test_invalid_item_control_fields_fail_at_the_shared_request_boundary(given):
    from etl_craft.core.errors import UsageError
    from etl_craft.services.operations.items import ItemControl

    args = {
        "pipeline_code": "P",
        "task_code": "T",
        "run_id": 1,
        "item_key": "one",
        "reason": "reviewed",
    }
    with pytest.raises(UsageError):
        ItemControl(**(args | given))
