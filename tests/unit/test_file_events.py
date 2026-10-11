"""Native file notifications and their polling fallback."""

import threading

import pytest
from watchdog.events import FileCreatedEvent, FileOpenedEvent

from etl_craft.execution.file_events import FileEvents, _Changes

pytestmark = pytest.mark.unit


def test_notifications_ignore_unrelated_files_and_read_access():
    changed = threading.Event()
    handler = _Changes(changed, "/inbox/*.csv")
    handler.on_any_event(FileCreatedEvent("/inbox/engine.db"))
    handler.on_any_event(FileOpenedEvent("/inbox/ready.csv"))
    assert not changed.is_set()
    handler.on_any_event(FileCreatedEvent("/inbox/ready.csv"))
    assert changed.is_set()


def test_native_notification_wakes_wait_for_a_future_directory_and_closes(tmp_path):
    watches = FileEvents()
    try:
        watches.watch(1, "inbox/*.csv", tmp_path)
        assert not watches.pending()
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        assert watches.changes[1].wait(5)
        watches.changed(1)
        (inbox / "delivery.csv").write_text("ready")
        assert watches.changes[1].wait(5)
        assert watches.changed(1)
    finally:
        watches.close()
    assert watches.observer is None


def test_unavailable_watch_keeps_polling_fallback(tmp_path, monkeypatch):
    def unavailable(*args, **kwargs):
        raise OSError("watch limit")

    monkeypatch.setattr("etl_craft.execution.file_events.Observer", unavailable)
    watches = FileEvents()
    watches.watch(1, "*.csv", tmp_path)
    assert not watches.pending()
    watches.close()


def test_completed_wait_releases_only_its_shared_directory_watch(tmp_path):
    watches = FileEvents()
    try:
        watches.watch(1, "*.csv", tmp_path)
        watches.watch(2, "*.json", tmp_path)
        watches.remove(1)
        (tmp_path / "ready.json").write_text("{}")
        assert watches.changes[2].wait(5)
        assert 1 not in watches.changes
        watches.remove(2)
        assert not watches.watches
        assert not watches.pending()
    finally:
        watches.close()
