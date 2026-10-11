"""Filesystem notifications hint when to repeat the authoritative durable file check."""

from __future__ import annotations

import fnmatch
import glob
import logging
import threading
from pathlib import Path

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.api import BaseObserver, ObservedWatch

logger = logging.getLogger(__name__)


class _Changes(FileSystemEventHandler):
    def __init__(self, changed: threading.Event, pattern: str) -> None:
        self.changed, self.pattern = changed, pattern

    def on_any_event(self, event: FileSystemEvent) -> None:
        if event.event_type not in {"created", "modified", "moved", "deleted", "closed"}:
            return
        if event.is_directory:
            if event.event_type in {"created", "moved", "deleted"}:
                self.changed.set()
        elif any(
            fnmatch.fnmatch(str(path), self.pattern)
            for path in (event.src_path, getattr(event, "dest_path", ""))
        ):
            self.changed.set()


class FileEvents:
    """Share one observer per run; periodic scans recover missing or unsupported notifications."""

    def __init__(self) -> None:
        """Allocate no threads until a directory can be watched."""
        self.observer: BaseObserver | None = None
        self.changes: dict[int, threading.Event] = {}
        self.watches: dict[int, tuple[ObservedWatch, _Changes]] = {}

    def watch(self, task_id: int, pattern: str, project_dir: Path) -> None:
        """Watch the fixed prefix of the glob, including future directories."""
        changed = self.changes.setdefault(task_id, threading.Event())
        path = Path(pattern)
        if not path.is_absolute():
            path = project_dir / path
        path = path.resolve()
        recursive = "**" in str(path) or any(glob.has_magic(p) for p in path.parts[:-1])
        directory = Path(path.anchor)
        for part in path.parts[1:-1]:
            if glob.has_magic(part):
                break
            directory /= part
        while not directory.is_dir() and directory != directory.parent:
            recursive = True
            directory = directory.parent
        # Avoid a recursive watch of the whole machine for an unbounded glob.
        if directory == Path(directory.anchor):
            return
        try:
            if self.observer is None:
                self.observer = Observer()
                self.observer.start()
            self.observer.schedule(
                _Changes(changed, str(path)), str(directory), recursive=recursive
            )
        except (OSError, ValueError) as error:
            logger.warning(
                "EVENT_WAIT task_id=%s: file notifications unavailable: %s; polling", task_id, error
            )

    def changed(self, task_id: int) -> bool:
        """Consume a hint before scanning, so a change during the scan remains pending."""
        event = self.changes.get(task_id)
        if event is None or not event.is_set():
            return False
        event.clear()
        return True

    def pending(self) -> bool:
        """Whether any directory change needs a check."""
        return any(event.is_set() for event in self.changes.values())

    def remove(self, task_id: int) -> None:
        """Release a completed task's watch without removing other tasks on that directory."""
        self.changes.pop(task_id, None)
        registered = self.watches.pop(task_id, None)
        if self.observer is not None and registered is not None:
            watch, handler = registered
            self.observer.remove_handler_for_watch(handler, watch)
            if not any(other == watch for other, _ in self.watches.values()):
                self.observer.unschedule(watch)

    def close(self) -> None:
        """Release all watch handles and join the observer's threads."""
        if self.observer is not None:
            self.observer.stop()
            self.observer.join()
            self.observer = None
