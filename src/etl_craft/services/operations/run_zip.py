"""Stream a pipeline run's logs and documents as one zip, with a manifest of every file.

The zip holds the run log, each attempt's log (one folder per task), and the run's status,
explain and history documents as JSON. Entries are compressed as they are read and handed on
chunk by chunk, so neither a log nor the zip is ever held whole in memory.
"""

import json
import zipfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import ClassVar

from etl_craft.core.errors import ResourceNotFoundError
from etl_craft.engine.connection import read_snapshot
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.runlog import select_run
from etl_craft.services.operations import inspect, run_log, status
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.requests import (
    HistoryInput,
    LogInput,
    RunFilter,
    RunSelection,
    TaskSelection,
)
from etl_craft.services.operations.serialization import to_json

Source = Callable[[], Iterator[bytes]]
UNREADABLE = (
    "log not readable here: removed by retention, not uploaded yet, or moved; "
    "`etl-craft logs --attempt-id {attempt_id}` names the exact reason"
)


@dataclass
class RunZip(Iterator[bytes]):
    """A zip being written as it is read; ``filename`` names it for a download."""

    CONTENT_TYPE: ClassVar[str] = "application/zip"
    filename: str
    chunks: Iterator[bytes]

    def __iter__(self) -> Iterator[bytes]:
        """Return the stream itself."""
        return self

    def __next__(self) -> bytes:
        """Return the next compressed bytes."""
        return next(self.chunks)


@dataclass
class _Sink:
    """A write-only stream the zip writer appends to and the generator empties."""

    parts: list[bytes] = field(default_factory=list)

    def write(self, data: bytes, /) -> int:
        """Keep written bytes until the next drain."""
        self.parts.append(bytes(data))
        return len(data)

    def flush(self) -> None:
        """Keep nothing beyond ``parts``."""

    def close(self) -> None:
        """Release nothing: the generator drains what remains."""

    def drain(self) -> Iterator[bytes]:
        """Hand on and forget what was written so far."""
        parts, self.parts = self.parts, []
        yield from parts


def run_zip(ctx: OperationContext, request: RunSelection) -> RunZip:
    """Select the run once, then stream its files under that exact run id."""
    with read_snapshot(ctx.engine) as conn:
        pipeline_id = resolve_pipeline_id(conn, request.pipeline_code)
        run_id = select_run(conn, pipeline_id, request.selector).pipeline_run_id
        rows = list(conn.execute(statement(conn, "run_log_attempts"), {"pipeline_run_id": run_id}))
    exact = RunSelection(pipeline_code=request.pipeline_code, run_id=run_id)
    files: list[tuple[str, Source, int | None]] = [
        ("run.log", lambda: run_log.run_log(ctx, exact), None),
        ("status.json", lambda: _document(status.pipeline_status(ctx, _filter(exact))), None),
        ("history.json", lambda: _document(inspect.run_history(ctx, _history(exact))), None),
    ]
    for task in dict.fromkeys(row.task_code for row in rows):
        selection = TaskSelection(pipeline_code=exact.pipeline_code, run_id=run_id, task_code=task)
        files.append((f"{task}/explain.json", _explained(ctx, selection), None))
    files += [
        (
            f"{row.task_code}/attempt-{row.attempt_number}.log",
            _attempt(ctx, row.attempt_id),
            row.attempt_id,
        )
        for row in rows
        if row.attempt_id is not None
    ]
    name = f"{request.pipeline_code}-run-{run_id}.zip"
    return RunZip(name, _write(files))


def _filter(selection: RunSelection) -> RunFilter:
    return RunFilter(pipeline_code=selection.pipeline_code, run_id=selection.run_id)


def _history(selection: RunSelection) -> HistoryInput:
    return HistoryInput(pipeline_code=selection.pipeline_code, run_id=selection.run_id)


def _explained(ctx: OperationContext, selection: TaskSelection) -> Source:
    return lambda: _document(status.explain_task(ctx, selection))


def _attempt(ctx: OperationContext, attempt_id: int) -> Source:
    return lambda: inspect.attempt_log(ctx, LogInput(attempt_id=attempt_id))


def _document(document: object) -> Iterator[bytes]:
    yield json.dumps(to_json(document), indent=2, sort_keys=True).encode() + b"\n"


def _write(files: list[tuple[str, Source, int | None]]) -> Iterator[bytes]:
    sink = _Sink()
    manifest: list[dict[str, object]] = []
    with zipfile.ZipFile(sink, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, source, attempt_id in files:
            try:
                chunks = source()
            except ResourceNotFoundError:
                if attempt_id is None:
                    raise
                manifest.append({"path": name, "missing": UNREADABLE.format(attempt_id=attempt_id)})
                continue
            size = 0
            with archive.open(name, "w", force_zip64=True) as entry:
                for chunk in chunks:
                    entry.write(chunk)
                    size += len(chunk)
                    yield from sink.drain()
            manifest.append({"path": name, "bytes": size})
            yield from sink.drain()
        archive.writestr("manifest.json", json.dumps({"files": manifest}, indent=2) + "\n")
    yield from sink.drain()
