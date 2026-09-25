"""What an ingestion script receives and returns: the ``HANDLER=PYTHON`` contract.

A task with ``HANDLER = 'PYTHON'`` names a script under the project's ``ingestion_scripts/`` in
``SCRIPT_NAME``. The script defines ``run``, which takes a ``ScriptTask`` and returns a
``ScriptResult``::

    from etl_craft.scripting import Offset, ScriptResult, ScriptTask

    def run(task: ScriptTask) -> ScriptResult:
        since = task.offset.value if task.offset else 0
        rows = read_source_after(since)            # the script's own code
        with task.warehouse() as engine, engine.begin() as conn:
            written = write(conn, rows, pipeline_run_id=task.pipeline_run_id)
        return ScriptResult(
            row_count=written,
            offset=Offset.number(max(r.id for r in rows)) if rows else None,
        )

The script reads its source from where the last successful run left off (``task.offset``) and
writes its table, stamping each row with ``task.pipeline_run_id``. It reports the rows it wrote,
which the engine records as the task's source, target and insert counts, and, when it moved on,
the new offset, which the engine stores for the next run. A script that needs neither the offset
nor ``INPUT_PARAMS`` may define ``run()`` without a parameter. Everything it prints or logs goes
to the task attempt's log.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.enums import OffsetType
from etl_craft.core.errors import HandlerError
from etl_craft.warehouse.connection import open_warehouse

OffsetValue = int | Decimal | str | datetime


@dataclass(frozen=True)
class Offset:
    """Where a script left off: a number, a text, or a timestamp.

    Build one with ``Offset.number``, ``Offset.text`` or ``Offset.timestamp``.
    """

    type: OffsetType
    value: OffsetValue

    @classmethod
    def number(cls, value: int | Decimal) -> Offset:
        """Build an offset that is a number, such as the largest id read."""
        if isinstance(value, bool) or not isinstance(value, int | Decimal):
            raise HandlerError(f"Offset.number needs an int or Decimal, got {value!r}")
        return cls(OffsetType.NUMBER, value)

    @classmethod
    def text(cls, value: str) -> Offset:
        """Build an offset that is text, such as a cursor or a file name."""
        if not isinstance(value, str):
            raise HandlerError(f"Offset.text needs a str, got {value!r}")
        return cls(OffsetType.TEXT, value)

    @classmethod
    def timestamp(cls, value: datetime) -> Offset:
        """Build an offset that is a point in time, such as the latest change read."""
        if not isinstance(value, datetime):
            raise HandlerError(f"Offset.timestamp needs a datetime, got {value!r}")
        return cls(OffsetType.TIMESTAMP, value)

    def stored(self) -> str:
        """Return the text the offset is stored as."""
        if isinstance(self.value, datetime):
            return self.value.isoformat()
        return str(self.value)

    @classmethod
    def from_stored(cls, offset_type: str, text: str) -> Offset:
        """Read an offset back from its stored type and text."""
        try:
            kind = OffsetType(offset_type)
            if kind == OffsetType.NUMBER:
                number = Decimal(text)
                return cls(kind, int(number) if number == number.to_integral_value() else number)
            if kind == OffsetType.TIMESTAMP:
                return cls(kind, datetime.fromisoformat(text))
            return cls(kind, text)
        except (ValueError, InvalidOperation) as error:
            raise HandlerError(
                f"the stored offset {text!r} is not a valid {offset_type}: {error}"
            ) from error


@dataclass(frozen=True)
class ScriptResult:
    """What a script reports: the rows it wrote, where it left off, and any values of its own.

    ``row_count`` is required, a whole number of 0 or more. ``offset`` of ``None`` keeps the
    stored offset. ``variables`` are listed in the task log as ``NAME = value`` lines.
    """

    row_count: int
    offset: Offset | None = None
    variables: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ScriptTask:
    """What a script is given to run.

    ``offset`` is where the last successful run left off, ``None`` on the first run.
    ``input_params`` is the task's ``INPUT_PARAMS``, a JSON array, as a list. ``force`` is true
    when the task was run with ``--force``.
    """

    pipeline_code: str
    task_code: str
    pipeline_run_id: int
    refresh_type: str
    offset: Offset | None
    input_params: list[Any]
    task_params: Mapping[str, str]
    force: bool
    config: ConnectorConfig
    engine_db: Engine
    logger: logging.Logger

    @contextmanager
    def warehouse(self) -> Iterator[Engine]:
        """Open the warehouse, queued behind other writers where it allows only one."""
        with open_warehouse(self.config, self.engine_db) as engine:
            yield engine
