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
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.config.targets import active_catalog
from etl_craft.core.enums import OffsetType
from etl_craft.core.errors import HandlerError
from etl_craft.core.text import qualify
from etl_craft.warehouse.connection import open_warehouse

OffsetValue = int | Decimal | str | datetime


@dataclass(frozen=True)
class Offset:
    """Where a script left off: a value and its datatype, ``NUMBER``, ``TEXT`` or ``TIMESTAMP``.

    The datatype is always given, never guessed from the value. The value is cast to it, as
    ``CAST(value AS datatype)`` would in SQL: ``Offset("7", "NUMBER")`` holds ``7``, and
    ``Offset("abc", "NUMBER")`` fails. A ``NUMBER`` is an ``int`` or a ``Decimal``, a ``TEXT`` a
    ``str``, a ``TIMESTAMP`` a ``datetime`` (ISO 8601 text is read as one). The offset is
    stored as text beside its datatype, and a script gets it back cast to that datatype.
    ``Offset.number``, ``Offset.text`` and ``Offset.timestamp`` are shorthands.
    """

    value: OffsetValue
    datatype: OffsetType

    def __post_init__(self) -> None:
        """Check the datatype and cast the value to it."""
        try:
            kind = OffsetType(str(self.datatype).strip().upper())
        except ValueError:
            raise HandlerError(
                f"offset datatype {self.datatype!r} is not one of NUMBER, TEXT, TIMESTAMP"
            ) from None
        object.__setattr__(self, "datatype", kind)
        object.__setattr__(self, "value", _cast(self.value, kind))

    @classmethod
    def number(cls, value: int | Decimal) -> Offset:
        """Build a ``NUMBER`` offset, such as the largest id read."""
        return cls(value, OffsetType.NUMBER)

    @classmethod
    def text(cls, value: str) -> Offset:
        """Build a ``TEXT`` offset, such as a cursor or a file name."""
        return cls(value, OffsetType.TEXT)

    @classmethod
    def timestamp(cls, value: datetime) -> Offset:
        """Build a ``TIMESTAMP`` offset, such as the latest change read."""
        return cls(value, OffsetType.TIMESTAMP)

    def stored(self) -> str:
        """Return the text the offset is stored as, beside its datatype."""
        if isinstance(self.value, datetime):
            return self.value.isoformat()
        return str(self.value)

    @classmethod
    def from_stored(cls, datatype: str, text: str) -> Offset:
        """Read an offset back from its stored datatype and text, cast to that datatype."""
        return cls(text, OffsetType(datatype))


def _cast(value: object, kind: OffsetType) -> OffsetValue:
    """Cast ``value`` to ``kind``; ``HandlerError`` when it cannot be, as a SQL CAST fails."""
    failure = HandlerError(f"cannot cast {type(value).__name__} {value!r} to a {kind} offset")
    if isinstance(value, bool) or value is None:
        raise failure
    if kind == OffsetType.NUMBER:
        if isinstance(value, int | Decimal):
            return value
        try:
            number = Decimal(str(value).strip())
        except InvalidOperation:
            raise failure from None
        if not number.is_finite():
            raise failure
        return int(number) if number == number.to_integral_value() else number
    if kind == OffsetType.TIMESTAMP:
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.strip())
            except ValueError:
                raise failure from None
        raise failure
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str | int | Decimal):
        return str(value)
    raise failure


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
    ``input_params`` is the task's ``INPUT_PARAMS``, a JSON object, as a dictionary. ``force`` is
    true when the task was run with ``--force``. ``run_date`` is the date the run runs as of: the
    day it started, or the date a backfill run is for. In a backfill (``backfill``), ``offset``
    is ``None`` and an offset the script returns is not stored: a backfill reads its source for
    ``run_date``, and leaves the next scheduled run where the last one left off.
    """

    pipeline_code: str
    task_code: str
    pipeline_run_id: int
    refresh_type: str
    offset: Offset | None
    input_params: Mapping[str, Any]
    task_params: Mapping[str, str]
    force: bool
    config: ConnectorConfig
    engine_db: Engine
    logger: logging.Logger
    run_date: date = field(default_factory=lambda: datetime.now(UTC).date())
    backfill: bool = False

    def table(self, name: str) -> str:
        """Return ``schema.table`` as its full name in the active warehouse database.

        ``database.schema.table`` is kept as written, so a script names its tables once and
        writes to the development, test or production database the profile selects.
        """
        return qualify(name, active_catalog(self.config))

    @contextmanager
    def warehouse(self) -> Iterator[Engine]:
        """Open the warehouse, queued behind other writers where it allows only one."""
        with open_warehouse(self.config, self.engine_db) as engine:
            yield engine
