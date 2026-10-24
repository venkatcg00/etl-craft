"""One immutable admitted metadata image, propagated with the execution context."""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass

from etl_craft.core.errors import RunStateError


@dataclass(frozen=True)
class Definition:
    """Stored table shapes and rows, with no security or capacity state."""

    text: str
    run_id: int | None
    columns: dict[str, tuple[str, ...]]
    rows: dict[str, tuple[dict[str, object], ...]]
    bundle_id: str | None = None

    @classmethod
    def read(cls, text: str, run_id: int | None = None, bundle_id: str | None = None) -> Definition:
        """Reject malformed persisted metadata rather than consulting today's definition."""
        problem = RunStateError(
            "admitted metadata snapshot is malformed; expected the recorded "
            "definition; restore the Engine DB from a verified backup"
        )
        try:
            data = json.loads(text)
            columns = {}
            rows = {}
            for name, table in data.items():
                if not re.fullmatch(r"CFG_[A-Z_]+", name) or not isinstance(table, dict):
                    raise problem
                fields, records = table["columns"], table["rows"]
                if (
                    not isinstance(fields, list)
                    or not fields
                    or any(
                        not isinstance(c, str) or not re.fullmatch(r"[a-z_]+", c) for c in fields
                    )
                    or not isinstance(records, list)
                    or any(not isinstance(r, dict) or set(r) != set(fields) for r in records)
                ):
                    raise problem
                columns[name] = tuple(fields)
                rows[name] = tuple(records)
            return cls(text, run_id, columns, rows, bundle_id)
        except (ValueError, TypeError, KeyError, AttributeError) as error:
            raise problem from error

    def row(self, table: str, column: str, value: object) -> dict[str, object]:
        """Select an exact definition, never substitute a newer row."""
        for row in self.rows[table]:
            if row[column] == value:
                return row
        raise RunStateError(
            f"metadata snapshot {table}.{column}={value!r}: missing; expected the "
            f"admitted row; inspect the run's metadata and restore its snapshot"
        )


current: ContextVar[Definition | None] = ContextVar("admitted_metadata", default=None)


@contextmanager
def using(definition: Definition | None) -> Iterator[None]:
    """Scope definition reads without freezing permissions, pools, leases or live outcomes."""
    token = current.set(definition)
    try:
        yield
    finally:
        current.reset(token)
