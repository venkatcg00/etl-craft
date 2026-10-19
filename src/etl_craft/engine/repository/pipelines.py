"""Pipelines: resolving a code, and a pipeline's row."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from sqlalchemy.engine import Connection

from etl_craft.core.errors import MetadataError
from etl_craft.core.text import suggest
from etl_craft.core.time import as_utc
from etl_craft.engine.queries import statement


def with_suggestions(message: str, unknown: str, candidates: list[str]) -> str:
    """Append a "did you mean" list to ``message`` when any candidate is close."""
    hints = suggest(unknown, candidates)
    return f"{message} — did you mean: {', '.join(hints)}" if hints else message


def resolve_pipeline_id(conn: Connection, pipeline_code: str) -> int:
    """Return the id of active pipeline ``pipeline_code``; ``MetadataError`` if there is none."""
    pipeline_id = conn.execute(
        statement(conn, "pipeline_id_by_code"), {"pipeline_code": pipeline_code}
    ).scalar_one_or_none()
    if pipeline_id is None:
        known = list(conn.execute(statement(conn, "active_pipeline_codes")).scalars())
        raise MetadataError(
            with_suggestions(
                f"no active pipeline with PIPELINE_CODE={pipeline_code!r}", pipeline_code, known
            )
        )
    return int(pipeline_id)


@dataclass(frozen=True)
class PipelineDetail:
    """A pipeline's row, with tags and alert settings from ``PIPELINE_PARAMETERS`` as fields."""

    pipeline_code: str
    pipeline_name: str
    description: str | None
    run_schedule: str | None
    sla_in_hours: float | None
    refresh_type: str
    created_by: str | None
    create_date: datetime | None = None
    schedule_timezone: str | None = None
    schedule_start_date: date | None = None
    tags: list[str] | None = None
    email_recipients: list[str] | None = None


def fetch_pipeline_detail(conn: Connection, pipeline_id: int) -> PipelineDetail:
    """Return the row of ``pipeline_id``, which must exist.

    ``PIPELINE_PARAMETERS`` is JSON: PostgreSQL returns it decoded, SQLite as text.
    """
    row = conn.execute(statement(conn, "pipeline_detail"), {"pipeline_id": pipeline_id}).one()
    values = dict(row._mapping)
    params: Any = values.pop("pipeline_parameters") or {}
    if isinstance(params, str):
        params = json.loads(params)
    values["create_date"] = None if row.create_date is None else as_utc(row.create_date)
    values["sla_in_hours"] = None if row.sla_in_hours is None else float(row.sla_in_hours)
    values["schedule_start_date"] = (
        None
        if row.schedule_start_date is None
        else date.fromisoformat(str(row.schedule_start_date))
    )
    return PipelineDetail(
        **values,
        **{name.lower(): params.get(name) for name in PIPELINE_PARAMETER_KINDS},
    )


def _is_strings(value: object) -> bool:
    return isinstance(value, list) and all(isinstance(item, str) for item in value)


PIPELINE_PARAMETER_KINDS: dict[str, tuple[str, Callable[[object], bool]]] = {
    "TAGS": ("a list of strings", _is_strings),
    "EMAIL_RECIPIENTS": ("a list of strings", _is_strings),
}
"""Each ``PIPELINE_PARAMETERS`` key, what its value must be, and the test for it."""


def pipeline_parameter_problems(stored: object) -> tuple[list[str], list[str]]:
    """Check a pipeline's ``PIPELINE_PARAMETERS`` as stored; return ``(problems, unknown keys)``.

    A value of the wrong type is a problem. Unknown keys are returned for refusal by validation.
    """
    if stored is None:
        return [], []
    try:
        params = json.loads(stored) if isinstance(stored, str) else stored
    except json.JSONDecodeError as error:
        return [f"PIPELINE_PARAMETERS is not valid JSON ({error})"], []
    if not isinstance(params, dict):
        return [f"PIPELINE_PARAMETERS must be a JSON object; it is {json.dumps(params)}"], []
    problems: list[str] = []
    unknown: list[str] = []
    for name, value in params.items():
        kind = PIPELINE_PARAMETER_KINDS.get(name)
        if kind is None:
            unknown.append(name)
        elif not kind[1](value):
            problems.append(f"PIPELINE_PARAMETERS.{name}={json.dumps(value)} must be {kind[0]}")
    return problems, unknown


def fetch_pipeline_handlers(conn: Connection, pipeline_id: int) -> set[str]:
    """Return the handler of every active task in ``pipeline_id``: what a run will connect to."""
    rows = conn.execute(statement(conn, "pipeline_handlers"), {"pipeline_id": pipeline_id})
    return {row.handler for row in rows}
