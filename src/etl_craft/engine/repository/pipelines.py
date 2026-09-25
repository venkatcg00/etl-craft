"""Pipelines: resolving a code, and a pipeline's row."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from sqlalchemy.engine import Connection

from etl_craft.core.errors import MetadataError
from etl_craft.core.text import suggest
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
    """A pipeline's row, with the DAG settings from ``PIPELINE_PARAMETERS`` as typed fields."""

    pipeline_code: str
    pipeline_name: str
    description: str | None
    run_schedule: str | None
    sla_in_hours: float | None
    refresh_type: str
    created_by: str | None
    catchup: bool | None = None
    tags: list[str] | None = None
    retries: int | None = None
    retry_delay_minutes: int | None = None
    depends_on_past: bool | None = None
    email_on_failure: bool | None = None
    email_recipients: list[str] | None = None


def fetch_pipeline_detail(conn: Connection, pipeline_id: int) -> PipelineDetail:
    """Return the row of ``pipeline_id``, which must exist.

    ``PIPELINE_PARAMETERS`` is JSON: PostgreSQL returns it decoded, SQLite as text.
    """
    row = conn.execute(statement(conn, "pipeline_detail"), {"pipeline_id": pipeline_id}).one()
    params: Any = row.pipeline_parameters or {}
    if isinstance(params, str):
        params = json.loads(params)
    return PipelineDetail(
        pipeline_code=row.pipeline_code,
        pipeline_name=row.pipeline_name,
        description=row.description,
        run_schedule=row.run_schedule,
        sla_in_hours=float(row.sla_in_hours) if row.sla_in_hours is not None else None,
        refresh_type=row.refresh_type,
        created_by=row.created_by,
        catchup=params.get("CATCHUP"),
        tags=params.get("TAGS"),
        retries=params.get("RETRIES"),
        retry_delay_minutes=params.get("RETRY_DELAY_MINUTES"),
        depends_on_past=params.get("DEPENDS_ON_PAST"),
        email_on_failure=params.get("EMAIL_ON_FAILURE"),
        email_recipients=params.get("EMAIL_RECIPIENTS"),
    )


def _is_count(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _is_strings(value: object) -> bool:
    return isinstance(value, list) and all(isinstance(item, str) for item in value)


PIPELINE_PARAMETER_KINDS: dict[str, tuple[str, Callable[[object], bool]]] = {
    "CATCHUP": ("true or false", lambda value: isinstance(value, bool)),
    "DEPENDS_ON_PAST": ("true or false", lambda value: isinstance(value, bool)),
    "EMAIL_ON_FAILURE": ("true or false", lambda value: isinstance(value, bool)),
    "RETRIES": ("a whole number, 0 or more", _is_count),
    "RETRY_DELAY_MINUTES": ("a whole number, 0 or more", _is_count),
    "TAGS": ("a list of strings", _is_strings),
    "EMAIL_RECIPIENTS": ("a list of strings", _is_strings),
}
"""Each ``PIPELINE_PARAMETERS`` key, what its value must be, and the test for it."""


def pipeline_parameter_problems(stored: object) -> tuple[list[str], list[str]]:
    """Check a pipeline's ``PIPELINE_PARAMETERS`` as stored; return ``(problems, unknown keys)``.

    A value of the wrong type is a problem. A key etl-craft does not read is returned apart:
    it may be a typo, and it is ignored either way.
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
