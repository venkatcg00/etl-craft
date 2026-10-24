"""Pipelines: resolving a code, and a pipeline's row."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from sqlalchemy.engine import Connection

from etl_craft.core.errors import MetadataError
from etl_craft.core.tags import tag_problems
from etl_craft.core.text import suggest
from etl_craft.core.time import as_utc
from etl_craft.engine.queries import project_statement, statement

_project_id: ContextVar[int | None] = ContextVar("etl_craft_project_id", default=None)


@contextmanager
def selected_project(project_id: int | None) -> Iterator[None]:
    """Scope code-based pipeline lookups to one selected project."""
    token = _project_id.set(project_id)
    try:
        yield
    finally:
        _project_id.reset(token)


def current_project_id() -> int:
    """Return the selected project, defaulting legacy local execution to its migrated project."""
    return _project_id.get() or 1


def with_suggestions(message: str, unknown: str, candidates: list[str]) -> str:
    """Append a "did you mean" list to ``message`` when any candidate is close."""
    hints = suggest(unknown, candidates)
    return f"{message} — did you mean: {', '.join(hints)}" if hints else message


def resolve_pipeline_id(conn: Connection, pipeline_code: str) -> int:
    """Return the id of active pipeline ``pipeline_code``; ``MetadataError`` if there is none."""
    pipeline_id = conn.execute(
        project_statement(conn, "pipeline_id_by_code"),
        {"pipeline_code": pipeline_code, "project_id": current_project_id()},
    ).scalar_one_or_none()
    if pipeline_id is None:
        known = list(
            conn.execute(
                project_statement(conn, "active_pipeline_codes"),
                {"project_id": current_project_id()},
            ).scalars()
        )
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
    fields = {name.lower(): params.get(name) for name in PIPELINE_PARAMETER_KINDS}
    if "TAGS" in params:
        fields["tags"] = list(read_tags(params, str(values["pipeline_code"])))
    return PipelineDetail(**values, **fields)


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
        elif name == "TAGS":
            problems.extend(tag_problems(value))
        elif not kind[1](value):
            problems.append(f"PIPELINE_PARAMETERS.{name}={json.dumps(value)} must be {kind[0]}")
    return problems, unknown


def fetch_pipeline_handlers(conn: Connection, pipeline_id: int) -> set[str]:
    """Return the handler of every active task in ``pipeline_id``: what a run will connect to."""
    rows = conn.execute(statement(conn, "pipeline_handlers"), {"pipeline_id": pipeline_id})
    return {row.handler for row in rows}


def read_tags(stored: object, pipeline_code: str) -> tuple[str, ...]:
    """Read current labels, refusing invalid metadata rather than hiding pipelines."""
    try:
        params = json.loads(stored) if isinstance(stored, str) else stored
    except json.JSONDecodeError as error:
        raise MetadataError(
            f"{pipeline_code}: PIPELINE_PARAMETERS={stored!r}; expected a JSON object; "
            "correct the config and apply it"
        ) from error
    if params is None:
        return ()
    if not isinstance(params, dict):
        raise MetadataError(
            f"{pipeline_code}: PIPELINE_PARAMETERS={params!r}; expected a JSON object; "
            "correct the config and apply it"
        )
    values = params.get("TAGS", [])
    problems = tag_problems(values)
    if problems:
        raise MetadataError(f"{pipeline_code}: " + "; ".join(problems))
    return tuple(values)


def tagged_pipelines(conn: Connection, tags: tuple[str, ...]) -> dict[str, int]:
    """Resolve an AND filter against current active definitions in code order."""
    return {
        row.pipeline_code: row.pipeline_id
        for row in conn.execute(statement(conn, "active_pipelines"))
        if set(tags) <= set(read_tags(row.pipeline_parameters, row.pipeline_code))
    }
