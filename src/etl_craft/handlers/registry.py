"""What a task handler receives and returns, and which handler runs each ``HANDLER`` value.

A handler is a function ``(context, engine_db) -> HandlerResult``. It runs inside the task's own
process; a problem it can explain is a ``HandlerError``, which records the task ``FAILED`` with
that message. Handlers are registered by import path, so a task process imports only the
handler it runs.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.errors import HandlerError


@dataclass(frozen=True)
class TaskContext:
    """Everything a handler needs about the task it runs, resolved before it starts.

    ``force`` is set by ``run --force``. ``rerun`` is set when the task already ended ``SUCCESS``
    or ``SKIPPED`` under the run and is run again, so nothing it did before is skipped.
    ``run_date`` is the date the run runs as of (SQL's ``$$run_date``), and ``backfill`` says the
    run is part of a backfill.
    """

    config: ConnectorConfig
    pipeline_id: int
    pipeline_code: str
    task_id: int
    task_code: str
    pipeline_run_id: int
    task_run_id: int
    attempt: int
    handler: str
    refresh_type: str
    task_params: Mapping[str, str]
    force: bool = False
    rerun: bool = False
    run_date: date = field(default_factory=lambda: datetime.now(UTC).date())
    backfill: bool = False


@dataclass(frozen=True)
class HandlerResult:
    """The counts a handler reports for ``AUD_TASK_RUN_LOG``, and any named values.

    ``variables`` are the values a task reports by name (an ingestion script's declared return
    values); they are listed in ``TASK_LOG`` as ``NAME = value`` lines.
    """

    source_count: int | None = None
    target_count: int | None = None
    insert_count: int | None = None
    update_count: int | None = None
    delete_count: int | None = None
    variables: Mapping[str, object] = field(default_factory=dict)


def format_task_log(result: HandlerResult) -> str | None:
    """Render ``result`` as ``NAME = value`` lines: its counts, then its variables."""
    values: Mapping[str, object | None] = {
        "SOURCE_COUNT": result.source_count,
        "TARGET_COUNT": result.target_count,
        "INSERT_COUNT": result.insert_count,
        "UPDATE_COUNT": result.update_count,
        "DELETE_COUNT": result.delete_count,
        **result.variables,
    }
    lines = [f"{name} = {value}" for name, value in values.items() if value is not None]
    return "\n".join(lines) or None


Handler = Callable[[TaskContext, Engine], HandlerResult]

HANDLERS: dict[str, str] = {
    "SQL": "etl_craft.handlers.sql:run",
    "BUSINESS_RULES": "etl_craft.handlers.business_rules:run",
    "PYTHON": "etl_craft.handlers.python_scripts:run",
    "EMAIL_ALERT": "etl_craft.handlers.email_alert:run",
}
"""Each ``HANDLER`` value and the ``module:function`` that runs it."""

COMMON_PARAMETERS = frozenset({"TASK_TIMEOUT_SECONDS", "DOCUMENTATION"})
"""The task parameters every handler's tasks may set."""


def resolve_handler(name: str) -> Handler:
    """Import and return the handler for ``name``; ``HandlerError`` when none is installed."""
    target = HANDLERS.get(name)
    if target is None:
        raise HandlerError(f"no handler is installed for HANDLER {name!r}")
    module_name, _, function_name = target.partition(":")
    handler: Handler = getattr(importlib.import_module(module_name), function_name)
    return handler
