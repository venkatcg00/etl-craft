"""Per-HANDLER task execution — the closed vocabulary from CLAUDE.md's Handlers section.

Not implemented yet. This module exists so runner.py has a stable seam to
call through (`dispatch`) while the four handler bodies — PYTHON ingestion
scripts, SQL action-wrapping, BUSINESS_RULES, EMAIL_ALERT — get built
separately; each is its own substantial piece (the closed SQL action
vocabulary, `$$pipeline_id` substitution, the Data DB connection, script
invocation, business-rule sequencing) that CLAUDE.md leaves largely
unspecified at the implementation level.
"""

from __future__ import annotations

from dataclasses import dataclass


class HandlerError(Exception):
    """Raised when a task's HANDLER has no implementation, or execution fails."""


@dataclass(frozen=True)
class HandlerResult:
    """Counts a handler reports back, to stamp onto AUD_TASK_RUN_LOG via update_task_run."""

    source_count: int | None = None
    target_count: int | None = None
    insert_count: int | None = None
    update_count: int | None = None
    delete_count: int | None = None


def _not_implemented(handler: str) -> HandlerResult:
    raise HandlerError(f"HANDLER={handler!r} has no execution implementation yet")


HANDLER_REGISTRY = {
    "PYTHON": lambda: _not_implemented("PYTHON"),
    "SQL": lambda: _not_implemented("SQL"),
    "BUSINESS_RULES": lambda: _not_implemented("BUSINESS_RULES"),
    "EMAIL_ALERT": lambda: _not_implemented("EMAIL_ALERT"),
}


def dispatch(handler: str) -> HandlerResult:
    """Run the handler body for `handler` and return its result counts."""
    handler_fn = HANDLER_REGISTRY.get(handler)
    if handler_fn is None:
        raise HandlerError(f"unknown HANDLER: {handler!r}")
    return handler_fn()
