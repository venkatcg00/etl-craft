"""Shared, DB-free types every HANDLER body (sql_actions/business_rules/scripts) is built around.

Split out from handlers.py specifically to avoid a circular import: handlers.py
dispatches to sql_actions.py/business_rules.py/scripts.py, and each of those
needs to raise HandlerError / return a HandlerResult / read a
TaskExecutionContext — importing those three from handlers.py itself would
make handlers.py import the very modules it dispatches to.
"""

from __future__ import annotations

from dataclasses import dataclass

from etl_craft.config import ConnectorConfig


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
    task_log: str | None = None


@dataclass(frozen=True)
class TaskExecutionContext:
    """Everything a HANDLER body needs, resolved once by runner.py before dispatch.

    Built in the parent process (runner.run_task) from CFG_ reads, then
    handed across the crash-detection fork alongside `config` — same as
    `config` already was before this existed, no new pickling concern (see
    runner.py's own [CHOICE] comment on why `fork`, not `spawn`, makes that
    safe: the child gets the parent's already-bound objects directly, not a
    serialized copy).
    """

    config: ConnectorConfig
    pipeline_id: int
    pipeline_code: str
    task_id: int
    task_code: str
    task_run_id: int
    pipeline_run_id: int
    handler: str
    refresh_type: str
    schema_evolution: bool
    script_name: str | None
    task_params: dict[str, str]
    # Per explicit instruction: a manually/ad-hoc-triggered task (--force)
    # runs a BUSINESS_RULES check against *all* data, not scoped to this
    # run's own PIPELINE_RUN_ID. [CHOICE] --force is reused as that signal
    # rather than inventing a second flag — it already means "bypass the
    # normal gating and just run this task standalone" everywhere else in
    # the CLI, so it's a consistent, not a new, meaning here.
    force: bool
