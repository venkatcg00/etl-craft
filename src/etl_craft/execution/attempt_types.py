"""The outcome of a task attempt, operator overrides, and the options a task's child runs with."""

from __future__ import annotations

import threading
from dataclasses import dataclass

from etl_craft.core.enums import (
    InterventionAction,
    RunStatus,
)
from etl_craft.execution.supervisor import (
    KILL_GRACE_SECONDS,
)

CHILD_MODULE = "etl_craft.execution.child"
"""The module a task attempt runs as, with ``python -m``."""

CANCEL_POLL_SECONDS = 2.0
"""How often a running task looks whether an operator cancelled its run."""


@dataclass(frozen=True)
class TaskOutcome:
    """How ``run --task_code`` ended: ``SUCCESS``, ``FAILED`` or ``SKIPPED``, and why.

    ``reopened`` is the status of the ended run that ``force`` put back ``IN-PROGRESS`` to run
    the task; the caller ends that run again (``pipeline.force_task``).
    """

    status: RunStatus
    message: str
    task_run_id: int | None = None
    reopened: str | None = None
    waiting: bool = False


@dataclass(frozen=True)
class Override:
    """An operator's override of the checks before a task runs, with the reason for it.

    ``rerun`` runs the task again although it already ended ``SUCCESS`` or ``SKIPPED``, as a
    new attempt that skips nothing, reopening the run if it had ended (``--rerun``). Otherwise
    the task runs without its dependencies being checked (``--ignore-dependencies``). Neither
    checks the task's dependencies or consumes an upstream run, and each is recorded in
    ``AUD_RUN_INTERVENTIONS``.
    """

    reason: str
    rerun: bool = False

    @property
    def action(self) -> InterventionAction:
        """How the override is recorded."""
        return InterventionAction.RERUN if self.rerun else InterventionAction.IGNORE_DEPENDENCIES


@dataclass(frozen=True)
class ChildOptions:
    """How the task process is started: its module, log settings and kill grace period.

    Setting ``cancel`` stops a running task process; the pipeline runner sets it when the run is
    interrupted. ``cancel_poll_seconds`` is how often a running task looks whether an operator
    cancelled its run.
    """

    module: str = CHILD_MODULE
    log_level: str = "INFO"
    log_format: str = "text"
    kill_grace_seconds: float = KILL_GRACE_SECONDS
    cancel: threading.Event | None = None
    cancel_poll_seconds: float = CANCEL_POLL_SECONDS
