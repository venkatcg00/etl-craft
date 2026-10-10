"""Where attempts run: the pool interface the scheduler, the overseer and the CLI use.

The caller admits an attempt (binds its task run, queues the attempt and records its gate
decisions) and submits its ``AttemptSpec``; the pool claims the attempt, runs its task process,
renews its lease and records how it ended. Nothing else starts a task process.

Rules every pool keeps: submitting the same ``attempt_id`` twice returns the same handle and
starts nothing new; the status of an unknown handle is ``UNKNOWN``, never an exception; and
``reconcile`` reports only what the pool can confirm.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol

from etl_craft.core.enums import Handler

if TYPE_CHECKING:
    from etl_craft.execution.runner import TaskOutcome

SLOT_KINDS = ("ingestion", "warehouse")
"""Python ingestion scripts take ``ingestion`` slots; every other handler, ``warehouse`` ones."""


def slot_kind(handler: str) -> str:
    """Return the kind of slot a task with ``handler`` runs in."""
    return "ingestion" if handler == Handler.PYTHON else "warehouse"


@dataclass(frozen=True)
class AttemptSpec:
    """An admitted attempt, with everything a pool needs to run it."""

    attempt_id: int
    attempt_number: int
    task_run_id: int
    pipeline_run_id: int
    pipeline_code: str
    task_code: str
    handler: str
    timeout_seconds: int
    lease_seconds: int
    force: bool = False
    rerun: bool = False

    @property
    def slot_kind(self) -> str:
        """The kind of slot this attempt runs in."""
        return slot_kind(self.handler)


@dataclass(frozen=True)
class Capacity:
    """Slots per kind, and how many of each are free now."""

    slots: Mapping[str, int]
    free: Mapping[str, int]


@dataclass(frozen=True)
class ExecutionHandle:
    """A submitted attempt, as the pool that runs it knows it."""

    pool: str
    attempt_id: int


class HandleState(StrEnum):
    """Where a submitted attempt is, as far as its pool can tell."""

    RUNNING = "RUNNING"
    ENDED = "ENDED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class HandleStatus:
    """A handle's state; when ``ENDED``, the attempt's outcome or the error that stopped it."""

    handle: ExecutionHandle
    state: HandleState
    outcome: TaskOutcome | None = None
    error: BaseException | None = None


class Pool(Protocol):
    """Runs admitted attempts; see the module's rules."""

    name: str

    def capacity(self) -> Capacity:
        """Return the slots per kind and how many are free."""
        ...

    def submit(self, spec: AttemptSpec) -> ExecutionHandle:
        """Start ``spec``'s attempt; the same attempt again returns the same handle."""
        ...

    def status(self, handle: ExecutionHandle) -> HandleStatus:
        """Return where ``handle``'s attempt is; ``UNKNOWN`` for a handle the pool never ran."""
        ...

    def cancel(self, handle: ExecutionHandle, grace_seconds: float) -> None:
        """Stop ``handle``'s task process: SIGTERM, then SIGKILL after ``grace_seconds``."""
        ...

    def reconcile(self) -> list[HandleStatus]:
        """Report the attempts the pool can confirm, after a restart or lost contact."""
        ...
