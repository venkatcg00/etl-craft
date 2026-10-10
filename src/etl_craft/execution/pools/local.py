"""The local pool: each attempt's task process on this host, within slots per kind.

It is the default pool, the only one on SQLite, and what the overseer and ``etl-craft run``
use. ``execute_attempt`` claims each attempt, runs its task process and records its outcome, on
one of the pool's threads.
"""

from __future__ import annotations

import contextvars
import threading
from collections import Counter
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import replace

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.execution.pools import (
    SLOT_KINDS,
    AttemptSpec,
    Capacity,
    ExecutionHandle,
    HandleState,
    HandleStatus,
)
from etl_craft.execution.runner import ChildOptions, TaskOutcome, execute_attempt


class LocalPool:
    """Run admitted attempts on this host: at most ``Max_parallel_tasks``, split by kind.

    A pool that never ran an attempt knows nothing of it: after a restart, attempts an earlier
    process started are found by their expired leases (``execution.reconcile``), not here.
    ``cancel`` stops a task process with the grace the pool's ``ChildOptions`` give it.
    """

    name = "local"

    def __init__(
        self, engine: Engine, config: ConnectorConfig, *, child: ChildOptions | None = None
    ) -> None:
        """Size the slots from the configuration; no thread starts until an attempt arrives."""
        limits = config.limits
        self.engine, self.config = engine, config
        self.child = child or ChildOptions()
        self.total = limits.max_parallel_tasks
        self.slots = {
            "ingestion": limits.local_ingestion_slots or self.total,
            "warehouse": limits.local_warehouse_slots or self.total,
        }
        self._executor = ThreadPoolExecutor(self.total, thread_name_prefix="etl-craft-task")
        self._attempts: dict[int, tuple[AttemptSpec, Future[TaskOutcome], threading.Event]] = {}
        self._lock = threading.Lock()

    def capacity(self) -> Capacity:
        """Return the slots per kind and how many of each are free now."""
        with self._lock:
            busy = Counter(
                spec.slot_kind for spec, future, _ in self._attempts.values() if not future.done()
            )
        left = self.total - sum(busy.values())
        free = {kind: max(0, min(self.slots[kind] - busy[kind], left)) for kind in SLOT_KINDS}
        return Capacity(slots=dict(self.slots), free=free)

    def submit(self, spec: AttemptSpec) -> ExecutionHandle:
        """Start ``spec``'s attempt on a pool thread, unless this pool already runs it."""
        with self._lock:
            if spec.attempt_id not in self._attempts:
                stop = threading.Event()
                future = self._executor.submit(
                    contextvars.copy_context().run,
                    execute_attempt,
                    self.engine,
                    self.config,
                    spec,
                    replace(self.child, cancel=stop),
                )
                self._attempts[spec.attempt_id] = (spec, future, stop)
        return ExecutionHandle(self.name, spec.attempt_id)

    def status(self, handle: ExecutionHandle) -> HandleStatus:
        """Return ``handle``'s state; an ended attempt is reported once, then forgotten."""
        with self._lock:
            entry = self._attempts.get(handle.attempt_id) if handle.pool == self.name else None
            if entry is None:
                return HandleStatus(handle, HandleState.UNKNOWN)
            _, future, _ = entry
            if not future.done():
                return HandleStatus(handle, HandleState.RUNNING)
            del self._attempts[handle.attempt_id]
        error = future.exception()
        if error is not None:
            return HandleStatus(handle, HandleState.ENDED, error=error)
        return HandleStatus(handle, HandleState.ENDED, outcome=future.result())

    def cancel(self, handle: ExecutionHandle, grace_seconds: float) -> None:
        """Stop ``handle``'s task process; one that has ended, or is unknown, is left alone."""
        # ponytail: the grace is ChildOptions.kill_grace_seconds from submit; pass it per attempt
        # once callers need different graces.
        with self._lock:
            entry = self._attempts.get(handle.attempt_id) if handle.pool == self.name else None
        if entry is not None:
            entry[2].set()

    def reconcile(self) -> list[HandleStatus]:
        """Report the attempts this pool is running."""
        with self._lock:
            running = [
                attempt_id
                for attempt_id, (_, future, _) in self._attempts.items()
                if not future.done()
            ]
        return [
            HandleStatus(ExecutionHandle(self.name, attempt_id), HandleState.RUNNING)
            for attempt_id in running
        ]

    def close(self) -> None:
        """Wait for every running attempt to end, then stop the pool's threads."""
        self._executor.shutdown(wait=True)
