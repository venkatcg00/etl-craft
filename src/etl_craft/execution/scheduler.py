"""Cooperative ready-task dispatch; gate waits never enter the worker pool."""

from __future__ import annotations

import contextvars
import logging
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import replace
from datetime import datetime

from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.config import ConnectorConfig
from etl_craft.core.enums import SETTLED_STATUSES
from etl_craft.core.errors import EtlCraftError
from etl_craft.core.graph import DependencyGraph, TaskRunState
from etl_craft.engine import runlog
from etl_craft.execution import leases
from etl_craft.execution.gates import Clock, CrossPipelineCheck, TrackedGate, check_gate
from etl_craft.execution.runner import (
    ChildOptions,
    TaskOutcome,
    _preflight,
    run_cancelled,
    run_task,
)

logger = logging.getLogger(__name__)


class Scheduler:
    """Advance one run without sleeping; start workers only after gate admission."""

    def __init__(
        self,
        engine: Engine,
        config: ConnectorConfig,
        pipeline_code: str,
        pipeline_id: int,
        pipeline_run_id: int,
        task_codes: dict[int, str],
        graph: DependencyGraph,
        *,
        force: bool,
        clock: Clock,
        child: ChildOptions,
        pool: ThreadPoolExecutor | None,
        paused: Callable[[], bool],
        settle: Callable[[bool], list[int]],
    ) -> None:
        """Keep run state and an executor; no threads start until a task is admitted."""
        self.engine, self.config = engine, config
        self.pipeline_code, self.pipeline_id = pipeline_code, pipeline_id
        self.pipeline_run_id, self.task_codes, self.graph = pipeline_run_id, task_codes, graph
        self.force, self.clock, self.paused, self.settle = force, clock, paused, settle
        self.cancel = leases.run_cancel()
        self.child = replace(child, cancel=self.cancel)
        self.gate = TrackedGate(
            clock, config.dependency_gates, config.limits.gate_wait_minutes * 60
        )
        self.own_pool = pool is None
        self.pool = pool or ThreadPoolExecutor(
            config.limits.max_parallel_tasks, thread_name_prefix="etl-craft-task"
        )
        self.jobs: dict[int, Future[TaskOutcome | None]] = {}
        self.attempted: set[int] = set()
        self.completed: set[int] = set()
        self.failures_final = False
        self.after_failure: list[int] = []
        self.never_ready: list[int] = []
        self.next_check_at: datetime | None = None

    def step(self) -> bool:
        """Harvest completions, recompute readiness and dispatch; return true when settled."""
        for task_id, future in list(self.jobs.items()):
            if future.done():
                del self.jobs[task_id]
                future.result()
                self.completed.add(task_id)
        if (
            self.cancel.is_set()
            or run_cancelled(self.engine, self.pipeline_run_id)
            or self.paused()
        ):
            return not self.jobs
        skipped = [] if self.force else self.settle(self.failures_final)
        if self.failures_final:
            self.after_failure.extend(skipped)
        with self.engine.connect() as conn:
            state = runlog.fetch_run_state(conn, self.pipeline_run_id, self.graph.task_ids)
            backfill = runlog.fetch_run_kind(conn, self.pipeline_run_id).backfill
        pending = [
            t
            for t in self.graph.task_ids
            if t not in self.attempted
            and (self.force or state.get(t, TaskRunState()).status not in SETTLED_STATUSES)
        ]
        ready = (
            [
                t
                for t in pending
                if all(
                    e.depends_on_task_id in self.completed for e in self.graph.dependencies_of(t)
                )
            ]
            if self.force
            else [t for t in self.graph.ready(state) if t in pending]
        )
        waiting: list[datetime] = []
        for task_id in ready:
            if len(self.jobs) >= self.config.limits.max_parallel_tasks:
                break
            admission = CrossPipelineCheck(0) if not self.force else None
            if not self.force:
                needed = self.graph.required_edge_count(task_id) - self.graph.satisfied_edge_count(
                    task_id, state, 0
                )
                if needed > 0 and not backfill:
                    result = check_gate(
                        self.engine,
                        self.pipeline_run_id,
                        self.pipeline_id,
                        task_id=task_id,
                        needed=needed,
                        clock=self.clock,
                        policy=self.config.dependency_gates,
                        wait_seconds=self.config.limits.gate_wait_minutes * 60,
                    )
                    if result.next_check_at is not None:
                        waiting.append(result.next_check_at)
                        continue
                    assert isinstance(result.check, CrossPipelineCheck)
                    admission = result.check
                blocked, _ = _preflight(
                    self.engine,
                    self.gate,
                    self.pipeline_id,
                    task_id,
                    self.task_codes[task_id],
                    self.pipeline_run_id,
                    admission,
                )
                if blocked is not None:
                    self.attempted.add(task_id)
                    self.completed.add(task_id)
                    continue
            self.attempted.add(task_id)
            logger.info("%s: dispatch %s", self.pipeline_code, self.task_codes[task_id])
            # ponytail: per-run submission cap; bound the shared queue if deployments outgrow it.
            self.jobs[task_id] = self.pool.submit(
                contextvars.copy_context().run, self._run_one, task_id, admission
            )
        self.next_check_at = min(waiting) if waiting else None
        if self.jobs or waiting:
            return False
        if not pending:
            return True
        if not self.failures_final:
            self.failures_final = True
            return False
        if skipped or ready:
            return False
        self.never_ready = pending
        return True

    def _run_one(self, task_id: int, admission: CrossPipelineCheck | None) -> TaskOutcome | None:
        if self.cancel.is_set() or self.paused():
            return None
        try:
            return run_task(
                self.engine,
                self.config,
                self.pipeline_code,
                self.task_codes[task_id],
                force=self.force,
                gate=self.gate,
                child=self.child,
                admission=admission,
                selector=runlog.RunSelector(run_id=self.pipeline_run_id),
            )
        except (EtlCraftError, OSError, SQLAlchemyError) as error:
            logger.error(
                "%s: could not run: %s: %s", self.task_codes[task_id], type(error).__name__, error
            )
            return None

    def close(self, *, interrupted: bool = False) -> None:
        """Stop children on interruption, then join workers before releasing run ownership."""
        if interrupted:
            self.cancel.set()
        if self.own_pool:
            self.pool.shutdown(wait=True, cancel_futures=True)
        else:
            wait(list(self.jobs.values()))
