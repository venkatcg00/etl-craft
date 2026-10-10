"""Cooperative ready-task dispatch; gate waits never enter the pool."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from datetime import datetime

from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.config import ConnectorConfig
from etl_craft.core.enums import SETTLED_STATUSES
from etl_craft.core.errors import EtlCraftError
from etl_craft.core.graph import DependencyGraph, TaskRunState
from etl_craft.engine import runlog
from etl_craft.engine.repository import trackers
from etl_craft.engine.repository.tasks import fetch_task_execution_detail
from etl_craft.execution import leases
from etl_craft.execution.gates import Clock, CrossPipelineCheck, TrackedGate, check_gate
from etl_craft.execution.interventions import record_gate_bypass
from etl_craft.execution.pools import ExecutionHandle, HandleState, Pool, slot_kind
from etl_craft.execution.pools.local import LocalPool
from etl_craft.execution.reconcile import reconcile
from etl_craft.execution.retries import refresh_retries
from etl_craft.execution.runner import ChildOptions, _preflight, admit_attempt, run_cancelled

logger = logging.getLogger(__name__)


class Scheduler:
    """Advance one run without sleeping; submit attempts to the pool only after admission."""

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
        pool: Pool | None,
        paused: Callable[[], bool],
        settle: Callable[[bool], list[int]],
    ) -> None:
        """Keep run state; a pool of its own is made when none is given."""
        self.engine, self.config = engine, config
        self.pipeline_code, self.pipeline_id = pipeline_code, pipeline_id
        self.pipeline_run_id, self.task_codes, self.graph = pipeline_run_id, task_codes, graph
        self.force, self.clock, self.paused, self.settle = force, clock, paused, settle
        self.cancel = leases.run_cancel()
        self.grace_seconds = child.kill_grace_seconds
        self.gate = TrackedGate(
            clock, config.dependency_gates, config.limits.gate_wait_minutes * 60
        )
        self.own_pool: LocalPool | None = None
        if pool is None:
            self.own_pool = LocalPool(engine, config, child=child)
            pool = self.own_pool
        self.pool: Pool = pool
        self.handlers: dict[int, str] = {}
        self.jobs: dict[int, ExecutionHandle] = {}
        self.attempted: set[int] = set()
        self.completed: set[int] = set()
        self.failures_final = False
        self.after_failure: list[int] = []
        self.never_ready: list[int] = []
        self.next_check_at: datetime | None = None

    def step(self) -> bool:
        """Harvest completions, recompute readiness and dispatch; return true when settled."""
        self._harvest()
        if self.cancel.is_set():
            self._stop_running()
        if (
            self.cancel.is_set()
            or run_cancelled(self.engine, self.pipeline_run_id)
            or self.paused()
        ):
            return not self.jobs
        retries, exhausted = refresh_retries(
            self.engine,
            self.config,
            self.pipeline_run_id,
            set(self.graph.task_ids) - self.jobs.keys(),
        )
        if not self.force:
            self.attempted.update(exhausted)
        for task_id in retries:
            if task_id not in self.jobs:
                self.attempted.discard(task_id)
                self.completed.discard(task_id)
        skipped = [] if self.force else self.settle(self.failures_final)
        if self.failures_final:
            self.after_failure.extend(skipped)
        with self.engine.connect() as conn:
            state = runlog.fetch_run_state(conn, self.pipeline_run_id, self.graph.task_ids)
            backfill = runlog.fetch_run_kind(conn, self.pipeline_run_id).backfill
        state.update({t: TaskRunState(status="IN-PROGRESS") for t in self.jobs})
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
            else [
                t
                for t in self.graph.ready({**state, **{t: TaskRunState() for t in retries}})
                if t in pending
            ]
        )
        waiting = [due for due in retries.values() if due > self.clock.now()]
        for task_id in ready:
            if task_id in retries and retries[task_id] > self.clock.now():
                continue
            if self.pool.capacity().free[slot_kind(self._handler(task_id))] == 0:
                continue
            admission = CrossPipelineCheck(0) if not self.force else None
            decisions: tuple[trackers.GateDecision, ...] = ()
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
                blocked, cross = _preflight(
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
                decisions = cross.decisions
                if cross.bypassed:
                    record_gate_bypass(
                        self.engine,
                        self.pipeline_id,
                        self.pipeline_run_id,
                        self.config.dependency_gates,
                        cross.bypassed,
                        task_id=task_id,
                    )
            self.attempted.add(task_id)
            self.failures_final = False
            logger.info("%s: dispatch %s", self.pipeline_code, self.task_codes[task_id])
            self._dispatch(task_id, decisions)
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

    def _handler(self, task_id: int) -> str:
        if task_id not in self.handlers:
            with self.engine.connect() as conn:
                self.handlers[task_id] = fetch_task_execution_detail(conn, task_id).handler
        return self.handlers[task_id]

    def _dispatch(self, task_id: int, decisions: tuple[trackers.GateDecision, ...]) -> None:
        """Admit the task's attempt and submit it; a task that cannot be admitted is logged."""
        code = self.task_codes[task_id]
        try:
            reconcile(self.engine, pipeline_id=self.pipeline_id)
            spec = admit_attempt(
                self.engine,
                self.config,
                task_id,
                code,
                self.pipeline_code,
                self.pipeline_run_id,
                self.force,
                decisions=decisions,
            )
        except (EtlCraftError, OSError, SQLAlchemyError) as error:
            logger.error("%s: could not run: %s: %s", code, type(error).__name__, error)
            self.completed.add(task_id)
            return
        self.jobs[task_id] = self.pool.submit(spec)

    def _harvest(self) -> None:
        """Take the outcome of every ended attempt; an unexpected error is raised."""
        for task_id, handle in list(self.jobs.items()):
            status = self.pool.status(handle)
            if status.state is HandleState.RUNNING:
                continue
            del self.jobs[task_id]
            self.completed.add(task_id)
            if isinstance(status.error, (EtlCraftError, OSError, SQLAlchemyError)):
                logger.error(
                    "%s: could not run: %s: %s",
                    self.task_codes[task_id],
                    type(status.error).__name__,
                    status.error,
                )
            elif status.error is not None:
                raise status.error

    def _stop_running(self) -> None:
        for handle in self.jobs.values():
            self.pool.cancel(handle, self.grace_seconds)

    def close(self, *, interrupted: bool = False) -> None:
        """Stop the run's task processes on interruption, then wait for every attempt to end."""
        if interrupted:
            self.cancel.set()
            self._stop_running()
        while any(self.pool.status(h).state is HandleState.RUNNING for h in self.jobs.values()):
            time.sleep(0.05)
        self.jobs.clear()
        if self.own_pool is not None:
            self.own_pool.close()
