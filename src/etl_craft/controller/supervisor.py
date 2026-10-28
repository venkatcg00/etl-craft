"""Supervise active local runs while holding deployment leadership."""

from __future__ import annotations

import contextvars
import logging
import threading
import time
from collections.abc import Generator
from dataclasses import dataclass, field
from datetime import UTC, datetime

from sqlalchemy.exc import SQLAlchemyError

from etl_craft.controller.leadership import leadership
from etl_craft.controller.retention import RetentionSchedule
from etl_craft.controller.schedules import Schedules
from etl_craft.core.actor import ActorKind, acting_as
from etl_craft.core.definitions import Definition, using
from etl_craft.core.enums import RunStatus, YesNo
from etl_craft.core.errors import ConfigurationError, EngineDbError, EtlCraftError
from etl_craft.core.outage import OutageBudget
from etl_craft.engine.metrics import install
from etl_craft.engine.queries import statement
from etl_craft.engine.repository import controllers
from etl_craft.engine.repository.dependencies import PipelineGraphData, fetch_pipeline_graph
from etl_craft.engine.repository.pipelines import selected_project
from etl_craft.engine.retry import TRANSIENT_ERRORS
from etl_craft.engine.runlog import RunSelector, fetch_pipeline_run_status
from etl_craft.execution import pipeline, run_types
from etl_craft.execution.pools.local import LocalPool
from etl_craft.execution.pools.provider import create_pool
from etl_craft.execution.pools.queue import QueuePool
from etl_craft.execution.reconcile import reconcile
from etl_craft.execution.trigger_waits import settle_ended_parent_waits
from etl_craft.services.cloning import run_hooks
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.child_admission import guarded_children
from etl_craft.services.operations.trigger_controls import cascade_children

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ActiveRun:
    """One exact run and the cached graph used for its execution."""

    project_id: int
    pipeline_id: int
    pipeline_run_id: int
    pipeline_code: str
    backfill: bool
    graph_data: PipelineGraphData


class WorkingSet:
    """Retain each active run's admitted graph until that run ends."""

    def __init__(self) -> None:
        """Start with no cached history or graphs."""
        self.graphs: dict[int, PipelineGraphData] = {}

    def refresh(self, ctx: OperationContext) -> list[ActiveRun]:
        """Read active unpaused runs; never scan or retain terminal history."""
        with ctx.engine.connect() as conn:
            rows = conn.execute(statement(conn, "controller_active_runs")).all()
            active = {row.pipeline_run_id for row in rows}
            self.graphs = {key: value for key, value in self.graphs.items() if key in active}
            result = []
            for row in rows:
                if row.pipeline_run_id not in self.graphs:
                    with using(Definition.read(row.metadata_snapshot, row.pipeline_run_id)):
                        self.graphs[row.pipeline_run_id] = fetch_pipeline_graph(
                            conn, row.pipeline_id
                        )
                result.append(
                    ActiveRun(
                        row.project_id,
                        row.pipeline_id,
                        row.pipeline_run_id,
                        row.pipeline_code,
                        row.backfill == YesNo.YES,
                        self.graphs[row.pipeline_run_id],
                    )
                )
            return result


Jobs = dict[int, tuple[Generator[float, None, run_types.PipelineOutcome], contextvars.Context]]


@dataclass
class _Supervisor:
    """The controller's working state: cached graphs, schedules and the runs it advances."""

    ctx: OperationContext
    stop: threading.Event
    pool: LocalPool | QueuePool
    working: WorkingSet = field(default_factory=WorkingSet)
    schedules: Schedules = field(default_factory=Schedules)
    retention: RetentionSchedule = field(default_factory=RetentionSchedule)
    jobs: Jobs = field(default_factory=dict)
    next_heartbeat: float = 0.0

    def advance(self, run_id: int) -> None:
        """Take one step of a run; drop it when it ends or cannot go on."""
        steps, context = self.jobs[run_id]
        try:
            context.run(next, steps)
        except StopIteration as done:
            del self.jobs[run_id]
            logger.info("%s", done.value.message)
        except (EtlCraftError, InterruptedError, SQLAlchemyError, OSError):
            del self.jobs[run_id]
            logger.exception("pipeline_run_id=%s: controller could not finish run", run_id)
            context.run(steps.close)

    def supervise_once(self, controller_id: int) -> None:
        """Heartbeat, reconcile, admit due schedules, advance runs and adopt unowned ones."""
        if time.monotonic() >= self.next_heartbeat:
            controllers.heartbeat(self.ctx.engine, controller_id)
            self.next_heartbeat = time.monotonic() + 15
        reconcile(self.ctx.engine)
        cascade_children(self.ctx)
        settle_ended_parent_waits(self.ctx.engine, datetime.now(UTC))
        self.schedules.refresh(self.ctx, datetime.now(UTC), self.stop)
        self.retention.refresh(self.ctx, datetime.now(UTC), self.stop)
        for run_id in list(self.jobs):
            self.advance(run_id)
        for run in self.working.refresh(self.ctx):
            if self.stop.is_set():
                break
            if run.pipeline_run_id not in self.jobs and self._unowned(run.pipeline_run_id):
                self._adopt(run)

    def _unowned(self, run_id: int) -> bool:
        with self.ctx.engine.connect() as conn:
            row = conn.execute(statement(conn, "run_lease"), {"row_id": run_id}).one()
        return row.owner_id is None

    def _adopt(self, run: ActiveRun) -> None:
        steps = pipeline.pipeline_steps(
            self.ctx.engine,
            self.ctx.config,
            run.pipeline_code,
            selector=RunSelector(run_id=run.pipeline_run_id),
            backfill="controller resumes backfill" if run.backfill else None,
            child=self.ctx.child,
            hooks=run_hooks(self.ctx.config, self.ctx.engine),
            stop_dispatch=self.stop,
            graph_data=run.graph_data,
            pool=self.pool,
        )
        with selected_project(run.project_id):
            self.jobs[run.pipeline_run_id] = (steps, contextvars.copy_context())
        self.advance(run.pipeline_run_id)

    def drain(self, controller_id: int) -> None:
        """Stop admitting, let running work end within the grace period, then record the stop."""
        self.stop.set()
        deadline = time.monotonic() + self.ctx.config.limits.shutdown_grace_seconds
        # Queued admissions have no child or lease to drain.
        for run_id, (steps, context) in list(self.jobs.items()):
            with self.ctx.engine.connect() as conn:
                status = fetch_pipeline_run_status(conn, run_id)
            if status == RunStatus.QUEUED:
                context.run(steps.close)
                del self.jobs[run_id]
        while self.jobs and time.monotonic() < deadline:
            for run_id in list(self.jobs):
                self.advance(run_id)
            if self.jobs:
                time.sleep(0.1)
        for steps, context in self.jobs.values():
            context.run(steps.close)
        self.pool.close()
        controllers.heartbeat(self.ctx.engine, controller_id, stopped=True)
        logger.info("controller_id=%s: controller stopped", controller_id)


def serve(ctx: OperationContext, stop: threading.Event) -> None:
    """Run until shutdown; ride out Engine DB outages within their budget."""
    if ctx.actor.kind != ActorKind.SERVICE:
        raise ConfigurationError(
            f"controller account={ctx.actor.name!r}, kind={ctx.actor.kind}; expected SERVICE; "
            "set ETL_CRAFT_TOKEN to an administrator service account token"
        )
    supervisor = _Supervisor(ctx, stop, create_pool(ctx.engine, ctx.config, guarded_children(ctx)))
    metrics = install(ctx.engine)
    outage = OutageBudget(ctx.config.limits.engine_db_outage_seconds)
    try:
        with acting_as(ctx.actor), leadership(ctx.engine, ctx.config) as leader:
            from etl_craft.controller.refresh import initialize

            initialize(ctx)
            controller_id = controllers.start(ctx.engine)
            metrics.leader = True
            logger.info("controller_id=%s: controller active", controller_id)
            if not ctx.config.allow_schedule:
                logger.info("Orchestration.Allow_schedule is false: no scheduled pipeline runs")
            try:
                while not stop.is_set():
                    try:
                        leader.hold()
                        started = time.perf_counter()
                        try:
                            supervisor.supervise_once(controller_id)
                        finally:
                            metrics.add(
                                "controller_tick_duration_seconds",
                                (),
                                time.perf_counter() - started,
                            )
                        leader.wait(stop)
                    except TRANSIENT_ERRORS as error:
                        stop.wait(outage.failed("supervising runs", error))
                        continue
                    outage.recovered("supervising runs")
            finally:
                try:
                    supervisor.drain(controller_id)
                finally:
                    supervisor.schedules.catalog.close()
                    supervisor.retention.close()
    except SQLAlchemyError as error:
        raise EngineDbError(
            f"controller lost its Engine DB connection: {error}; restore it and restart controller"
        ) from error
    finally:
        metrics.leader = False
        supervisor.schedules.catalog.close()
        supervisor.retention.close()
        supervisor.pool.close()
