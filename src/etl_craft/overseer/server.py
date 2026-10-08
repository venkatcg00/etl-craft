"""Supervise active local runs while holding deployment leadership."""

from __future__ import annotations

import contextvars
import logging
import threading
import time
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC, datetime

from sqlalchemy.exc import SQLAlchemyError

from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
from etl_craft.core.enums import Mode
from etl_craft.core.errors import EngineDbError, EtlCraftError, RunRefusedError
from etl_craft.core.time import as_utc
from etl_craft.engine.queries import statement
from etl_craft.engine.repository import overseers
from etl_craft.engine.repository.dependencies import PipelineGraphData, fetch_pipeline_graph
from etl_craft.engine.runlog import RunSelector, fetch_pipeline_run_status
from etl_craft.execution import pipeline
from etl_craft.execution.reconcile import reconcile
from etl_craft.overseer.leadership import leadership
from etl_craft.overseer.schedules import Schedules
from etl_craft.services.cloning import run_hooks
from etl_craft.services.operations import OperationContext

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ActiveRun:
    """One exact run and the cached graph used for its execution."""

    pipeline_id: int
    pipeline_run_id: int
    pipeline_code: str
    backfill: bool
    graph_data: PipelineGraphData


class WorkingSet:
    """Retain graphs only for active runs, refreshing on metadata edits."""

    def __init__(self) -> None:
        """Start with no cached history or graphs."""
        self.graphs: dict[int, tuple[tuple[datetime | None, int], PipelineGraphData]] = {}

    def refresh(self, ctx: OperationContext) -> list[ActiveRun]:
        """Read active unpaused runs; never scan or retain terminal history."""
        with ctx.engine.connect() as conn:
            rows = conn.execute(statement(conn, "overseer_active_runs")).all()
            active = {row.pipeline_id for row in rows}
            self.graphs = {key: value for key, value in self.graphs.items() if key in active}
            result = []
            for row in rows:
                version = (
                    None if row.metadata_version is None else as_utc(row.metadata_version),
                    row.change_id or 0,
                )
                cached = self.graphs.get(row.pipeline_id)
                if cached is None or cached[0] != version:
                    self.graphs[row.pipeline_id] = (
                        version,
                        fetch_pipeline_graph(conn, row.pipeline_id),
                    )
                result.append(
                    ActiveRun(
                        row.pipeline_id,
                        row.pipeline_run_id,
                        row.pipeline_code,
                        row.backfill == "Y",
                        self.graphs[row.pipeline_id][1],
                    )
                )
            return result


def serve(ctx: OperationContext, stop: threading.Event) -> None:
    """Run until shutdown; keep exact ownership and existing execution semantics."""
    if ctx.config.mode != Mode.LOCAL:
        raise RunRefusedError("server supervises local execution; set Orchestration.Mode to local")
    working = WorkingSet()
    schedules = Schedules()
    next_heartbeat = 0.0
    quiesce = stop
    jobs: dict[
        int,
        tuple[
            Generator[float, None, pipeline.PipelineOutcome],
            contextvars.Context,
            threading.Event,
        ],
    ] = {}
    pool = ThreadPoolExecutor(
        ctx.config.limits.max_parallel_tasks, thread_name_prefix="etl-craft-task"
    )

    def advance(run_id: int) -> None:
        steps, context, _ = jobs[run_id]
        try:
            context.run(next, steps)
        except StopIteration as done:
            del jobs[run_id]
            logger.info("%s", done.value.message)
        except (EtlCraftError, InterruptedError, SQLAlchemyError, OSError):
            del jobs[run_id]
            logger.exception("pipeline_run_id=%s: overseer could not finish run", run_id)
            context.run(steps.close)

    try:
        with acting_as(SYSTEM_ACTOR), leadership(ctx.engine) as leader:
            with acting_as(ctx.actor):
                overseer_id = overseers.start(ctx.engine)
            logger.info("overseer_id=%s: server active", overseer_id)
            try:
                while not stop.is_set():
                    if time.monotonic() >= next_heartbeat:
                        overseers.heartbeat(ctx.engine, overseer_id)
                        next_heartbeat = time.monotonic() + 15
                    reconcile(ctx.engine)
                    schedules.refresh(ctx, datetime.now(UTC), stop)
                    for run_id in list(jobs):
                        advance(run_id)
                    for run in working.refresh(ctx):
                        if stop.is_set():
                            break
                        if run.pipeline_run_id in jobs:
                            continue
                        with ctx.engine.connect() as conn:
                            owner = (
                                conn.execute(
                                    statement(conn, "run_lease"), {"row_id": run.pipeline_run_id}
                                )
                                .one()
                                .owner_id
                            )
                        if owner is not None:
                            continue
                        cancel = threading.Event()
                        steps = pipeline.pipeline_steps(
                            ctx.engine,
                            ctx.config,
                            run.pipeline_code,
                            selector=RunSelector(run_id=run.pipeline_run_id),
                            backfill="overseer resumes backfill" if run.backfill else None,
                            child=replace(ctx.child, cancel=cancel),
                            hooks=run_hooks(ctx.config, ctx.engine),
                            stop_dispatch=quiesce,
                            graph_data=run.graph_data,
                            pool=pool,
                        )
                        jobs[run.pipeline_run_id] = (steps, contextvars.copy_context(), cancel)
                        advance(run.pipeline_run_id)
                    leader.wait(stop)
            finally:
                quiesce.set()
                deadline = time.monotonic() + ctx.config.limits.shutdown_grace_seconds
                # Queued admissions have no child or lease to drain.
                for run_id, (steps, context, _) in list(jobs.items()):
                    with ctx.engine.connect() as conn:
                        status = fetch_pipeline_run_status(conn, run_id)
                    if status == "QUEUED":
                        context.run(steps.close)
                        del jobs[run_id]
                while jobs and time.monotonic() < deadline:
                    for run_id in list(jobs):
                        advance(run_id)
                    if jobs:
                        time.sleep(0.1)
                for _, _, cancel in jobs.values():
                    cancel.set()
                for steps, context, _ in jobs.values():
                    context.run(steps.close)
                pool.shutdown(wait=True, cancel_futures=True)
                overseers.heartbeat(ctx.engine, overseer_id, stopped=True)
                logger.info("overseer_id=%s: server stopped", overseer_id)
    except SQLAlchemyError as error:
        raise EngineDbError(
            f"server lost its Engine DB connection: {error}; restore it and restart server"
        ) from error
    finally:
        pool.shutdown(wait=True, cancel_futures=True)
