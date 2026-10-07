"""Supervise active local runs while holding deployment leadership."""

from __future__ import annotations

import contextvars
import logging
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, replace
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
from etl_craft.core.enums import Mode
from etl_craft.core.errors import EngineDbError, EtlCraftError, RunRefusedError
from etl_craft.engine.queries import statement
from etl_craft.engine.repository import overseers
from etl_craft.engine.repository.dependencies import PipelineGraphData, fetch_pipeline_graph
from etl_craft.engine.runlog import RunSelector
from etl_craft.execution import pipeline
from etl_craft.execution.gates import Clock
from etl_craft.execution.leases import as_utc
from etl_craft.execution.reconcile import reconcile
from etl_craft.overseer.leadership import leadership
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
    next_heartbeat = 0.0
    quiesce = stop
    jobs: dict[int, tuple[Future[pipeline.PipelineOutcome], threading.Event]] = {}
    pool = ThreadPoolExecutor(
        max_workers=ctx.config.limits.max_parallel_tasks, thread_name_prefix="etl-craft-run"
    )

    def gate_sleep(seconds: float) -> None:
        if quiesce.wait(seconds):
            raise InterruptedError("server stopped admitting gate waits")

    def dispatch(run: ActiveRun, cancel: threading.Event) -> pipeline.PipelineOutcome:
        return pipeline.run_pipeline(
            ctx.engine,
            ctx.config,
            run.pipeline_code,
            clock=Clock(sleep=gate_sleep),
            selector=RunSelector(run_id=run.pipeline_run_id),
            backfill="overseer resumes backfill" if run.backfill else None,
            child=replace(ctx.child, cancel=cancel),
            hooks=run_hooks(ctx.config, ctx.engine),
            stop_dispatch=quiesce,
            graph_data=run.graph_data,
        )

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
                    for run_id, (future, _) in list(jobs.items()):
                        if future.done():
                            del jobs[run_id]
                            try:
                                done = future.result()
                                logger.info("%s", done.message)
                            except (EtlCraftError, InterruptedError, SQLAlchemyError, OSError):
                                logger.exception(
                                    "pipeline_run_id=%s: overseer could not finish run", run_id
                                )
                    for run in working.refresh(ctx):
                        if stop.is_set():
                            break
                        if (
                            run.pipeline_run_id in jobs
                            or len(jobs) >= ctx.config.limits.max_parallel_tasks
                        ):
                            continue
                        with ctx.engine.connect() as conn:
                            owner = conn.execute(
                                text(
                                    "SELECT OWNER_ID AS owner_id FROM AUD_PIPELINES_RUN_LOG "
                                    "WHERE PIPELINE_RUN_ID=:id"
                                ),
                                {"id": run.pipeline_run_id},
                            ).scalar_one()
                        if owner is not None:
                            continue
                        cancel = threading.Event()
                        future = pool.submit(contextvars.copy_context().run, dispatch, run, cancel)
                        jobs[run.pipeline_run_id] = (future, cancel)
                    leader.wait(stop)
            finally:
                quiesce.set()
                futures = [future for future, _ in jobs.values()]
                wait(futures, timeout=ctx.config.limits.shutdown_grace_seconds)
                for future, cancel in jobs.values():
                    if not future.done():
                        cancel.set()
                pool.shutdown(wait=True, cancel_futures=True)
                overseers.heartbeat(ctx.engine, overseer_id, stopped=True)
                logger.info("overseer_id=%s: server stopped", overseer_id)
    except SQLAlchemyError as error:
        raise EngineDbError(
            f"server lost its Engine DB connection: {error}; restore it and restart server"
        ) from error
    finally:
        pool.shutdown(wait=True, cancel_futures=True)
