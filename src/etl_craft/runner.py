"""The single-task execution primitive: what `run --task_code` actually does.

Per CLAUDE.md's Execution section, this is the literal form Airflow's
generated BashOperator tasks shell out to, and it's also what the local
orchestrator spawns one subprocess of per ready task.
"""

# Per CLAUDE.md's "Crash detection": `run` forks the actual task logic (the
# handler dispatch specifically, not the whole of run_task — dependency
# checks/binding stay in this process) as a child process and watches it. A
# child that exits normally — success, or a handled HandlerError — writes
# its own final log row, as always. A child that dies unannounced (OOM-kill,
# segfault, or anything else that crashes before it can write a result)
# gets FAILED written on its behalf by the still-alive parent, but only if
# the row is still IN-PROGRESS — a child that already wrote its real outcome
# before some unrelated exit-time trouble is never overwritten.
#
# [CHOICE] multiprocessing with the "fork" context, not "spawn" or a genuine
# `subprocess.Popen` of a new `python -m etl_craft` invocation (unlike
# orchestrator.py's task-level subprocesses). Two reasons: (1) the child
# always builds its own fresh Engine via build_engine(config) rather than
# reusing anything the parent has open, so fork's usual "don't share
# inherited DB connections" hazard doesn't apply here; (2) fork duplicates
# the parent's already-patched in-memory state, so tests can monkeypatch
# handlers.dispatch to simulate a real crash (os._exit) and see the parent
# react correctly — spawn would re-import everything fresh in the child and
# silently ignore any monkeypatch applied in the test process.
#
# Deliberately NOT included yet:
#   * Cross-pipeline dependency polling — `cfg.fetch_pipeline_graph` already
#     surfaces which tasks have a cross-pipeline edge
#     (`cross_pipeline_task_ids`) but this module doesn't act on it yet.

from __future__ import annotations

import multiprocessing
from dataclasses import dataclass

from sqlalchemy.engine import Engine

from etl_craft.cfg import (
    fetch_pipeline_graph,
    fetch_task_handler,
    resolve_pipeline_id,
    resolve_task_id,
)
from etl_craft.config import ConnectorConfig
from etl_craft.db import build_engine
from etl_craft.handlers import HandlerError, dispatch
from etl_craft.resolver import build_graph
from etl_craft.runlog import (
    fetch_run_state,
    fetch_task_run_result,
    fetch_task_run_status,
    find_or_create_task_run,
    resolve_run_for_task,
    update_task_run,
)


class ForceNotAllowedError(Exception):
    """Raised when --force is used under Mode=orchestrator (CLAUDE.md: refused outright)."""


class DependenciesNotMetError(Exception):
    """Raised when a task is invoked directly but its same-pipeline deps aren't satisfied."""


@dataclass(frozen=True)
class TaskOutcome:
    """The result of one run_task() call — enough to set the process exit code and log a message."""

    status: str  # "SUCCESS", "FAILED", or "SKIPPED" (already done, short-circuited)
    message: str


def run_task(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    task_code: str,
    *,
    force: bool = False,
) -> TaskOutcome:
    """Run exactly one task, resolving its own pipeline_run_id per CLAUDE.md's Run-id resolution."""
    if force and config.mode == "orchestrator":
        raise ForceNotAllowedError("--force is only legal under Mode=local, not Mode=orchestrator")

    with engine.connect() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        task_id = resolve_task_id(conn, pipeline_id, task_code)

    with engine.begin() as conn:
        pipeline_run_id = resolve_run_for_task(conn, pipeline_id)

    if not force:
        with engine.connect() as conn:
            existing_status = fetch_task_run_status(conn, task_id, pipeline_run_id)
        if existing_status == "SUCCESS":
            # CLAUDE.md "Run-id resolution" #4: an existing SUCCESS binding
            # short-circuits without re-running — nothing is written, since
            # nothing needs to change.
            return TaskOutcome(
                status="SKIPPED",
                message=(
                    f"{task_code}: already SUCCESS under pipeline_run_id={pipeline_run_id}, "
                    "skipping"
                ),
            )

        with engine.connect() as conn:
            graph_data = fetch_pipeline_graph(conn, pipeline_id)
            graph = build_graph(graph_data.tasks, graph_data.same_pipeline_edges)
            run_state = fetch_run_state(
                conn, pipeline_run_id, [task.task_id for task in graph_data.tasks]
            )
        if task_id not in set(graph.ready(run_state)):
            # Deliberately raise rather than write anything to
            # AUD_TASK_RUN_LOG: the task never started, so there's nothing
            # to mark FAILED, and creating a dangling IN-PROGRESS row here
            # would be worse than leaving no row at all. Per CLAUDE.md:
            # "any path that bypasses the DAG's own ordering ... has no
            # structural ordering to lean on, so the engine still has to
            # verify same-pipeline dependencies itself."
            raise DependenciesNotMetError(
                f"{task_code}: same-pipeline dependencies not met for "
                f"pipeline_run_id={pipeline_run_id}"
            )

    with engine.begin() as conn:
        binding = find_or_create_task_run(conn, task_id, pipeline_run_id)
        handler = fetch_task_handler(conn, task_id)

    _dispatch_with_crash_detection(engine, config, binding.task_run_id, handler)

    with engine.connect() as conn:
        result = fetch_task_run_result(conn, binding.task_run_id)
    if result.status == "SUCCESS":
        return TaskOutcome(status="SUCCESS", message=f"{task_code}: SUCCESS")
    return TaskOutcome(status="FAILED", message=f"{task_code}: {result.error_message}")


def _dispatch_with_crash_detection(
    engine: Engine, config: ConnectorConfig, task_run_id: int, handler: str
) -> None:
    """Fork the handler dispatch into a child process; write FAILED if it dies unannounced."""
    ctx = multiprocessing.get_context("fork")
    process = ctx.Process(target=_dispatch_and_record, args=(config, task_run_id, handler))
    process.start()
    process.join()

    if process.exitcode != 0:
        with engine.begin() as conn:
            current = fetch_task_run_result(conn, task_run_id)
            if current.status == "IN-PROGRESS":
                update_task_run(
                    conn,
                    task_run_id,
                    status="FAILED",
                    error_message=(
                        f"task process died unexpectedly (exit code {process.exitcode}) "
                        "before recording its own outcome"
                    ),
                )


def _dispatch_and_record(config: ConnectorConfig, task_run_id: int, handler: str) -> None:
    """Run in the forked child: dispatch the handler and write its own terminal status."""
    # A fresh Engine, never the parent's — see this module's own [CHOICE]
    # comment on why fork is safe here specifically because of this.
    engine = build_engine(config)
    try:
        result = dispatch(handler)
    except HandlerError as exc:
        with engine.begin() as conn:
            update_task_run(conn, task_run_id, status="FAILED", error_message=str(exc))
        return
    with engine.begin() as conn:
        update_task_run(
            conn,
            task_run_id,
            status="SUCCESS",
            source_count=result.source_count,
            target_count=result.target_count,
            insert_count=result.insert_count,
            update_count=result.update_count,
            delete_count=result.delete_count,
        )
