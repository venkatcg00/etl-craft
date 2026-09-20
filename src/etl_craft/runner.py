"""The single-task execution primitive: what `run --task_code` actually does.

Per CLAUDE.md's Execution section, this is the literal form Airflow's
generated BashOperator tasks shell out to, and it's also what the local
orchestrator spawns one subprocess of per ready task. A `run --task_code`
invocation only ever runs that one task — it never cascades into running
the rest of the pipeline, confirmed explicitly: whoever (Airflow or a
human) triggers a single task gets exactly that task attempted, and a
clean SUCCESS/SKIPPED/FAILED for it alone.

Per direct instruction, working through what should happen every time this
runs, regardless of who invokes it: resolve which pipeline this is and
build its dependency graph; resolve which task this is within that graph;
check same-pipeline *and* cross-pipeline dependencies — if a checked
dependency is itself still running, wait using the poll cadence
(crosspipe.py); if a dependency will never be satisfied, don't error —
record this task SKIPPED and still report success (exit 0) upstream, since
"gated off by design" isn't a failure Airflow's own retry/alerting should
react to. Only once dependencies genuinely clear does the actual handler
dispatch (fork + monitor, below) happen.

[DEVIATION] This replaces the previous behavior, where an unmet
same-pipeline dependency raised `DependenciesNotMetError` (exit code 1, no
row written) instead of writing SKIPPED (exit code 0). Same-pipeline and
cross-pipeline unmet dependencies are now handled identically for this
reason: an Airflow task whose upstream FAILURE-edge condition wasn't met
is expected, routine behavior, not an execution error — a real error
(secret unresolvable, DB unreachable) still surfaces as a genuine failure.
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
from __future__ import annotations

import multiprocessing
import time
from dataclasses import dataclass

from sqlalchemy.engine import Engine

from etl_craft.cfg import (
    fetch_pipeline_graph,
    fetch_task_execution_detail,
    fetch_task_parameters,
    resolve_pipeline_id,
    resolve_task_id,
)
from etl_craft.config import ConnectorConfig
from etl_craft.crosspipe import (
    NowFn,
    SleepFn,
    _default_now,
    check_task_cross_pipeline_dependencies,
    consume_task_dependency_edges,
)
from etl_craft.db import build_engine
from etl_craft.execution import TaskExecutionContext, format_task_log
from etl_craft.handlers import HandlerError, dispatch
from etl_craft.resolver import DependencyGraph, build_graph
from etl_craft.runlog import (
    fetch_pipeline_run_status,
    fetch_run_state,
    fetch_task_run_result,
    fetch_task_run_status,
    find_or_create_task_run,
    resolve_run_for_task,
    update_task_run,
)


class ForceNotAllowedError(Exception):
    """Raised when --force is used under Mode=orchestrator (CLAUDE.md: refused outright)."""


@dataclass(frozen=True)
class TaskOutcome:
    """The result of one run_task() call — enough to set the process exit code and log a message."""

    status: str  # "SUCCESS", "FAILED", or "SKIPPED" (already done, or gated off)
    message: str


def run_task(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    task_code: str,
    *,
    force: bool = False,
    sleep: SleepFn = time.sleep,
    now: NowFn = _default_now,
) -> TaskOutcome:
    """Run exactly one task, resolving its own pipeline_run_id per CLAUDE.md's Run-id resolution."""
    if force and config.mode == "orchestrator":
        raise ForceNotAllowedError("--force is only legal under Mode=local, not Mode=orchestrator")

    with engine.connect() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        task_id = resolve_task_id(conn, pipeline_id, task_code)

    with engine.begin() as conn:
        pipeline_run_id = resolve_run_for_task(conn, pipeline_id, force=force)

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

        if existing_status == "IN-PROGRESS":
            # [ADDITION, 2026-09-20, E2-02] Its own distinct outcome, and it
            # must come before the dependency check below. resolver.ready()
            # excludes an IN-PROGRESS task deliberately ("never re-dispatch"),
            # which that check could only read as "dependencies not met" —
            # so a second invocation used to call _bind_as_skipped and
            # overwrite the *live* row with STATUS='SKIPPED'. That lied about
            # a task that was still executing, disarmed the original
            # process's crash detection (which only writes FAILED while the
            # row still reads IN-PROGRESS), and left the final status
            # depending on which process wrote last. Reachable by ordinary
            # means: an Airflow retry firing while the first attempt still
            # runs, or a human running a task the local orchestrator already
            # spawned. Write nothing at all, and exit 0 — the run already
            # under way owns this row.
            return TaskOutcome(
                status="SKIPPED",
                message=(
                    f"{task_code}: already IN-PROGRESS under "
                    f"pipeline_run_id={pipeline_run_id} — not re-dispatching"
                ),
            )

        with engine.connect() as conn:
            pipeline_run_status = fetch_pipeline_run_status(conn, pipeline_run_id)
        if pipeline_run_status == "SKIPPED":
            # The run itself was already declared moot — its own
            # cross-pipeline dependency (checked by whatever minted it,
            # orchestrator.py) was never satisfied. Every task under it is
            # SKIPPED too, uniformly, rather than each independently
            # re-deriving the same conclusion.
            return _bind_as_skipped(
                engine,
                task_id,
                pipeline_run_id,
                task_code,
                f"pipeline_run_id={pipeline_run_id} is itself SKIPPED",
            )

        with engine.connect() as conn:
            graph_data = fetch_pipeline_graph(conn, pipeline_id)
            graph = build_graph(graph_data.tasks, graph_data.same_pipeline_edges)
            run_state = fetch_run_state(
                conn, pipeline_run_id, [task.task_id for task in graph_data.tasks]
            )

        # Per direct instruction: same-pipeline and cross-pipeline
        # dependencies are checked uniformly, and an unmet one is never an
        # error — it's recorded SKIPPED (still exit 0) so Airflow doesn't
        # treat "correctly gated off" as a task failure. [DEVIATION] this
        # replaces the previous same-pipeline-only DependenciesNotMetError
        # (raise, exit 1, nothing written) — see this module's own
        # docstring for the full reasoning.
        # [DEVIATION, 2026-09-20, E2-45] Same-pipeline and cross-pipeline
        # edges are counted against one requirement, not gated one after the
        # other. RUN_CONDITION ranges over all of a task's dependencies, so
        # checking the two halves independently made 'ANY' mean "any
        # same-pipeline edge AND every cross-pipeline edge".
        required = graph.required_edge_count(task_id)
        same_satisfied = graph.satisfied_edge_count(task_id, run_state, 0)
        still_needed = required - same_satisfied
        cross_reasons: tuple[str, ...] = ()
        if still_needed > 0 and task_id in graph_data.cross_pipeline_task_ids:
            # Only polled when the same-pipeline half alone isn't already
            # enough — an 'ANY' task whose condition is met has no reason to
            # block for up to an hour on an edge it doesn't need.
            cross = check_task_cross_pipeline_dependencies(
                engine, task_id, needed=still_needed, sleep=sleep, now=now
            )
            still_needed -= cross.satisfied_count
            cross_reasons = cross.reasons

        if still_needed > 0:
            # [DEVIATION, 2026-09-20, E2-47] "Not yet" and "never" are no
            # longer recorded the same way. SKIPPED is terminal — it's in
            # NOT_RETRYABLE and SETTLED_STATUSES — so writing it for a task
            # whose dependency simply hasn't run *yet* permanently disqualified
            # that task from the run, and the pipeline then finalized SUCCESS
            # with the task never having run. CLAUDE.md explicitly supports the
            # paths that hit this ("a manual single-task run, a backfill, a
            # re-triggered task"), so the verification must not be destructive.
            #
            # A cross-pipeline edge that came back unsatisfied *has* had its
            # chance — crosspipe.py just polled it to its budget — so that
            # stays terminal, as CLAUDE.md's "correctly gated off by design"
            # describes.
            if cross_reasons:
                return _bind_as_skipped(
                    engine, task_id, pipeline_run_id, task_code, "; ".join(cross_reasons)
                )
            if task_id in set(graph.unsatisfiable(run_state)):
                return _bind_as_skipped(
                    engine,
                    task_id,
                    pipeline_run_id,
                    task_code,
                    _describe_unready(graph, task_id, pipeline_run_id, can_never=True),
                )
            # Nothing written at all, and exit 0 — the same shape the
            # IN-PROGRESS branch above uses. A later invocation, once the
            # upstream has run, finds the task exactly as it left it.
            return TaskOutcome(
                status="SKIPPED",
                message=(
                    f"{task_code}: {_describe_unready(graph, task_id, pipeline_run_id)} "
                    "— nothing recorded, re-run once it is"
                ),
            )

    with engine.begin() as conn:
        binding = find_or_create_task_run(conn, task_id, pipeline_run_id)
        detail = fetch_task_execution_detail(conn, task_id)
        task_params = fetch_task_parameters(conn, task_id)

    ctx = TaskExecutionContext(
        config=config,
        pipeline_id=pipeline_id,
        pipeline_code=pipeline_code,
        task_id=task_id,
        task_code=task_code,
        task_run_id=binding.task_run_id,
        pipeline_run_id=pipeline_run_id,
        handler=detail.handler,
        refresh_type=detail.refresh_type,
        task_params=task_params,
        force=force,
    )
    _dispatch_with_crash_detection(engine, ctx)

    # Per CLAUDE.md: "The tracker only updates after the gated task/
    # pipeline completes" — completion, not success specifically, since
    # what an edge is waiting for (SUCCESS/FAILURE/ALWAYS/HAS_DATA) is
    # about *this* task's own outcome, independent of whether it succeeded.
    # A no-op when task_id has no cross-pipeline edges of its own.
    consume_task_dependency_edges(engine, task_id)

    with engine.connect() as conn:
        result = fetch_task_run_result(conn, binding.task_run_id)
    if result.status == "SUCCESS":
        return TaskOutcome(status="SUCCESS", message=f"{task_code}: SUCCESS")
    return TaskOutcome(status="FAILED", message=f"{task_code}: {result.error_message}")


def _describe_unready(
    graph: DependencyGraph,
    task_id: int,
    pipeline_run_id: int,
    *,
    can_never: bool = False,
) -> str:
    """Say why `task_id` is not ready, distinguishing "not yet" from "never will be"."""
    counts = (
        f"(needs {graph.required_edge_count(task_id)} of "
        f"{graph.total_edge_count(task_id)} edge(s) satisfied)"
    )
    if can_never:
        return (
            f"dependencies can never be satisfied under "
            f"pipeline_run_id={pipeline_run_id} {counts}"
        )
    return f"dependencies not met yet for pipeline_run_id={pipeline_run_id} {counts}"


def _bind_as_skipped(
    engine: Engine, task_id: int, pipeline_run_id: int, task_code: str, reason: str
) -> TaskOutcome:
    """Bind (if needed) and finalize `task_id` as SKIPPED, with `reason` logged as usual."""
    with engine.begin() as conn:
        binding = find_or_create_task_run(conn, task_id, pipeline_run_id)
        update_task_run(conn, binding.task_run_id, status="SKIPPED", error_message=reason)
    return TaskOutcome(status="SKIPPED", message=f"{task_code}: SKIPPED — {reason}")


def _dispatch_with_crash_detection(engine: Engine, ctx: TaskExecutionContext) -> None:
    """Fork the handler dispatch into a child process; write FAILED if it dies unannounced."""
    mp_ctx = multiprocessing.get_context("fork")
    process = mp_ctx.Process(target=_dispatch_and_record, args=(ctx,))
    process.start()
    process.join()

    if process.exitcode != 0:
        with engine.begin() as conn:
            current = fetch_task_run_result(conn, ctx.task_run_id)
            if current.status == "IN-PROGRESS":
                update_task_run(
                    conn,
                    ctx.task_run_id,
                    status="FAILED",
                    error_message=(
                        f"task process died unexpectedly (exit code {process.exitcode}) "
                        "before recording its own outcome"
                    ),
                )


def _dispatch_and_record(ctx: TaskExecutionContext) -> None:
    """Run in the forked child: dispatch the handler and write its own terminal status."""
    # A fresh Engine, never the parent's — see this module's own [CHOICE]
    # comment on why fork is safe here specifically because of this.
    engine = build_engine(ctx.config)
    try:
        result = dispatch(engine, ctx)
    except HandlerError as exc:
        with engine.begin() as conn:
            update_task_run(conn, ctx.task_run_id, status="FAILED", error_message=str(exc))
        return
    with engine.begin() as conn:
        update_task_run(
            conn,
            ctx.task_run_id,
            status="SUCCESS",
            source_count=result.source_count,
            target_count=result.target_count,
            insert_count=result.insert_count,
            update_count=result.update_count,
            delete_count=result.delete_count,
            task_log=format_task_log(result),
        )
