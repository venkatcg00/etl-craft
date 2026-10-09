"""Read execution snapshots and explain them without admitting work or changing history."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import ClassVar

from sqlalchemy.engine import Connection, RowMapping

from etl_craft.core.actor import acting_as
from etl_craft.core.enums import GatePolicy
from etl_craft.core.graph import DependencyGraph, RunState, TaskRunState, build_graph
from etl_craft.core.time import as_utc
from etl_craft.engine import runlog
from etl_craft.engine.connection import read_snapshot
from etl_craft.engine.queries import statement
from etl_craft.engine.repository import trackers
from etl_craft.engine.repository.dependencies import (
    fetch_cross_pipeline_task_edges,
    fetch_pipeline_dependency_edges,
    fetch_pipeline_graph,
)
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.repository.tasks import resolve_task_id
from etl_craft.execution.gates import judge, satisfies
from etl_craft.execution.runner import BACKFILL_ASSUMED
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.models import PipelineView, RunView, TaskRunView
from etl_craft.services.operations.snapshots import (
    pipeline_view,
    run_view,
    task_run_view,
    timestamp,
)


@dataclass(frozen=True)
class DependencyView:
    """One dependency's observed state or recorded admission decision."""

    upstream: str
    dependency_type: str
    status: str | None
    met: bool
    reason: str
    task_dependency_id: int | None = None
    selected_pipeline_run_id: int | None = None
    selected_task_run_id: int | None = None
    selected_revision: int | None = None
    recorded: bool = False
    pipeline_dependency_id: int | None = None


@dataclass(frozen=True)
class GateWaitView:
    """A persisted wait, including a pipeline gate when task_id is absent."""

    task_id: int | None
    first_check_at: datetime
    next_check_at: datetime | None
    looks: int
    wait_until: datetime


@dataclass(frozen=True)
class TaskSnapshot:
    """All inputs to a task explanation captured under one database snapshot."""

    pipeline: PipelineView
    run: RunView
    task_id: int
    task_code: str
    task: TaskRunView | None
    run_condition: str
    required_count: int
    dependencies: tuple[DependencyView, ...]
    pipeline_dependencies: tuple[DependencyView, ...]
    gate_waits: tuple[GateWaitView, ...]
    unsatisfiable: bool
    now: datetime
    blocked_by_failure: bool = False


@dataclass(frozen=True)
class Explanation:
    """A task's observed state and the change needed before it can run."""

    SCHEMA: ClassVar[str] = "etl-craft/explanation/1"
    pipeline: PipelineView
    run: RunView
    task_id: int
    task_code: str
    task: TaskRunView | None
    run_condition: str
    required_count: int
    dependencies: tuple[DependencyView, ...]
    pipeline_dependencies: tuple[DependencyView, ...]
    gate_waits: tuple[GateWaitView, ...]
    retry_at: datetime | None
    state: str
    next_action: str


def explain(snapshot: TaskSnapshot) -> Explanation:
    """Explain a captured state with no database calls, clock reads or mutations."""
    task = snapshot.task
    last = task.attempts[-1] if task is not None and task.attempts else None
    retry_at = last.not_before if last is not None and last.status == "QUEUED" else None
    unmet = [d for d in snapshot.dependencies if not d.met]
    met = len(snapshot.dependencies) - len(unmet)
    if task is not None and task.status in {"SUCCESS", "SKIPPED"}:
        state, action = task.status.lower(), "No work is needed; this task is already settled."
    elif snapshot.pipeline.paused is not None:
        state, action = "paused", "Resume the pipeline before starting this task."
    elif retry_at is not None:
        state = "retry scheduled"
        action = f"The supervisor can claim the retry at {retry_at.isoformat()}."
        if retry_at <= snapshot.now:
            action = "The retry is due; an available supervisor can claim it."
    elif task is not None and task.status == "IN-PROGRESS":
        state, action = "running", "Wait for the current attempt to finish."
    elif snapshot.unsatisfiable:
        state, action = (
            "unsatisfiable",
            "Repair the upstream outcome or dependency before rerunning.",
        )
    elif snapshot.blocked_by_failure or (
        met < snapshot.required_count and any(d.status == "FAILED" for d in unmet)
    ):
        state, action = (
            "blocked by failure",
            "Repair or retry the failed upstream to satisfy the dependencies.",
        )
    elif (
        met < snapshot.required_count
        or any(w.next_check_at for w in snapshot.gate_waits)
        or any(not d.met for d in snapshot.pipeline_dependencies)
    ):
        state, action = (
            "waiting on a gate",
            "Wait for enough upstream dependencies to be satisfied.",
        )
    elif task is not None and task.status == "FAILED":
        state, action = "failed", "Correct the recorded error before retrying this task."
    elif snapshot.run.status not in {"QUEUED", "IN-PROGRESS"}:
        state, action = "run ended", "Explicitly reopen or rerun the selected ended run."
    else:
        state, action = "not run", "An available supervisor can start this ready task."
    return Explanation(
        snapshot.pipeline,
        snapshot.run,
        snapshot.task_id,
        snapshot.task_code,
        task,
        snapshot.run_condition,
        snapshot.required_count,
        snapshot.dependencies,
        snapshot.pipeline_dependencies,
        snapshot.gate_waits,
        retry_at,
        state,
        action,
    )


@dataclass(frozen=True)
class TaskStatusView:
    """A configured task's summary and its explanation under the selected run."""

    task_id: int
    task_code: str
    status: str | None
    attempts: int
    rows_written: int | None
    duration_seconds: float | None
    error: str | None
    explanation: Explanation


@dataclass(frozen=True)
class StatusView:
    """A run's header and every active configured task, including tasks not run."""

    SCHEMA: ClassVar[str] = "etl-craft/status/1"
    pipeline: PipelineView
    run: RunView
    duration_seconds: float | None
    tasks: tuple[TaskStatusView, ...]
    blocked_by_failures: tuple[str, ...]


def _duration(start: datetime | None, end: datetime | None, now: datetime) -> float | None:
    return None if start is None else ((end or now) - start).total_seconds()


def _snapshots(
    conn: Connection,
    pipeline_id: int,
    selector: runlog.RunSelector,
    policy: GatePolicy,
    task_code: str | None = None,
) -> list[TaskSnapshot]:
    selected = runlog.select_run(conn, pipeline_id, selector)
    run = run_view(conn, pipeline_id, selected.pipeline_run_id)
    pipeline = pipeline_view(conn, pipeline_id)
    data = fetch_pipeline_graph(conn, pipeline_id)
    graph = build_graph(data.tasks, data.same_pipeline_edges)
    state = runlog.fetch_run_state(conn, run.pipeline_run_id, graph.task_ids)
    impossible = set(graph.unsatisfiable(state))
    failed_blocks = set(graph.unsatisfiable(state, failures_final=True)) - impossible
    definitions = conn.execute(
        statement(conn, "pipeline_steps"),
        {"pipeline_id": pipeline_id, "run_id": run.pipeline_run_id},
    ).all()
    codes = {r.task_id: r.task_code for r in definitions}
    decisions = (
        conn.execute(
            statement(conn, "inspection_gate_decisions"), {"pipeline_run_id": run.pipeline_run_id}
        )
        .mappings()
        .all()
    )
    waits = tuple(
        GateWaitView(
            r.task_id,
            as_utc(r.first_check_at),
            timestamp(r.next_check_at),
            r.looks,
            as_utc(r.wait_until),
        )
        for r in conn.execute(
            statement(conn, "inspection_gate_waits"), {"pipeline_run_id": run.pipeline_run_id}
        )
    )
    now = datetime.now(UTC)
    pipeline_dependencies = []
    for edge in fetch_pipeline_dependency_edges(conn, pipeline_id):
        recorded = [
            d for d in decisions if d["pipeline_dependency_id"] == edge.pipeline_dependency_id
        ]
        if recorded:
            d = recorded[-1]
            pipeline_dependencies.append(
                DependencyView(
                    edge.depends_on_pipeline_code,
                    edge.dependency_type,
                    d["upstream_status"],
                    d["result"] in {"SATISFIED", "BYPASSED"},
                    d["reason"],
                    selected_pipeline_run_id=d["selected_pipeline_run_id"],
                    selected_revision=d["selected_revision"],
                    recorded=True,
                    pipeline_dependency_id=edge.pipeline_dependency_id,
                )
            )
        else:
            upstream = trackers.fetch_latest_finished_pipeline_run(
                conn, edge.depends_on_pipeline_id, now
            )
            consumed = trackers.fetch_pipeline_last_consumed(conn, edge.pipeline_dependency_id)
            admitted, reason = judge(
                edge.dependency_type, upstream, consumed, consume_repairs=edge.consume_repairs
            )
            latest = trackers.fetch_latest_pipeline_run(conn, edge.depends_on_pipeline_id)
            met = admitted is not None
            status = upstream.status if upstream else None
            if run.backfill or policy == GatePolicy.OFF:
                met, reason = (
                    True,
                    "Pipeline gates are bypassed by backfills."
                    if run.backfill
                    else "Dependency gates are off.",
                )
            elif latest is not None and latest.status == "IN-PROGRESS":
                met, status, upstream = False, latest.status, None
                reason = "Wait for the running upstream pipeline to finish."
            elif not met and policy == GatePolicy.WARN:
                met, reason = True, f"Dependency is bypassed under warn policy: {reason}"
            pipeline_dependencies.append(
                DependencyView(
                    edge.depends_on_pipeline_code,
                    edge.dependency_type,
                    status,
                    met,
                    reason,
                    selected_pipeline_run_id=upstream.pipeline_run_id if upstream else None,
                    selected_revision=upstream.revision if upstream else None,
                    pipeline_dependency_id=edge.pipeline_dependency_id,
                )
            )
    snapshots = []
    for definition in definitions:
        if task_code is not None and definition.task_code != task_code:
            continue
        task = task_run_view(
            conn, pipeline_id, pipeline_run_id=run.pipeline_run_id, task_id=definition.task_id
        )
        dependencies = _dependencies(
            conn, graph, state, codes, definition.task_id, task, decisions, policy, run.backfill
        )
        snapshots.append(
            TaskSnapshot(
                pipeline,
                run,
                definition.task_id,
                definition.task_code,
                task,
                definition.run_condition or "ALL",
                graph.required_edge_count(definition.task_id),
                dependencies,
                tuple(pipeline_dependencies),
                tuple(w for w in waits if w.task_id in (None, definition.task_id)),
                definition.task_id in impossible,
                now,
                definition.task_id in failed_blocks,
            )
        )
    return snapshots


def _dependencies(
    conn: Connection,
    graph: DependencyGraph,
    state: RunState,
    codes: dict[int, str],
    task_id: int,
    task: TaskRunView | None,
    decisions: Sequence[RowMapping],
    policy: GatePolicy,
    backfill: bool,
) -> tuple[DependencyView, ...]:
    found = []
    for edge in graph.dependencies_of(task_id):
        upstream = state.get(edge.depends_on_task_id, TaskRunState())
        met = satisfies(
            edge.dependency_type,
            trackers.FinishedRun(0, upstream.status or "NOT RUN", upstream.wrote_rows),
        )
        found.append(
            DependencyView(
                codes[edge.depends_on_task_id],
                edge.dependency_type,
                upstream.status,
                met,
                "Dependency is satisfied." if met else "Upstream does not satisfy this dependency.",
            )
        )
    last_id = task.attempts[-1].attempt_id if task is not None and task.attempts else None
    for cross in fetch_cross_pipeline_task_edges(conn, task_id):
        recorded = [
            d
            for d in decisions
            if d["task_dependency_id"] == cross.task_dependency_id and d["attempt_id"] == last_id
        ]
        if recorded:
            d = recorded[-1]
            found.append(
                DependencyView(
                    cross.depends_on_label,
                    cross.dependency_type,
                    d["upstream_status"],
                    d["result"] in {"SATISFIED", "BYPASSED"},
                    d["reason"],
                    cross.task_dependency_id,
                    d["selected_pipeline_run_id"],
                    d["selected_task_run_id"],
                    d["selected_revision"],
                    True,
                )
            )
        else:
            upstream_run = trackers.fetch_latest_finished_task_run(conn, cross.depends_on_task_id)
            consumed = trackers.fetch_task_last_consumed(conn, cross.task_dependency_id)
            admitted, reason = judge(
                cross.dependency_type, upstream_run, consumed, consume_repairs=cross.consume_repairs
            )
            latest = trackers.fetch_latest_task_run(conn, cross.depends_on_task_id)
            met = admitted is not None
            status = upstream_run.status if upstream_run is not None else None
            if backfill:
                met = cross.dependency_type in BACKFILL_ASSUMED
                reason = (
                    "Cross-pipeline dependencies are assumed in backfills."
                    if met
                    else "A backfill does not assume an upstream failure."
                )
                upstream_run, status = None, None
            elif policy == GatePolicy.OFF:
                met, reason = True, "Dependency gates are off."
            elif latest is not None and latest.status == "IN-PROGRESS":
                met, status, upstream_run = False, latest.status, None
                reason = "Wait for the running upstream task to finish."
            elif not met and policy == GatePolicy.WARN:
                met, reason = True, f"Dependency is bypassed under warn policy: {reason}"
            found.append(
                DependencyView(
                    cross.depends_on_label,
                    cross.dependency_type,
                    status,
                    met,
                    reason,
                    cross.task_dependency_id,
                    upstream_run.pipeline_run_id if upstream_run is not None else None,
                    upstream_run.run_id if upstream_run is not None else None,
                    upstream_run.revision if upstream_run is not None else None,
                )
            )
    return tuple(found)


def pipeline_status(
    ctx: OperationContext, pipeline_code: str, *, selector: runlog.RunSelector = runlog.ACTIVE_RUN
) -> StatusView:
    """Inspect an exact run; never reconcile or resolve it by recency."""
    with acting_as(ctx.actor), read_snapshot(ctx.engine) as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        snapshots = _snapshots(conn, pipeline_id, selector, ctx.config.dependency_gates)
        selected = runlog.select_run(conn, pipeline_id, selector)
        run = run_view(conn, pipeline_id, selected.pipeline_run_id)
        pipeline = pipeline_view(conn, pipeline_id)
    now = snapshots[0].now if snapshots else datetime.now(UTC)
    tasks = []
    for snapshot in snapshots:
        task = snapshot.task
        why = explain(snapshot)
        tasks.append(
            TaskStatusView(
                snapshot.task_id,
                snapshot.task_code,
                task.status if task is not None else None,
                task.attempt_count if task is not None else 0,
                task.rows_written if task is not None else None,
                _duration(task.start_date, task.end_date, now) if task is not None else None,
                task.error_message.splitlines()[0]
                if task is not None and task.error_message
                else None,
                why,
            )
        )
    return StatusView(
        pipeline,
        run,
        _duration(run.start_date, run.end_date, now),
        tuple(tasks),
        tuple(t.task_code for t in tasks if t.explanation.state == "blocked by failure"),
    )


def explain_task(
    ctx: OperationContext,
    pipeline_code: str,
    task_code: str,
    *,
    selector: runlog.RunSelector = runlog.ACTIVE_RUN,
) -> Explanation:
    """Capture the selected task's state and explain it without modifying the run."""
    with acting_as(ctx.actor), read_snapshot(ctx.engine) as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        resolve_task_id(conn, pipeline_id, task_code)
        snapshots = _snapshots(conn, pipeline_id, selector, ctx.config.dependency_gates, task_code)
    return explain(snapshots[0])
