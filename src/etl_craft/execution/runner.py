"""``run --task-code``: run one task of a pipeline, in its own process.

The caller selects the task's run by id or key, or by a single non-terminal identity.
It runs only when it may: not again once ``SUCCESS`` or ``SKIPPED`` under the run, not
while it is already ``IN-PROGRESS``, and only when enough of its dependencies are satisfied. A
task whose dependencies can never be satisfied under the run is recorded ``SKIPPED``.

The task itself runs in a freshly started interpreter, supervised with its time limit. Its
output goes to the attempt's log file, whose tail is kept in ``TASK_LOG``. When that process
ends without recording an outcome (it crashed, was killed, or ran out of time), the task is
recorded ``FAILED`` with the reason. While it runs, the run is watched: once an operator
aborts it (``etl-craft abort``), the task process is stopped and the task is ``CANCELLED``.
"""

from __future__ import annotations

import logging
import os
import socket
import sys
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy.engine import Connection, Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.actor import SYSTEM_ACTOR, acting_as, current_actor
from etl_craft.core.enums import (
    SETTLED_STATUSES,
    AttemptStatus,
    DeploymentProfile,
    Handler,
    RunStatus,
)
from etl_craft.core.errors import (
    EtlCraftError,
    UsageError,
)
from etl_craft.core.faults import fault_point
from etl_craft.core.graph import build_graph
from etl_craft.core.text import byte_tail
from etl_craft.engine import run_scopes, runlog, transitions
from etl_craft.engine.process_reservations import connection_slot
from etl_craft.engine.queries import statement
from etl_craft.engine.repository import trackers
from etl_craft.engine.repository.dependencies import (
    fetch_pipeline_graph,
)
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.repository.tasks import (
    fetch_task_execution_detail,
    resolve_task_id,
)
from etl_craft.engine.retry import retrying
from etl_craft.engine.run_definitions import for_run, for_selection
from etl_craft.execution import control_flow, leases
from etl_craft.execution.attempt_admission import admit_attempt as admit_attempt
from etl_craft.execution.attempt_records import (
    CancelWatch,
    attempt_started,
    record_attempt,
    refuse_attempt,
    run_cancelled,
)
from etl_craft.execution.attempt_types import (
    CHILD_MODULE,
    ChildOptions,
    Override,
    TaskOutcome,
    TaskRun,
)
from etl_craft.execution.events import check_event
from etl_craft.execution.gates import CrossPipelineCheck, TrackedGate
from etl_craft.execution.interventions import (
    check_override,
    open_pause,
    record_change,
    record_gate_bypass,
)
from etl_craft.execution.pools import AttemptSpec
from etl_craft.execution.processes import (
    ChildSpec,
    run_child,
)
from etl_craft.execution.readiness import backfill_cross_check, describe_unready, upstream_pending
from etl_craft.execution.reconcile import reconcile
from etl_craft.execution.retries import refresh_retries

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _LocalOptions:
    """Controls shared by local task admission and its retry loop."""

    force: bool
    gate: TrackedGate | None
    child: ChildOptions | None
    selector: runlog.RunSelector
    admission: CrossPipelineCheck | None
    automatic_retries: bool


def run_task(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    task_code: str,
    *,
    force: bool = False,
    gate: TrackedGate | None = None,
    child: ChildOptions | None = None,
    override: Override | None = None,
    selector: runlog.RunSelector = runlog.ACTIVE_RUN,
    admission: CrossPipelineCheck | None = None,
    automatic_retries: bool = True,
) -> TaskOutcome:
    """Run one task under its pipeline's active run and return how it ended.

    ``force`` runs the task even when it already succeeded or its dependencies are not met, and
    reopens an ended run. ``override`` is an operator's recorded override (see ``Override``).
    Raises ``MetadataError`` for an unknown code and
    ``RunStateError`` when there is no run to bind to.
    """
    with engine.connect() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
    reconcile(engine, pipeline_id=pipeline_id)
    if override is not None:
        option = "`rerun`" if override.rerun else "`ignore_dependencies`"
        check_override(config, option, override.reason)
        if force:
            raise UsageError(f"{option} and `force` are different overrides: choose one")
    paused = open_pause(engine, pipeline_code, selector=selector)
    if paused is not None:
        return _skipped(
            f"{task_code}: {pipeline_code} is {paused.describe()}; nothing started or "
            f"recorded. `etl-craft resume --pipeline-code {pipeline_code}` lets it run again"
        )
    if override is not None:
        with for_selection(engine, pipeline_id, selector):
            return _run_overridden(
                engine, config, pipeline_code, pipeline_id, task_code, override, child, selector
            )
    return _run_local(
        engine,
        config,
        pipeline_code,
        pipeline_id,
        task_code,
        _LocalOptions(force, gate, child, selector, admission, automatic_retries),
    )


def _run_local(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    pipeline_id: int,
    task_code: str,
    options: _LocalOptions,
) -> TaskOutcome:
    """Resolve the selected run, check admission and drive its attempts and durable retries."""
    from etl_craft.core.definitions import current
    from etl_craft.execution.bundle_files import run_config

    with for_selection(engine, pipeline_id, options.selector):
        admitted = current.get()
        if admitted is not None and admitted.run_id is not None:
            config = run_config(engine, config, admitted.run_id)
        gate = options.gate or TrackedGate(
            policy=config.dependency_gates, wait_seconds=config.limits.gate_wait_minutes * 60
        )
        with engine.connect() as conn:
            task_id = resolve_task_id(conn, pipeline_id, task_code)
        with engine.begin() as conn:
            pipeline_run_id, reopened = transitions.resolve_run(
                conn,
                pipeline_id,
                force=options.force,
                reason=f"force: {task_code} runs again under the ended run",
                selector=options.selector,
            )
        decisions: tuple[trackers.GateDecision, ...] = ()
        if not options.force:
            blocked, cross = preflight(
                engine, gate, pipeline_id, task_id, task_code, pipeline_run_id, options.admission
            )
            if blocked is not None:
                logger.info(blocked.message)
                return blocked
            decisions = cross.decisions
            if cross.bypassed:
                record_gate_bypass(
                    engine,
                    pipeline_id,
                    pipeline_run_id,
                    config.dependency_gates,
                    cross.bypassed,
                    task_id=task_id,
                )
        task = TaskRun(
            pipeline_code, task_code, task_id, pipeline_run_id, options.force, decisions, reopened
        )
        return _run_with_retries(engine, config, task, gate, options)


def _run_with_retries(
    engine: Engine,
    config: ConnectorConfig,
    task: TaskRun,
    gate: TrackedGate,
    options: _LocalOptions,
) -> TaskOutcome:
    """Wait for durable retry deadlines outside the agent, preserving each attempt's outcome."""
    with for_run(engine, task.pipeline_run_id):
        if options.automatic_retries:
            waiting, _ = refresh_retries(engine, config, task.pipeline_run_id, {task.task_id})
            if task.task_id in waiting and not _wait_retry(
                engine,
                task.pipeline_run_id,
                task.pipeline_code,
                waiting[task.task_id],
                gate,
                options.child,
            ):
                return TaskOutcome(
                    RunStatus.CANCELLED
                    if run_cancelled(engine, task.pipeline_run_id)
                    else RunStatus.IN_PROGRESS,
                    f"{task.task_code}: retry was not started",
                )
        outcome = _run_attempt(engine, config, task, options.child)
        while options.automatic_retries and outcome.status == RunStatus.FAILED:
            waiting, _ = refresh_retries(engine, config, task.pipeline_run_id, {task.task_id})
            if task.task_id not in waiting:
                break
            if not _wait_retry(
                engine,
                task.pipeline_run_id,
                task.pipeline_code,
                waiting[task.task_id],
                gate,
                options.child,
            ):
                return TaskOutcome(
                    RunStatus.CANCELLED
                    if run_cancelled(engine, task.pipeline_run_id)
                    else RunStatus.IN_PROGRESS,
                    f"{task.task_code}: retry was not started",
                    outcome.task_run_id,
                )
            outcome = _run_attempt(engine, config, task, options.child)
        return replace(outcome, reopened=task.reopened) if task.reopened is not None else outcome


def _wait_retry(
    engine: Engine,
    pipeline_run_id: int,
    pipeline_code: str,
    due: datetime,
    gate: TrackedGate,
    child: ChildOptions | None,
) -> bool:
    """Wait outside an agent pool; interruption leaves the delayed attempt durable."""
    while True:
        if (child is not None and child.cancel is not None and child.cancel.is_set()) or (
            run_cancelled(engine, pipeline_run_id)
            or open_pause(
                engine, pipeline_code, selector=runlog.RunSelector(run_id=pipeline_run_id)
            )
            is not None
        ):
            return False
        remaining = (due - gate.clock.now()).total_seconds()
        if remaining <= 0:
            return True
        gate.clock.sleep(min(0.5, remaining))


def _run_overridden(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    pipeline_id: int,
    task_code: str,
    override: Override,
    child: ChildOptions | None,
    selector: runlog.RunSelector,
) -> TaskOutcome:
    """Run the task past the checks the override names, and record it."""
    with engine.begin() as conn:
        task_id = resolve_task_id(conn, pipeline_id, task_code)
        if override.rerun:
            # Decide before reopening anything, so a refused rerun changes nothing.
            pipeline_run_id = runlog.select_run(conn, pipeline_id, selector).pipeline_run_id
        else:
            pipeline_run_id, _ = transitions.resolve_run(conn, pipeline_id, selector=selector)
        status = runlog.fetch_task_run_status(conn, task_id, pipeline_run_id)
        if override.rerun and status != RunStatus.IN_PROGRESS:
            pipeline_run_id, _ = transitions.resolve_run(
                conn, pipeline_id, rerun=True, reason=override.reason, selector=selector
            )
    if status == RunStatus.IN_PROGRESS:
        return _skipped(
            f"{task_code}: already IN-PROGRESS under pipeline_run_id={pipeline_run_id}; not "
            "starting it twice"
        )
    if status in SETTLED_STATUSES and not override.rerun:
        return _skipped(
            f"{task_code}: already {status} under pipeline_run_id={pipeline_run_id}; give "
            "`rerun` to run it again"
        )
    logger.warning(
        "%s: %s under pipeline_run_id=%d, without checking its dependencies: %s",
        task_code,
        "running again" if override.rerun else "running",
        pipeline_run_id,
        override.reason,
    )
    outcome = _run_attempt(
        engine,
        config,
        TaskRun(pipeline_code, task_code, task_id, pipeline_run_id, False),
        child,
        rerun=status in SETTLED_STATUSES,
    )
    record_change(
        engine,
        pipeline_id=pipeline_id,
        pipeline_run_id=pipeline_run_id,
        task_id=task_id,
        action=override.action,
        from_status=status,
        to_status=outcome.status,
        reason=override.reason,
    )
    return outcome


NO_CROSS = CrossPipelineCheck(0)
"""What a preflight that stopped before the cross-pipeline gate reports from it."""


def _queued_retry(conn: Connection, pipeline_run_id: int, task_id: int) -> bool:
    """Whether a task already has a delayed queued retry."""
    queued = (
        conn.execute(statement(conn, "retry_attempts"), {"pipeline_run_id": pipeline_run_id})
        .mappings()
        .all()
    )
    return any(
        row["task_id"] == task_id
        and row["status"] == AttemptStatus.QUEUED
        and row["not_before"] is not None
        for row in queued
    )


def preflight(
    engine: Engine,
    gate: TrackedGate,
    pipeline_id: int,
    task_id: int,
    task_code: str,
    pipeline_run_id: int,
    admission: CrossPipelineCheck | None = None,
) -> tuple[TaskOutcome | None, CrossPipelineCheck]:
    """Return the outcome that stops the task from running now, if any, and the gate's check.

    The check says what the task consumes once it succeeds, and what ``Dependency_gates``
    bypassed.
    """
    with for_run(engine, pipeline_run_id):
        with engine.connect() as conn:
            status = runlog.fetch_task_run_status(conn, task_id, pipeline_run_id)
            run_status = runlog.fetch_pipeline_run_status(conn, pipeline_run_id)
            backfill = runlog.fetch_run_kind(conn, pipeline_run_id).backfill
            queued_here = _queued_retry(conn, pipeline_run_id, task_id)
            run_scopes.check_task(conn, pipeline_run_id, task_id)
            graph_data = fetch_pipeline_graph(conn, pipeline_id)
            scope = run_scopes.fetch_selection(conn, pipeline_run_id)
            if scope is not None:
                graph_data = run_scopes.selected_graph(graph_data, scope)
            run_state = runlog.fetch_run_state(
                conn, pipeline_run_id, [task.task_id for task in graph_data.tasks]
            )
        if status in SETTLED_STATUSES:
            return _skipped(
                f"{task_code}: already {status} under pipeline_run_id={pipeline_run_id}"
            ), NO_CROSS
        if status == RunStatus.IN_PROGRESS and not queued_here:
            return TaskOutcome(
                RunStatus.IN_PROGRESS,
                f"{task_code}: already IN-PROGRESS under pipeline_run_id={pipeline_run_id}; not "
                "starting it twice",
            ), NO_CROSS
        if run_status == RunStatus.SKIPPED:
            reason = f"pipeline_run_id={pipeline_run_id} is itself SKIPPED"
            return _record_skipped(engine, task_id, pipeline_run_id, task_code, reason), NO_CROSS

        graph = build_graph(graph_data.tasks, graph_data.same_pipeline_edges)
        still_needed = graph.required_edge_count(task_id) - graph.satisfied_edge_count(
            task_id, run_state, 0
        )
        cross = CrossPipelineCheck(0)
        if still_needed > 0 and task_id in graph_data.cross_pipeline_task_ids:
            if backfill:
                cross = backfill_cross_check(engine, task_id, still_needed)
            else:
                cross = (
                    admission
                    if admission is not None
                    else gate.check(engine, task_id, still_needed)
                )
            still_needed -= cross.satisfied_count
        if still_needed <= 0:
            return None, cross

        unready = describe_unready(graph, task_id, pipeline_run_id)
        if cross.reasons:
            reasons = "; ".join(cross.reasons)
            if cross.definitive and not upstream_pending(graph, task_id, run_state):
                return _record_skipped(
                    engine, task_id, pipeline_run_id, task_code, reasons, decisions=cross.decisions
                ), NO_CROSS
            return _skipped(
                f"{task_code}: {reasons}, and {unready}; nothing recorded", waiting=True
            ), NO_CROSS
        if task_id in graph.unsatisfiable(run_state):
            never = describe_unready(graph, task_id, pipeline_run_id, can_never=True)
            return _record_skipped(engine, task_id, pipeline_run_id, task_code, never), NO_CROSS
        waiting = f"{task_code}: {unready}; nothing recorded, run it again once they are"
        return _skipped(waiting, waiting=True), NO_CROSS


"""The dependency types on another pipeline that a backfill counts as met without checking."""


def _skipped(message: str, *, waiting: bool = False) -> TaskOutcome:
    return TaskOutcome(RunStatus.SKIPPED, message, waiting=waiting)


@acting_as(SYSTEM_ACTOR)
def _record_skipped(
    engine: Engine,
    task_id: int,
    pipeline_run_id: int,
    task_code: str,
    reason: str,
    *,
    decisions: tuple[trackers.GateDecision, ...] = (),
) -> TaskOutcome:
    """Record the task ``SKIPPED`` with ``reason``, unless it already has an outcome.

    A task that failed before keeps its row, error and log: it is reported as not retried,
    and stays ``FAILED``.
    """
    with engine.begin() as conn:
        binding = transitions.find_or_create_task_run(conn, task_id, pipeline_run_id)
        if not binding.created and binding.status != RunStatus.SKIPPED:
            return TaskOutcome(
                RunStatus(binding.status),
                f"{task_code}: {binding.status} earlier under pipeline_run_id={pipeline_run_id} "
                f"and not run again: {reason}",
                binding.task_run_id,
            )
        trackers.record_decisions(conn, pipeline_run_id, decisions)
        transitions.finish_task_run(
            conn, binding.task_run_id, status=RunStatus.SKIPPED, error_message=reason
        )
    return TaskOutcome(RunStatus.SKIPPED, f"{task_code}: SKIPPED — {reason}", binding.task_run_id)


def _run_attempt(
    engine: Engine,
    config: ConnectorConfig,
    task: TaskRun,
    child: ChildOptions | None,
    *,
    rerun: bool = False,
) -> TaskOutcome:
    """Admit an attempt and run it in the foreground; see ``execute_attempt``."""
    with engine.connect() as conn:
        handler = fetch_task_execution_detail(conn, task.task_id).handler
    if handler == Handler.EVENT_WAIT:
        result = check_event(
            engine,
            config,
            task.pipeline_run_id,
            task.task_id,
            datetime.now(UTC),
            decisions=task.decisions,
        )
        return TaskOutcome(
            RunStatus(result.status or RunStatus.IN_PROGRESS),
            result.message,
            result.task_run_id,
            waiting=result.next_check_at is not None,
        )
    spec = admit_attempt(engine, config, task, rerun=rerun)
    return execute_attempt(engine, config, spec, child or ChildOptions())


def execute_attempt(
    engine: Engine, config: ConnectorConfig, spec: AttemptSpec, child: ChildOptions
) -> TaskOutcome:
    """Wait for shared claim limits without failing capacity-blocked work."""
    from etl_craft.execution.placement import wait_for_attempt

    return wait_for_attempt(engine, config, spec, child)


def try_execute_attempt(
    engine: Engine,
    config: ConnectorConfig,
    spec: AttemptSpec,
    child: ChildOptions,
    candidates: tuple[int, ...] | None = None,
) -> TaskOutcome | None:
    """Claim if capacity fits, then supervise; a refused capacity check leaves work queued."""
    if config.deployment == DeploymentProfile.CONTROLLER_AGENTS:
        from etl_craft.execution.pools.queue import wait_attempt

        return wait_attempt(engine, config, spec, child)
    with connection_slot(engine) as available:
        if not available:
            return None
        return _try_local_attempt(engine, config, spec, child, candidates)


def _try_local_attempt(
    engine: Engine,
    config: ConnectorConfig,
    spec: AttemptSpec,
    child: ChildOptions,
    candidates: tuple[int, ...] | None,
) -> TaskOutcome | None:
    """Hold the shared process allowance through claim, child exit and outcome recording."""
    owner = leases.owner_id()
    with engine.begin() as conn:
        from etl_craft.engine.agent_transitions import claim_with_capacity
        from etl_craft.execution.placement import local_capacity, local_headroom

        claimed = claim_with_capacity(
            conn,
            spec.attempt_id,
            current_actor(),
            {
                "owner": owner,
                "lease": datetime.now(UTC) + timedelta(seconds=spec.lease_seconds),
                "host": socket.gethostname(),
                "candidate_ids": candidates or (spec.attempt_id,),
                **local_capacity(config).parameters(config),
            },
            wait=True,
        )
        if not claimed:
            from etl_craft.engine.placement import record_wait_reasons

            record_wait_reasons(
                conn,
                config.max_warehouse_queries,
                attempt_id=spec.attempt_id,
                resources=local_headroom(conn, config),
            )
            return None
        refusal = (
            None if child.before_start is None else child.before_start(conn, spec.pipeline_run_id)
        )
        if refusal is not None:
            refuse_attempt(conn, spec.attempt_id, refusal, config.logs.tail_bytes)
            return TaskOutcome(RunStatus.CANCELLED, refusal, spec.task_run_id)
        log_path = Path(
            conn.execute(
                statement(conn, "attempt_log_path"),
                {
                    "attempt_id": spec.attempt_id,
                    "owner": owner,
                    "log_dir": str(config.log_dir.absolute()),
                },
            ).scalar_one()
        )
    try:
        fault_point("runner.after_bind")
        return _start_and_record(engine, config, spec, child, (owner, log_path))
    except BaseException as error:
        _fail_after_bind(engine, spec, error, config, owner)
        raise


def _fail_after_bind(
    engine: Engine, spec: AttemptSpec, error: BaseException, config: ConnectorConfig, owner: str
) -> None:
    """Record an attempt that could not run to its end; never hide the error being raised."""
    if isinstance(error, KeyboardInterrupt):
        received = str(error) or "SIGINT"
        message = f"stopped: the `etl-craft run` running the task received {received}"
    else:
        message = f"could not run the task process: {type(error).__name__}: {error}"
    try:
        with engine.begin() as conn:
            active = transitions.active_attempt(conn, spec.task_run_id)
            if (
                active is not None
                and active.attempt_id == spec.attempt_id
                and active.owner == owner
            ):
                from etl_craft.execution.classify import classify_failure

                transitions.record_attempt_failure(
                    conn, spec.attempt_id, classify_failure(error, config)
                )
                transitions.finish_attempt(
                    conn,
                    spec.attempt_id,
                    AttemptStatus.FAILED,
                    current_actor(),
                    owner=owner,
                    error_message=message,
                    retryable=error.retryable
                    if isinstance(error, EtlCraftError)
                    else not isinstance(error, KeyboardInterrupt),
                )
                logger.error("%s: FAILED — %s", spec.task_code, message)
    except Exception:
        logger.exception("%s: could not record that its attempt failed", spec.task_code)


def _start_and_record(
    engine: Engine,
    config: ConnectorConfig,
    spec: AttemptSpec,
    child: ChildOptions,
    claim: tuple[str, Path],
) -> TaskOutcome:
    """Start the bound attempt's task process, wait for it, and record how it ended."""
    assert config.config_path is not None
    owner, log_path = claim
    argv = _child_argv(config, spec, child, owner)
    logger.info(
        "starting %s attempt %d (task_run_id=%d), time limit %s, log %s",
        spec.task_code,
        spec.attempt_number,
        spec.task_run_id,
        f"{spec.timeout_seconds}s" if spec.timeout_seconds else "none",
        log_path,
    )
    with (
        CancelWatch(engine, spec.pipeline_run_id, child, spec_task_id(engine, spec)) as stop,
        leases.heartbeat(engine, "attempt", spec.attempt_id, owner, stop),
    ):
        result = run_child(
            ChildSpec(
                argv=(sys.executable, *argv),
                timeout_seconds=spec.timeout_seconds,
                log_path=log_path,
                # Output reaches the log as it is written, so a hang or a kill loses none of it.
                env={
                    **os.environ,
                    "PYTHONUNBUFFERED": "1",
                    "ETL_CRAFT_ATTEMPT_ID": str(spec.attempt_id),
                    "ETL_CRAFT_ATTEMPT_OWNER": owner,
                },
            ),
            tail_bytes=config.logs.tail_bytes,
            kill_grace_seconds=child.kill_grace_seconds,
            cancel=stop,
            on_started=lambda pid: attempt_started(engine, spec.attempt_id, owner, pid),
        )
    query_error = None
    queries_ended = control_flow.process_local_queries(config)
    if result.cancelled or result.timed_out or result.returncode < 0:
        from etl_craft.core.ownership import attempt_holder
        from etl_craft.engine.resource_locks import holder_tag
        from etl_craft.execution.query_cancellation import confirm

        query_error = confirm(config, holder_tag(attempt_holder(spec.attempt_id, owner)))
        queries_ended = query_error is None
        with log_path.open("a", encoding="utf-8") as output:
            output.write((query_error or "warehouse queries confirmed stopped") + "\n")
    result = replace(
        result,
        queries_ended=queries_ended,
        output_tail=byte_tail(result.output_tail, config.logs.tail_bytes),
    )
    outcome = retrying(
        f"recording how {spec.task_code} ended",
        lambda: record_attempt(
            engine,
            spec.task_run_id,
            spec.pipeline_run_id,
            spec.task_code,
            result,
            log_path,
            attempt_id=spec.attempt_id,
            owner=owner,
        ),
    )

    control_flow.after_exit(engine, spec.pipeline_run_id, reason=query_error)
    return outcome


def _child_argv(
    config: ConnectorConfig, spec: AttemptSpec, child: ChildOptions, owner: str
) -> list[str]:
    """Build the child invocation for this exact claimed attempt."""
    argv = [
        "-m",
        "etl_craft.services.operations.task_process"
        if spec.handler == Handler.RUN_PIPELINE and child.module == CHILD_MODULE
        else child.module,
        "--config",
        str(config._host_config_path or config.config_path),
        "--task-run-id",
        str(spec.task_run_id),
        "--attempt-id",
        str(spec.attempt_id),
        "--owner-id",
        owner,
        "--log-level",
        child.log_level,
        "--log-format",
        child.log_format,
    ]
    if spec.force:
        argv.append("--force")
    if spec.rerun:
        argv.append("--rerun")
    return argv


def spec_task_id(engine: Engine, spec: AttemptSpec) -> int:
    """Read the exact task of a bound attempt without selecting another run."""
    with engine.connect() as conn:
        row = conn.execute(
            statement(conn, "control_task_context"), {"task_run_id": spec.task_run_id}
        ).one()
        return int(row.task_id)
