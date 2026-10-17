"""Admitting and starting a pipeline run, and its skipped, paused and cancelled outcomes."""

from __future__ import annotations

import logging
from collections.abc import Generator
from datetime import date
from typing import Required, TypedDict, Unpack

from sqlalchemy.engine import Connection, Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.actor import current_actor
from etl_craft.core.enums import (
    InterventionAction,
    Mode,
    RunStatus,
)
from etl_craft.core.errors import RunStateError, StaleTransitionError
from etl_craft.core.faults import fault_point
from etl_craft.core.graph import DependencyGraph, build_graph
from etl_craft.engine import runlog, transitions
from etl_craft.engine.repository import trackers
from etl_craft.engine.repository.dependencies import PipelineGraphData, fetch_pipeline_graph
from etl_craft.engine.repository.pauses import Pause
from etl_craft.engine.repository.pipelines import (
    PipelineDetail,
    fetch_pipeline_detail,
    fetch_pipeline_handlers,
    resolve_pipeline_id,
)
from etl_craft.engine.repository.tasks import fetch_task_codes
from etl_craft.execution.attempt_records import run_cancelled
from etl_craft.execution.connections import check_run_connections
from etl_craft.execution.gates import (
    Clock,
    PipelineGateResult,
)
from etl_craft.execution.gates import (
    check_gate as check_gate_now,
)
from etl_craft.execution.interventions import (
    open_pause,
    record_change,
    record_gate_bypass,
)
from etl_craft.execution.reconcile import reconcile
from etl_craft.execution.remote import require_supported
from etl_craft.execution.run_types import PipelineOutcome, RunHooks, call_hook, default_hooks, drive

logger = logging.getLogger(__name__)


def init_pipeline_run(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    *,
    clock: Clock | None = None,
    hooks: RunHooks | None = None,
    run_date: date | None = None,
    selector: runlog.RunSelector = runlog.ACTIVE_RUN,
) -> PipelineOutcome:
    """Start or resume the run of ``pipeline_code``, for an orchestrator's first step.

    ``run_date`` is the date a new run runs as of, such as the orchestrator's logical date.

    Tests the run's connections and, for a new run in local mode, checks the pipeline's gate. The
    outcome is ``IN-PROGRESS``, or ``SKIPPED`` when the gate was not satisfied. In remote mode the
    orchestrator applies the pipeline's rules, so ``RemoteUnsupportedError`` is raised first when
    it has rules the orchestrator does not support, and no gate is checked.
    """
    remote = config.mode == Mode.REMOTE
    if remote:
        with engine.connect() as conn:
            require_supported(conn, config, pipeline_code)
    else:
        paused = open_pause(engine, pipeline_code)
        if paused is not None:
            return paused_outcome(pipeline_code, paused)
    pipeline_id, _ = prepare(engine, config, pipeline_code)
    pipeline_run_id, skip_reason = _start_run(
        engine,
        config,
        pipeline_code,
        pipeline_id,
        clock or Clock(),
        check_gate=not remote,
        run_date=run_date,
        remote=remote,
        selector=selector,
    )
    if run_cancelled(engine, pipeline_run_id):
        with engine.connect() as conn:
            task_codes = fetch_task_codes(conn, pipeline_id)
        return cancelled_run(
            engine,
            pipeline_code,
            pipeline_run_id,
            task_codes,
            hooks or default_hooks(config, engine),
        )
    if skip_reason is not None:
        return skipped_run(
            pipeline_code, pipeline_run_id, skip_reason, hooks or default_hooks(config, engine)
        )
    return PipelineOutcome(
        RunStatus.IN_PROGRESS,
        f"{pipeline_code}: pipeline_run_id={pipeline_run_id} IN-PROGRESS",
        pipeline_run_id,
    )


def load_pipeline(
    conn: Connection,
    pipeline_id: int,
    *,
    detail: PipelineDetail | None = None,
    graph_data: PipelineGraphData | None = None,
) -> tuple[PipelineDetail, DependencyGraph, dict[int, str]]:
    """Load one pipeline's details, dependency graph and active task codes."""
    detail = detail or fetch_pipeline_detail(conn, pipeline_id)
    data = graph_data or fetch_pipeline_graph(conn, pipeline_id)
    return (
        detail,
        build_graph(data.tasks, data.same_pipeline_edges),
        fetch_task_codes(conn, pipeline_id),
    )


def prepare(
    engine: Engine, config: ConnectorConfig, pipeline_code: str
) -> tuple[int, PipelineDetail]:
    """Resolve the pipeline and test the connections its run uses."""
    with engine.connect() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
        detail = fetch_pipeline_detail(conn, pipeline_id)
        handlers = fetch_pipeline_handlers(conn, pipeline_id)
    sends_sla_email = config.limits.enforce_sla and detail.sla_in_hours is not None
    check_run_connections(engine, config, pipeline_code, handlers, sends_sla_email=sends_sla_email)
    return pipeline_id, detail


class StartOptions(TypedDict, total=False):
    """How ``start_steps`` admits and starts a run."""

    check_gate: Required[bool]
    run_date: date | None
    backfill: str | None
    remote: bool
    selector: runlog.RunSelector


def _start_run(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    pipeline_id: int,
    clock: Clock,
    **kwargs: Unpack[StartOptions],
) -> tuple[int, str | None]:
    """Initialize or resume a run, driving the shared gate admission in the foreground."""
    return drive(
        start_steps(
            engine, config, pipeline_code, pipeline_id, clock, **kwargs, initialize_only=True
        ),
        clock,
    )


def start_steps(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    pipeline_id: int,
    clock: Clock,
    *,
    check_gate: bool,
    run_date: date | None = None,
    backfill: str | None = None,
    remote: bool = False,
    selector: runlog.RunSelector = runlog.ACTIVE_RUN,
    initialize_only: bool = False,
) -> Generator[float, None, tuple[int, str | None]]:
    """Return the run to use and, when the gate refused a new one, why it was ``SKIPPED``.

    A new run runs as of ``run_date`` (today unless given). ``backfill``, the reason for a
    backfill, makes it a backfill run, which checks no dependency on other pipelines. A run in
    progress is resumed, unless it runs as of another date than ``run_date``. A gate
    ``Dependency_gates`` bypassed, or skipped by a backfill, is recorded against the new run.
    """
    reconcile(engine, pipeline_id=pipeline_id)
    queued: int | None = None
    with engine.begin() as conn:
        candidates = runlog.run_candidates(conn, pipeline_id)
        explicit = selector.run_id is not None or selector.run_key is not None
        selected = None
        if explicit:
            if selector.run_id is not None or any(
                r.run_key == selector.run_key for r in candidates
            ):
                selected = runlog.select_run(conn, pipeline_id, selector)
        elif any(r.status in {RunStatus.QUEUED, RunStatus.IN_PROGRESS} for r in candidates):
            selected = runlog.select_run(conn, pipeline_id)
        if selected is not None:
            if explicit and run_date is not None and selected.run_date != run_date:
                raise RunStateError(
                    f"{pipeline_code}: run_id={selected.pipeline_run_id} has run_date="
                    f"{selected.run_date}, not {run_date}; a run's date cannot change"
                )
            if selected.status not in {RunStatus.QUEUED, RunStatus.IN_PROGRESS}:
                if not remote:
                    raise RunStateError(
                        f"{pipeline_code}: run_id={selected.pipeline_run_id} is {selected.status}; "
                        "use --task_code --rerun with a reason, or start a new run"
                    )
                transitions.reopen_run(
                    conn,
                    selected.pipeline_run_id,
                    current_actor(),
                    reason="orchestrator initialized its run again",
                )
            if selected.status == RunStatus.QUEUED:
                queued = selected.pipeline_run_id
                logical_date = selected.run_date
                existing, kind = None, None
            else:
                existing = selected.pipeline_run_id
                kind = runlog.fetch_run_kind(conn, existing)
        else:
            existing, kind = None, None
    if existing is not None and kind is not None:
        if kind.backfill and backfill is None:
            raise RunStateError(
                f"{pipeline_code} is running backfill run pipeline_run_id={existing} as of "
                f"{kind.run_date.isoformat()}; a scheduled run starts once it ends (or cancel it "
                f"with `etl-craft cancel --pipeline_code {pipeline_code} --reason ...`)"
            )
        if backfill is not None and not kind.backfill:
            raise RunStateError(
                f"{pipeline_code} has a scheduled run in progress (pipeline_run_id={existing}); "
                "a backfill never takes it over"
            )
        if run_date is not None and kind.run_date != run_date:
            finalize = " --finalize-only" if remote else ""
            raise RunStateError(
                f"{pipeline_code} has a run in progress (pipeline_run_id={existing}) as of "
                f"{kind.run_date.isoformat()}, not {run_date.isoformat()}; finish it with "
                f"`etl-craft run --pipeline_code {pipeline_code}{finalize}`, or cancel it, first"
            )
        logger.info("%s: resuming pipeline_run_id=%d", pipeline_code, existing)
        return existing, None
    logical_date = (
        logical_date if queued is not None else (run_date or runlog.today(config.timezone))
    )
    reason = None
    decisions: tuple[trackers.GateDecision, ...] = ()
    bypassed: tuple[str, ...] = ()
    if check_gate:
        if queued is None:
            with engine.begin() as conn:
                queued = transitions.create_run(
                    conn,
                    pipeline_id,
                    current_actor(),
                    run_date=logical_date,
                    run_key=selector.run_key,
                    status=RunStatus.QUEUED,
                )
        while True:
            if run_cancelled(engine, queued):
                return queued, None
            result = check_gate_now(
                engine,
                queued,
                pipeline_id,
                clock=clock,
                policy=config.dependency_gates,
                wait_seconds=config.limits.gate_wait_minutes * 60,
            )
            if result.next_check_at is None:
                break
            yield max(0.0, (result.next_check_at - clock.now()).total_seconds())
        assert isinstance(result.check, PipelineGateResult)
        gate = result.check
        bypassed, decisions = gate.bypassed, gate.decisions
        if not gate.satisfied:
            reason = "; ".join(gate.reasons)
    created: int | None
    with engine.begin() as conn:
        if queued is not None:
            try:
                transitions.admit_run(conn, queued, current_actor())
            except StaleTransitionError as error:
                other = runlog.fetch_active_pipeline_run_id(conn, pipeline_id)
                if initialize_only and other == queued:
                    return queued, None
                raise RunStateError(
                    f"{pipeline_code}: another process started pipeline_run_id={other}; "
                    f"pipeline_run_id={queued} stays QUEUED; select it explicitly to resume"
                ) from error
            created = queued
        elif selector.run_key is not None:
            created = transitions.create_run(
                conn,
                pipeline_id,
                current_actor(),
                run_date=logical_date,
                run_key=selector.run_key,
                trigger_kind="ORCHESTRATOR" if remote else "MANUAL",
            )
        else:
            created = transitions.create_active_run(
                conn, pipeline_id, run_date=logical_date, backfill=backfill is not None
            )
        if created is None:
            other = runlog.fetch_active_pipeline_run_id(conn, pipeline_id)
            raise RunStateError(
                f"{pipeline_code}: another process started pipeline_run_id={other} while this "
                "one was checking the pipeline's dependencies; that run is left as it is. See "
                f"`etl-craft history --pipeline_code {pipeline_code}`"
            )
        pipeline_run_id = created
        trackers.record_decisions(conn, pipeline_run_id, decisions)
        fault_point("pipeline.after_insert")
        if reason is not None and not transitions.end_run_if(
            conn, pipeline_run_id, RunStatus.IN_PROGRESS, RunStatus.SKIPPED
        ):
            raise RunStateError(
                f"{pipeline_code}: pipeline_run_id={pipeline_run_id} changed before it could "
                "be recorded SKIPPED; it is left as it is"
            )
    logger.info("%s: started pipeline_run_id=%d", pipeline_code, pipeline_run_id)
    if bypassed:
        record_gate_bypass(engine, pipeline_id, pipeline_run_id, config.dependency_gates, bypassed)
    if backfill is not None:
        record_change(
            engine,
            pipeline_id=pipeline_id,
            pipeline_run_id=pipeline_run_id,
            action=InterventionAction.GATE_BYPASS,
            reason=(
                f"backfill for {logical_date.isoformat()}: {backfill}; "
                "dependencies on other pipelines are not checked, and nothing is consumed"
            ),
        )
    return pipeline_run_id, reason


def skipped_run(
    pipeline_code: str, pipeline_run_id: int, reason: str, hooks: RunHooks
) -> PipelineOutcome:
    """Report a run its schedule skipped, and call the finalized hook."""
    outcome = PipelineOutcome(
        RunStatus.SKIPPED,
        f"{pipeline_code}: pipeline_run_id={pipeline_run_id} SKIPPED — {reason}",
        pipeline_run_id,
    )
    logger.warning("%s", outcome.message)
    call_hook("on_finalized", hooks.on_finalized, outcome)
    return outcome


def paused_outcome(pipeline_code: str, pause: Pause) -> PipelineOutcome:
    """Report a pipeline that started nothing because it is paused."""
    message = (
        f"{pipeline_code}: {pause.describe()}; nothing started. `etl-craft resume "
        f"--pipeline_code {pipeline_code}` lets it run again"
    )
    logger.warning("%s", message)
    return PipelineOutcome(RunStatus.SKIPPED, message, None)


def cancelled_run(
    engine: Engine,
    pipeline_code: str,
    pipeline_run_id: int,
    task_codes: dict[int, str],
    hooks: RunHooks,
) -> PipelineOutcome:
    """Report a run an operator cancelled; it already ended ``CANCELLED``."""
    with engine.connect() as conn:
        rows = runlog.fetch_run_state(conn, pipeline_run_id, list(task_codes))
    stopped = sorted(
        task_codes[task_id]
        for task_id, state in rows.items()
        if state.status == RunStatus.CANCELLED
    )
    message = f"{pipeline_code}: pipeline_run_id={pipeline_run_id} CANCELLED"
    if stopped:
        message += f"; stopped: {', '.join(stopped)}"
    outcome = PipelineOutcome(RunStatus.CANCELLED, message, pipeline_run_id)
    logger.warning("%s", message)
    call_hook("on_finalized", hooks.on_finalized, outcome)
    return outcome
