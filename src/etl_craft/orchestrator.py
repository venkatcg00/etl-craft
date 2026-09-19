"""The pipeline-level orchestration loop: what `run --pipeline_code X` (no --task_code) does."""

# Per CLAUDE.md's Execution section: "the engine becomes its own tiny
# scheduler: resolve the ready wave from CFG_TASK_DEPENDENCY ..., spawn one
# `run --task_code` subprocess per ready task, wait on the wave, repeat."
# Each wave's tasks run in parallel (one subprocess each); waves themselves
# run sequentially. Every subprocess is a genuine `python -m etl_craft run
# --task_code` invocation, not an in-process function call, so this mirrors
# what Airflow-side execution does too (CLAUDE.md's Crash detection section)
# and each task independently resolves its own pipeline_run_id per "Run-id
# resolution" rather than having one passed down to it.
#
# [CHOICE] run_pipeline() — the full wave-spawning scheduler — is refused
# outright under Mode=orchestrator, regardless of --force. CLAUDE.md's own
# framing ("local runs are built to mimic exactly what an orchestrator-driven
# run does") implies this wave-spawning loop is a *local stand-in* for what
# Airflow's own scheduler already does natively via the DAG generate-yml
# produces — under real Airflow there is never a reason to also run our
# Python-level subprocess loop, and doing so would be redundant with (and
# race against) Airflow's own per-task scheduling. CLAUDE.md doesn't say
# this explicitly, and does say "run is the only execution primitive, in
# both modes" — the generated DAG's synthetic first step (mint the run,
# eventually poll cross-pipeline deps, per "an additional pipeline id
# creation step that starts in step 1 before all named steps") still goes
# through `run`, just via the new `--init-only` flag (`init_pipeline_run`
# below) rather than the bare no-`--task_code` form.
#
# Cross-pipeline dependency polling ("the self-check/poll step inserted
# before step 1, alongside run-id minting") is wired into both
# init_pipeline_run and run_pipeline below, but only on the mint-a-*new*-run
# path — never re-checked against an already-IN-PROGRESS run, since that
# run's own gate already passed when it was minted. If the gate finds an
# unmet dependency, the run is still minted (so any task invocation that
# was waiting on it has a real pipeline_run_id to bind to) but immediately
# finalized SKIPPED rather than left IN-PROGRESS — runner.py's run_task
# checks for exactly this and marks every task under it SKIPPED too.
#
# Closes the gap flagged during cross-pipeline polling: under
# Mode=orchestrator, nothing used to mark AUD_PIPELINES_RUN_LOG SUCCESS/
# FAILED once a run's tasks were all done, since finalize_pipeline_run was
# only ever called from run_pipeline() (refused under that mode). The
# generated DAG's synthetic *last* step, finalize_active_run() below,
# mirrors init_pipeline_run's synthetic first one and closes it — wired
# into generate_yml.py as a __finalize__ task depending (ALWAYS) on every
# leaf task.
#
# Deliberately NOT included yet:
#   * Orchestrator-level crash detection for a subprocess that dies without
#     writing its own terminal status. CLAUDE.md's crash detection is
#     specifically about run_task's *own* internal fork/monitor (still
#     deferred — see runner.py), not an extra layer here. A task stuck that
#     way can never become ready again (IN-PROGRESS blocks retry — see
#     resolver.NOT_RETRYABLE), so the stuck-detection below still catches
#     it and finalizes the pipeline FAILED rather than hanging forever.

from __future__ import annotations

import subprocess
import sys
import time
from dataclasses import dataclass

from sqlalchemy.engine import Engine

from etl_craft.cfg import fetch_pipeline_graph, fetch_task_codes, resolve_pipeline_id
from etl_craft.config import ConnectorConfig
from etl_craft.crosspipe import (
    NowFn,
    SleepFn,
    _default_now,
    check_pipeline_dependencies,
    consume_pipeline_dependency_edges,
)
from etl_craft.resolver import DependencyGraph, TaskRunState, build_graph
from etl_craft.runlog import (
    RunLogError,
    fetch_active_pipeline_run_id,
    fetch_run_state,
    finalize_pipeline_run,
    find_or_create_active_run,
)

# Tasks in this state need no further action. FAILED is deliberately not
# included here — per "retry resumes", a FAILED task is still retry-eligible
# and graph.ready() will offer it again once its own upstream deps allow.
SETTLED_STATUSES = frozenset({"SUCCESS", "SKIPPED"})


class OrchestratorModeRefusedError(Exception):
    """Raised when the local wave-spawning scheduler is invoked under Mode=orchestrator."""


@dataclass(frozen=True)
class PipelineOutcome:
    """The result of one run_pipeline() call."""

    status: str  # "SUCCESS", "FAILED", or "SKIPPED" (a cross-pipeline dependency was never met)
    message: str


@dataclass(frozen=True)
class InitOutcome:
    """The result of one init_pipeline_run() call."""

    pipeline_run_id: int
    message: str


@dataclass(frozen=True)
class FinalizeOutcome:
    """The result of one finalize_active_run() call."""

    status: str  # "SUCCESS" or "FAILED"
    message: str


def _finalize_from_task_states(
    engine: Engine, pipeline_id: int, pipeline_run_id: int, all_task_ids: list[int]
) -> tuple[str, list[int]]:
    """Compute SUCCESS/FAILED from every task's own settled status, finalize, consume trackers.

    Returns (final_status, unsettled_task_ids) — the caller decides how
    much detail about `unsettled` to put in its own outcome message.
    """
    with engine.connect() as conn:
        final_state = fetch_run_state(conn, pipeline_run_id, all_task_ids)
    unsettled = [
        task_id
        for task_id in all_task_ids
        if final_state.get(task_id, TaskRunState()).status not in SETTLED_STATUSES
    ]
    final_status = "FAILED" if unsettled else "SUCCESS"
    with engine.begin() as conn:
        finalize_pipeline_run(conn, pipeline_run_id, final_status)
    # Per CLAUDE.md: tracker updates only after the gated pipeline
    # completes — this pipeline's own outgoing cross-pipeline edges (if
    # any) are advanced now, regardless of whether it succeeded or failed.
    consume_pipeline_dependency_edges(engine, pipeline_id)
    return final_status, unsettled


def finalize_active_run(
    engine: Engine, config: ConnectorConfig, pipeline_code: str
) -> FinalizeOutcome:
    """Finalize `pipeline_code`'s active run — the generated DAG's synthetic last step.

    Mirrors init_pipeline_run's synthetic first step: computes SUCCESS/
    FAILED from every active task's own current status (same rule
    run_pipeline's own finalize step uses) and writes it to
    AUD_PIPELINES_RUN_LOG. Exists specifically for Mode=orchestrator, where
    nothing else ever finalizes the *pipeline* row — run_pipeline(), the
    only other caller of this same logic, is refused under that mode.
    """
    del config  # not needed — finalizing reads AUD_TASK_RUN_LOG state only
    with engine.connect() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)

    with engine.connect() as conn:
        pipeline_run_id = fetch_active_pipeline_run_id(conn, pipeline_id)
    if pipeline_run_id is None:
        raise RunLogError(
            f"{pipeline_code}: no active (IN-PROGRESS) run to finalize — "
            "--finalize-only runs after --init-only and the pipeline's tasks, not before them"
        )

    with engine.connect() as conn:
        graph_data = fetch_pipeline_graph(conn, pipeline_id)
    all_task_ids = [task.task_id for task in graph_data.tasks]

    final_status, _ = _finalize_from_task_states(engine, pipeline_id, pipeline_run_id, all_task_ids)
    return FinalizeOutcome(
        status=final_status,
        message=f"{pipeline_code}: pipeline_run_id={pipeline_run_id} {final_status}",
    )


def init_pipeline_run(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    *,
    sleep: SleepFn = time.sleep,
    now: NowFn = _default_now,
) -> InitOutcome:
    """Mint/reuse `pipeline_code`'s active run — the generated DAG's synthetic first step."""
    del config  # not needed for the cross-pipeline gate itself
    with engine.connect() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)

    with engine.connect() as conn:
        existing = fetch_active_pipeline_run_id(conn, pipeline_id)
    if existing is not None:
        return InitOutcome(
            pipeline_run_id=existing, message=f"{pipeline_code}: pipeline_run_id={existing}"
        )

    # Only reached when about to mint a genuinely new run — an already
    # IN-PROGRESS run's own gate already passed when it was minted, so it's
    # never re-checked here. (A concurrent caller could in principle mint
    # its own run between this gate check and find_or_create_active_run
    # below winning that race; find_or_create_active_run's own unique-index
    # fallback still returns the right row either way, but in that rare
    # window this caller's gate conclusion — not the winner's — is what
    # gets recorded. Accepted as rare enough not to engineer around.)
    skip_reason = check_pipeline_dependencies(engine, pipeline_id, sleep=sleep, now=now)
    with engine.begin() as conn:
        pipeline_run_id = find_or_create_active_run(conn, pipeline_id)
        if skip_reason is not None:
            finalize_pipeline_run(conn, pipeline_run_id, "SKIPPED")
    if skip_reason is not None:
        return InitOutcome(
            pipeline_run_id=pipeline_run_id,
            message=f"{pipeline_code}: pipeline_run_id={pipeline_run_id} SKIPPED — {skip_reason}",
        )
    return InitOutcome(
        pipeline_run_id=pipeline_run_id,
        message=f"{pipeline_code}: pipeline_run_id={pipeline_run_id}",
    )


def run_pipeline(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_code: str,
    *,
    force: bool = False,
    sleep: SleepFn = time.sleep,
    now: NowFn = _default_now,
) -> PipelineOutcome:
    """Run every active task in `pipeline_code`'s dependency graph, wave by wave."""
    if config.mode == "orchestrator":
        raise OrchestratorModeRefusedError(
            "run --pipeline_code X (no --task_code) is refused under Mode=orchestrator — "
            "Airflow's own DAG structure handles per-task scheduling; the generated DAG's "
            "synthetic first step uses --init-only instead"
        )

    with engine.connect() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)

    if not force:
        # Same "only gate when actually minting a new run" rule as
        # init_pipeline_run — --force bypasses this gate entirely too, per
        # CLAUDE.md ("--force bypasses all dependency/state checks").
        with engine.connect() as conn:
            existing = fetch_active_pipeline_run_id(conn, pipeline_id)
        if existing is None:
            skip_reason = check_pipeline_dependencies(engine, pipeline_id, sleep=sleep, now=now)
            if skip_reason is not None:
                with engine.begin() as conn:
                    pipeline_run_id = find_or_create_active_run(conn, pipeline_id)
                    finalize_pipeline_run(conn, pipeline_run_id, "SKIPPED")
                consume_pipeline_dependency_edges(engine, pipeline_id)
                return PipelineOutcome(
                    status="SKIPPED",
                    message=(
                        f"{pipeline_code}: pipeline_run_id={pipeline_run_id} SKIPPED — "
                        f"{skip_reason}"
                    ),
                )

    with engine.begin() as conn:
        pipeline_run_id = find_or_create_active_run(conn, pipeline_id)

    with engine.connect() as conn:
        graph_data = fetch_pipeline_graph(conn, pipeline_id)
        task_codes = fetch_task_codes(conn, pipeline_id)
    graph = build_graph(graph_data.tasks, graph_data.same_pipeline_edges)
    all_task_ids = [task.task_id for task in graph_data.tasks]

    if not all_task_ids:
        with engine.begin() as conn:
            finalize_pipeline_run(conn, pipeline_run_id, "SUCCESS")
        consume_pipeline_dependency_edges(engine, pipeline_id)
        return PipelineOutcome(status="SUCCESS", message=f"{pipeline_code}: no active tasks")

    if force:
        # Dependency checks are bypassed entirely, so there's nothing left
        # for graph.ready() to gate on — use the static topological waves
        # instead, which still preserves execution order without caring
        # about anyone's current AUD_TASK_RUN_LOG status.
        for wave in graph.waves():
            _run_wave(wave, task_codes, pipeline_code, force=True)
        never_ready: list[int] = []
    else:
        never_ready = _run_until_settled(
            engine, graph, pipeline_run_id, all_task_ids, task_codes, pipeline_code
        )

    final_status, unsettled = _finalize_from_task_states(
        engine, pipeline_id, pipeline_run_id, all_task_ids
    )

    if never_ready:
        failed_count = len(unsettled) - len(never_ready)
        message = (
            f"{pipeline_code}: stuck — {len(never_ready)} task(s) never became ready "
            f"(blocked on an unmet dependency), {failed_count} failed outright"
        )
    elif final_status == "FAILED":
        message = f"{pipeline_code}: FAILED — {len(unsettled)} task(s) did not succeed"
    else:
        message = f"{pipeline_code}: SUCCESS"
    return PipelineOutcome(status=final_status, message=message)


def _run_until_settled(
    engine: Engine,
    graph: DependencyGraph,
    pipeline_run_id: int,
    all_task_ids: list[int],
    task_codes: dict[int, str],
    pipeline_code: str,
) -> list[int]:
    """Loop waves until every task is settled or none are ready. Return the never-ready ones."""
    # attempted tracks task_ids already spawned in *this* invocation. Per
    # resolver.ready(), a FAILED task stays retry-eligible in general — but
    # that's for a *later*, separate `run` invocation to pick up (CLAUDE.md's
    # "retry resumes" is about re-running the command, not the orchestrator
    # looping internally). Without this, a permanently-failing task would
    # get re-selected and re-spawned by graph.ready() every single pass,
    # forever, since its own status never becomes SUCCESS/SKIPPED/IN-PROGRESS.
    attempted: set[int] = set()
    while True:
        with engine.connect() as conn:
            run_state = fetch_run_state(conn, pipeline_run_id, all_task_ids)
        pending = [
            task_id
            for task_id in all_task_ids
            if run_state.get(task_id, TaskRunState()).status not in SETTLED_STATUSES
            and task_id not in attempted
        ]
        if not pending:
            return []
        ready = [task_id for task_id in graph.ready(run_state) if task_id not in attempted]
        if not ready:
            return pending  # none of these ever got a chance to run this pass — stuck
        attempted.update(ready)
        _run_wave(ready, task_codes, pipeline_code, force=False)


def _run_wave(
    task_ids: list[int], task_codes: dict[int, str], pipeline_code: str, *, force: bool
) -> None:
    """Spawn one `python -m etl_craft run --task_code` subprocess per task_id, wait for all."""
    processes = []
    for task_id in task_ids:
        cmd = [
            sys.executable,
            "-m",
            "etl_craft",
            "run",
            "--pipeline_code",
            pipeline_code,
            "--task_code",
            task_codes[task_id],
        ]
        if force:
            cmd.append("--force")
        processes.append(subprocess.Popen(cmd))
    for process in processes:
        process.wait()
