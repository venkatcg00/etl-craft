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
# Deliberately NOT included yet:
#   * Cross-pipeline dependency polling ("the self-check/poll step inserted
#     before step 1, alongside run-id minting") — init_pipeline_run mints
#     the run but doesn't yet check this pipeline's own
#     CFG_PIPELINE_DEPENDENCY edges before doing so.
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
from dataclasses import dataclass

from sqlalchemy.engine import Engine

from etl_craft.cfg import fetch_pipeline_graph, fetch_task_codes, resolve_pipeline_id
from etl_craft.config import ConnectorConfig
from etl_craft.resolver import DependencyGraph, TaskRunState, build_graph
from etl_craft.runlog import fetch_run_state, finalize_pipeline_run, find_or_create_active_run

# Tasks in this state need no further action. FAILED is deliberately not
# included here — per "retry resumes", a FAILED task is still retry-eligible
# and graph.ready() will offer it again once its own upstream deps allow.
SETTLED_STATUSES = frozenset({"SUCCESS", "SKIPPED"})


class OrchestratorModeRefusedError(Exception):
    """Raised when the local wave-spawning scheduler is invoked under Mode=orchestrator."""


@dataclass(frozen=True)
class PipelineOutcome:
    """The result of one run_pipeline() call."""

    status: str  # "SUCCESS" or "FAILED"
    message: str


@dataclass(frozen=True)
class InitOutcome:
    """The result of one init_pipeline_run() call."""

    pipeline_run_id: int
    message: str


def init_pipeline_run(engine: Engine, config: ConnectorConfig, pipeline_code: str) -> InitOutcome:
    """Mint/reuse `pipeline_code`'s active run — the generated DAG's synthetic first step."""
    del config  # unused for now; will gate cross-pipeline polling once that's built
    with engine.connect() as conn:
        pipeline_id = resolve_pipeline_id(conn, pipeline_code)
    with engine.begin() as conn:
        pipeline_run_id = find_or_create_active_run(conn, pipeline_id)
    return InitOutcome(
        pipeline_run_id=pipeline_run_id,
        message=f"{pipeline_code}: pipeline_run_id={pipeline_run_id}",
    )


def run_pipeline(
    engine: Engine, config: ConnectorConfig, pipeline_code: str, *, force: bool = False
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
