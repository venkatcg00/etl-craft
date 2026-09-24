"""Build the dict form of a pipeline's DAG — what `generate-yml` dumps to YAML."""

# Per CLAUDE.md's Non-goals: entirely hand-rolled, no dag-factory or other
# orchestrator-specific library dependency. The shape below takes visual
# inspiration from dag-factory-style YAML (a `tasks:` block, `bash_command`,
# a dependency structure) for familiarity, but isn't required to match any
# external tool's schema, and doesn't.
#
# [ADDITION] Nothing in CLAUDE.md specifies the exact YAML shape beyond
# "Airflow/dag-factory-style" — every field name and structural choice below
# is this module's own invention, flagged for confirmation before anything
# downstream (a real Airflow DAG file, documentation, a reference
# implementation repo) comes to depend on it.
#
# [DEVIATION, 2026-09-20, E2-14/E2-41] Each `depends_on` entry carries a
# `trigger_rule`, not the raw `dependency_type` it used to emit — one
# vocabulary, not two side by side. The raw type was Airflow-meaningless and
# pushed the mapping onto whoever wrote a loader, which is exactly the thing
# that makes a generated EMAIL_ALERT task behave wrongly. The rule is derived
# from (CFG_TASKS.RUN_CONDITION, CFG_TASK_DEPENDENCY.DEPENDENCY_TYPE) by
# _TRIGGER_RULES below.
#
# Two combinations genuinely have no Airflow equivalent, and are stated
# rather than papered over:
#   * RUN_CONDITION = 'N' ("at least N of these edges"). Airflow has
#     all_*/one_* and nothing in between, so the emitted rule is `all_done`
#     and the engine's own gate (resolver.required_edge_count) is what
#     actually enforces the count when the task runs.
#   * DEPENDENCY_TYPE = 'HAS_DATA' (upstream succeeded *and* wrote rows).
#     Airflow cannot see TARGET_COUNT at all, so the emitted rule is the
#     upstream-success half (`all_success`/`one_success`) and the engine's
#     own in-task check enforces the rest — which it already did.
#   * A task whose edges have *mixed* DEPENDENCY_TYPEs. One Airflow task has
#     one trigger rule, so there is nothing to map several types onto —
#     `all_done` again, engine gates.
# All three are safe in the same direction: Airflow lets the task start,
# and the engine then records it SKIPPED if the real condition isn't met.
#
# [CHOICE, E2-52] The informational `pipeline_dependencies` and
# `cross_pipeline_task_dependencies` blocks keep emitting the raw
# `dependency_type`, not a trigger rule. They describe CFG_ rows rather than
# anything Airflow executes — there is no task for a rule to attach to — so
# the raw vocabulary is the honest one there. The generated file says so in
# its own header, so a reader meeting both words knows why.

# Task-level cross-pipeline dependencies (CFG_TASK_DEPENDENCY edges pointing
# at another pipeline) have no DAG-native equivalent (CLAUDE.md: "one DAG
# can't natively depend on a task in a separate DAG") and are surfaced here
# purely informationally, under their own top-level key, never wired into
# `tasks:`. Pipeline-level dependencies (CFG_PIPELINE_DEPENDENCY) are
# different: they now drive the optional global DAG below.
#
# [ADDITION] `default_args`/`catchup`/`tags`/`email_on_failure` (added per
# explicit request to make the output easier to wire straight into a real
# Airflow DAG). Every value resolves through three tiers, most-specific
# first, and every tier traces back to something real — nothing is an
# arbitrary placeholder baked into this module:
#   1. The pipeline's own CFG_PIPELINES override column (CATCHUP, TAGS,
#      RETRIES, RETRY_DELAY_MINUTES, DEPENDS_ON_PAST, EMAIL_ON_FAILURE,
#      EMAIL_RECIPIENTS), if not NULL.
#   2. The active Orchestration profile in craft-connector.yml (the same
#      field names, title-cased), if set — a per-environment default.
#   3. A final hardcoded default in `_DEFAULT_*` below, used only if
#      neither of the above set it. Reasoning for each (unchanged from
#      before these became configurable): `depends_on_past: false` because
#      the run-id model has no notion of depending on the previous DAG
#      run; `catchup: false` because a run minted via
#      `ux_pipeline_run_one_active` has no backfill semantics — leaving it
#      on would let missed-interval runs race to mint/reuse the same
#      active run; `retries`/`retry_delay_minutes` nonzero (1, 5) because
#      `run --task_code`'s idempotent retry-resumes design makes an
#      Airflow-level retry of the same bash_command safe by construction;
#      `email_on_failure: false` / `tags` <- `[REFRESH_TYPE.lower()]`.
#   `owner` is not part of this three-tier resolution — it stays derived
#   from CFG_PIPELINES.CREATED_BY (the closest real analog to "owner"),
#   unchanged, since there's no per-pipeline/global override for who
#   registered a pipeline.

from __future__ import annotations

from typing import Any

from sqlalchemy.engine import Connection

from etl_craft.cfg import (
    fetch_all_pipeline_dependency_edges,
    fetch_cross_pipeline_task_edges,
    fetch_pipeline_dependencies,
    fetch_pipeline_detail,
    fetch_pipeline_graph,
    fetch_task_codes,
    resolve_pipeline_id,
)
from etl_craft.config import ConnectorConfig
from etl_craft.resolver import ResolverError, build_graph

# The synthetic first task every generated DAG gets, per CLAUDE.md's old
# design-notes phrasing: "an additional pipeline id creation step that
# starts in step 1 before all named steps." Every real CFG_TASKS-derived
# task with no same-pipeline dependency of its own depends on this instead
# — downstream tasks get ordered after it transitively through their own
# chain, so it's not repeated as a redundant direct edge everywhere.
INIT_TASK_ID = "__init__"

# [ADDITION] The synthetic *last* task, mirroring INIT_TASK_ID — closes the
# gap flagged in orchestrator.py: under Mode=orchestrator, nothing marks
# AUD_PIPELINES_RUN_LOG SUCCESS/FAILED once every task is done, since
# finalize_pipeline_run only otherwise lives in run_pipeline(), refused
# under that mode. Depends (ALWAYS) on every leaf task — one with nothing
# else depending on it — so it always runs last, regardless of whether
# those leaves succeeded or failed, and correctly resolves the pipeline's
# own final status from whatever they actually ended up as.
FINALIZE_TASK_ID = "__finalize__"

_DEFAULT_RETRIES = 1
_DEFAULT_RETRY_DELAY_MINUTES = 5

# [ADDITION] Name of the optional cross-pipeline trigger DAG — nowhere
# specified, this module's own choice, changeable if a different name is
# preferred before anything downstream depends on it.
GLOBAL_DAG_ID = "etl_craft_global_orchestration"


def _resolve(pipeline_value: Any, global_value: Any, default: Any) -> Any:
    """Three-tier resolution: pipeline override, then global default, then the final fallback."""
    if pipeline_value is not None:
        return pipeline_value
    if global_value is not None:
        return global_value
    return default


# [ADDITION, 2026-09-20, E2-41] (RUN_CONDITION, DEPENDENCY_TYPE) -> Airflow
# trigger rule. See this module's own docstring for the two combinations that
# have no exact Airflow equivalent and what is emitted for them instead.
_TRIGGER_RULES: dict[tuple[str, str], str] = {
    ("ALL", "SUCCESS"): "all_success",
    ("ALL", "FAILURE"): "all_failed",
    ("ALL", "ALWAYS"): "all_done",
    ("ALL", "HAS_DATA"): "all_success",
    ("ANY", "SUCCESS"): "one_success",
    ("ANY", "FAILURE"): "one_failed",
    ("ANY", "ALWAYS"): "one_done",
    ("ANY", "HAS_DATA"): "one_success",
    # 'N' has no Airflow equivalent at any dependency type — Airflow offers
    # all_*/one_* and nothing in between. all_done lets the task start and
    # leaves the real count to the engine's own gate.
    ("N", "SUCCESS"): "all_done",
    ("N", "FAILURE"): "all_done",
    ("N", "ALWAYS"): "all_done",
    ("N", "HAS_DATA"): "all_done",
}


def trigger_rule_for(run_condition: str, dependency_type: str) -> str:
    """Map a task's RUN_CONDITION plus one edge's DEPENDENCY_TYPE to an Airflow trigger rule."""
    try:
        return _TRIGGER_RULES[(run_condition, dependency_type)]
    except KeyError:  # pragma: no cover - build_graph rejects both values first
        raise ResolverError(
            f"no Airflow trigger rule for run_condition={run_condition!r} "
            f"dependency_type={dependency_type!r}"
        ) from None


# [ADDITION, 2026-09-20, E2-52] Prepended to every generated file by the CLI.
# Its job is to stop a reader meeting two different dependency vocabularies in
# one file and having to guess which is authoritative.
GENERATED_HEADER = """\
# Generated by `etl-craft generate-yml`. Do not edit by hand — regenerate it.
#
# `tasks:` is what an orchestrator executes. Each task carries one
# `trigger_rule`, because that is what Airflow accepts: one value per task,
# applied to all of its upstreams. It is derived from the task's own
# CFG_TASKS.RUN_CONDITION and its CFG_TASK_DEPENDENCY rows' DEPENDENCY_TYPEs.
# Where no exact Airflow equivalent exists — RUN_CONDITION='N', HAS_DATA, or
# edges with mixed DEPENDENCY_TYPEs — the permissive rule is emitted and the
# engine's own gate decides, recording the task SKIPPED if the real condition
# is not met.
#
# `pipeline_dependencies:` and `cross_pipeline_task_dependencies:` are
# informational only and are never wired into `tasks:`. They describe CFG_
# rows that have no DAG-native equivalent (one DAG cannot depend on a task in
# another DAG), so they keep the raw `dependency_type` vocabulary rather than
# a trigger rule — there is no task for a rule to attach to. The engine
# resolves them at runtime by polling.
"""


def task_trigger_rule(run_condition: str, dependency_types: list[str]) -> str:
    """Resolve one Airflow trigger rule for a whole task, from all its edges' types.

    [DEVIATION, 2026-09-20, E2-46] Airflow's `trigger_rule` is a property of
    the *task* — one value, applied to all of its upstreams. An earlier version
    of this module emitted it per `depends_on` edge, which is only coherent
    while every edge of a task shares one DEPENDENCY_TYPE, and nothing requires
    that: DEPENDENCY_TYPE is a per-row value on CFG_TASK_DEPENDENCY. A task
    with a SUCCESS edge and an ALWAYS edge emitted two conflicting rules and
    handed a loader a choice it could not make correctly — which was precisely
    the argument for emitting trigger_rule instead of the raw type.

    Mixed types resolve to `all_done`, the permissive rule: Airflow starts the
    task and the engine's own per-edge gate decides, the same documented
    fail-safe already used for RUN_CONDITION='N' and HAS_DATA.
    """
    distinct = set(dependency_types)
    if not distinct:
        return "all_success"
    if len(distinct) > 1:
        return "all_done"
    return trigger_rule_for(run_condition, distinct.pop())


def generate_pipeline_dag(
    conn: Connection, config: ConnectorConfig, pipeline_code: str
) -> dict[str, Any]:
    """Build `pipeline_code`'s generated DAG as a plain dict, ready for yaml.safe_dump."""
    pipeline_id = resolve_pipeline_id(conn, pipeline_code)
    detail = fetch_pipeline_detail(conn, pipeline_id)
    graph_data = fetch_pipeline_graph(conn, pipeline_id)
    task_codes = fetch_task_codes(conn, pipeline_id)

    # Raises CycleError/SelfDependencyError/UnknownTaskError (all
    # ResolverError) on a graph that can't become a valid DAG — there's no
    # meaningful YAML to emit for one.
    build_graph(graph_data.tasks, graph_data.same_pipeline_edges)

    same_pipeline_edges_by_task: dict[int, list] = {task.task_id: [] for task in graph_data.tasks}
    for edge in graph_data.same_pipeline_edges:
        same_pipeline_edges_by_task[edge.task_id].append(edge)

    tasks: dict[str, Any] = {
        INIT_TASK_ID: {
            "bash_command": f"etl-craft run --pipeline_code {pipeline_code} --init-only",
            "depends_on": [],
            "trigger_rule": "all_success",
        }
    }
    for task in graph_data.tasks:
        task_code = task_codes[task.task_id]
        edges = same_pipeline_edges_by_task[task.task_id]
        if edges:
            depends_on = [task_codes[edge.depends_on_task_id] for edge in edges]
            rule = task_trigger_rule(
                task.run_condition or "ALL", [edge.dependency_type for edge in edges]
            )
        else:
            # [DEVIATION, 2026-09-20, E2-48] all_success, not all_done. With
            # all_done, a failed __init__ (Engine DB blip, unmet
            # cross-pipeline gate, bad secret) still started every root task,
            # and each then took resolve_run_for_task's dev/ad-hoc fallback
            # into the *previous, already-finalized* run and rewrote its rows.
            # A task whose run was never minted has nothing correct to do.
            depends_on = [INIT_TASK_ID]
            rule = "all_success"
        tasks[task_code] = {
            "bash_command": (
                f"etl-craft run --pipeline_code {pipeline_code} --task_code {task_code}"
            ),
            "depends_on": depends_on,
            "trigger_rule": rule,
        }

    depended_on_task_ids = {edge.depends_on_task_id for edge in graph_data.same_pipeline_edges}
    leaf_task_codes = sorted(
        task_codes[task.task_id]
        for task in graph_data.tasks
        if task.task_id not in depended_on_task_ids
    )
    tasks[FINALIZE_TASK_ID] = {
        # all_done on purpose, unlike every other task: finalizing is exactly
        # what must still happen when the work failed.
        "trigger_rule": "all_done",
        "bash_command": f"etl-craft run --pipeline_code {pipeline_code} --finalize-only",
        "depends_on": (list(leaf_task_codes) if leaf_task_codes else [INIT_TASK_ID]),
    }

    orch = config.orchestrator
    email_on_failure = _resolve(detail.email_on_failure, orch.email_on_failure, False)
    default_args: dict[str, Any] = {
        "owner": detail.created_by,
        "retries": _resolve(detail.retries, orch.retries, _DEFAULT_RETRIES),
        "retry_delay_minutes": _resolve(
            detail.retry_delay_minutes, orch.retry_delay_minutes, _DEFAULT_RETRY_DELAY_MINUTES
        ),
        "depends_on_past": _resolve(detail.depends_on_past, orch.depends_on_past, False),
        "email_on_failure": email_on_failure,
    }
    if email_on_failure:
        # Airflow's own email_on_failure is inert without a recipient list
        # — only emitted when there's actually something to send to.
        default_args["email"] = _resolve(detail.email_recipients, orch.email_recipients, [])

    dag: dict[str, Any] = {
        "dag_id": detail.pipeline_code,
        "description": detail.description,
        # Allow_schedule: false keeps the pipeline's definition but emits no
        # schedule, so an environment can hold every pipeline and run each one
        # only when triggered (2026-09-24).
        "schedule": detail.run_schedule if config.orchestrator.allow_schedule else None,
        "sla_hours": detail.sla_in_hours,
        "refresh_type": detail.refresh_type,
        "catchup": _resolve(detail.catchup, orch.catchup, False),
        "tags": _resolve(detail.tags, orch.tags, [detail.refresh_type.lower()]),
        "default_args": default_args,
        "tasks": tasks,
    }

    pipeline_deps = fetch_pipeline_dependencies(conn, pipeline_id)
    if pipeline_deps:
        # Not DAG-native (CLAUDE.md) — informational here regardless of the
        # global DAG below, since a team generating just this one pipeline's
        # YAML still needs to see what it depends on.
        dag["pipeline_dependencies"] = [
            {
                "depends_on_pipeline": dep.depends_on_pipeline_code,
                "dependency_type": dep.dependency_type,
            }
            for dep in pipeline_deps
        ]

    cross_task_deps = fetch_cross_pipeline_task_edges(conn, pipeline_id)
    if cross_task_deps:
        dag["cross_pipeline_task_dependencies"] = [
            {
                "task": dep.task_code,
                "depends_on_pipeline": dep.depends_on_pipeline_code,
                "depends_on_task": dep.depends_on_task_code,
                "dependency_type": dep.dependency_type,
            }
            for dep in cross_task_deps
        ]

    return dag


def generate_global_dag(conn: Connection) -> dict[str, Any]:
    """Build the optional cross-pipeline trigger DAG — every pipeline touched by a real edge.

    Orchestration's Global_dag must be enabled (checked by the caller, not
    here — this function only needs `conn`, since resolving what to emit
    is purely CFG_PIPELINE_DEPENDENCY data, not config).

    Confirmed with the user: each node represents triggering that
    pipeline's own already-generated DAG (Airflow's TriggerDagRunOperator,
    described in this hand-rolled shape via `trigger_dag_id` — never
    imported, same spirit as `bash_command` in generate_pipeline_dag),
    gated on the CFG_PIPELINE_DEPENDENCY edge's own DEPENDENCY_TYPE. A
    pipeline with nothing depending on it and nothing it depends on isn't
    included — it has no cross-DAG ordering to represent here.
    """
    edges = fetch_all_pipeline_dependency_edges(conn)

    edges_by_pipeline: dict[str, list] = {}
    pipeline_codes: set[str] = set()
    for edge in edges:
        edges_by_pipeline.setdefault(edge.pipeline_code, []).append(edge)
        pipeline_codes.add(edge.pipeline_code)
        pipeline_codes.add(edge.depends_on_pipeline_code)

    pipelines: dict[str, Any] = {}
    for pipeline_code in sorted(pipeline_codes):
        own_edges = edges_by_pipeline.get(pipeline_code, [])
        pipelines[pipeline_code] = {
            "trigger_dag_id": pipeline_code,
            "depends_on": [edge.depends_on_pipeline_code for edge in own_edges],
            # Pipeline-level edges have no RUN_CONDITION of their own (that
            # column is on CFG_TASKS), so they always resolve as ALL.
            "trigger_rule": task_trigger_rule("ALL", [edge.dependency_type for edge in own_edges]),
        }

    return {"dag_id": GLOBAL_DAG_ID, "pipelines": pipelines}
