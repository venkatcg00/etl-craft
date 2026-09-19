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
# Cross-pipeline dependencies (pipeline-level CFG_PIPELINE_DEPENDENCY, and
# task-level CFG_TASK_DEPENDENCY edges pointing at another pipeline) have no
# DAG-native equivalent (CLAUDE.md: "one DAG can't natively depend on a task
# in a separate DAG") and aren't resolved by anything built yet (the
# self-check/poll step — see orchestrator.py). They're surfaced here purely
# informationally, under their own top-level keys, never wired into `tasks:`.
#
# [ADDITION] `default_args`/`catchup`/`tags` (added per explicit request to
# make the output easier to wire straight into a real Airflow DAG). Every
# value below is either read from real CFG_PIPELINES data or a deliberate,
# explained design choice — nothing is an arbitrary placeholder:
#   * `default_args.owner` <- CREATED_BY (whoever registered the pipeline;
#     the closest real column to "owner" — CFG_PIPELINES has no dedicated
#     owner field).
#   * `default_args.email_on_failure: false` — CLAUDE.md's own EMAIL_ALERT
#     handler is the engine's alerting mechanism (a task gated on another
#     task's FAILURE); turning on Airflow's native email-on-failure too
#     would double-alert on the same failure through two unrelated paths.
#   * `default_args.depends_on_past: false` — CLAUDE.md's run-id model has
#     no notion of "this run depends on the previous DAG run"; every run's
#     dependencies come entirely from CFG_TASK_DEPENDENCY/
#     CFG_PIPELINE_DEPENDENCY, so leaving Airflow's own past-run gating on
#     would silently add ordering this design doesn't have.
#   * `default_args.retries` / `retry_delay_minutes` — arbitrary starting
#     numbers (1 retry, 5 minutes), *not* derived from CFG_ data, but a
#     deliberate default rather than Airflow's bare 0: safe specifically
#     because `run --task_code` is idempotent-retry-resumes (CLAUDE.md), so
#     an Airflow-level retry of the exact same bash_command just continues
#     the same task run rather than restarting it.
#   * `catchup: false` — a metadata-driven run whose pipeline_run_id is
#     minted via `ux_pipeline_run_one_active` doesn't have a meaningful
#     notion of "backfill every missed schedule interval"; leaving Airflow's
#     default catchup on would let a large number of missed-interval DAG
#     runs all race to mint/reuse the same active run at once.
#   * `tags` <- [REFRESH_TYPE.lower()] — genuinely derived from CFG_ data,
#     for Airflow UI filtering.

from __future__ import annotations

from typing import Any

from sqlalchemy.engine import Connection

from etl_craft.cfg import (
    fetch_cross_pipeline_task_edges,
    fetch_pipeline_dependencies,
    fetch_pipeline_detail,
    fetch_pipeline_graph,
    fetch_task_codes,
    resolve_pipeline_id,
)
from etl_craft.resolver import build_graph

# The synthetic first task every generated DAG gets, per CLAUDE.md's old
# design-notes phrasing: "an additional pipeline id creation step that
# starts in step 1 before all named steps." Every real CFG_TASKS-derived
# task with no same-pipeline dependency of its own depends on this instead
# — downstream tasks get ordered after it transitively through their own
# chain, so it's not repeated as a redundant direct edge everywhere.
INIT_TASK_ID = "__init__"


def generate_pipeline_dag(conn: Connection, pipeline_code: str) -> dict[str, Any]:
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
        }
    }
    for task in graph_data.tasks:
        task_code = task_codes[task.task_id]
        edges = same_pipeline_edges_by_task[task.task_id]
        depends_on = (
            [
                {
                    "task": task_codes[edge.depends_on_task_id],
                    "dependency_type": edge.dependency_type,
                }
                for edge in edges
            ]
            if edges
            else [{"task": INIT_TASK_ID, "dependency_type": "ALWAYS"}]
        )
        tasks[task_code] = {
            "bash_command": (
                f"etl-craft run --pipeline_code {pipeline_code} --task_code {task_code}"
            ),
            "depends_on": depends_on,
        }

    dag: dict[str, Any] = {
        "dag_id": detail.pipeline_code,
        "description": detail.description,
        "schedule": detail.run_schedule,
        "sla_hours": detail.sla_in_hours,
        "refresh_type": detail.refresh_type,
        "catchup": False,
        "tags": [detail.refresh_type.lower()],
        "default_args": {
            "owner": detail.created_by,
            "retries": 1,
            "retry_delay_minutes": 5,
            "depends_on_past": False,
            "email_on_failure": False,
        },
        "tasks": tasks,
    }

    pipeline_deps = fetch_pipeline_dependencies(conn, pipeline_id)
    if pipeline_deps:
        # Not DAG-native (CLAUDE.md) — informational only, for whoever wires
        # up the actual self-check/poll step this pipeline's own run needs.
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
