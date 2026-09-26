"""Remote mode: the orchestrator is the only source of truth for scheduling.

``generate-yml`` writes every scheduling rule into the orchestrator's DAGs, and the engine checks
none of them itself: ``run --task_code`` runs what it is told. A task's run condition and its
dependencies' types become one trigger rule on its upstream steps. A dependency on a task in
another pipeline becomes a sensor step that succeeds when the dependency is satisfied, and so
does a dependency on another pipeline, unless ``Global_dag`` is on and the global DAG triggers
the pipelines in dependency order instead. ``__init__`` starts the run before any sensor, so
every task the orchestrator runs has a run to bind to.

Some rules have no equivalent in an orchestrator. Rather than drop them, remote mode fails
wherever they would be dropped (``validate``, ``generate-yml`` and ``run --init-only``), naming
each one with its remedy:

- run condition ``N``: a trigger rule counts all or one of the upstream steps, never N of them;
- a ``HAS_DATA`` dependency: an orchestrator sees whether a step succeeded, not how many rows it
  wrote;
- a task whose dependencies have different types: a trigger rule applies to every upstream step
  alike (a sensor counts as a ``SUCCESS`` dependency, since it succeeds when its dependency is
  satisfied);
- with ``Global_dag`` on, a pipeline whose dependencies on other pipelines have different types,
  for the same reason.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.engine import Connection

from etl_craft.config import ConnectorConfig
from etl_craft.core.enums import DependencyType, RunCondition
from etl_craft.core.errors import RemoteUnsupportedError
from etl_craft.engine.repository.dependencies import (
    fetch_cross_pipeline_task_edges,
    fetch_pipeline_dependency_edges,
    fetch_pipeline_graph,
)
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.repository.tasks import fetch_task_codes

LOCAL_MODE_REMEDY = (
    "or run it in local mode (Orchestration.Mode: local), where etl-craft applies it"
)

TASK_SENSOR_STATES: dict[str, tuple[list[str], list[str]]] = {
    DependencyType.SUCCESS: (["success"], ["failed", "upstream_failed", "skipped"]),
    DependencyType.FAILURE: (["failed", "upstream_failed"], ["success", "skipped"]),
    DependencyType.ALWAYS: (["success", "failed", "upstream_failed", "skipped"], []),
}
"""For a dependency on another pipeline's task: the upstream task states that satisfy it, and
those that never will, as a sensor's ``allowed_states`` and ``failed_states``."""

DAG_SENSOR_STATES: dict[str, tuple[list[str], list[str]]] = {
    DependencyType.SUCCESS: (["success"], ["failed"]),
    DependencyType.FAILURE: (["failed"], ["success"]),
    DependencyType.ALWAYS: (["success", "failed"], []),
}
"""For a dependency on another pipeline: the upstream DAG run states that satisfy it, and those
that never will."""

SENSOR_TYPE = DependencyType.SUCCESS
"""The dependency type a sensor step counts as for the trigger rule of the task after it."""


@dataclass(frozen=True)
class UnsupportedRule:
    """A rule of a pipeline that a remote orchestrator does not support.

    ``where`` is ``PIPELINE`` or ``PIPELINE.TASK``; ``rule`` is the rule as it is configured.
    """

    where: str
    rule: str
    remedy: str

    def describe(self) -> str:
        """Return one line naming the rule, why it fails, and the remedy."""
        return (
            f"{self.where}: {self.rule}; the remote orchestrator does not support this. "
            f"{self.remedy}"
        )


def unsupported_rules(
    conn: Connection, pipeline_id: int, pipeline_code: str, *, global_dag: bool
) -> list[UnsupportedRule]:
    """Return every rule of ``pipeline_id`` a remote orchestrator does not support."""
    data = fetch_pipeline_graph(conn, pipeline_id)
    codes = fetch_task_codes(conn, pipeline_id)
    found: list[UnsupportedRule] = []
    for task in sorted(data.tasks, key=lambda t: codes[t.task_id]):
        where = f"{pipeline_code}.{codes[task.task_id]}"
        if task.run_condition == RunCondition.N:
            found.append(
                UnsupportedRule(
                    where,
                    f"RUN_CONDITION = 'N' (RUN_CONDITION_COUNT = {task.run_condition_count})",
                    "An orchestrator's trigger rule waits for all or one of the upstream steps. "
                    f"Use RUN_CONDITION 'ALL' or 'ANY', {LOCAL_MODE_REMEDY}.",
                )
            )
        types: dict[str, str] = {}
        for edge in data.same_pipeline_edges:
            if edge.task_id == task.task_id:
                types[codes[edge.depends_on_task_id]] = edge.dependency_type
        if task.task_id in data.cross_pipeline_task_ids:
            for cross in fetch_cross_pipeline_task_edges(conn, task.task_id):
                if cross.dependency_type == DependencyType.HAS_DATA:
                    types[cross.depends_on_label] = DependencyType.HAS_DATA
                else:
                    types[f"the sensor on {cross.depends_on_label}"] = SENSOR_TYPE
        for upstream, kind in sorted(types.items()):
            if kind == DependencyType.HAS_DATA:
                found.append(
                    UnsupportedRule(
                        where,
                        f"depends on {upstream} with DEPENDENCY_TYPE = 'HAS_DATA'",
                        "An orchestrator sees whether a step succeeded, not whether it wrote "
                        "rows. Use DEPENDENCY_TYPE 'SUCCESS' and let the task handle an empty "
                        f"input, {LOCAL_MODE_REMEDY}.",
                    )
                )
        kinds = {
            DependencyType.SUCCESS if kind == DependencyType.HAS_DATA else kind
            for kind in types.values()
        }
        if len(kinds) > 1:
            listed = ", ".join(f"{upstream}: {kind}" for upstream, kind in sorted(types.items()))
            found.append(
                UnsupportedRule(
                    where,
                    f"its dependencies have different types ({listed})",
                    "An orchestrator applies one trigger rule to every upstream step. Give them "
                    "one DEPENDENCY_TYPE, or split the task in two, "
                    f"{LOCAL_MODE_REMEDY}.",
                )
            )
    pipeline_edges = fetch_pipeline_dependency_edges(conn, pipeline_id)
    for upstream_pipeline in pipeline_edges:
        if upstream_pipeline.dependency_type == DependencyType.HAS_DATA:
            found.append(
                UnsupportedRule(
                    pipeline_code,
                    f"depends on pipeline {upstream_pipeline.depends_on_pipeline_code} with "
                    "DEPENDENCY_TYPE = 'HAS_DATA'",
                    "An orchestrator sees whether a DAG run succeeded, not whether it wrote rows. "
                    f"Use DEPENDENCY_TYPE 'SUCCESS', {LOCAL_MODE_REMEDY}.",
                )
            )
    pipeline_kinds = {
        edge.dependency_type
        for edge in pipeline_edges
        if edge.dependency_type != DependencyType.HAS_DATA
    }
    if global_dag and len(pipeline_kinds) > 1:
        listed = ", ".join(
            f"{edge.depends_on_pipeline_code}: {edge.dependency_type}"
            for edge in sorted(pipeline_edges, key=lambda e: e.depends_on_pipeline_code)
        )
        found.append(
            UnsupportedRule(
                pipeline_code,
                f"its dependencies on other pipelines have different types ({listed}), and "
                "Global_dag is on",
                "The global DAG applies one trigger rule to every upstream pipeline. Give them "
                "one DEPENDENCY_TYPE, or turn Global_dag off so that each dependency becomes a "
                f"sensor of its own, {LOCAL_MODE_REMEDY}.",
            )
        )
    return found


def require_supported(conn: Connection, config: ConnectorConfig, pipeline_code: str) -> None:
    """Raise ``RemoteUnsupportedError`` naming every rule of the pipeline the orchestrator lacks."""
    pipeline_id = resolve_pipeline_id(conn, pipeline_code)
    found = unsupported_rules(
        conn, pipeline_id, pipeline_code, global_dag=config.dag_defaults.global_dag
    )
    if found:
        raise RemoteUnsupportedError(
            f"{pipeline_code} has {len(found)} rule(s) the remote orchestrator does not support, "
            "so they cannot be applied in remote mode: "
            + " | ".join(rule.describe() for rule in found)
        )
