"""Shared trigger target and combined pipeline graph validation."""

from collections.abc import Mapping

from sqlalchemy.engine import Connection

from etl_craft.core.enums import TaskType
from etl_craft.core.errors import MetadataError
from etl_craft.core.graph import cycles
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pipelines import current_project_id, resolve_pipeline_id
from etl_craft.engine.repository.validation import fetch_pipeline_edges
from etl_craft.handlers.run_pipeline import Trigger, read_trigger


def trigger_definition(conn: Connection, pipeline_id: int, params: Mapping[str, str]) -> Trigger:
    """Require a different active child definition without normalizing its code."""
    trigger = read_trigger(params)
    try:
        target = resolve_pipeline_id(conn, trigger.pipeline_code)
    except MetadataError as error:
        raise MetadataError(
            f"TRIGGER_PIPELINE_CODE={trigger.pipeline_code!r}; expected an active child pipeline; "
            "create or activate that pipeline, or correct the target in config files"
        ) from error
    if target == pipeline_id:
        raise MetadataError(
            f"TRIGGER_PIPELINE_CODE={trigger.pipeline_code!r} targets its own pipeline; "
            "expected another active pipeline; change the trigger target"
        )
    return trigger


def trigger_graph_problems(
    conn: Connection, executing_edge: tuple[str, str] | None = None
) -> list[tuple[str, str]]:
    """Combine trigger edges and pipeline dependencies to name every trigger cycle."""
    rows = conn.execute(statement(conn, "pipeline_triggers")).all()
    edges = {(r.pipeline_code, r.target_code) for r in rows if r.target_code}
    if executing_edge is not None:
        edges.add(executing_edge)
    edges.update(
        (e.pipeline_code, e.depends_on_pipeline_code)
        for e in fetch_pipeline_edges(conn)
        if e.depends_on_pipeline_active
    )
    problems = [
        (
            r.pipeline_code,
            f"{r.pipeline_code}.{r.task_code}: TASK_TYPE={r.task_type!r}; "
            "expected ETL for RUN_PIPELINE; set TASK_TYPE=ETL",
        )
        for r in rows
        if r.task_type != TaskType.ETL and r.project_id == current_project_id()
    ]
    problems.extend(
        (
            next(code for code in cycle if "." not in code),
            f"trigger/dependency cycle ({' -> '.join([*cycle, cycle[0]])}); "
            "expected an acyclic pipeline graph; remove a trigger or dependency edge",
        )
        for cycle in cycles(edges)
        if any("." not in code for code in cycle)
    )
    return problems
