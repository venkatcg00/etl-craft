"""`validate` — config integrity checks no Engine DB constraint can enforce.

Per CLAUDE.md's CLI surface: "Config integrity check — including the
cross-database checks (e.g. single-column PK on a TARGET_TABLE) that no
Engine DB constraint can enforce." Two checks make up the scope here, both
concrete and explicitly called for elsewhere — nothing added is guessed at:

  * Dependency-graph integrity for every active pipeline (cycles,
    self-dependencies, unknown task ids). `resolver.build_graph` already
    does this per pipeline for `graph`/`generate-yml`, but nothing
    previously ran it across *every* pipeline in one pass, and a cyclic
    CFG_TASK_DEPENDENCY graph is exactly the kind of thing no Engine DB
    CHECK constraint can catch.
  * The single-column-primary-key convention CFG_BUSINESS_RULES.
    BUSINESS_RULE_KEY_COLUMN relies on — sql/schema.sql's own comment on
    that table names this exact check: "Enforce it at `validate` time via
    introspection, not here." Checked via SQLAlchemy's `Inspector` against
    the Data DB, the same dialect-agnostic approach warehouse.py uses,
    never a hardcoded driver call.

[ADDITION] CLAUDE.md's wording ("including") implies `validate` could grow
more checks later; these two are the only ones it or schema.sql explicitly
call for today. Every issue found is collected into a flat list rather than
raised on the first failure, so one bad pipeline or rule doesn't hide the
rest — and so a future check can be added as just another function feeding
the same list.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import inspect
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import NoSuchTableError

from etl_craft.cfg import (
    fetch_all_pipelines,
    fetch_business_rule_targets,
    fetch_pipeline_graph,
    resolve_pipeline_id,
)
from etl_craft.resolver import ResolverError, build_graph


@dataclass(frozen=True)
class ValidationIssue:
    """One thing `validate` found wrong — collected, never raised individually."""

    category: str
    message: str


def validate_graphs(conn: Connection) -> list[ValidationIssue]:
    """Check every active pipeline's same-pipeline dependency graph for structural problems."""
    issues: list[ValidationIssue] = []
    for pipeline in fetch_all_pipelines(conn):
        pipeline_id = resolve_pipeline_id(conn, pipeline.pipeline_code)
        graph_data = fetch_pipeline_graph(conn, pipeline_id)
        try:
            build_graph(graph_data.tasks, graph_data.same_pipeline_edges)
        except ResolverError as exc:
            issues.append(
                ValidationIssue(
                    category="graph", message=f"pipeline {pipeline.pipeline_code!r}: {exc}"
                )
            )
    return issues


def validate_business_rule_keys(
    conn: Connection, data_engine: Engine | None
) -> list[ValidationIssue]:
    """Check every active business rule's TARGET_TABLE has a single-column PK matching its key."""
    targets = fetch_business_rule_targets(conn)
    if not targets:
        return []
    if data_engine is None:
        return [
            ValidationIssue(
                category="business_rule_pk",
                message=(
                    f"{len(targets)} active business rule(s) declare a TARGET_TABLE, but no "
                    "[Warehouse] is configured in craft-connector.yml to check them against"
                ),
            )
        ]

    issues: list[ValidationIssue] = []
    inspector = inspect(data_engine)
    for target in targets:
        schema, _, table = target.target_table.rpartition(".")
        try:
            pk = inspector.get_pk_constraint(table, schema=schema or None)
        except NoSuchTableError:
            issues.append(
                ValidationIssue(
                    category="business_rule_pk",
                    message=f"{target.business_rule_name!r}: TARGET_TABLE "
                    f"{target.target_table!r} does not exist in the Data DB",
                )
            )
            continue
        columns = pk.get("constrained_columns") or []
        if len(columns) != 1:
            issues.append(
                ValidationIssue(
                    category="business_rule_pk",
                    message=(
                        f"{target.business_rule_name!r}: {target.target_table!r} must have "
                        f"exactly one primary key column, found {columns}"
                    ),
                )
            )
        elif columns[0].lower() != target.key_column.lower():
            issues.append(
                ValidationIssue(
                    category="business_rule_pk",
                    message=(
                        f"{target.business_rule_name!r}: BUSINESS_RULE_KEY_COLUMN="
                        f"{target.key_column!r} does not match {target.target_table!r}'s actual "
                        f"primary key column {columns[0]!r}"
                    ),
                )
            )
    return issues
