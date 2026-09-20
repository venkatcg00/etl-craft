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
  * [ADDITION] A third check, per explicit instruction: "every task should
    have atleast 1 source_table and target_table" — every active task
    (any HANDLER) must declare CFG_TASK_PARAMETERS.SOURCE_OBJECT and
    TARGET_OBJECT (cfg.py's own lineage convention — see its module-level
    comment on LINEAGE_SOURCE_PARAM/LINEAGE_TARGET_PARAM). Purely
    declarative bookkeeping for the `lineage` CLI command's traceability
    goal, checked here rather than enforced as a hard runtime failure in
    the handler modules themselves — same "not enforceable as a schema
    constraint, so check it at validate time" reasoning schema.sql's own
    closing summary already uses for CFG_TASK_PARAMETERS conventions in
    general.

[ADDITION] CLAUDE.md's wording ("including") implies `validate` could grow
more checks later; these three are the only ones it, schema.sql, or later
explicit instruction call for today. Every issue found is collected into a
flat list rather than raised on the first failure, so one bad pipeline or
rule doesn't hide the rest — and so a future check can be added as just
another function feeding the same list.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from sqlalchemy import inspect
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import NoSuchTableError

from etl_craft.cfg import (
    KNOWN_PARAMETERS,
    fetch_all_pipelines,
    fetch_business_rule_targets,
    fetch_pipeline_graph,
    fetch_sql_snippets,
    fetch_tasks_missing_source_or_target,
    fetch_tasks_with_parameters,
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


# [ADDITION, 2026-09-20, E2-07] Statements a read-only SELECT has no business
# containing. Matched as whole words, after comments and string literals are
# stripped, so a column called `update_date` or a literal 'DROP' is not a hit.
_WRITE_KEYWORDS = (
    "INSERT",
    "UPDATE",
    "DELETE",
    "MERGE",
    "TRUNCATE",
    "DROP",
    "ALTER",
    "CREATE",
    "GRANT",
    "REVOKE",
    "COPY",
)
_COMMENT_RE = re.compile(r"/\*.*?\*/|--[^\n]*", re.DOTALL)
_LITERAL_RE = re.compile(r"'(?:[^']|'')*'")


def _strip_noise(sql: str) -> str:
    """Remove comments and string literals so keyword matching sees only real SQL."""
    return _LITERAL_RE.sub("''", _COMMENT_RE.sub(" ", sql))


def looks_read_only(sql: str) -> str | None:
    """Return why `sql` does not look like a read-only SELECT, or None if it does.

    [ADDITION, 2026-09-20, E2-07] CLAUDE.md's core principle is that each SQL
    task supplies "a bare, **validated**, read-only SELECT" and that "a step
    cannot touch the warehouse outside its declared action" — but nothing
    validated it. SOURCE_SQL is interpolated straight into
    `CREATE TEMPORARY TABLE stage AS {select}`, and Postgres supports
    data-modifying CTEs, so `WITH x AS (DELETE FROM other RETURNING *)
    SELECT * FROM x` is a perfectly valid "SELECT" that writes.

    **This is a lint, not a security boundary.** Adding a real SQL parser is
    ruled out by Non-goals, and a determined author can defeat any string
    check. The real control is that CFG_ rows are git-reviewed; this catches
    the honest mistake and makes the intent explicit. It lives in `validate`
    rather than at runtime for the same reason: a config problem should be
    findable before 3 a.m., and a runtime check on every execution would cost
    something for no extra safety.
    """
    stripped = _strip_noise(sql).strip()
    if not stripped:
        return "is empty"
    first = re.match(r"[(\s]*(\w+)", stripped)
    if first is None or first.group(1).upper() not in {"SELECT", "WITH", "TABLE", "VALUES"}:
        got = first.group(1) if first else stripped[:20]
        return f"starts with {got!r}, not SELECT/WITH"
    found = [kw for kw in _WRITE_KEYWORDS if re.search(rf"\b{kw}\b", stripped, re.IGNORECASE)]
    if found:
        return f"contains {found} — a read-only SELECT should not"
    return None


def validate_read_only_sql(conn: Connection) -> list[ValidationIssue]:
    """Lint every active SOURCE_SQL and BUSINESS_RULE_SQL for statements that write."""
    issues = [
        ValidationIssue(
            category="sql_read_only",
            message=(
                f"{entry.pipeline_code}.{entry.task_code}: CFG_TASK_PARAMETERS."
                f"{entry.parameter_name} {reason}"
            ),
        )
        for entry in fetch_sql_snippets(conn)
        if (reason := looks_read_only(entry.sql)) is not None
    ]
    return issues


# [ADDITION, 2026-09-20, E2-25] What each SQL_ACTION requires. These are
# conventions the execution code already depends on; checking them here means
# a config mistake surfaces from `validate` rather than from a task failing at
# 3 a.m. halfway through a run.
_REQUIRED_SQL_PARAMS: dict[str, tuple[str, ...]] = {
    "CREATE_TABLE": ("SOURCE_SQL",),
    "SETUP_TABLE": ("SOURCE_SQL",),
    "OVERWRITE_TABLE": ("SOURCE_SQL",),
    "SCD1_MERGE": ("SOURCE_SQL", "MERGE_KEY", "MERGE_COMPARE_COLUMNS"),
    "SCD2_MERGE": ("SOURCE_SQL", "MERGE_KEY", "MERGE_COMPARE_COLUMNS"),
    "DROP_TABLE": (),
    "DELETE_ROWS": ("MERGE_KEY",),
}

# Identifiers are interpolated unquoted into SQL text and into generate-yml's
# bash_command, so they must be safe in both.
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SAFE_OBJECT_REF = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")


def validate_task_parameters(conn: Connection) -> list[ValidationIssue]:
    """Check each task declares the parameters its own HANDLER and SQL_ACTION require."""
    issues: list[ValidationIssue] = []
    for task in fetch_tasks_with_parameters(conn):
        where = f"{task.pipeline_code}.{task.task_code}"
        params = task.parameters

        def add(message: str, where: str = where) -> None:
            issues.append(
                ValidationIssue(category="task_parameters", message=f"{where}: {message}")
            )

        if not _SAFE_IDENTIFIER.match(task.task_code):
            add(
                f"TASK_CODE {task.task_code!r} is not a safe identifier — codes are "
                "interpolated unquoted into SQL and into generate-yml's bash_command"
            )

        target = params.get("TARGET_OBJECT")
        if target and "|" in target and task.handler == "SQL":
            # E2-32: lineage allows pipe-separated multi-values, but a SQL
            # action writes exactly one table and qualify() would produce
            # "db.a.b|c.d".
            add("HANDLER='SQL' requires a single TARGET_OBJECT, not a pipe-separated list")
        elif target and not _SAFE_OBJECT_REF.match(target) and task.handler == "SQL":
            add(f"TARGET_OBJECT {target!r} must be exactly 'schema.table'")

        if task.handler == "SQL":
            action = params.get("SQL_ACTION")
            if action is None:
                add("HANDLER='SQL' requires a SQL_ACTION parameter")
            elif action not in _REQUIRED_SQL_PARAMS:
                add(f"SQL_ACTION={action!r} is not one of {sorted(_REQUIRED_SQL_PARAMS)}")
            else:
                for required in _REQUIRED_SQL_PARAMS[action]:
                    if not params.get(required):
                        add(f"SQL_ACTION={action} requires a {required} parameter")

        if task.handler == "PYTHON":
            if not params.get("SCRIPT_NAME"):
                add("HANDLER='PYTHON' requires a SCRIPT_NAME parameter")
            declared = {
                part.strip() for part in (params.get("RETURN_VALUES") or "").split("|") if part
            }
            for mandatory in ("INGESTION_COUNT", "LATEST_OFFSET_UPDATE"):
                if mandatory not in declared:
                    add(f"HANDLER='PYTHON' must declare {mandatory} in RETURN_VALUES")

        if task.handler == "EMAIL_ALERT":
            if not params.get("EMAIL_TO"):
                add("HANDLER='EMAIL_ALERT' requires an EMAIL_TO parameter")
            has_body = any(
                params.get(name)
                for name in ("EMAIL_BODY", "EMAIL_BODY_SUCCESS", "EMAIL_BODY_FAILED")
            )
            if not has_body and not params.get("EMAIL_PIPELINES"):
                add("HANDLER='EMAIL_ALERT' requires EMAIL_BODY (or EMAIL_PIPELINES)")

        unknown = sorted(set(params) - KNOWN_PARAMETERS)
        if unknown:
            add(f"unrecognized CFG_TASK_PARAMETERS name(s) {unknown} — a typo will be ignored")
    return issues


def validate_task_lineage_declarations(conn: Connection) -> list[ValidationIssue]:
    """Check every active task (any HANDLER) declares SOURCE_OBJECT and TARGET_OBJECT."""
    return [
        ValidationIssue(
            category="task_lineage",
            message=(
                f"{gap.pipeline_code}.{gap.task_code}: missing CFG_TASK_PARAMETERS "
                f"{', '.join(gap.missing)}"
            ),
        )
        for gap in fetch_tasks_missing_source_or_target(conn)
    ]
