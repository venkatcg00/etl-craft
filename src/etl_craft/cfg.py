"""Read-only queries against CFG_ tables needed by `run` and the resolver."""

# Writing CFG_ rows stays outside the CLI entirely, per CLAUDE.md's CLI
# surface section — pipeline/task/dependency registration is manual,
# git-managed migrations. Everything here is SELECT-only.

from __future__ import annotations

import difflib
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.engine import Connection

from etl_craft.resolver import TaskEdge, TaskNode


class CfgError(Exception):
    """Raised when a --pipeline_code/--task_code doesn't resolve to an active CFG_ row."""


def suggest(unknown: str, candidates: list[str], *, limit: int = 3) -> list[str]:
    """Rank `candidates` by similarity to `unknown`, for a "did you mean" hint.

    [ADDITION, 2026-09-20] Per explicit instruction to add fuzzy matching.
    Uses difflib from the standard library rather than a dependency: a CLI
    suggestion needs to be roughly right and instant, and rapidfuzz-grade
    scoring would buy nothing a user could notice. (The generated docs site is
    the other half of that instruction, and does use a real fuzzy library —
    see docs_generator.py — because ranking hundreds of entries as you type is
    a genuinely different problem.)

    The cutoff is deliberately generous: a typo is usually one or two
    characters, and a wrong suggestion costs a glance, while no suggestion
    costs a round trip through `etl-craft list`. Case-insensitive, since
    CODE-style identifiers are routinely typed in the wrong case.
    """
    folded = {candidate.lower(): candidate for candidate in candidates}
    matches = difflib.get_close_matches(unknown.lower(), list(folded), n=limit, cutoff=0.5)
    ranked = [folded[m] for m in matches]
    # A prefix match is what a half-typed code looks like, and difflib ranks
    # those poorly when the candidate is much longer than the input.
    for candidate in candidates:
        if candidate.lower().startswith(unknown.lower()) and candidate not in ranked:
            ranked.append(candidate)
    return ranked[:limit]


def _with_suggestions(message: str, unknown: str, candidates: list[str]) -> str:
    hints = suggest(unknown, candidates)
    if not hints:
        return message
    return message + " — did you mean: " + ", ".join(hints)


def resolve_pipeline_id(conn: Connection, pipeline_code: str) -> int:
    """Resolve an active PIPELINE_CODE to its PIPELINE_ID."""
    pipeline_id = conn.execute(
        text(
            "SELECT PIPELINE_ID FROM CFG_PIPELINES "
            "WHERE PIPELINE_CODE = :pipeline_code AND ACTIVE_FLAG = 'Y'"
        ),
        {"pipeline_code": pipeline_code},
    ).scalar_one_or_none()
    if pipeline_id is None:
        known = (
            conn.execute(text("SELECT PIPELINE_CODE FROM CFG_PIPELINES WHERE ACTIVE_FLAG = 'Y'"))
            .scalars()
            .all()
        )
        raise CfgError(
            _with_suggestions(
                f"no active pipeline with PIPELINE_CODE={pipeline_code!r}",
                pipeline_code,
                list(known),
            )
        )
    return pipeline_id


def resolve_task_id(conn: Connection, pipeline_id: int, task_code: str) -> int:
    """Resolve an active TASK_CODE (scoped to `pipeline_id`) to its TASK_ID."""
    task_id = conn.execute(
        text(
            "SELECT TASK_ID FROM CFG_TASKS "
            "WHERE PIPELINE_ID = :pipeline_id AND TASK_CODE = :task_code AND ACTIVE_FLAG = 'Y'"
        ),
        {"pipeline_id": pipeline_id, "task_code": task_code},
    ).scalar_one_or_none()
    if task_id is None:
        known = (
            conn.execute(
                text(
                    "SELECT TASK_CODE FROM CFG_TASKS "
                    "WHERE PIPELINE_ID = :pipeline_id AND ACTIVE_FLAG = 'Y'"
                ),
                {"pipeline_id": pipeline_id},
            )
            .scalars()
            .all()
        )
        raise CfgError(
            _with_suggestions(
                f"no active task with TASK_CODE={task_code!r} under pipeline_id={pipeline_id}",
                task_code,
                list(known),
            )
        )
    return task_id


@dataclass(frozen=True)
class TaskExecutionDetail:
    """Everything handlers.dispatch() needs about a task beyond its HANDLER value.

    [DEVIATION, post-signoff 2026-09-20] Used to also carry SCHEMA_EVOLUTION/
    SCRIPT_NAME/RETURN_VALUES, each fetched from its own CFG_TASKS column.
    All three moved to CFG_TASK_PARAMETERS (see that table's own comment in
    schema.sql) — handler modules now read them via `ctx.task_params.get(...)`
    like any other parameter, so there's nothing left for this dataclass to
    carry beyond what's true of every task regardless of HANDLER.
    """

    handler: str
    task_code: str
    pipeline_code: str
    refresh_type: str


def fetch_task_execution_detail(conn: Connection, task_id: int) -> TaskExecutionDetail:
    """Fetch `task_id`'s full execution context (assumed to already be a valid, active task)."""
    row = conn.execute(
        text(
            "SELECT t.HANDLER AS handler, t.TASK_CODE AS task_code, "
            "p.PIPELINE_CODE AS pipeline_code, p.REFRESH_TYPE AS refresh_type "
            "FROM CFG_TASKS t JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID "
            "WHERE t.TASK_ID = :task_id"
        ),
        {"task_id": task_id},
    ).one()
    return TaskExecutionDetail(
        handler=row.handler,
        task_code=row.task_code,
        pipeline_code=row.pipeline_code,
        refresh_type=row.refresh_type,
    )


def fetch_task_parameters(conn: Connection, task_id: int) -> dict[str, str]:
    """Fetch `task_id`'s active CFG_TASK_PARAMETERS as a PARAMETER_NAME -> PARAMETER_VALUE dict.

    Per sql_actions.py's own module docstring, PARAMETER_NAME is a closed,
    engine-interpreted vocabulary for HANDLER=SQL tasks (SQL_ACTION,
    TARGET_OBJECT, SOURCE_SQL, MERGE_KEY, MERGE_COMPARE_COLUMNS,
    HARD_DELETE) — this function itself stays generic, same spirit as every
    other read here.
    """
    rows = conn.execute(
        text(
            "SELECT PARAMETER_NAME AS parameter_name, PARAMETER_VALUE AS parameter_value "
            "FROM CFG_TASK_PARAMETERS WHERE TASK_ID = :task_id AND ACTIVE_FLAG = 'Y'"
        ),
        {"task_id": task_id},
    ).all()
    return {row.parameter_name: row.parameter_value for row in rows}


@dataclass(frozen=True)
class SiblingTargetWriter:
    """Another active SQL task in the same pipeline that writes a given TARGET_OBJECT."""

    task_id: int
    sql_action: str


def fetch_sibling_target_writer(
    conn: Connection, pipeline_id: int, task_id: int, target_object: str
) -> SiblingTargetWriter | None:
    """Find another active SQL task in `pipeline_id` that writes the same TARGET_OBJECT.

    [ADDITION] Backs two things in sql_actions.py: SETUP_TABLE's "infer
    audit columns from the rest of the pipeline where a task writes to it"
    (a SETUP_TABLE task establishes a target's *shape* ahead of the real
    writer, so its audit-column set must mirror whatever that real writer's
    own SQL_ACTION would add) and DROP_TABLE's "only ever drop a table this
    pipeline itself created via CREATE_TABLE" guard. Excludes any other
    SETUP_TABLE sibling in both cases — it never adds audit columns of its
    own (nothing to infer), and it's never what DROP_TABLE looks for either
    (a SETUP_TABLE writer isn't a CREATE_TABLE one), so leaving one in the
    running could only ever produce a false result via the tie-break below.
    [CHOICE] If more than one sibling writes the same TARGET_OBJECT (a real
    but unusual config), the lowest TASK_ID wins — deterministic, not a
    conflict check; flagged rather than silently ambiguous.
    """
    row = conn.execute(
        text(
            "SELECT t.TASK_ID AS task_id, a.PARAMETER_VALUE AS sql_action "
            "FROM CFG_TASK_PARAMETERS target_param "
            "JOIN CFG_TASKS t ON t.TASK_ID = target_param.TASK_ID "
            "JOIN CFG_TASK_PARAMETERS a "
            "ON a.TASK_ID = t.TASK_ID AND a.PARAMETER_NAME = 'SQL_ACTION' "
            "WHERE t.PIPELINE_ID = :pipeline_id AND t.TASK_ID <> :task_id "
            "AND t.ACTIVE_FLAG = 'Y' AND t.HANDLER = 'SQL' "
            "AND target_param.ACTIVE_FLAG = 'Y' AND target_param.PARAMETER_NAME = 'TARGET_OBJECT' "
            "AND target_param.PARAMETER_VALUE = :target_object "
            "AND a.ACTIVE_FLAG = 'Y' AND a.PARAMETER_VALUE <> 'SETUP_TABLE' "
            "ORDER BY t.TASK_ID LIMIT 1"
        ),
        {"pipeline_id": pipeline_id, "task_id": task_id, "target_object": target_object},
    ).one_or_none()
    if row is None:
        return None
    return SiblingTargetWriter(task_id=row.task_id, sql_action=row.sql_action)


@dataclass(frozen=True)
class BusinessRuleDetail:
    """One active CFG_BUSINESS_RULES row, as business_rules.py needs it."""

    business_rule_id: int
    business_rule_name: str
    business_rule_sql: str
    business_rule_type: str
    business_rule_key_column: str
    target_table: str
    sequence_number: int


def fetch_business_rules_for_task(conn: Connection, task_id: int) -> list[BusinessRuleDetail]:
    """Fetch `task_id`'s active CFG_BUSINESS_RULES rows, ordered by SEQUENCE_NUMBER."""
    rows = conn.execute(
        text(
            "SELECT BUSINESS_RULE_ID AS business_rule_id, "
            "BUSINESS_RULE_NAME AS business_rule_name, "
            "BUSINESS_RULE_SQL AS business_rule_sql, BUSINESS_RULE_TYPE AS business_rule_type, "
            "BUSINESS_RULE_KEY_COLUMN AS business_rule_key_column, TARGET_TABLE AS target_table, "
            "SEQUENCE_NUMBER AS sequence_number "
            "FROM CFG_BUSINESS_RULES WHERE TASK_ID = :task_id AND ACTIVE_FLAG = 'Y' "
            "ORDER BY SEQUENCE_NUMBER, BUSINESS_RULE_ID"
        ),
        {"task_id": task_id},
    ).all()
    return [
        BusinessRuleDetail(
            business_rule_id=row.business_rule_id,
            business_rule_name=row.business_rule_name,
            business_rule_sql=row.business_rule_sql,
            business_rule_type=row.business_rule_type,
            business_rule_key_column=row.business_rule_key_column,
            target_table=row.target_table,
            sequence_number=row.sequence_number,
        )
        for row in rows
    ]


def fetch_task_codes(conn: Connection, pipeline_id: int) -> dict[int, str]:
    """Map TASK_ID -> TASK_CODE for every active task in `pipeline_id`."""
    rows = conn.execute(
        text(
            "SELECT TASK_ID AS task_id, TASK_CODE AS task_code FROM CFG_TASKS "
            "WHERE PIPELINE_ID = :pipeline_id AND ACTIVE_FLAG = 'Y'"
        ),
        {"pipeline_id": pipeline_id},
    ).all()
    return {row.task_id: row.task_code for row in rows}


@dataclass(frozen=True)
class PipelineGraphData:
    """The CFG_TASKS/CFG_TASK_DEPENDENCY rows resolver.build_graph needs for one pipeline."""

    tasks: list[TaskNode]
    same_pipeline_edges: list[TaskEdge]
    # Tasks with >=1 cross-pipeline dependency edge (DEPENDS_ON_PIPELINE_ID !=
    # this pipeline's own id) — excluded from same_pipeline_edges since the
    # resolver only handles same-pipeline structure (see resolver.py's own
    # module docstring). Surfaced here, not resolved: cross-pipeline edges
    # are the self-check/poll step's job, not built yet.
    cross_pipeline_task_ids: frozenset[int]


def fetch_pipeline_graph(conn: Connection, pipeline_id: int) -> PipelineGraphData:
    """Fetch active tasks and same-pipeline dependency edges for `pipeline_id`."""
    edge_rows = conn.execute(
        text(
            "SELECT TASK_ID AS task_id, DEPENDS_ON_TASK_ID AS depends_on_task_id, "
            "DEPENDS_ON_PIPELINE_ID AS depends_on_pipeline_id, "
            "DEPENDENCY_TYPE AS dependency_type "
            "FROM CFG_TASK_DEPENDENCY WHERE PIPELINE_ID = :pipeline_id AND ACTIVE_FLAG = 'Y'"
        ),
        {"pipeline_id": pipeline_id},
    ).all()
    same_pipeline_edges = [
        TaskEdge(
            task_id=row.task_id,
            depends_on_task_id=row.depends_on_task_id,
            dependency_type=row.dependency_type,
        )
        for row in edge_rows
        if row.depends_on_pipeline_id == pipeline_id
    ]
    cross_pipeline_task_ids = frozenset(
        row.task_id for row in edge_rows if row.depends_on_pipeline_id != pipeline_id
    )
    # [ADDITION, 2026-09-20, E2-44/E2-45] The cross-pipeline edges themselves
    # stay out of the graph — the resolver has no way to settle them — but
    # RUN_CONDITION ranges over every one of a task's dependencies, so their
    # *count* has to reach the resolver. Without it, "ANY" meant "any
    # same-pipeline edge and every cross-pipeline one", and "N" rejected
    # perfectly valid configs as impossible.
    cross_pipeline_edge_counts: dict[int, int] = {}
    for row in edge_rows:
        if row.depends_on_pipeline_id != pipeline_id:
            cross_pipeline_edge_counts[row.task_id] = (
                cross_pipeline_edge_counts.get(row.task_id, 0) + 1
            )

    task_rows = conn.execute(
        text(
            "SELECT TASK_ID AS task_id, RUN_CONDITION AS run_condition, "
            "RUN_CONDITION_COUNT AS run_condition_count FROM CFG_TASKS "
            "WHERE PIPELINE_ID = :pipeline_id AND ACTIVE_FLAG = 'Y'"
        ),
        {"pipeline_id": pipeline_id},
    ).all()
    tasks = [
        TaskNode(
            task_id=row.task_id,
            run_condition=row.run_condition,
            run_condition_count=row.run_condition_count,
            cross_pipeline_edge_count=cross_pipeline_edge_counts.get(row.task_id, 0),
        )
        for row in task_rows
    ]

    return PipelineGraphData(
        tasks=tasks,
        same_pipeline_edges=same_pipeline_edges,
        cross_pipeline_task_ids=cross_pipeline_task_ids,
    )


@dataclass(frozen=True)
class PipelineSummary:
    """One row of `list`'s output."""

    pipeline_code: str
    pipeline_name: str
    refresh_type: str


def fetch_all_pipelines(conn: Connection) -> list[PipelineSummary]:
    """List every active pipeline, ordered by PIPELINE_CODE."""
    rows = conn.execute(
        text(
            "SELECT PIPELINE_CODE AS pipeline_code, PIPELINE_NAME AS pipeline_name, "
            "REFRESH_TYPE AS refresh_type FROM CFG_PIPELINES "
            "WHERE ACTIVE_FLAG = 'Y' ORDER BY PIPELINE_CODE"
        )
    ).all()
    return [
        PipelineSummary(
            pipeline_code=row.pipeline_code,
            pipeline_name=row.pipeline_name,
            refresh_type=row.refresh_type,
        )
        for row in rows
    ]


@dataclass(frozen=True)
class PipelineDependencyEdge:
    """One CFG_PIPELINE_DEPENDENCY row, resolved to codes for display."""

    depends_on_pipeline_code: str
    dependency_type: str


def fetch_pipeline_dependencies(conn: Connection, pipeline_id: int) -> list[PipelineDependencyEdge]:
    """Fetch `pipeline_id`'s own active cross-pipeline dependency edges."""
    rows = conn.execute(
        text(
            "SELECT p.PIPELINE_CODE AS depends_on_pipeline_code, "
            "d.DEPENDENCY_TYPE AS dependency_type "
            "FROM CFG_PIPELINE_DEPENDENCY d "
            "JOIN CFG_PIPELINES p ON p.PIPELINE_ID = d.DEPENDS_ON_PIPELINE_ID "
            "WHERE d.PIPELINE_ID = :pipeline_id AND d.ACTIVE_FLAG = 'Y'"
        ),
        {"pipeline_id": pipeline_id},
    ).all()
    return [
        PipelineDependencyEdge(
            depends_on_pipeline_code=row.depends_on_pipeline_code,
            dependency_type=row.dependency_type,
        )
        for row in rows
    ]


@dataclass(frozen=True)
class GlobalPipelineDependencyEdge:
    """One active CFG_PIPELINE_DEPENDENCY row, both sides resolved to codes — for the global DAG."""

    pipeline_code: str
    depends_on_pipeline_code: str
    dependency_type: str


def fetch_all_pipeline_dependency_edges(conn: Connection) -> list[GlobalPipelineDependencyEdge]:
    """Fetch every active CFG_PIPELINE_DEPENDENCY edge, across all pipelines, for the global DAG."""
    rows = conn.execute(
        text(
            "SELECT p.PIPELINE_CODE AS pipeline_code, "
            "dp.PIPELINE_CODE AS depends_on_pipeline_code, "
            "d.DEPENDENCY_TYPE AS dependency_type "
            "FROM CFG_PIPELINE_DEPENDENCY d "
            "JOIN CFG_PIPELINES p ON p.PIPELINE_ID = d.PIPELINE_ID "
            "JOIN CFG_PIPELINES dp ON dp.PIPELINE_ID = d.DEPENDS_ON_PIPELINE_ID "
            "WHERE d.ACTIVE_FLAG = 'Y' AND p.ACTIVE_FLAG = 'Y' AND dp.ACTIVE_FLAG = 'Y'"
        )
    ).all()
    return [
        GlobalPipelineDependencyEdge(
            pipeline_code=row.pipeline_code,
            depends_on_pipeline_code=row.depends_on_pipeline_code,
            dependency_type=row.dependency_type,
        )
        for row in rows
    ]


@dataclass(frozen=True)
class CrossPipelineTaskEdge:
    """One task-level CFG_TASK_DEPENDENCY row that crosses pipelines, resolved to codes."""

    task_code: str
    depends_on_pipeline_code: str
    depends_on_task_code: str
    dependency_type: str


def fetch_cross_pipeline_task_edges(
    conn: Connection, pipeline_id: int
) -> list[CrossPipelineTaskEdge]:
    """Fetch `pipeline_id`'s active task-level edges that depend on another pipeline's task."""
    rows = conn.execute(
        text(
            "SELECT t.TASK_CODE AS task_code, p.PIPELINE_CODE AS depends_on_pipeline_code, "
            "dt.TASK_CODE AS depends_on_task_code, d.DEPENDENCY_TYPE AS dependency_type "
            "FROM CFG_TASK_DEPENDENCY d "
            "JOIN CFG_TASKS t ON t.TASK_ID = d.TASK_ID "
            "JOIN CFG_PIPELINES p ON p.PIPELINE_ID = d.DEPENDS_ON_PIPELINE_ID "
            "JOIN CFG_TASKS dt ON dt.TASK_ID = d.DEPENDS_ON_TASK_ID "
            "WHERE d.PIPELINE_ID = :pipeline_id AND d.ACTIVE_FLAG = 'Y' "
            "AND d.DEPENDS_ON_PIPELINE_ID <> :pipeline_id"
        ),
        {"pipeline_id": pipeline_id},
    ).all()
    return [
        CrossPipelineTaskEdge(
            task_code=row.task_code,
            depends_on_pipeline_code=row.depends_on_pipeline_code,
            depends_on_task_code=row.depends_on_task_code,
            dependency_type=row.dependency_type,
        )
        for row in rows
    ]


@dataclass(frozen=True)
class PipelineDetail:
    """The CFG_PIPELINES fields `generate-yml` needs beyond what `list`/`graph` already fetch."""

    pipeline_code: str
    pipeline_name: str
    description: str | None
    run_schedule: str | None
    sla_in_hours: float | None
    refresh_type: str
    created_by: str | None
    # Per-pipeline overrides for generate-yml's Airflow-facing fields — all
    # None when not set at this pipeline, in which case generate-yml falls
    # back to craft-connector.yml's [Orchestrator] section, then a final
    # hardcoded default. See config.OrchestratorConfig's own docstring.
    catchup: bool | None
    tags: list[str] | None
    retries: int | None
    retry_delay_minutes: int | None
    depends_on_past: bool | None
    email_on_failure: bool | None
    email_recipients: list[str] | None


def fetch_pipeline_detail(conn: Connection, pipeline_id: int) -> PipelineDetail:
    """Fetch `pipeline_id`'s full CFG_PIPELINES row (assumed to already be a valid, active id).

    [DEVIATION, post-signoff 2026-09-20] The seven Airflow-facing override
    fields used to be their own CFG_PIPELINES columns; now read out of the
    single PIPELINE_PARAMETERS JSONB column instead (psycopg3 already
    deserializes JSONB into a plain dict — no json.loads needed). Kept as
    individual typed fields on PipelineDetail itself, unchanged, so
    generate_yml.py — the only consumer — needed no changes at all.
    """
    row = conn.execute(
        text(
            "SELECT PIPELINE_CODE AS pipeline_code, PIPELINE_NAME AS pipeline_name, "
            "DESCRIPTION AS description, RUN_SCHEDULE AS run_schedule, "
            "SLA_IN_HOURS AS sla_in_hours, REFRESH_TYPE AS refresh_type, "
            "CREATED_BY AS created_by, PIPELINE_PARAMETERS AS pipeline_parameters "
            "FROM CFG_PIPELINES WHERE PIPELINE_ID = :pipeline_id"
        ),
        {"pipeline_id": pipeline_id},
    ).one()
    params = row.pipeline_parameters or {}
    return PipelineDetail(
        pipeline_code=row.pipeline_code,
        pipeline_name=row.pipeline_name,
        description=row.description,
        run_schedule=row.run_schedule,
        # NUMERIC comes back as decimal.Decimal, which yaml.safe_dump can't
        # represent — this module exists specifically to feed YAML output,
        # so convert here rather than push that concern onto every caller.
        sla_in_hours=float(row.sla_in_hours) if row.sla_in_hours is not None else None,
        refresh_type=row.refresh_type,
        created_by=row.created_by,
        catchup=params.get("CATCHUP"),
        tags=params.get("TAGS"),
        retries=params.get("RETRIES"),
        retry_delay_minutes=params.get("RETRY_DELAY_MINUTES"),
        depends_on_past=params.get("DEPENDS_ON_PAST"),
        email_on_failure=params.get("EMAIL_ON_FAILURE"),
        email_recipients=params.get("EMAIL_RECIPIENTS"),
    )


@dataclass(frozen=True)
class BusinessRuleTarget:
    """One active CFG_BUSINESS_RULES row's warehouse target — what `validate`'s PK check needs."""

    business_rule_name: str
    target_table: str
    key_column: str


def fetch_business_rule_targets(conn: Connection) -> list[BusinessRuleTarget]:
    """Fetch every active business rule's TARGET_TABLE/BUSINESS_RULE_KEY_COLUMN, all pipelines."""
    rows = conn.execute(
        text(
            "SELECT BUSINESS_RULE_NAME AS business_rule_name, TARGET_TABLE AS target_table, "
            "BUSINESS_RULE_KEY_COLUMN AS key_column FROM CFG_BUSINESS_RULES "
            "WHERE ACTIVE_FLAG = 'Y' ORDER BY BUSINESS_RULE_NAME"
        )
    ).all()
    return [
        BusinessRuleTarget(
            business_rule_name=row.business_rule_name,
            target_table=row.target_table,
            key_column=row.key_column,
        )
        for row in rows
    ]


@dataclass(frozen=True)
class PipelineDependencyEdgeId:
    """One active CFG_PIPELINE_DEPENDENCY row, by id — what crosspipe.py's polling needs."""

    pipeline_dependency_id: int
    depends_on_pipeline_id: int
    dependency_type: str


def fetch_pipeline_dependency_edge_ids(
    conn: Connection, pipeline_id: int
) -> list[PipelineDependencyEdgeId]:
    """Fetch `pipeline_id`'s own active CFG_PIPELINE_DEPENDENCY edges, by id."""
    rows = conn.execute(
        text(
            "SELECT PIPELINE_DEPENDENCY_ID AS pipeline_dependency_id, "
            "DEPENDS_ON_PIPELINE_ID AS depends_on_pipeline_id, "
            "DEPENDENCY_TYPE AS dependency_type "
            "FROM CFG_PIPELINE_DEPENDENCY WHERE PIPELINE_ID = :pipeline_id AND ACTIVE_FLAG = 'Y'"
        ),
        {"pipeline_id": pipeline_id},
    ).all()
    return [
        PipelineDependencyEdgeId(
            pipeline_dependency_id=row.pipeline_dependency_id,
            depends_on_pipeline_id=row.depends_on_pipeline_id,
            dependency_type=row.dependency_type,
        )
        for row in rows
    ]


@dataclass(frozen=True)
class TaskCrossPipelineDependencyId:
    """One active, genuinely cross-pipeline CFG_TASK_DEPENDENCY row, by id."""

    task_dependency_id: int
    pipeline_id: int
    depends_on_pipeline_id: int
    depends_on_task_id: int
    dependency_type: str


def fetch_task_cross_pipeline_dependency_ids(
    conn: Connection, task_id: int
) -> list[TaskCrossPipelineDependencyId]:
    """Fetch `task_id`'s own active cross-pipeline CFG_TASK_DEPENDENCY edges, by id."""
    rows = conn.execute(
        text(
            "SELECT TASK_DEPENDENCY_ID AS task_dependency_id, PIPELINE_ID AS pipeline_id, "
            "DEPENDS_ON_PIPELINE_ID AS depends_on_pipeline_id, "
            "DEPENDS_ON_TASK_ID AS depends_on_task_id, DEPENDENCY_TYPE AS dependency_type "
            "FROM CFG_TASK_DEPENDENCY t "
            "WHERE TASK_ID = :task_id AND ACTIVE_FLAG = 'Y' "
            "AND DEPENDS_ON_PIPELINE_ID <> PIPELINE_ID"
        ),
        {"task_id": task_id},
    ).all()
    return [
        TaskCrossPipelineDependencyId(
            task_dependency_id=row.task_dependency_id,
            pipeline_id=row.pipeline_id,
            depends_on_pipeline_id=row.depends_on_pipeline_id,
            depends_on_task_id=row.depends_on_task_id,
            dependency_type=row.dependency_type,
        )
        for row in rows
    ]


# [ADDITION] Lineage convention, per explicit instruction ("every task
# should have atleast 1 source_table and target_table" / "one of our goals
# to maintain traceability"): every active task, regardless of HANDLER,
# declares SOURCE_OBJECT and TARGET_OBJECT in CFG_TASK_PARAMETERS —
# pipe-separated for tasks with more than one of either (an ingestion
# script pulling from several sources, a BUSINESS_RULES task whose rules
# target different tables). This is purely declarative bookkeeping for
# lineage: HANDLER=SQL already has its own real, functionally-load-bearing
# TARGET_OBJECT (sql_actions.py); HANDLER=BUSINESS_RULES already has a real
# per-rule TARGET_TABLE (CFG_BUSINESS_RULES); this convention doesn't
# replace either — validate.py checks it's present, fetch_table_lineage
# below is the only thing that reads it back.
LINEAGE_SOURCE_PARAM = "SOURCE_OBJECT"
LINEAGE_TARGET_PARAM = "TARGET_OBJECT"


@dataclass(frozen=True)
class TaskLineageGap:
    """One active task missing SOURCE_OBJECT and/or TARGET_OBJECT — a validate.py finding."""

    pipeline_code: str
    task_code: str
    missing: tuple[str, ...]


def fetch_tasks_missing_source_or_target(conn: Connection) -> list[TaskLineageGap]:
    """Find every active task (any HANDLER) missing SOURCE_OBJECT and/or TARGET_OBJECT.

    [Bug caught and fixed before shipping]: a task with *no* active
    CFG_TASK_PARAMETERS rows at all makes the LEFT JOIN produce a single
    all-NULL row for it — `bool_or(NULL = 'SOURCE_OBJECT')` over that is
    `NULL`, not FALSE (bool_or ignores NULL inputs and only returns FALSE
    when it saw a real FALSE), so an un-COALESCEd `NOT bool_or(...)` in the
    HAVING clause evaluates to NULL, which HAVING treats as "excluded" —
    the exact tasks this check most needs to catch (declaring nothing at
    all) silently vanished from the result. Reproduced directly against
    Postgres (`SELECT bool_or(NULL::boolean)` -> NULL) before fixing.
    """
    rows = conn.execute(
        text(
            "SELECT p.PIPELINE_CODE AS pipeline_code, t.TASK_CODE AS task_code, "
            "COALESCE(bool_or(tp.PARAMETER_NAME = 'SOURCE_OBJECT'), FALSE) AS has_source, "
            "COALESCE(bool_or(tp.PARAMETER_NAME = 'TARGET_OBJECT'), FALSE) AS has_target "
            "FROM CFG_TASKS t "
            "JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID "
            "LEFT JOIN CFG_TASK_PARAMETERS tp ON tp.TASK_ID = t.TASK_ID AND tp.ACTIVE_FLAG = 'Y' "
            "WHERE t.ACTIVE_FLAG = 'Y' AND p.ACTIVE_FLAG = 'Y' "
            "GROUP BY t.TASK_ID, p.PIPELINE_CODE, t.TASK_CODE "
            "HAVING NOT COALESCE(bool_or(tp.PARAMETER_NAME = 'SOURCE_OBJECT'), FALSE) "
            "OR NOT COALESCE(bool_or(tp.PARAMETER_NAME = 'TARGET_OBJECT'), FALSE) "
            "ORDER BY p.PIPELINE_CODE, t.TASK_CODE"
        )
    ).all()
    gaps = []
    for row in rows:
        missing = tuple(
            name
            for name, present in (
                (LINEAGE_SOURCE_PARAM, row.has_source),
                (LINEAGE_TARGET_PARAM, row.has_target),
            )
            if not present
        )
        gaps.append(
            TaskLineageGap(
                pipeline_code=row.pipeline_code, task_code=row.task_code, missing=missing
            )
        )
    return gaps


# [ADDITION, 2026-09-20, E2-25] Every PARAMETER_NAME any handler actually
# reads. A name outside this set is silently ignored at runtime, which makes a
# typo ("MEREG_KEY") behave exactly like forgetting the parameter — so
# `validate` flags it instead.
KNOWN_PARAMETERS = frozenset(
    {
        # every task
        "SOURCE_OBJECT",
        "TARGET_OBJECT",
        "DOCUMENTATION",
        "TASK_TIMEOUT_SECONDS",
        # HANDLER=SQL
        "SQL_ACTION",
        "SOURCE_SQL",
        "MERGE_KEY",
        "MERGE_COMPARE_COLUMNS",
        "MERGE_DEDUPE_ORDER",
        "PRESERVE_TARGET",
        "SCHEMA_EVOLUTION",
        "HARD_DELETE",
        # HANDLER=SQL, Snowflake Iceberg targets only. Snowflake cannot
        # express Iceberg as a clause the way Databricks can -- it needs
        # CREATE ICEBERG TABLE plus storage that is deployment-specific, so
        # these name it. See sql_actions.ICEBERG_CREATE_PREFIX.
        "EXTERNAL_VOLUME",
        "BASE_LOCATION",
        # iceberg | native. Overrides [Warehouse].Table_format for one target.
        "TABLE_FORMAT",
        # HANDLER=PYTHON
        "SCRIPT_NAME",
        "RETURN_VALUES",
        # HANDLER=EMAIL_ALERT
        "EMAIL_TO",
        "EMAIL_SUBJECT",
        "EMAIL_BODY",
        "EMAIL_SUBJECT_SUCCESS",
        "EMAIL_BODY_SUCCESS",
        "EMAIL_SUBJECT_COMPLETED_WITH_ERRORS",
        "EMAIL_BODY_COMPLETED_WITH_ERRORS",
        "EMAIL_SUBJECT_FAILED",
        "EMAIL_BODY_FAILED",
        "EMAIL_ON_STATUS",
        "EMAIL_PIPELINES",
    }
)


@dataclass(frozen=True)
class TaskWithParameters:
    """One active task and every parameter it declares — what `validate` checks."""

    pipeline_code: str
    task_code: str
    handler: str
    parameters: dict[str, str]


def fetch_tasks_with_parameters(conn: Connection) -> list[TaskWithParameters]:
    """Fetch every active task across every pipeline, with its own parameters."""
    rows = conn.execute(
        text(
            "SELECT p.PIPELINE_CODE AS pipeline_code, t.TASK_CODE AS task_code, "
            "t.HANDLER AS handler, par.PARAMETER_NAME AS parameter_name, "
            "par.PARAMETER_VALUE AS parameter_value "
            "FROM CFG_TASKS t "
            "JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID "
            "LEFT JOIN CFG_TASK_PARAMETERS par "
            "ON par.TASK_ID = t.TASK_ID AND par.ACTIVE_FLAG = 'Y' "
            "WHERE t.ACTIVE_FLAG = 'Y' AND p.ACTIVE_FLAG = 'Y' "
            "ORDER BY p.PIPELINE_CODE, t.TASK_CODE"
        )
    ).all()
    grouped: dict[tuple[str, str, str], dict[str, str]] = {}
    for row in rows:
        key = (row.pipeline_code, row.task_code, row.handler)
        params = grouped.setdefault(key, {})
        if row.parameter_name is not None:
            params[row.parameter_name] = row.parameter_value
    return [
        TaskWithParameters(pipeline_code=code, task_code=task, handler=handler, parameters=params)
        for (code, task, handler), params in grouped.items()
    ]


@dataclass(frozen=True)
class DependencyEdgeDetail:
    """One same-pipeline edge with both ends' handlers — what `validate` needs to judge it."""

    task_code: str
    handler: str
    depends_on_task_code: str
    depends_on_handler: str
    dependency_type: str


def fetch_dependency_edge_detail(conn: Connection, pipeline_id: int) -> list[DependencyEdgeDetail]:
    """Fetch this pipeline's active same-pipeline edges, with the handler at each end."""
    rows = conn.execute(
        text(
            "SELECT t.TASK_CODE AS task_code, t.HANDLER AS handler, "
            "u.TASK_CODE AS depends_on_task_code, u.HANDLER AS depends_on_handler, "
            "d.DEPENDENCY_TYPE AS dependency_type "
            "FROM CFG_TASK_DEPENDENCY d "
            "JOIN CFG_TASKS t ON t.TASK_ID = d.TASK_ID "
            "JOIN CFG_TASKS u ON u.TASK_ID = d.DEPENDS_ON_TASK_ID "
            "WHERE d.PIPELINE_ID = :pipeline_id AND d.ACTIVE_FLAG = 'Y' "
            "AND d.DEPENDS_ON_PIPELINE_ID = :pipeline_id "
            "AND t.ACTIVE_FLAG = 'Y' AND u.ACTIVE_FLAG = 'Y'"
        ),
        {"pipeline_id": pipeline_id},
    ).all()
    return [
        DependencyEdgeDetail(
            task_code=r.task_code,
            handler=r.handler,
            depends_on_task_code=r.depends_on_task_code,
            depends_on_handler=r.depends_on_handler,
            dependency_type=r.dependency_type,
        )
        for r in rows
    ]


@dataclass(frozen=True)
class SqlSnippet:
    """One author-supplied SQL string, with enough context to name it in a report.

    [ADDITION, 2026-09-20, E2-07] Backs validate.validate_read_only_sql, which
    lints every SOURCE_SQL and BUSINESS_RULE_SQL for statements a read-only
    SELECT has no business containing.
    """

    pipeline_code: str
    task_code: str
    parameter_name: str
    sql: str


def fetch_sql_snippets(conn: Connection) -> list[SqlSnippet]:
    """Fetch every active task's SOURCE_SQL and every active rule's BUSINESS_RULE_SQL."""
    rows = conn.execute(
        text(
            "SELECT p.PIPELINE_CODE AS pipeline_code, t.TASK_CODE AS task_code, "
            "par.PARAMETER_NAME AS parameter_name, par.PARAMETER_VALUE AS sql_text "
            "FROM CFG_TASK_PARAMETERS par "
            "JOIN CFG_TASKS t ON t.TASK_ID = par.TASK_ID "
            "JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID "
            "WHERE par.PARAMETER_NAME = 'SOURCE_SQL' AND par.ACTIVE_FLAG = 'Y' "
            "AND t.ACTIVE_FLAG = 'Y' AND p.ACTIVE_FLAG = 'Y' "
            "UNION ALL "
            "SELECT p.PIPELINE_CODE, t.TASK_CODE, "
            "'BUSINESS_RULE_SQL (' || br.BUSINESS_RULE_NAME || ')', br.BUSINESS_RULE_SQL "
            "FROM CFG_BUSINESS_RULES br "
            "JOIN CFG_TASKS t ON t.TASK_ID = br.TASK_ID "
            "JOIN CFG_PIPELINES p ON p.PIPELINE_ID = br.PIPELINE_ID "
            "WHERE br.ACTIVE_FLAG = 'Y' AND t.ACTIVE_FLAG = 'Y' AND p.ACTIVE_FLAG = 'Y'"
        )
    ).all()
    return [
        SqlSnippet(
            pipeline_code=row.pipeline_code,
            task_code=row.task_code,
            parameter_name=row.parameter_name,
            sql=row.sql_text,
        )
        for row in rows
    ]


@dataclass(frozen=True)
class TableLineageEntry:
    """One task that reads (SOURCE_OBJECT) or writes (TARGET_OBJECT) a given table."""

    pipeline_code: str
    task_code: str
    role: str  # "SOURCE" or "TARGET"


@dataclass(frozen=True)
class PipelineStep:
    """One active task in a pipeline, with its own declared parameters — the `steps` CLI's data.

    [ADDITION] Closes CLAUDE.md's "steps-in-a-pipeline" read-only query verb
    (listed under CLI surface as "conceptually agreed but not yet named or
    built"). Deliberately generic — dumps whatever CFG_TASK_PARAMETERS a task
    declares rather than special-casing per HANDLER, the same "one canonical
    read path, no bespoke per-handler formatting logic in the read layer"
    spirit as fetch_table_lineage above.
    """

    task_code: str
    handler: str
    parameters: dict[str, str]


def fetch_pipeline_steps(conn: Connection, pipeline_id: int) -> list[PipelineStep]:
    """Fetch every active task in `pipeline_id`, each with its own active CFG_TASK_PARAMETERS."""
    task_rows = conn.execute(
        text(
            "SELECT TASK_ID AS task_id, TASK_CODE AS task_code, HANDLER AS handler "
            "FROM CFG_TASKS WHERE PIPELINE_ID = :pipeline_id AND ACTIVE_FLAG = 'Y' "
            "ORDER BY TASK_CODE"
        ),
        {"pipeline_id": pipeline_id},
    ).all()
    return [
        PipelineStep(
            task_code=row.task_code,
            handler=row.handler,
            parameters=fetch_task_parameters(conn, row.task_id),
        )
        for row in task_rows
    ]


@dataclass(frozen=True)
class PipelineRunHistoryEntry:
    """One AUD_PIPELINES_RUN_LOG row — the `history` CLI's pipeline-level data."""

    pipeline_run_id: int
    status: str
    start_date: datetime
    end_date: datetime | None


def fetch_pipeline_run_history(
    conn: Connection, pipeline_id: int, limit: int = 20
) -> list[PipelineRunHistoryEntry]:
    """Fetch `pipeline_id`'s most recent runs, newest first.

    [ADDITION] Closes CLAUDE.md's "run history" read-only query verb.
    """
    rows = conn.execute(
        text(
            "SELECT PIPELINE_RUN_ID AS pipeline_run_id, STATUS AS status, "
            "START_DATE AS start_date, END_DATE AS end_date "
            "FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_ID = :pipeline_id "
            "ORDER BY START_DATE DESC LIMIT :limit"
        ),
        {"pipeline_id": pipeline_id, "limit": limit},
    ).all()
    return [
        PipelineRunHistoryEntry(
            pipeline_run_id=row.pipeline_run_id,
            status=row.status,
            start_date=row.start_date,
            end_date=row.end_date,
        )
        for row in rows
    ]


@dataclass(frozen=True)
class TaskRunHistoryEntry:
    """One AUD_TASK_RUN_LOG row — the `history --task_code` CLI's data."""

    pipeline_run_id: int
    status: str
    start_date: datetime
    end_date: datetime | None
    error_message: str | None


def fetch_task_run_history(
    conn: Connection, task_id: int, limit: int = 20
) -> list[TaskRunHistoryEntry]:
    """Fetch `task_id`'s most recent runs (across every pipeline_run_id it's bound to)."""
    rows = conn.execute(
        text(
            "SELECT PIPELINE_RUN_ID AS pipeline_run_id, STATUS AS status, "
            "START_DATE AS start_date, END_DATE AS end_date, ERROR_MESSAGE AS error_message "
            "FROM AUD_TASK_RUN_LOG WHERE TASK_ID = :task_id "
            "ORDER BY START_DATE DESC LIMIT :limit"
        ),
        {"task_id": task_id, "limit": limit},
    ).all()
    return [
        TaskRunHistoryEntry(
            pipeline_run_id=row.pipeline_run_id,
            status=row.status,
            start_date=row.start_date,
            end_date=row.end_date,
            error_message=row.error_message,
        )
        for row in rows
    ]


@dataclass(frozen=True)
class FailureWatchMessage:
    """One FAILURE-typed CFG_TASK_DEPENDENCY edge's watched task and its latest ERROR_MESSAGE.

    [ADDITION] Backs email_alert.py's $$error_message substitution token.
    Reads the *latest* logged AUD_TASK_RUN_LOG row for the watched task,
    regardless of pipeline_run_id — deliberately, since a watched task can be
    in another pipeline entirely (a cross-pipeline FAILURE edge), where there
    is no shared pipeline_run_id to match against in the first place. For the
    much more common same-pipeline case this is equivalent to matching on
    the current run, since a same-pipeline watched task's most recent row
    already belongs to it.
    """

    depends_on_task_code: str
    error_message: str | None


def fetch_failure_watch_messages(conn: Connection, task_id: int) -> list[FailureWatchMessage]:
    """Fetch every active FAILURE-typed dependency `task_id` watches, with its watched message."""
    rows = conn.execute(
        text(
            "SELECT dt.TASK_CODE AS depends_on_task_code, "
            "(SELECT l.ERROR_MESSAGE FROM AUD_TASK_RUN_LOG l WHERE l.TASK_ID = dt.TASK_ID "
            "ORDER BY l.START_DATE DESC LIMIT 1) AS error_message "
            "FROM CFG_TASK_DEPENDENCY d "
            "JOIN CFG_TASKS dt ON dt.TASK_ID = d.DEPENDS_ON_TASK_ID "
            "WHERE d.TASK_ID = :task_id AND d.ACTIVE_FLAG = 'Y' AND d.DEPENDENCY_TYPE = 'FAILURE' "
            "ORDER BY dt.TASK_CODE"
        ),
        {"task_id": task_id},
    ).all()
    return [
        FailureWatchMessage(
            depends_on_task_code=row.depends_on_task_code, error_message=row.error_message
        )
        for row in rows
    ]


@dataclass(frozen=True)
class TaskStatusEntry:
    """One active task's status under a specific pipeline_run_id — email_alert.py's digest data.

    [ADDITION] Backs the per-pipeline collapsible detail block in
    email_alert.py's EMAIL_PIPELINES status digest. A task with no
    AUD_TASK_RUN_LOG row at all under this run (never got a chance to run,
    e.g. gated off or the run stopped before reaching it) reports as
    "PENDING" rather than NULL/None — a real, renderable status, not an
    absence a template author has to special-case.
    """

    # [ADDITION, 2026-09-20, E2-43] task_id is carried so email_alert.py can
    # exclude the alerting task itself when computing a run's flavour — it is
    # necessarily IN-PROGRESS while it runs, so counting it would make every
    # run look unfinished.
    task_id: int
    task_code: str
    status: str
    error_message: str | None
    # [ADDITION, 2026-09-22, E2-77] The handler, so email_alert.py can exclude
    # *every* EMAIL_ALERT task from a run's flavour rather than only itself.
    # Several alerts per pipeline is a supported configuration -- validate's
    # own leaf check treats them as a set, and EMAIL_ON_STATUS exists so one
    # can go to ops on FAILED and another to stakeholders on SUCCESS -- and
    # they land in the same wave, so whichever ran first saw the other as
    # PENDING and reported a false COMPLETED_WITH_ERRORS.
    handler: str = ""
    # [ADDITION, 2026-09-20, E2-21] Lets email_alert distinguish "succeeded"
    # from "succeeded on the third try" — the case COMPLETED_WITH_ERRORS was
    # designed for but could not previously see.
    attempt_count: int = 1


def fetch_task_statuses_for_run(
    conn: Connection, pipeline_id: int, pipeline_run_id: int
) -> list[TaskStatusEntry]:
    """Fetch every active task's status under `pipeline_run_id` (PENDING if never bound)."""
    rows = conn.execute(
        text(
            "SELECT t.TASK_ID AS task_id, t.TASK_CODE AS task_code, t.HANDLER AS handler, "
            "l.STATUS AS status, l.ERROR_MESSAGE AS error_message, "
            "COALESCE(l.ATTEMPT_COUNT, 1) AS attempt_count "
            "FROM CFG_TASKS t LEFT JOIN AUD_TASK_RUN_LOG l "
            "ON l.TASK_ID = t.TASK_ID AND l.PIPELINE_RUN_ID = :pipeline_run_id "
            "WHERE t.PIPELINE_ID = :pipeline_id AND t.ACTIVE_FLAG = 'Y' "
            "ORDER BY t.TASK_CODE"
        ),
        {"pipeline_id": pipeline_id, "pipeline_run_id": pipeline_run_id},
    ).all()
    return [
        TaskStatusEntry(
            task_id=row.task_id,
            task_code=row.task_code,
            status=row.status or "PENDING",
            error_message=row.error_message,
            attempt_count=row.attempt_count,
            handler=row.handler,
        )
        for row in rows
    ]


def fetch_table_lineage(conn: Connection, table_ref: str) -> list[TableLineageEntry]:
    """Find every active task that declares `table_ref` as a SOURCE_OBJECT or TARGET_OBJECT.

    [CHOICE] Reads only CFG_TASK_PARAMETERS' own SOURCE_OBJECT/TARGET_OBJECT
    convention, not CFG_BUSINESS_RULES.TARGET_TABLE — keeping one canonical
    place lineage is read from, rather than two overlapping ones that could
    disagree for a BUSINESS_RULES task with several differently-targeted
    rules. Filtered in Python, not SQL: PARAMETER_VALUE can be a
    pipe-separated list, and building a single portable SQL predicate that
    correctly matches "one exact element of a pipe-separated list" (exact
    value, or a prefix/suffix/middle segment) is more fragile than just
    fetching the (small) candidate rows and splitting them here.
    """
    rows = conn.execute(
        text(
            "SELECT p.PIPELINE_CODE AS pipeline_code, t.TASK_CODE AS task_code, "
            "tp.PARAMETER_NAME AS parameter_name, tp.PARAMETER_VALUE AS parameter_value "
            "FROM CFG_TASK_PARAMETERS tp "
            "JOIN CFG_TASKS t ON t.TASK_ID = tp.TASK_ID "
            "JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID "
            "WHERE tp.ACTIVE_FLAG = 'Y' AND t.ACTIVE_FLAG = 'Y' AND p.ACTIVE_FLAG = 'Y' "
            "AND tp.PARAMETER_NAME IN ('SOURCE_OBJECT', 'TARGET_OBJECT')"
        )
    ).all()
    entries = []
    for row in rows:
        values = [v.strip() for v in row.parameter_value.split("|") if v.strip()]
        if table_ref in values:
            role = "SOURCE" if row.parameter_name == LINEAGE_SOURCE_PARAM else "TARGET"
            entries.append(
                TableLineageEntry(
                    pipeline_code=row.pipeline_code, task_code=row.task_code, role=role
                )
            )
    return sorted(entries, key=lambda e: (e.pipeline_code, e.task_code, e.role))
