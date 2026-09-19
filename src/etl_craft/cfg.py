"""Read-only queries against CFG_ tables needed by `run` and the resolver."""

# Writing CFG_ rows stays outside the CLI entirely, per CLAUDE.md's CLI
# surface section — pipeline/task/dependency registration is manual,
# git-managed migrations. Everything here is SELECT-only.

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.engine import Connection

from etl_craft.resolver import TaskEdge, TaskNode


class CfgError(Exception):
    """Raised when a --pipeline_code/--task_code doesn't resolve to an active CFG_ row."""


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
        raise CfgError(f"no active pipeline with PIPELINE_CODE={pipeline_code!r}")
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
        raise CfgError(
            f"no active task with TASK_CODE={task_code!r} under pipeline_id={pipeline_id}"
        )
    return task_id


def fetch_task_handler(conn: Connection, task_id: int) -> str:
    """Fetch the HANDLER value for `task_id` (assumed to already be a valid, active task)."""
    return conn.execute(
        text("SELECT HANDLER FROM CFG_TASKS WHERE TASK_ID = :task_id"),
        {"task_id": task_id},
    ).scalar_one()


@dataclass(frozen=True)
class TaskExecutionDetail:
    """Everything handlers.dispatch() needs about a task beyond its HANDLER value."""

    handler: str
    task_code: str
    pipeline_code: str
    refresh_type: str
    schema_evolution: bool
    script_name: str | None


def fetch_task_execution_detail(conn: Connection, task_id: int) -> TaskExecutionDetail:
    """Fetch `task_id`'s full execution context (assumed to already be a valid, active task)."""
    row = conn.execute(
        text(
            "SELECT t.HANDLER AS handler, t.TASK_CODE AS task_code, "
            "p.PIPELINE_CODE AS pipeline_code, p.REFRESH_TYPE AS refresh_type, "
            "t.SCHEMA_EVOLUTION AS schema_evolution, t.SCRIPT_NAME AS script_name "
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
        schema_evolution=row.schema_evolution,
        script_name=row.script_name,
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


def fetch_sibling_target_sql_action(
    conn: Connection, pipeline_id: int, task_id: int, target_object: str
) -> str | None:
    """Find another active SQL task in `pipeline_id` that writes the same TARGET_OBJECT.

    [ADDITION] Backs SETUP_TABLE's "infer audit columns from the rest of the
    pipeline where a task writes to it" behavior (sql_actions.py) — a
    SETUP_TABLE task establishes a target's *shape* ahead of the real writer,
    so its audit-column set must mirror whatever that real writer's own
    SQL_ACTION would add, not guess independently. Excludes `task_id` itself
    and any other SETUP_TABLE task (SETUP_TABLE never adds audit columns of
    its own — there is nothing useful to infer from another SETUP_TABLE).
    [CHOICE] If more than one sibling writes the same TARGET_OBJECT (a real
    but unusual config), the lowest TASK_ID wins — deterministic, not a
    conflict check; flagged rather than silently ambiguous.
    """
    return conn.execute(
        text(
            "SELECT a.PARAMETER_VALUE AS sql_action "
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
    ).scalar_one_or_none()


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
    task_rows = conn.execute(
        text(
            "SELECT TASK_ID AS task_id FROM CFG_TASKS "
            "WHERE PIPELINE_ID = :pipeline_id AND ACTIVE_FLAG = 'Y'"
        ),
        {"pipeline_id": pipeline_id},
    ).all()
    tasks = [TaskNode(task_id=row.task_id) for row in task_rows]

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
    """Fetch `pipeline_id`'s full CFG_PIPELINES row (assumed to already be a valid, active id)."""
    row = conn.execute(
        text(
            "SELECT PIPELINE_CODE AS pipeline_code, PIPELINE_NAME AS pipeline_name, "
            "DESCRIPTION AS description, RUN_SCHEDULE AS run_schedule, "
            "SLA_IN_HOURS AS sla_in_hours, REFRESH_TYPE AS refresh_type, "
            "CREATED_BY AS created_by, CATCHUP AS catchup, TAGS AS tags, "
            "RETRIES AS retries, RETRY_DELAY_MINUTES AS retry_delay_minutes, "
            "DEPENDS_ON_PAST AS depends_on_past, EMAIL_ON_FAILURE AS email_on_failure, "
            "EMAIL_RECIPIENTS AS email_recipients "
            "FROM CFG_PIPELINES WHERE PIPELINE_ID = :pipeline_id"
        ),
        {"pipeline_id": pipeline_id},
    ).one()
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
        catchup=row.catchup,
        tags=row.tags,
        retries=row.retries,
        retry_delay_minutes=row.retry_delay_minutes,
        depends_on_past=row.depends_on_past,
        email_on_failure=row.email_on_failure,
        email_recipients=row.email_recipients,
    )


@dataclass(frozen=True)
class BusinessRuleTarget:
    """One active CFG_BUSINESS_RULES row's Data DB target — what `validate`'s PK check needs."""

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
