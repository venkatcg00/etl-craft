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
    the warehouse, the same dialect-agnostic approach warehouse.py uses,
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

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import NoSuchTableError

from etl_craft.cfg import (
    KNOWN_PARAMETERS,
    DependencyEdgeDetail,
    TaskWithParameters,
    fetch_all_pipelines,
    fetch_business_rule_targets,
    fetch_dependency_edge_detail,
    fetch_pipeline_graph,
    fetch_sql_snippets,
    fetch_tasks_missing_source_or_target,
    fetch_tasks_with_parameters,
    resolve_pipeline_id,
)
from etl_craft.config import VALID_TABLE_FORMATS, ConnectorConfig
from etl_craft.resolver import ResolverError, build_graph
from etl_craft.sql_actions import ICEBERG_CREATE_PREFIX, ROW_ID_COLUMN, SNOWFLAKE_MANAGED_VOLUME
from etl_craft.warehouse import verify_iceberg_catalog


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


# Warehouses that genuinely enforce a primary key, and so can be introspected
# for one. Everything else in the supported set is a SQL engine over Iceberg,
# which has no constraint concept -- Databricks accepts primary keys only as
# unenforced informational metadata, and Trino/Iceberg rejects them outright.
ENFORCED_PRIMARY_KEY_DIALECTS = frozenset({"postgresql", "duckdb"})


def _primary_key_columns(
    warehouse_engine: Engine, inspector: Inspector, table: str, schema: str | None
) -> list[str]:
    """Return a table's primary-key columns, working around DuckDB's missing reflection.

    [ADDITION, 2026-09-21] `duckdb_engine` does not reflect primary keys:
    `Inspector.get_pk_constraint()` returns an empty `constrained_columns`
    list even for a table DuckDB is genuinely enforcing one on — verified
    directly, a duplicate insert fails at commit. Taken at face value that
    makes this whole check report *every* target on DuckDB as having no
    primary key, i.e. fail precisely where the convention is being honoured.

    The catalog knows, so ask it. Keyed off the dialect *name*, never an
    import, and only when reflection came back empty, so a future
    `duckdb_engine` that does reflect keys silently takes over.
    """
    pk = inspector.get_pk_constraint(table, schema=schema)
    columns = list(pk.get("constrained_columns") or [])
    if columns or warehouse_engine.dialect.name != "duckdb":
        return columns
    with warehouse_engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT constraint_column_names FROM duckdb_constraints() "
                "WHERE table_name = :table AND constraint_type = 'PRIMARY KEY' "
                "AND (CAST(:schema AS VARCHAR) IS NULL OR schema_name = :schema)"
            ),
            {"table": table, "schema": schema},
        ).first()
    return list(row[0]) if row else []


def validate_business_rule_keys(
    conn: Connection, warehouse_engine: Engine | None
) -> list[ValidationIssue]:
    """Check every active business rule's TARGET_TABLE has a single-column primary key.

    [DEVIATION, 2026-09-22] No longer requires BUSINESS_RULE_KEY_COLUMN to *be*
    that primary key. Since E2-54 made ROW_ID the primary key of every
    engine-created table, that equality check forced every business rule to
    key on ROW_ID -- which a full-replace action regenerates, so flags could
    never be deactivated. The engine was pushing users into the broken
    configuration and failing them for the correct one.

    CLAUDE.md's convention is that a target *has* a single-column primary key,
    "which is why BUSINESS_RULE_KEY_COLUMN can safely stay a single column
    rather than a list" -- it never said they had to be the same column. The
    key column must exist on the target; validate_business_rule_key_stability
    checks it is one that survives a run.
    """
    targets = fetch_business_rule_targets(conn)
    if not targets:
        return []
    if warehouse_engine is None:
        return [
            ValidationIssue(
                category="business_rule_pk",
                message=(
                    f"{len(targets)} active business rule(s) declare a TARGET_TABLE, but no "
                    "[Warehouse] is configured in craft-connector.yml to check them against"
                ),
            )
        ]

    # [ADDITION, 2026-09-22] Iceberg has no constraint concept at all -- no
    # primary keys to introspect -- so running this check against an
    # Iceberg-backed warehouse would report *every* target as failing, the
    # same false-failure class as duckdb's missing PK reflection below. The
    # engine still gives each target a single-column ROW_ID
    # (sql_actions._add_computed_surrogate_key), so the convention holds; what
    # does not hold is database *enforcement* of it. Say that, once, rather
    # than repeating a failure per rule.
    if warehouse_engine.dialect.name not in ENFORCED_PRIMARY_KEY_DIALECTS:
        return [
            ValidationIssue(
                category="business_rule_pk",
                message=(
                    f"{len(targets)} active business rule(s) checked against a "
                    f"{warehouse_engine.dialect.name} warehouse, which cannot enforce primary keys "
                    "(Iceberg has no constraint concept) — the engine still assigns each target "
                    "a single-column ROW_ID, but uniqueness is not database-enforced"
                ),
            )
        ]

    issues: list[ValidationIssue] = []
    inspector = inspect(warehouse_engine)
    for target in targets:
        schema, _, table = target.target_table.rpartition(".")
        try:
            columns = _primary_key_columns(warehouse_engine, inspector, table, schema or None)
        except NoSuchTableError:
            issues.append(
                ValidationIssue(
                    category="business_rule_pk",
                    message=f"{target.business_rule_name!r}: TARGET_TABLE "
                    f"{target.target_table!r} does not exist in the warehouse",
                )
            )
            continue
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
        # Relaxing "the key must BE the primary key" must not relax "the key
        # must be a real column" — a typo'd key column silently flags nothing.
        present = {c["name"].lower() for c in inspector.get_columns(table, schema=schema or None)}
        if present and target.key_column.strip().lower() not in present:
            issues.append(
                ValidationIssue(
                    category="business_rule_pk",
                    message=(
                        f"{target.business_rule_name!r}: BUSINESS_RULE_KEY_COLUMN="
                        f"{target.key_column!r} is not a column of {target.target_table!r}"
                    ),
                )
            )
    return issues


# SQL actions that replace a target's rows wholesale, and therefore regenerate
# every ROW_ID. An engine-generated key is not stable across these.
FULL_REPLACE_ACTIONS = frozenset({"CREATE_TABLE", "OVERWRITE_TABLE"})


def validate_business_rule_key_stability(conn: Connection) -> list[ValidationIssue]:
    """Check no business rule keys its results on a column its target regenerates.

    [ADDITION, 2026-09-22] AUD_BUSINESS_RULES_RESULTS records a flagged row by
    its BUSINESS_RULE_KEY_COLUMN value, and business_rules.py deactivates a
    flag only for keys the "no longer violates" query *returns* -- which means
    keys still present in the target. A key that did not survive the run is
    never deactivated.

    ROW_ID is regenerated by every full-replace action, so using it as the
    business-rule key breaks that, and the two warehouses break differently:

      * Postgres keeps counting, so the old key simply no longer exists. The
        flag stays active forever, pointing at nothing.
      * On Iceberg ROW_ID is computed as max-present + row_number, and after
        the truncate the max is 0 -- so it restarts. Reproduced: a flag
        recorded against ROW_ID 1 for customer 100 came back pointing at
        customer 999 after the next run. A data-quality flag silently
        re-attributed to a different entity is worse than a stale one.

    BUSINESS_RULE_KEY_COLUMN wants a key that is *stable across runs* -- the
    natural/business key. That is a different requirement from "the target has
    a single-column primary key", which is what CLAUDE.md's convention is
    actually for ("...which is why BUSINESS_RULE_KEY_COLUMN can safely stay a
    single column rather than a list"). Conflating the two is what made this
    possible.
    """
    writers: dict[str, str] = {}
    for task in fetch_tasks_with_parameters(conn):
        action = (task.parameters.get("SQL_ACTION") or "").strip().upper()
        written = (task.parameters.get("TARGET_OBJECT") or "").strip().lower()
        if action and written:
            writers[written] = action

    issues: list[ValidationIssue] = []
    for target in fetch_business_rule_targets(conn):
        if target.key_column.strip().lower() != ROW_ID_COLUMN.lower():
            continue
        writer = writers.get(target.target_table.strip().lower())
        if writer in FULL_REPLACE_ACTIONS:
            issues.append(
                ValidationIssue(
                    category="business_rule_key",
                    message=(
                        f"{target.business_rule_name!r}: BUSINESS_RULE_KEY_COLUMN is "
                        f"{target.key_column!r}, but {target.target_table!r} is written by "
                        f"{writer}, which regenerates every ROW_ID. Flags recorded against a "
                        "ROW_ID can never be deactivated once that row is replaced, and on an "
                        "Iceberg warehouse the value is reused for a different row entirely. "
                        "Name the target's stable business key instead."
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

# [ADDITION, 2026-09-22, E2-85] Parameters whose pipe-separated elements are
# column names interpolated unquoted into SQL text. _split_pipe_list only
# strips whitespace, so `MERGE_KEY: "customer id"` passed validate and failed
# the task at run time with a warehouse syntax error naming neither the
# parameter nor the task -- exactly the class of thing E2-25 was raised to
# surface here instead of at 3 a.m.
_COLUMN_LIST_PARAMS = ("MERGE_KEY", "MERGE_COMPARE_COLUMNS")

# [ADDITION, 2026-09-22, E2-85] MERGE_DEDUPE_ORDER is deliberately a SQL
# fragment (an ORDER BY body), so it cannot be an identifier check. This is
# the most that fits without a parser: comma-separated column names, each with
# an optional ASC/DESC and an optional NULLS FIRST/LAST. A fragment that is
# legitimately more exotic than this is rejected, which is the trade -- and it
# is stated in the message so the author knows what was expected.
_SAFE_ORDER_TERM = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(\s+(ASC|DESC))?(\s+NULLS\s+(FIRST|LAST))?$",
    re.IGNORECASE,
)


def requested_table_formats(conn: Connection, config: ConnectorConfig) -> set[str]:
    """Every storage format this deployment actually asks for.

    [ADDITION, 2026-09-22, E2-72] The warehouse default plus every active
    task's own TABLE_FORMAT override -- resolved the way execution resolves it,
    because the more specific setting is the one that wins at execution.

    Checking only the warehouse default (which is all `doctor` could see, since
    it reads no CFG_ rows) got this backwards in both directions: with
    `Table_format: native` and one task overriding to `iceberg`, the catalog
    check was skipped for a task that genuinely needed it; with the defaults
    swapped, it failed for a catalog nothing needed.
    """
    formats = {config.warehouse_table_format}
    for task in fetch_tasks_with_parameters(conn):
        declared = (task.parameters.get("TABLE_FORMAT") or "").strip().lower()
        if declared in VALID_TABLE_FORMATS:
            formats.add(declared)
    return formats


def validate_warehouse_storage(
    conn: Connection, config: ConnectorConfig, warehouse_engine: Engine | None
) -> list[ValidationIssue]:
    """Check the warehouse can actually produce the formats the config asks for.

    [ADDITION, 2026-09-22, E2-72/E2-73] Two cross-database questions that only
    have an answer once both the CFG_ rows and the live warehouse are in hand,
    which is exactly what this module already does for the business-rule
    primary-key check:

      * On Trino the storage format is a property of the *catalog*, so a task
        asking for Iceberg against a Hive catalog gets Hive tables while
        everything reports success.
      * On Snowflake a customer external volume requires BASE_LOCATION;
        Snowflake-managed Iceberg tables need neither storage parameter.
        Cloning retains its own explicit external-storage requirements.
    """
    if warehouse_engine is None or config.warehouse is None:
        return []
    issues: list[ValidationIssue] = []
    formats = requested_table_formats(conn, config)

    if "iceberg" in formats:
        problem = verify_iceberg_catalog(config, warehouse_engine)
        if problem:
            issues.append(ValidationIssue(category="warehouse_storage", message=problem))

    if warehouse_engine.dialect.name in ICEBERG_CREATE_PREFIX:
        for task in fetch_tasks_with_parameters(conn):
            params = task.parameters
            declared = (params.get("TABLE_FORMAT") or "").strip().lower()
            effective = declared or config.warehouse_table_format
            if effective != "iceberg" or not params.get("SQL_ACTION"):
                continue
            volume = (params.get("EXTERNAL_VOLUME") or "").strip() or SNOWFLAKE_MANAGED_VOLUME
            if (
                volume != SNOWFLAKE_MANAGED_VOLUME
                and not (params.get("BASE_LOCATION") or "").strip()
            ):
                issues.append(
                    ValidationIssue(
                        category="warehouse_storage",
                        message=(
                            f"{task.pipeline_code}.{task.task_code}: an Iceberg table on "
                            f"{warehouse_engine.dialect.name} with a customer EXTERNAL_VOLUME "
                            "needs BASE_LOCATION — use Snowflake-managed storage or specify "
                            "the path within that volume"
                        ),
                    )
                )
        if config.cloning.enabled and "iceberg" in formats:
            missing_clone = [
                label
                for label, value in (
                    ("Cloning.External_volume", config.cloning.external_volume),
                    ("Cloning.Base_location", config.cloning.base_location),
                )
                if not value
            ]
            if missing_clone:
                issues.append(
                    ValidationIssue(
                        category="warehouse_storage",
                        message=(
                            "Cloning is enabled and mirrors would be Iceberg tables, but "
                            f"{', '.join(missing_clone)} is not set"
                        ),
                    )
                )
    return issues


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

        declared_format = (params.get("TABLE_FORMAT") or "").strip().lower()
        if "PRESERVE_TARGET" in params:
            if task.handler != "SQL" or params.get("SQL_ACTION") != "SCD1_MERGE":
                add("PRESERVE_TARGET is supported only for SCD1_MERGE")
            if params["PRESERVE_TARGET"].strip().lower() not in {"true", "false"}:
                add("PRESERVE_TARGET must be true or false")
        if declared_format and declared_format not in VALID_TABLE_FORMATS:
            # [ADDITION, 2026-09-22, E2-73] Enforced in sql_actions at
            # execution, which means `TABLE_FORMAT: icberg` passed validate and
            # failed the task. The newest parameters had the least pre-flight
            # checking, on the warehouse path with the fewest people able to
            # test it.
            add(f"TABLE_FORMAT={declared_format!r} is not one of " f"{sorted(VALID_TABLE_FORMATS)}")

        if not _SAFE_IDENTIFIER.match(task.task_code):
            add(
                f"TASK_CODE {task.task_code!r} is not a safe identifier — codes are "
                "interpolated unquoted into SQL and into generate-yml's bash_command"
            )

        # [ADDITION, 2026-09-22, E2-84] PIPELINE_CODE gets the identical
        # treatment in the identical places and was checked by nothing -- not
        # here, and not by a CHECK in schema.sql. generate_yml emits it into
        # `bash_command`, where a space, quote or `;` produces a command that
        # does something other than what it reads as; and docs_generator writes
        # `f"{pipeline_code}.html"`, where a `/` or `..` writes outside the
        # output directory and a space or colon produces a file the generated
        # href does not point at. The shell case is covered by CFG_ rows being
        # git-reviewed; the generate-docs case is a plain bug for an entirely
        # innocent code.
        if not _SAFE_IDENTIFIER.match(task.pipeline_code):
            add(
                f"PIPELINE_CODE {task.pipeline_code!r} is not a safe identifier — codes are "
                "interpolated unquoted into generate-yml's bash_command and into "
                "generate-docs' output filenames"
            )

        # E2-85: the column-name parameters reach SQL text unquoted too.
        for name in _COLUMN_LIST_PARAMS:
            raw = params.get(name)
            if not raw:
                continue
            bad = [
                part.strip()
                for part in raw.split("|")
                if part.strip() and not _SAFE_IDENTIFIER.match(part.strip())
            ]
            if bad:
                add(
                    f"{name} names {bad}, which are not safe identifiers — they are "
                    "interpolated unquoted into the merge SQL"
                )

        dedupe_order = params.get("MERGE_DEDUPE_ORDER")
        if dedupe_order:
            bad_terms = [
                term.strip()
                for term in dedupe_order.split(",")
                if term.strip() and not _SAFE_ORDER_TERM.match(term.strip())
            ]
            if bad_terms:
                add(
                    f"MERGE_DEDUPE_ORDER term(s) {bad_terms} are not a recognized shape — "
                    "expected comma-separated column names, each optionally followed by "
                    "ASC/DESC and NULLS FIRST/LAST"
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


# [ADDITION, 2026-09-20, E2-59] Handlers that never report a TARGET_COUNT, so
# a HAS_DATA edge on one can never be satisfied. E2-08 fixed this for PYTHON by
# having INGESTION_COUNT populate target_count; these two report no row count
# at all, and there is no obvious one to report.
HANDLERS_WITHOUT_ROW_COUNTS = frozenset({"BUSINESS_RULES", "EMAIL_ALERT"})


def validate_dependency_edges(conn: Connection) -> list[ValidationIssue]:
    """Check edges whose upstream handler can never satisfy them, and alert-task ordering.

    Two checks that only make sense across a whole pipeline:

    * **E2-59** A `HAS_DATA` edge means "upstream SUCCESS *and* TARGET_COUNT >
      0". `BUSINESS_RULES` and `EMAIL_ALERT` report no row count, so such an
      edge is permanently unsatisfiable. E2-01's `unsatisfiable()` made this
      worse rather than better: the downstream task is now *silently* recorded
      SKIPPED and the run finalizes SUCCESS, where before it at least showed up
      as stuck. A config mistake the engine can detect statically should not
      look like a clean run.
    * **E2-60** `EMAIL_ALERT` is a pipeline-level completion alert (E2-43) whose
      flavour is computed from every task's status — but nothing makes it run
      last. Gate it on one task rather than on every leaf and it runs mid-flight,
      sees unsettled tasks, and sends the amber "something went wrong" email for
      a run that goes on to finish cleanly. This requires what the design
      already assumes.
    """
    issues: list[ValidationIssue] = []
    # [ADDITION, 2026-09-23, E3-04] The task list _alert_ordering_issues
    # judges completeness against — see that function's own comment for why
    # edges alone (the old source) undercounted it.
    all_tasks = fetch_tasks_with_parameters(conn)
    for pipeline in fetch_all_pipelines(conn):
        pipeline_id = resolve_pipeline_id(conn, pipeline.pipeline_code)
        edges = fetch_dependency_edge_detail(conn, pipeline_id)
        for edge in edges:
            if (
                edge.dependency_type == "HAS_DATA"
                and edge.depends_on_handler in HANDLERS_WITHOUT_ROW_COUNTS
            ):
                issues.append(
                    ValidationIssue(
                        category="dependency",
                        message=(
                            f"{pipeline.pipeline_code}.{edge.task_code}: HAS_DATA edge on "
                            f"{edge.depends_on_task_code!r}, whose HANDLER="
                            f"{edge.depends_on_handler} never reports a TARGET_COUNT — this "
                            "edge can never be satisfied, and the task will be silently "
                            "recorded SKIPPED on every run"
                        ),
                    )
                )
        pipeline_tasks = [t for t in all_tasks if t.pipeline_code == pipeline.pipeline_code]
        issues.extend(_alert_ordering_issues(pipeline.pipeline_code, edges, pipeline_tasks))
    return issues


def _alert_ordering_issues(
    pipeline_code: str, edges: list[DependencyEdgeDetail], tasks: list[TaskWithParameters]
) -> list[ValidationIssue]:
    """Require each EMAIL_ALERT task to wait on every non-alert leaf in its pipeline.

    [DEVIATION, 2026-09-23, E3-04] `tasks` — every active task in the
    pipeline, edges or not — is now what both `alerts` and `all_tasks` (and
    so `leaves`) are built from, not the edge endpoints. An ordinary
    standalone task with no dependency edges of its own (nothing depends on
    it, it depends on nothing — a single ingestion step feeding nothing
    downstream is a completely normal shape) never appeared as either
    e.task_code or e.depends_on_task_code, so it was invisible to `all_tasks`
    and this check reported the pipeline clean even when its EMAIL_ALERT task
    had no dependency on it at all. The same edges-only source also meant an
    EMAIL_ALERT task with *no* edges of its own — arguably the worse
    misconfiguration, since E2-60's whole rule is that it must depend on
    every leaf — was invisible to `alerts` too, so it was never even
    considered by this check in the first place.
    """
    alerts = {t.task_code for t in tasks if t.handler == "EMAIL_ALERT"}
    if not alerts:
        return []
    all_tasks = {t.task_code for t in tasks}
    depended_on = {e.depends_on_task_code for e in edges}
    leaves = {t for t in all_tasks - depended_on if t not in alerts}
    issues: list[ValidationIssue] = []
    for alert in sorted(alerts):
        waits_for = {e.depends_on_task_code for e in edges if e.task_code == alert}
        missing = sorted(leaves - waits_for)
        if missing:
            issues.append(
                ValidationIssue(
                    category="alert_ordering",
                    message=(
                        f"{pipeline_code}.{alert}: HANDLER='EMAIL_ALERT' reports on the whole "
                        f"run, but does not depend on {missing} — it can run before those "
                        "finish and report COMPLETED_WITH_ERRORS for a run that succeeds"
                    ),
                )
            )
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
