"""HANDLER=SQL execution — the closed action vocabulary CLAUDE.md describes but leaves unbuilt.

Per CLAUDE.md's "Closed vocabulary of SQL actions": a SQL-handler task
supplies a bare, validated, read-only SELECT; the engine wraps it in
whatever statement the declared action calls for, and owns every write. This
module is that wrapping layer, for all seven actions confirmed by explicit
instruction: CREATE_TABLE, SETUP_TABLE, OVERWRITE_TABLE, SCD1_MERGE,
SCD2_MERGE, DROP_TABLE, DELETE_ROWS.

[ADDITION] CFG_TASK_PARAMETERS.PARAMETER_NAME convention this module reads
(neither pasted schema draft nor CLAUDE.md pins these down — this module is
the authoritative definition, mirrored in schema.sql's own COMMENT ON TABLE
CFG_TASK_PARAMETERS). Every task with HANDLER='SQL' needs:
  SQL_ACTION      one of CREATE_TABLE | SETUP_TABLE | OVERWRITE_TABLE |
                  SCD1_MERGE | SCD2_MERGE | DROP_TABLE | DELETE_ROWS
  TARGET_OBJECT   "schema.table" in the Data DB — deliberately never a
                  database/catalog prefix. Per explicit instruction, the
                  database name always comes from the active [Warehouse]
                  profile for the running environment, not from CFG_
                  metadata — the same schema.table pair should mean "the
                  same object" whether the active profile is dev, uat, or
                  prod. qualify() below prepends it, producing the literal
                  ANSI catalog.schema.table form.
  SOURCE_SQL      the bare, read-only SELECT (required for every action
                  except DROP_TABLE). Per explicit instruction, this SELECT
                  must never itself project an engine-managed audit column
                  (PIPELINE_RUN_ID, CREATE_DATE, CREATED_BY, UPDATE_DATE,
                  UPDATED_BY, DELETE_FLAG, ACTIVE_FLAG) — the engine adds
                  exactly the set each action calls for (AUDIT_COLUMNS
                  below), at the position the SELECT's own column order
                  implies. This module does not itself reject a SELECT that
                  breaks that rule (there is no reliable, portable way to
                  parse arbitrary SQL text without a real parser dependency,
                  which CLAUDE.md's Non-goals rules out for anything
                  dialect-specific) — a SELECT that ignores the convention
                  will surface as a schema mismatch at the check-or-evolve
                  step below instead.
  MERGE_KEY       pipe-separated column list — required for SCD1_MERGE,
                  SCD2_MERGE, DELETE_ROWS.
  MERGE_COMPARE_COLUMNS
                  pipe-separated column list — required for SCD1_MERGE,
                  SCD2_MERGE. Columns compared with IS DISTINCT FROM (the
                  ANSI SQL:1999 null-safe comparison operator) to decide
                  whether a matched row actually changed.
  HARD_DELETE     optional, DELETE_ROWS only. "true" performs a real DELETE;
                  anything else (including absent) soft-deletes via
                  DELETE_FLAG='Y' instead — per explicit instruction.

[ADDITION] Engine-managed audit columns, per action (never present in the
author's own SELECT — the engine appends them). Every action also always
appends PIPELINE_RUN_ID (CLAUDE.md's pre-existing "every table written by a
SQL action carries a pipeline_run_id column that the engine auto-stamps"),
placed immediately after the SELECT's own business columns and before these:
  CREATE_TABLE     (none)
  SETUP_TABLE      inferred from whichever other active task in the same
                    pipeline actually writes this TARGET_OBJECT (see
                    cfg.fetch_sibling_target_sql_action) — SETUP_TABLE only
                    ever establishes a *shape* ahead of the real writer, so
                    its column set must match what that writer will need.
                    Falls back to CREATE_TABLE's (none) if no sibling writer
                    is found.
  OVERWRITE_TABLE  UPDATE_DATE
  SCD1_MERGE       CREATE_DATE, CREATED_BY, UPDATE_DATE, UPDATED_BY,
                    DELETE_FLAG
  SCD2_MERGE       CREATE_DATE, CREATED_BY, UPDATE_DATE, UPDATED_BY,
                    DELETE_FLAG, ACTIVE_FLAG
[CHOICE] CREATED_BY/UPDATED_BY are stamped with the active [Warehouse]
profile's `user` (the DB user actually executing the write), not a SQL
current_user() call — the Data DB, unlike the Engine DB, has no "one
Postgres role per human" convention to make current_user() meaningful, and a
literal bind value works identically across every dialect.

[ADDITION] Schema check / evolution, per explicit instruction: no separate
schema-registry table. The task's own SOURCE_SQL is materialized into a
uniquely-named temp table (also what supplies row counts and drives every
merge statement below, so the SELECT only ever executes once), then its
shape is compared — via information_schema.columns on both sides, which
every mainstream SQL engine (Postgres, ClickHouse included) exposes — against
the target's own business columns (its full column set minus PIPELINE_RUN_ID
and this action's own audit columns). A staged SELECT missing a column the
target already has is always a hard failure, evolvable or not (this module
only ever adds columns, never silently drops one). A staged SELECT with a
genuinely new column fails with a clear reason when CFG_TASKS.SCHEMA_EVOLUTION
is false (the default); when true, the target is rebuilt with that column
added at the position the SELECT's own column order implies, existing rows
backfilled NULL for it. [CHOICE] Postgres itself has no CREATE OR REPLACE
TABLE (unlike some other dialects) — "use CREATE OR REPLACE" is implemented
as the portable, three-statement equivalent every ANSI-ish engine supports:
CREATE TABLE <new-shape> AS SELECT ... FROM <target>, DROP TABLE <target>,
ALTER TABLE <new-shape> RENAME TO <target>.

Atomicity/idempotency ("all of them should be atomic and idempotent", per
explicit instruction): every action's full statement sequence runs inside
the one Data DB transaction the caller already opened (handlers.py wraps
dispatch in `data_engine.begin()`) — a failure partway through rolls back
everything this module did, so a retried task always starts from the
target's last genuinely-committed state, not a half-written one. Idempotency
follows from each action's own logic re-deriving its effect from current
state on every run (CREATE_TABLE/OVERWRITE_TABLE always fully replace;
SCD1_MERGE/SCD2_MERGE only touch rows the comparison actually finds changed;
DELETE_ROWS/DELETE_FLAG re-matching the same keys is a no-op the second
time) — nothing here depends on a retry counter or other external state.

Row counts ("implement ways where you can easily get impacted row counts...
we correct impact counts", per explicit instruction): this module never
relies on a DBAPI cursor's own `rowcount` — some drivers/statement shapes
don't report it per-statement reliably. Every count reported back is instead
computed directly with its own COUNT(*)/EXISTS query, evaluated *before* the
mutating statement that count describes runs (so an UPDATE's own WHERE
condition, which stops matching once the write lands, is still counted
correctly).
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.engine import Connection

from etl_craft.cfg import fetch_sibling_target_sql_action
from etl_craft.config import ConnectorConfig
from etl_craft.execution import HandlerError, HandlerResult, TaskExecutionContext
from etl_craft.warehouse import translate_jdbc_url

SQL_ACTIONS = frozenset(
    {
        "CREATE_TABLE",
        "SETUP_TABLE",
        "OVERWRITE_TABLE",
        "SCD1_MERGE",
        "SCD2_MERGE",
        "DROP_TABLE",
        "DELETE_ROWS",
    }
)

# Engine-managed audit columns, in the order they're appended after
# PIPELINE_RUN_ID and the SELECT's own business columns. SETUP_TABLE is
# resolved dynamically (see _setup_table's own audit-column inference), not
# listed here.
AUDIT_COLUMNS: dict[str, tuple[str, ...]] = {
    "CREATE_TABLE": (),
    "OVERWRITE_TABLE": ("UPDATE_DATE",),
    "SCD1_MERGE": ("CREATE_DATE", "CREATED_BY", "UPDATE_DATE", "UPDATED_BY", "DELETE_FLAG"),
    "SCD2_MERGE": (
        "CREATE_DATE",
        "CREATED_BY",
        "UPDATE_DATE",
        "UPDATED_BY",
        "DELETE_FLAG",
        "ACTIVE_FLAG",
    ),
}

# CAST target type for each engine-managed audit column, when a column needs
# to exist with zero rows (SETUP_TABLE's empty-shape creation) — an untyped
# NULL literal would otherwise default to whatever the dialect's "unknown"
# type is, which some engines reject outright in a persisted CREATE TABLE AS
# SELECT.
AUDIT_COLUMN_TYPES: dict[str, str] = {
    "CREATE_DATE": "TIMESTAMP",
    "UPDATE_DATE": "TIMESTAMP",
    "CREATED_BY": "VARCHAR",
    "UPDATED_BY": "VARCHAR",
    "DELETE_FLAG": "VARCHAR(1)",
    "ACTIVE_FLAG": "VARCHAR(1)",
}

PIPELINE_ID_TOKEN = "$$pipeline_id"


class SchemaMismatchError(HandlerError):
    """Raised when a staged SELECT's shape doesn't match its target, and can't be evolved."""


def substitute_pipeline_id(
    sql: str, *, refresh_type: str, pipeline_run_id: int, force_all: bool = False
) -> str:
    """Replace every literal $$pipeline_id token per CLAUDE.md's substitution rule.

    FULL refresh (or `force_all`, business_rules.py's manual-invocation path)
    becomes unconditional (`1=1`); INCREMENTAL becomes a real
    `pipeline_run_id = <this run's id>` filter. `pipeline_run_id` is an
    engine-resolved integer, never external input, so direct interpolation
    here (rather than a bind param) is the plain-text substitution pass
    CLAUDE.md itself describes — it must land before the driver's own bind
    handling ever sees the SQL text.
    """
    replacement = (
        "1=1" if (force_all or refresh_type == "FULL") else f"pipeline_run_id = {pipeline_run_id}"
    )
    return sql.replace(PIPELINE_ID_TOKEN, replacement)


def qualify(object_ref: str, database: str) -> str:
    """Prefix a `schema.table` reference with `database` — the ANSI catalog.schema.table form.

    [ADDITION] Per explicit instruction: CFG_TASK_PARAMETERS.TARGET_OBJECT (and
    CFG_BUSINESS_RULES.TARGET_TABLE) are always just "schema.table",
    deliberately environment-agnostic. The catalog/database prefix always
    comes from whichever [Warehouse] profile is active — so the identical
    schema.table pair resolves to a different real object in dev vs. uat vs.
    prod without any CFG_ row ever changing across a promotion.
    """
    return f"{database}.{object_ref}"


def active_database(config: ConnectorConfig) -> str:
    """Resolve the active [Warehouse] profile's database name."""
    if config.warehouse is None:
        raise HandlerError("no [Warehouse] section configured in craft-connector.yml")
    _, parts = translate_jdbc_url(config.warehouse.active.jdbc_url)
    return parts["database"]


def _split_pipe_list(value: str | None, *, param_name: str) -> list[str]:
    if not value:
        raise HandlerError(f"CFG_TASK_PARAMETERS.{param_name} is required for this SQL_ACTION")
    return [part.strip() for part in value.split("|") if part.strip()]


def _fetch_columns(
    conn: Connection, table_name: str, *, schema: str | None = None
) -> list[tuple[str, str]]:
    """Return [(column_name, data_type), ...] in ordinal position, via information_schema.

    [CHOICE] No schema.sql / registry involvement at all, per explicit
    direction — this queries the Data DB's own information_schema.columns
    live, both for the target (schema-qualified) and the staging temp table
    (matched by name alone: Postgres exposes a session's temp tables under a
    per-backend pg_temp_N schema that varies at runtime, and this module's
    staging tables are already uniquely named per task_run_id, so matching
    by table_name alone is unambiguous in practice).
    """
    if schema is not None:
        rows = conn.execute(
            text(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE lower(table_schema) = lower(:schema) AND lower(table_name) = lower(:table) "
                "ORDER BY ordinal_position"
            ),
            {"schema": schema, "table": table_name},
        ).all()
    else:
        rows = conn.execute(
            text(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE lower(table_name) = lower(:table) ORDER BY ordinal_position"
            ),
            {"table": table_name},
        ).all()
    return [(row.column_name, row.data_type) for row in rows]


def _stage_name(task_run_id: int) -> str:
    return f"etl_stage_{task_run_id}"


def _build_stage(
    conn: Connection, task_run_id: int, select_sql: str, *, empty: bool = False
) -> str:
    """Materialize `select_sql` into a uniquely-named temp table; return its bare name."""
    stage = _stage_name(task_run_id)
    conn.execute(text(f"DROP TABLE IF EXISTS {stage}"))
    if empty:
        # ANSI-portable "no rows, same shape" trick — used by SETUP_TABLE,
        # which only ever wants the column shape, never real data.
        conn.execute(
            text(
                f"CREATE TEMPORARY TABLE {stage} AS "
                f"SELECT * FROM ({select_sql}) AS etl_src WHERE 1=0"
            )
        )
    else:
        conn.execute(text(f"CREATE TEMPORARY TABLE {stage} AS {select_sql}"))
    return stage


def _drop_stage(conn: Connection, stage: str) -> None:
    conn.execute(text(f"DROP TABLE IF EXISTS {stage}"))


def _check_or_evolve_schema(
    conn: Connection,
    *,
    target_object: str,
    database: str,
    action: str,
    stage: str,
    schema_evolution: bool,
) -> list[tuple[str, str]]:
    """Verify (or evolve) the target's shape against `stage`; return the target's columns.

    Raises HandlerError if the target doesn't exist yet (OVERWRITE_TABLE/
    SCD1_MERGE/SCD2_MERGE all assume a SETUP_TABLE or CREATE_TABLE task
    already established it), or SchemaMismatchError if the shapes genuinely
    disagree and can't (or aren't allowed to) evolve.
    """
    schema_name, table_name = target_object.split(".", 1)
    target_columns = _fetch_columns(conn, table_name, schema=schema_name)
    if not target_columns:
        raise HandlerError(
            f"target table {qualify(target_object, database)!r} does not exist — run a "
            "SETUP_TABLE or CREATE_TABLE task against it first"
        )
    stage_columns = _fetch_columns(conn, stage)

    engine_managed = {c.lower() for c in (("PIPELINE_RUN_ID", *AUDIT_COLUMNS[action]))}
    stage_names = [name for name, _ in stage_columns]
    stage_name_set = {name.lower() for name in stage_names}
    target_business_names = [
        name for name, _ in target_columns if name.lower() not in engine_managed
    ]
    target_business_set = {name.lower() for name in target_business_names}

    if stage_name_set == target_business_set:
        return target_columns

    missing_in_stage = [n for n in target_business_names if n.lower() not in stage_name_set]
    if missing_in_stage:
        raise SchemaMismatchError(
            f"{qualify(target_object, database)}: staged SELECT is missing column(s) "
            f"{missing_in_stage} that the target already has — schema evolution only adds "
            "columns, it never removes them"
        )

    new_columns = [n for n in stage_names if n.lower() not in target_business_set]
    if not schema_evolution:
        raise SchemaMismatchError(
            f"{qualify(target_object, database)}: staged SELECT has new column(s) "
            f"{new_columns} not present in the target, and SCHEMA_EVOLUTION is false for "
            "this task"
        )
    _evolve_schema(
        conn,
        target_object=target_object,
        database=database,
        engine_managed=engine_managed,
        stage_columns=stage_columns,
        target_columns=target_columns,
    )
    return _fetch_columns(conn, table_name, schema=schema_name)


def _evolve_schema(
    conn: Connection,
    *,
    target_object: str,
    database: str,
    engine_managed: set[str],
    stage_columns: list[tuple[str, str]],
    target_columns: list[tuple[str, str]],
) -> None:
    """Rebuild the target with `stage_columns`' business-column shape/order, data preserved.

    [CHOICE] Emulates "CREATE OR REPLACE TABLE" portably (Postgres has no
    such statement): CREATE TABLE <new> AS SELECT ... FROM <target>, DROP
    TABLE <target>, ALTER TABLE <new> RENAME TO <target>. A genuinely new
    column is NULL-cast to its staged data_type — safe since it's only ever
    added, never given a value the old rows couldn't have had.
    """
    old_business_types = {
        name.lower(): dtype for name, dtype in target_columns if name.lower() not in engine_managed
    }
    engine_cols = [name for name, _ in target_columns if name.lower() in engine_managed]

    select_parts = [
        f"t.{name}" if name.lower() in old_business_types else f"CAST(NULL AS {dtype}) AS {name}"
        for name, dtype in stage_columns
    ]
    select_parts.extend(f"t.{col}" for col in engine_cols)

    schema_name, table_name = target_object.split(".", 1)
    evolve_table = f"{table_name}__etl_evolve"
    qualified_target = qualify(target_object, database)
    qualified_evolve = qualify(f"{schema_name}.{evolve_table}", database)

    conn.execute(text(f"DROP TABLE IF EXISTS {qualified_evolve}"))
    conn.execute(
        text(
            f"CREATE TABLE {qualified_evolve} AS "
            f"SELECT {', '.join(select_parts)} FROM {qualified_target} AS t"
        )
    )
    conn.execute(text(f"DROP TABLE {qualified_target}"))
    conn.execute(text(f"ALTER TABLE {qualified_evolve} RENAME TO {table_name}"))


def _count(conn: Connection, sql: str, params: dict) -> int:
    return conn.execute(text(sql), params).scalar_one()


def execute(
    data_conn: Connection, cfg_conn: Connection, ctx: TaskExecutionContext
) -> HandlerResult:
    """Run this task's SQL_ACTION against the Data DB; return counts for AUD_TASK_RUN_LOG."""
    params = ctx.task_params
    action = params.get("SQL_ACTION")
    if action not in SQL_ACTIONS:
        raise HandlerError(f"CFG_TASK_PARAMETERS.SQL_ACTION missing or unrecognized: {action!r}")
    target_object = params.get("TARGET_OBJECT")
    if not target_object:
        raise HandlerError("CFG_TASK_PARAMETERS.TARGET_OBJECT is required for every SQL_ACTION")
    database = active_database(ctx.config)
    updated_by = (
        ctx.config.warehouse.active.user
    )  # active_database() already proved warehouse is set
    now = datetime.now(UTC)

    if action == "DROP_TABLE":
        return _drop_table(data_conn, cfg_conn, ctx, target_object, database)
    if action == "DELETE_ROWS":
        return _delete_rows(data_conn, ctx, params, target_object, database, updated_by, now)

    source_sql_raw = params.get("SOURCE_SQL")
    if not source_sql_raw:
        raise HandlerError(f"CFG_TASK_PARAMETERS.SOURCE_SQL is required for SQL_ACTION={action}")
    select_sql = substitute_pipeline_id(
        source_sql_raw, refresh_type=ctx.refresh_type, pipeline_run_id=ctx.pipeline_run_id
    )

    if action == "CREATE_TABLE":
        return _create_table(data_conn, ctx, select_sql, target_object, database)
    if action == "SETUP_TABLE":
        return _setup_table(data_conn, cfg_conn, ctx, select_sql, target_object, database)
    if action == "OVERWRITE_TABLE":
        return _overwrite_table(data_conn, ctx, select_sql, target_object, database, now)
    # SCD1_MERGE / SCD2_MERGE
    merge_key = _split_pipe_list(params.get("MERGE_KEY"), param_name="MERGE_KEY")
    merge_compare_columns = _split_pipe_list(
        params.get("MERGE_COMPARE_COLUMNS"), param_name="MERGE_COMPARE_COLUMNS"
    )
    if action == "SCD1_MERGE":
        return _scd1_merge(
            data_conn,
            ctx,
            select_sql,
            target_object,
            database,
            merge_key,
            merge_compare_columns,
            updated_by,
            now,
        )
    return _scd2_merge(
        data_conn,
        ctx,
        select_sql,
        target_object,
        database,
        merge_key,
        merge_compare_columns,
        updated_by,
        now,
    )


def _create_table(
    conn: Connection, ctx: TaskExecutionContext, select_sql: str, target_object: str, database: str
) -> HandlerResult:
    stage = _build_stage(conn, ctx.task_run_id, select_sql)
    source_count = _count(conn, f"SELECT COUNT(*) FROM {stage}", {})
    qualified_target = qualify(target_object, database)
    conn.execute(text(f"DROP TABLE IF EXISTS {qualified_target}"))
    conn.execute(
        text(
            f"CREATE TABLE {qualified_target} AS "
            f"SELECT s.*, CAST(:pipeline_run_id AS BIGINT) AS PIPELINE_RUN_ID FROM {stage} AS s"
        ),
        {"pipeline_run_id": ctx.pipeline_run_id},
    )
    _drop_stage(conn, stage)
    return HandlerResult(
        source_count=source_count, target_count=source_count, insert_count=source_count
    )


def _setup_table(
    conn: Connection,
    cfg_conn: Connection,
    ctx: TaskExecutionContext,
    select_sql: str,
    target_object: str,
    database: str,
) -> HandlerResult:
    sibling_action = fetch_sibling_target_sql_action(
        cfg_conn, ctx.pipeline_id, ctx.task_id, target_object
    )
    audit_columns = AUDIT_COLUMNS.get(sibling_action, ())

    stage = _build_stage(conn, ctx.task_run_id, select_sql, empty=True)
    stage_columns = _fetch_columns(conn, stage)
    select_parts = [f"s.{name}" for name, _ in stage_columns]
    select_parts.append("CAST(NULL AS BIGINT) AS PIPELINE_RUN_ID")
    for col in audit_columns:
        select_parts.append(f"CAST(NULL AS {AUDIT_COLUMN_TYPES[col]}) AS {col}")

    qualified_target = qualify(target_object, database)
    conn.execute(text(f"DROP TABLE IF EXISTS {qualified_target}"))
    conn.execute(
        text(
            f"CREATE TABLE {qualified_target} AS SELECT {', '.join(select_parts)} FROM {stage} AS s"
        )
    )
    _drop_stage(conn, stage)
    return HandlerResult(source_count=0, target_count=0, insert_count=0)


def _overwrite_table(
    conn: Connection,
    ctx: TaskExecutionContext,
    select_sql: str,
    target_object: str,
    database: str,
    now: datetime,
) -> HandlerResult:
    stage = _build_stage(conn, ctx.task_run_id, select_sql)
    source_count = _count(conn, f"SELECT COUNT(*) FROM {stage}", {})
    _check_or_evolve_schema(
        conn,
        target_object=target_object,
        database=database,
        action="OVERWRITE_TABLE",
        stage=stage,
        schema_evolution=ctx.schema_evolution,
    )
    stage_columns = [name for name, _ in _fetch_columns(conn, stage)]
    qualified_target = qualify(target_object, database)
    conn.execute(text(f"TRUNCATE TABLE {qualified_target}"))
    columns_sql = ", ".join(stage_columns)
    conn.execute(
        text(
            f"INSERT INTO {qualified_target} ({columns_sql}, PIPELINE_RUN_ID, UPDATE_DATE) "
            f"SELECT {columns_sql}, :pipeline_run_id, :now FROM {stage}"
        ),
        {"pipeline_run_id": ctx.pipeline_run_id, "now": now},
    )
    _drop_stage(conn, stage)
    return HandlerResult(
        source_count=source_count, target_count=source_count, insert_count=source_count
    )


def _scd1_merge(
    conn: Connection,
    ctx: TaskExecutionContext,
    select_sql: str,
    target_object: str,
    database: str,
    merge_key: list[str],
    merge_compare_columns: list[str],
    updated_by: str,
    now: datetime,
) -> HandlerResult:
    stage = _build_stage(conn, ctx.task_run_id, select_sql)
    source_count = _count(conn, f"SELECT COUNT(*) FROM {stage}", {})
    _check_or_evolve_schema(
        conn,
        target_object=target_object,
        database=database,
        action="SCD1_MERGE",
        stage=stage,
        schema_evolution=ctx.schema_evolution,
    )
    stage_columns = [name for name, _ in _fetch_columns(conn, stage)]
    non_key_columns = [c for c in stage_columns if c.lower() not in {k.lower() for k in merge_key}]
    qualified_target = qualify(target_object, database)

    key_match = " AND ".join(f"t.{k} = s.{k}" for k in merge_key)
    changed = " OR ".join(f"t.{c} IS DISTINCT FROM s.{c}" for c in merge_compare_columns)

    update_count = _count(
        conn,
        f"SELECT COUNT(*) FROM {stage} s WHERE EXISTS "
        f"(SELECT 1 FROM {qualified_target} t WHERE {key_match} AND ({changed}))",
        {},
    )
    # Correlated scalar subquery per non-key column, not UPDATE...FROM (a
    # Postgres/SQL Server extension, not ANSI) and not MERGE (excluded by
    # explicit instruction) — the outer `t` in `key_match` correlates
    # against this UPDATE's own target row, same as any ANSI-portable
    # correlated UPDATE.
    set_pieces = [f"{c} = (SELECT s.{c} FROM {stage} s WHERE {key_match})" for c in non_key_columns]
    set_pieces.append("PIPELINE_RUN_ID = :pipeline_run_id")
    set_pieces.append("UPDATE_DATE = :now")
    set_pieces.append("UPDATED_BY = :updated_by")
    conn.execute(
        text(
            f"UPDATE {qualified_target} t SET {', '.join(set_pieces)} "
            f"WHERE EXISTS (SELECT 1 FROM {stage} s WHERE {key_match} AND ({changed}))"
        ),
        {"pipeline_run_id": ctx.pipeline_run_id, "now": now, "updated_by": updated_by},
    )

    insert_count = _count(
        conn,
        f"SELECT COUNT(*) FROM {stage} s WHERE NOT EXISTS "
        f"(SELECT 1 FROM {qualified_target} t WHERE {key_match})",
        {},
    )
    columns_sql = ", ".join(stage_columns)
    conn.execute(
        text(
            f"INSERT INTO {qualified_target} ({columns_sql}, PIPELINE_RUN_ID, CREATE_DATE, "
            "CREATED_BY, UPDATE_DATE, UPDATED_BY, DELETE_FLAG) "
            f"SELECT {columns_sql}, :pipeline_run_id, :now, :updated_by, :now, :updated_by, 'N' "
            f"FROM {stage} s WHERE NOT EXISTS "
            f"(SELECT 1 FROM {qualified_target} t WHERE {key_match})"
        ),
        {"pipeline_run_id": ctx.pipeline_run_id, "now": now, "updated_by": updated_by},
    )
    _drop_stage(conn, stage)
    target_count = _count(conn, f"SELECT COUNT(*) FROM {qualified_target}", {})
    return HandlerResult(
        source_count=source_count,
        target_count=target_count,
        insert_count=insert_count,
        update_count=update_count,
    )


def _scd2_merge(
    conn: Connection,
    ctx: TaskExecutionContext,
    select_sql: str,
    target_object: str,
    database: str,
    merge_key: list[str],
    merge_compare_columns: list[str],
    updated_by: str,
    now: datetime,
) -> HandlerResult:
    stage = _build_stage(conn, ctx.task_run_id, select_sql)
    source_count = _count(conn, f"SELECT COUNT(*) FROM {stage}", {})
    _check_or_evolve_schema(
        conn,
        target_object=target_object,
        database=database,
        action="SCD2_MERGE",
        stage=stage,
        schema_evolution=ctx.schema_evolution,
    )
    stage_columns = [name for name, _ in _fetch_columns(conn, stage)]
    qualified_target = qualify(target_object, database)

    key_match = " AND ".join(f"t.{k} = s.{k}" for k in merge_key)
    changed = " OR ".join(f"t.{c} IS DISTINCT FROM s.{c}" for c in merge_compare_columns)

    # Materialize the changed-and-matched key set once, so the deactivate
    # step and the new-version-insert step agree on exactly the same rows —
    # avoids re-deriving "which rows just got deactivated" from ACTIVE_FLAG
    # alone, which could also match historically-inactive rows from earlier
    # SCD2 runs of the same target.
    changed_keys = f"etl_changed_keys_{ctx.task_run_id}"
    conn.execute(text(f"DROP TABLE IF EXISTS {changed_keys}"))
    key_columns_sql = ", ".join(merge_key)
    conn.execute(
        text(
            f"CREATE TEMPORARY TABLE {changed_keys} AS "
            f"SELECT DISTINCT {key_columns_sql} FROM {stage} s WHERE EXISTS "
            f"(SELECT 1 FROM {qualified_target} t "
            f"WHERE t.ACTIVE_FLAG = 'Y' AND {key_match} AND ({changed}))"
        )
    )
    changed_key_match = " AND ".join(f"t.{k} = ck.{k}" for k in merge_key)
    stage_changed_key_match = " AND ".join(f"s.{k} = ck.{k}" for k in merge_key)

    deactivate_count = _count(conn, f"SELECT COUNT(*) FROM {changed_keys}", {})
    conn.execute(
        text(
            f"UPDATE {qualified_target} t SET ACTIVE_FLAG = 'N', UPDATE_DATE = :now, "
            f"UPDATED_BY = :updated_by "
            f"WHERE t.ACTIVE_FLAG = 'Y' AND EXISTS "
            f"(SELECT 1 FROM {changed_keys} ck WHERE {changed_key_match})"
        ),
        {"now": now, "updated_by": updated_by},
    )

    columns_sql = ", ".join(stage_columns)
    conn.execute(
        text(
            f"INSERT INTO {qualified_target} ({columns_sql}, PIPELINE_RUN_ID, CREATE_DATE, "
            "CREATED_BY, UPDATE_DATE, UPDATED_BY, DELETE_FLAG, ACTIVE_FLAG) "
            "SELECT "
            f"{columns_sql}, :pipeline_run_id, :now, :updated_by, :now, :updated_by, 'N', 'Y' "
            f"FROM {stage} s WHERE EXISTS "
            f"(SELECT 1 FROM {changed_keys} ck WHERE {stage_changed_key_match})"
        ),
        {"pipeline_run_id": ctx.pipeline_run_id, "now": now, "updated_by": updated_by},
    )

    new_count = _count(
        conn,
        f"SELECT COUNT(*) FROM {stage} s WHERE NOT EXISTS "
        f"(SELECT 1 FROM {qualified_target} t WHERE {key_match})",
        {},
    )
    conn.execute(
        text(
            f"INSERT INTO {qualified_target} ({columns_sql}, PIPELINE_RUN_ID, CREATE_DATE, "
            "CREATED_BY, UPDATE_DATE, UPDATED_BY, DELETE_FLAG, ACTIVE_FLAG) "
            "SELECT "
            f"{columns_sql}, :pipeline_run_id, :now, :updated_by, :now, :updated_by, 'N', 'Y' "
            f"FROM {stage} s WHERE NOT EXISTS "
            f"(SELECT 1 FROM {qualified_target} t WHERE {key_match})"
        ),
        {"pipeline_run_id": ctx.pipeline_run_id, "now": now, "updated_by": updated_by},
    )

    conn.execute(text(f"DROP TABLE IF EXISTS {changed_keys}"))
    _drop_stage(conn, stage)
    target_count = _count(conn, f"SELECT COUNT(*) FROM {qualified_target}", {})
    return HandlerResult(
        source_count=source_count,
        target_count=target_count,
        insert_count=deactivate_count + new_count,
        update_count=deactivate_count,
    )


def _drop_table(
    conn: Connection,
    cfg_conn: Connection,
    ctx: TaskExecutionContext,
    target_object: str,
    database: str,
) -> HandlerResult:
    sibling_action = fetch_sibling_target_sql_action(
        cfg_conn, ctx.pipeline_id, ctx.task_id, target_object
    )
    if sibling_action != "CREATE_TABLE":
        raise HandlerError(
            f"DROP_TABLE refused for {target_object!r}: no other active task in this pipeline "
            "creates it via SQL_ACTION=CREATE_TABLE — DROP_TABLE only ever removes tables this "
            "pipeline itself is responsible for creating"
        )
    conn.execute(text(f"DROP TABLE IF EXISTS {qualify(target_object, database)}"))
    return HandlerResult()


def _delete_rows(
    conn: Connection,
    ctx: TaskExecutionContext,
    params: dict[str, str],
    target_object: str,
    database: str,
    updated_by: str,
    now: datetime,
) -> HandlerResult:
    source_sql_raw = params.get("SOURCE_SQL")
    if not source_sql_raw:
        raise HandlerError("CFG_TASK_PARAMETERS.SOURCE_SQL is required for SQL_ACTION=DELETE_ROWS")
    merge_key = _split_pipe_list(params.get("MERGE_KEY"), param_name="MERGE_KEY")
    hard_delete = (params.get("HARD_DELETE") or "").strip().lower() == "true"

    select_sql = substitute_pipeline_id(
        source_sql_raw, refresh_type=ctx.refresh_type, pipeline_run_id=ctx.pipeline_run_id
    )
    stage = _build_stage(conn, ctx.task_run_id, select_sql)
    qualified_target = qualify(target_object, database)
    key_match = " AND ".join(f"t.{k} = s.{k}" for k in merge_key)

    delete_count = _count(
        conn,
        f"SELECT COUNT(*) FROM {qualified_target} t "
        f"WHERE EXISTS (SELECT 1 FROM {stage} s WHERE {key_match})",
        {},
    )
    if hard_delete:
        conn.execute(
            text(
                f"DELETE FROM {qualified_target} t "
                f"WHERE EXISTS (SELECT 1 FROM {stage} s WHERE {key_match})"
            )
        )
    else:
        conn.execute(
            text(
                f"UPDATE {qualified_target} t SET DELETE_FLAG = 'Y', UPDATE_DATE = :now, "
                f"UPDATED_BY = :updated_by "
                f"WHERE EXISTS (SELECT 1 FROM {stage} s WHERE {key_match})"
            ),
            {"now": now, "updated_by": updated_by},
        )
    _drop_stage(conn, stage)
    return HandlerResult(delete_count=delete_count)
