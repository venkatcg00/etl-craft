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
                  SCD2_MERGE. Per explicit instruction, hashed into a single
                  HASH_KEY column (see below) at staging time; a matched row
                  is "changed" when its target and staged HASH_KEY differ
                  (IS DISTINCT FROM, the ANSI SQL:1999 null-safe comparison
                  operator), not by OR-chaining a per-column comparison.
  MERGE_DEDUPE_ORDER
                  optional, SCD1_MERGE/SCD2_MERGE only. [ADDITION, E2-04] An
                  ORDER BY fragment (e.g. "updated_at DESC") deciding which
                  row wins when SOURCE_SQL returns more than one for the same
                  MERGE_KEY. Absent, duplicates are a clean failure *before*
                  any statement touches the target — the engine will not
                  invent an ordering nobody declared, since which row survived
                  would then be undefined and could differ between runs.
  PRIMARY_KEY     optional, every creating action. [ADDITION, E2-03] Applied
                  as ALTER TABLE ... ADD PRIMARY KEY once the target has been
                  created, and re-applied after a schema-evolution rebuild.
                  Deliberately independent of MERGE_KEY, per explicit
                  instruction ("a merge can have both primary key and merge
                  key") — a target's identity and the columns a merge matches
                  on are different questions even when they coincide. This is
                  what lets an engine-created table satisfy the single-column
                  primary key convention `validate` enforces.
  HARD_DELETE     optional, DELETE_ROWS only. "true" performs a real DELETE;
                  anything else (including absent) soft-deletes via
                  DELETE_FLAG='Y' instead — per explicit instruction.
  SCHEMA_EVOLUTION
                  optional, OVERWRITE_TABLE/SCD1_MERGE/SCD2_MERGE only.
                  "true" opts this task into the schema-evolution rebuild
                  path below; anything else (including absent) means false.
                  [DEVIATION, post-signoff 2026-09-20] Was its own CFG_TASKS
                  BOOLEAN column — moved here per explicit instruction (see
                  execution.TaskExecutionContext's own docstring): it's only
                  ever meaningful for a subset of SQL_ACTIONs, not every
                  task regardless of HANDLER.

[ADDITION] Engine-managed audit columns, per action (never present in the
author's own SELECT — the engine appends them). Every action also always
appends PIPELINE_RUN_ID (CLAUDE.md's pre-existing "every table written by a
SQL action carries a pipeline_run_id column that the engine auto-stamps"),
placed immediately after the SELECT's own business columns and before these:
  CREATE_TABLE     (none)
  SETUP_TABLE      inferred from whichever other active task in the same
                    pipeline actually writes this TARGET_OBJECT (see
                    cfg.fetch_sibling_target_writer) — SETUP_TABLE only
                    ever establishes a *shape* ahead of the real writer, so
                    its column set must match what that writer will need.
                    Falls back to CREATE_TABLE's (none) if no sibling writer
                    is found.
  OVERWRITE_TABLE  UPDATE_DATE
  SCD1_MERGE       HASH_KEY, CREATE_DATE, CREATED_BY, UPDATE_DATE,
                    UPDATED_BY, DELETE_FLAG
  SCD2_MERGE       HASH_KEY, CREATE_DATE, CREATED_BY, UPDATE_DATE,
                    UPDATED_BY, DELETE_FLAG, ACTIVE_FLAG
[ADDITION] HASH_KEY (`_hash_expression`/`_add_hash_key`): an MD5 hash of
MERGE_COMPARE_COLUMNS, computed once at staging time (after the schema
check, so the comparison never sees it — see `_add_hash_key`'s own
docstring) and stamped onto every row the merge touches. Per explicit
instruction ("scd tables should also have hashkey created by merge_compare
columns"). MD5 isn't ANSI SQL, but no hash function is — the same accepted
exception this module already makes for TRUNCATE.
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
every mainstream SQL engine (both supported warehouses included) exposes — against
the target's own business columns (its full column set minus PIPELINE_RUN_ID
and this action's own audit columns). A staged SELECT missing a column the
target already has is always a hard failure, evolvable or not (this module
only ever adds columns, never silently drops one). A staged SELECT with a
genuinely new column fails with a clear reason when CFG_TASK_PARAMETERS.SCHEMA_EVOLUTION
is false (the default); when true, the target is rebuilt with that column
added at the position the SELECT's own column order implies, existing rows
backfilled NULL for it. [CHOICE] Postgres itself has no CREATE OR REPLACE
TABLE (unlike some other dialects) — "use CREATE OR REPLACE" is implemented
as the portable, three-statement equivalent every ANSI-ish engine supports:
CREATE TABLE <new-shape> AS SELECT ... FROM <target>, DROP TABLE <target>,
ALTER TABLE <new-shape> RENAME TO <target>.

[ADDITION, post-signoff 2026-09-20] Audit-column-presence check, per explicit
instruction ("fail the task if the target is already present and does not
have the scoped audit columns for that sql action"): before comparing
business columns at all, _check_or_evolve_schema now verifies every one of
the action's own engine-managed columns (PIPELINE_RUN_ID plus whatever
AUDIT_COLUMNS[action] lists) is actually present on an already-existing
target, raising a clear HandlerError naming what's missing if not — instead
of letting a stale/hand-built target fail later with a confusing raw
"column ... does not exist" error from the real UPDATE/INSERT statement.
This runs unconditionally, regardless of SCHEMA_EVOLUTION: that flag only
ever governs adding new *business* columns the staged SELECT introduces, and
was never meant to repair a target missing its own engine-managed columns.
_delete_rows' soft-delete path (HARD_DELETE not "true") gets the same
treatment for DELETE_FLAG specifically, since it depends on that column the
same way OVERWRITE_TABLE/SCD1_MERGE/SCD2_MERGE depend on their own audit
columns, but never goes through _check_or_evolve_schema itself (DELETE_ROWS
does no schema comparison — it only matches on MERGE_KEY). [CHOICE] Presence
only, not type — e.g. not verifying ACTIVE_FLAG is really VARCHAR(1) rather
than, say, BOOLEAN. Column-type drift is a real but much rarer failure mode
than "table predates this convention or was edited by hand"; per explicit
instruction to scope this to "highly possible data engineering possibilities"
rather than chase every hypothetical, that stays unhandled for now.

Atomicity/idempotency ("all of them should be atomic and idempotent", per
explicit instruction): every action's full statement sequence runs inside
the one Data DB transaction the caller already opened (handlers.py wraps
dispatch in `data_engine.begin()`) — a failure partway through rolls back
everything this module did, so a retried task always starts from the
target's last genuinely-committed state, not a half-written one.

[DEVIATION, 2026-09-20, E2-31] That guarantee is **not** universal, and
saying so plainly here rather than leaving it implied: CREATE_TABLE,
SETUP_TABLE, _create_target_shape, _evolve_schema and TRUNCATE are all DDL,
and DDL auto-commits on MySQL and Oracle. On those engines a failure partway
through leaves the completed DDL in place. Postgres (this project's own
Engine DB, and what every test runs against) has transactional DDL, so the
guarantee holds there in full. A team adopting a non-transactional-DDL
warehouse should expect "retry resumes" to mean re-deriving from whatever
state the last attempt left, which every action's own logic already does. Idempotency
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

from etl_craft.cfg import fetch_sibling_target_writer
from etl_craft.config import ConnectorConfig
from etl_craft.execution import HandlerError, HandlerResult, TaskExecutionContext
from etl_craft.runlog import fetch_task_run_status
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
    "SCD1_MERGE": (
        "HASH_KEY",
        "CREATE_DATE",
        "CREATED_BY",
        "UPDATE_DATE",
        "UPDATED_BY",
        "DELETE_FLAG",
    ),
    "SCD2_MERGE": (
        "HASH_KEY",
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
# [DEVIATION, 2026-09-20, E2-31/E2-33] Two corrections here. CREATE_DATE and
# UPDATE_DATE were plain TIMESTAMP while schema.sql uses TIMESTAMPTZ
# throughout and the engine writes datetime.now(UTC) — every warehouse-side
# audit timestamp silently lost its offset. And bare VARCHAR with no length is
# rejected in DDL by several dialects (Oracle, MySQL in strict mode), so
# CREATED_BY/UPDATED_BY carry one.
AUDIT_COLUMN_TYPES: dict[str, str] = {
    "HASH_KEY": "VARCHAR(32)",
    "CREATE_DATE": "TIMESTAMP WITH TIME ZONE",
    "UPDATE_DATE": "TIMESTAMP WITH TIME ZONE",
    "CREATED_BY": "VARCHAR(255)",
    "UPDATED_BY": "VARCHAR(255)",
    "DELETE_FLAG": "VARCHAR(1)",
    "ACTIVE_FLAG": "VARCHAR(1)",
}


# [ADDITION, 2026-09-22] Per explicit instruction: "if the warehouse is not
# postgres, every table we create or operate should be iceberg compatible."
#
# Postgres is the one warehouse that stores its own tables natively; every
# other supported warehouse is a SQL engine *over Iceberg*, so every table
# this module creates there has to be an Iceberg table rather than that
# engine's own default format. Getting this wrong is silent: the table is
# created, the pipeline succeeds, and it is simply not readable by anything
# else in the lakehouse — which is the entire reason for choosing Iceberg.
#
# Expressed as the clause each dialect needs between `CREATE TABLE <name>` and
# `AS <select>`, because that is the only part that differs:
#
#   * Databricks — `USING ICEBERG` names the format in Unity Catalog.
#   * Trino — tables in an Iceberg catalog are Iceberg by construction; the
#     catalog in the URL is what decides it, so no clause is needed and
#     adding one would be a syntax error.
#   * Snowflake — deliberately absent, see ICEBERG_CREATE_PREFIX below.
#
# A dialect not listed here gets no clause. That is the right default for
# "any sql tool over plain iceberg" (Trino's case, generalized): an engine
# pointed at an Iceberg catalog writes Iceberg without being told to.
ICEBERG_TABLE_CLAUSE: dict[str, str] = {"databricks": "USING ICEBERG"}

# Snowflake is the one that cannot be expressed as a clause: its Iceberg
# tables use a different statement (`CREATE ICEBERG TABLE`) and require an
# EXTERNAL_VOLUME plus a BASE_LOCATION that are deployment-specific and have
# no home in CFG_ metadata today.
#
# [CHOICE] Refused up front with a clear reason rather than silently creating
# an ordinary Snowflake table that looks fine and is not Iceberg. Same
# reasoning as refusing SCD merges on ClickHouse: producing the wrong thing
# successfully is worse than failing.
#
# Worth stating plainly, because the instruction named them together:
# Snowflake **hybrid tables and Iceberg tables are different features** — the
# first is a row-store with enforced primary keys, the second is external
# Iceberg format — and a table cannot be both. "Iceberg-compatible" is the
# binding requirement, so Iceberg tables are the target here.
ICEBERG_CREATE_PREFIX: dict[str, str] = {"snowflake": "ICEBERG TABLE"}

# The warehouse that stores its own tables, and so needs no Iceberg handling.
NATIVE_STORAGE_DIALECTS = frozenset({"postgresql", "duckdb"})


def iceberg_clause(dialect_name: str) -> str:
    """Return the clause that makes a CREATE TABLE produce an Iceberg table here."""
    if dialect_name.split("+", 1)[0] in NATIVE_STORAGE_DIALECTS:
        return ""
    return ICEBERG_TABLE_CLAUSE.get(dialect_name, "")


def create_table_as(conn: Connection, qualified_name: str, select_sql: str) -> None:
    """Issue CREATE TABLE ... AS SELECT, as an Iceberg table where that is not the default.

    [DEVIATION, 2026-09-20] This briefly carried a ClickHouse branch (a literal
    `ENGINE = MergeTree() ORDER BY tuple()`, mandatory there and rejected
    everywhere else). ClickHouse is no longer a supported warehouse. Kept as a
    named helper, which is what made adding Iceberg support a change here
    rather than at five call sites.
    """
    dialect_name = conn.dialect.name
    if dialect_name in ICEBERG_CREATE_PREFIX:
        raise HandlerError(
            f"{dialect_name} needs CREATE {ICEBERG_CREATE_PREFIX[dialect_name]} with an "
            "EXTERNAL_VOLUME and BASE_LOCATION to produce an Iceberg table, and neither has "
            "a home in CFG_ metadata yet — refusing rather than silently creating a "
            "non-Iceberg table. See sql_actions.ICEBERG_CREATE_PREFIX."
        )
    clause = iceberg_clause(dialect_name)
    prefix = f"CREATE TABLE {qualified_name}"
    statement = f"{prefix} {clause} AS {select_sql}" if clause else f"{prefix} AS {select_sql}"
    conn.execute(text(statement))


PIPELINE_ID_TOKEN = "$$pipeline_id"


class SchemaMismatchError(HandlerError):
    """Raised when a staged SELECT's shape doesn't match its target, and can't be evolved."""


def substitute_pipeline_id(
    sql: str, *, refresh_type: str, pipeline_run_id: int, force_all: bool = False
) -> str:
    """Substitute a literal $$pipeline_id token in `sql`, if one is present. Nothing else.

    FULL refresh (or `force_all`, business_rules.py's manual-invocation path)
    resolves to the unconditional `1=1`; INCREMENTAL resolves to a real
    `pipeline_run_id = <this run's id>` filter. `pipeline_run_id` is an
    engine-resolved integer, never external input, so direct interpolation
    here (rather than a bind param) is the plain-text substitution pass
    CLAUDE.md itself describes — it must land before the driver's own bind
    handling ever sees the SQL text.

    [DEVIATION, post-signoff 2026-09-20] An earlier version of this function
    also auto-appended a `WHERE <condition>` when `$$pipeline_id` was absent
    and no `WHERE` clause existed at all, on a "protect an author who forgot
    the token" theory. Removed per explicit instruction ("this was a bad
    idea"): it made a SELECT's real behavior depend on a hidden heuristic
    (a regex `WHERE` search, which also had a real known blind spot for
    subqueries) the author can't see just by reading their own SQL — a
    bigger overreach than plain token substitution, and inconsistent with
    CLAUDE.md's own model that the author supplies and owns "a bare,
    validated, read-only SELECT." Only two cases now: the token is present
    (substitute it) or it isn't (leave `sql` completely untouched, `WHERE`
    clause or not) — the same rule whether or not the target happens to have
    a real PIPELINE_RUN_ID-compatible filter to write. A task that forgets
    the token on a genuinely incremental source will scan more than
    intended, but that is a pipeline-definition mistake for review to catch
    (pipeline creation is always manual/reviewed, per CLAUDE.md), not
    something the engine should try to silently rescue by guessing.
    """
    replacement = (
        "1=1" if (force_all or refresh_type == "FULL") else f"pipeline_run_id = {pipeline_run_id}"
    )
    return sql.replace(PIPELINE_ID_TOKEN, replacement)


def split_object_ref(object_ref: str, *, param_name: str = "TARGET_OBJECT") -> tuple[str, str]:
    """Split a `schema.table` reference, rejecting anything that isn't exactly that.

    [ADDITION, 2026-09-20, E2-25] Every call site used to do a bare
    `object_ref.split(".", 1)` straight into a two-name unpack, so a value with
    no dot raised `ValueError: not enough values to unpack` — a traceback, from
    inside a forked child, about a config typo. `qualify()` didn't validate
    either: it just prefixed the database, so CREATE_TABLE/SETUP_TABLE/
    OVERWRITE_TABLE silently emitted a malformed two-part name instead.
    """
    parts = [part.strip() for part in object_ref.split(".")]
    if len(parts) != 2 or not all(parts):
        raise HandlerError(
            f"CFG_TASK_PARAMETERS.{param_name}={object_ref!r} must be exactly "
            "'schema.table' — no database/catalog prefix (that comes from the active "
            "[Warehouse] profile at runtime) and no bare table name"
        )
    return parts[0], parts[1]


def qualify(object_ref: str, database: str) -> str:
    """Prefix a `schema.table` reference with `database` — the ANSI catalog.schema.table form.

    [ADDITION] Per explicit instruction: CFG_TASK_PARAMETERS.TARGET_OBJECT (and
    CFG_BUSINESS_RULES.TARGET_TABLE) are always just "schema.table",
    deliberately environment-agnostic. The catalog/database prefix always
    comes from whichever [Warehouse] profile is active — so the identical
    schema.table pair resolves to a different real object in dev vs. uat vs.
    prod without any CFG_ row ever changing across a promotion.
    """
    schema_name, table_name = split_object_ref(object_ref)
    return f"{database}.{schema_name}.{table_name}"


def active_database(config: ConnectorConfig) -> str:
    """Resolve the active [Warehouse] profile's database name."""
    if config.warehouse is None:
        raise HandlerError("no [Warehouse] section configured in craft-connector.yml")
    _, parts = translate_jdbc_url(config.warehouse.active.jdbc_url)
    # [ADDITION, 2026-09-22] `catalog` where the two differ. Snowflake and
    # Trino carry database and schema in one `database/schema` URL segment,
    # because that is what their dialects split themselves — but qualify()'s
    # three-part `catalog.schema.table` needs the catalog half alone, and the
    # schema comes from CFG_TASK_PARAMETERS.TARGET_OBJECT, not the profile.
    database = parts.get("catalog") or parts["database"]
    if not database:
        raise HandlerError(
            "the active [Warehouse] profile's jdbc_url names no catalog/database, so "
            "TARGET_OBJECT's schema.table cannot be resolved to a full name — add one "
            "(e.g. ConnCatalog=<catalog> for Databricks, db=<database> for Snowflake)"
        )
    return database


def _split_pipe_list(value: str | None, *, param_name: str) -> list[str]:
    if not value:
        raise HandlerError(f"CFG_TASK_PARAMETERS.{param_name} is required for this SQL_ACTION")
    return [part.strip() for part in value.split("|") if part.strip()]


def _schema_evolution_enabled(ctx: TaskExecutionContext) -> bool:
    """Parse CFG_TASK_PARAMETERS.SCHEMA_EVOLUTION; absent/anything but "true" means false.

    [DEVIATION, post-signoff 2026-09-20] Used to be its own CFG_TASKS
    BOOLEAN NOT NULL DEFAULT FALSE column; moved into CFG_TASK_PARAMETERS
    per explicit instruction (see execution.TaskExecutionContext's own
    docstring) — it's only ever meaningful for SQL_ACTIONs that write into
    an existing target (OVERWRITE_TABLE/SCD1_MERGE/SCD2_MERGE), not every
    task regardless of HANDLER.
    """
    return (ctx.task_params.get("SCHEMA_EVOLUTION") or "").strip().lower() == "true"


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


def _hash_expression(columns: list[str], alias: str) -> str:
    """Build an MD5 hash expression over `columns`, NULL-safe, for change detection.

    [ADDITION] "scd tables should also have hashkey created by merge_compare
    columns" — per explicit instruction. MD5 isn't ANSI SQL (no hash
    function is), but it's the one near-universally available exception
    already accepted elsewhere in this module for the same reason TRUNCATE
    is: every mainstream engine has *some* MD5, even though the exact
    function/return shape varies (Postgres returns hex text directly;
    others may differ) — flagged, not solved further, same spirit as
    translate_jdbc_url's own documented dialect limits. COALESCE to empty
    string per column stops one NULL from collapsing the whole concatenation
    to NULL, which would make every NULL-containing row hash identically
    regardless of its other values.
    """
    # ANSI CAST, not Postgres's `::text` shorthand — this module avoids the
    # shorthand everywhere. Both supported engines return MD5 as 32 hex
    # characters, which is what HASH_KEY VARCHAR(32) expects.
    parts = " || '|' || ".join(f"COALESCE(CAST({alias}.{c} AS VARCHAR), '')" for c in columns)
    return f"MD5({parts})"


def _build_stage(
    conn: Connection, task_run_id: int, select_sql: str, *, empty: bool = False
) -> str:
    """Materialize `select_sql` into a uniquely-named temporary table; return its name."""
    stage = _stage_name(task_run_id)
    conn.execute(text(f"DROP TABLE IF EXISTS {stage}"))
    # ANSI-portable "no rows, same shape" trick for `empty` — used by
    # SETUP_TABLE, which only ever wants the column shape, never real data.
    body = f"SELECT * FROM ({select_sql}) AS etl_src WHERE 1=0" if empty else select_sql
    conn.execute(text(f"CREATE TEMPORARY TABLE {stage} AS {body}"))
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
) -> None:
    """Verify (or evolve) the target's shape against `stage`.

    Creates the target if it doesn't exist yet (E2-42), or raises
    SchemaMismatchError if the shapes genuinely disagree and can't (or aren't
    allowed to) evolve.
    """
    schema_name, table_name = split_object_ref(target_object)
    target_columns = _fetch_columns(conn, table_name, schema=schema_name)
    if not target_columns:
        # [DEVIATION, 2026-09-20, E2-42] Used to raise "does not exist — run a
        # SETUP_TABLE or CREATE_TABLE task against it first". Per explicit
        # instruction every action but DROP_TABLE/DELETE_ROWS bootstraps its
        # own target, so a first run no longer needs a separate setup task.
        _create_target_shape(
            conn,
            stage=stage,
            target_object=target_object,
            database=database,
            audit_columns=AUDIT_COLUMNS[action],
        )
        return
    stage_columns = _fetch_columns(conn, stage)

    required_audit_columns = ("PIPELINE_RUN_ID", *AUDIT_COLUMNS[action])
    # [ADDITION, 2026-09-20, E2-54] ROW_ID is engine-managed but deliberately
    # *not* required: a target created before the surrogate key existed simply
    # has none, which `validate` reports as a missing primary key. Requiring it
    # here would instead hard-fail every such target with no remedy but
    # recreating it — a worse answer than a validate finding.
    engine_managed = {c.lower() for c in required_audit_columns} | {ROW_ID_COLUMN.lower()}
    target_name_set = {name.lower() for name, _ in target_columns}
    missing_audit_columns = [c for c in required_audit_columns if c.lower() not in target_name_set]
    if missing_audit_columns:
        raise HandlerError(
            f"{qualify(target_object, database)}: target table already "
            "exists but is missing "
            f"the audit column(s) {missing_audit_columns} that SQL_ACTION={action} requires — "
            "run a SETUP_TABLE task against it first, or fix its schema by hand. This check "
            "runs regardless of SCHEMA_EVOLUTION, which only ever adds new business columns, "
            "never repairs missing engine-managed ones."
        )

    stage_names = [name for name, _ in stage_columns]
    stage_name_set = {name.lower() for name in stage_names}
    target_business_names = [
        name for name, _ in target_columns if name.lower() not in engine_managed
    ]
    target_business_set = {name.lower() for name in target_business_names}

    if stage_name_set == target_business_set:
        return

    missing_in_stage = [n for n in target_business_names if n.lower() not in stage_name_set]
    if missing_in_stage:
        raise SchemaMismatchError(
            f"{qualify(target_object, database)}: staged SELECT is "
            "missing column(s) "
            f"{missing_in_stage} that the target already has — schema evolution only adds "
            "columns, it never removes them"
        )

    new_columns = [n for n in stage_names if n.lower() not in target_business_set]
    if not schema_evolution:
        raise SchemaMismatchError(
            f"{qualify(target_object, database)}: staged SELECT has "
            "new column(s) "
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
        (f"t.{name}" if name.lower() in old_business_types else f"CAST(NULL AS {dtype}) AS {name}")
        for name, dtype in stage_columns
    ]
    select_parts.extend(f"t.{col}" for col in engine_cols)

    schema_name, table_name = split_object_ref(target_object)
    evolve_table = f"{table_name}__etl_evolve"
    qualified_target = qualify(target_object, database)
    qualified_evolve = qualify(f"{schema_name}.{evolve_table}", database)

    conn.execute(text(f"DROP TABLE IF EXISTS {qualified_evolve}"))
    create_table_as(
        conn,
        qualified_evolve,
        f"SELECT {', '.join(select_parts)} FROM {qualified_target} AS t",
    )
    conn.execute(text(f"DROP TABLE {qualified_target}"))
    conn.execute(text(f"ALTER TABLE {qualified_evolve} RENAME TO {table_name}"))
    # [DEVIATION, 2026-09-20, E2-54] The drop-and-rename destroys the primary
    # key along with the old table. The surrogate key's *values* were carried
    # across above (ROW_ID is in engine_managed, so it rides along as a plain
    # BIGINT), which matters: AUD_BUSINESS_RULES_RESULTS rows reference them,
    # and regenerating would orphan every flagged row. So the column is
    # re-promoted to an identity primary key rather than re-created, with the
    # sequence restarted past the largest value already in it.
    #
    # Known, accepted limit of the same mechanism, unchanged: indexes and
    # grants on the original are *not* recreated.
    _restore_surrogate_key(conn, target_object, database)


def _add_hash_key(conn: Connection, stage: str, merge_compare_columns: list[str]) -> None:
    """Add HASH_KEY to `stage` after its shape is already confirmed against the target.

    Deliberately a follow-up ALTER, not baked into the stage's own CREATE ...
    AS SELECT: the schema check (_check_or_evolve_schema) compares stage's
    columns against the target's *business* columns, and HASH_KEY is one of
    SCD1_MERGE/SCD2_MERGE's own engine-managed audit columns (excluded from
    that comparison on the target side) — computing it before the check
    would make stage carry a column the target-side comparison never
    expects to see, breaking the "shapes agree" check for every SCD run.
    """
    conn.execute(text(f"ALTER TABLE {stage} ADD COLUMN HASH_KEY VARCHAR(32)"))
    # [DEVIATION, 2026-09-20, E2-31] No alias on the UPDATE target — an alias
    # there is not ANSI and several dialects reject it. The hash expression is
    # built against the table name instead.
    conn.execute(
        text(f"UPDATE {stage} SET HASH_KEY = " f"{_hash_expression(merge_compare_columns, stage)}")
    )


def _dedupe_stage(
    conn: Connection,
    *,
    stage: str,
    merge_key: list[str],
    dedupe_order: str | None,
    target_object: str,
) -> str:
    """Ensure `stage` holds one row per MERGE_KEY. Returns the stage to merge from.

    [ADDITION, 2026-09-20, E2-04] Nothing checked this before, and the target
    has no primary key of its own to catch it either (E2-03), so two source
    rows sharing a key silently corrupted an SCD1 target on run 1 — both rows
    took the NOT EXISTS insert leg, leaving two "current" rows for one key,
    reported SUCCESS. Run 2, once any compared value changed, then died on
    `SET col = (SELECT ... WHERE t.k = s.k)` with a cardinality violation, and
    stayed dead: the duplicates were now in the *target*, so no retry could
    recover it without manual SQL.

    The check runs before any merge statement touches the target, so a bad
    source fails the run rather than corrupting anything.

    Per explicit decision, duplicates are resolved by a declared ordering
    (CFG_TASK_PARAMETERS.MERGE_DEDUPE_ORDER, an ORDER BY fragment such as
    "updated_at DESC"), and rejected outright when none is declared — the
    engine will not invent an ordering nobody gave it, since which row
    survives would then be undefined and could differ between runs.
    """
    key_sql = ", ".join(merge_key)
    duplicates = conn.execute(
        text(f"SELECT {key_sql} FROM {stage} GROUP BY {key_sql} HAVING COUNT(*) > 1 LIMIT 5")
    ).all()
    if not duplicates:
        return stage

    if not dedupe_order:
        sample = ", ".join(str(tuple(row)) for row in duplicates)
        raise HandlerError(
            f"{target_object}: SOURCE_SQL returns more than one row for the same "
            f"MERGE_KEY ({key_sql}) — e.g. {sample}. A merge needs one row per key. "
            "Either make SOURCE_SQL return one, or declare "
            "CFG_TASK_PARAMETERS.MERGE_DEDUPE_ORDER (an ORDER BY fragment, e.g. "
            "'updated_at DESC') to say which row should win."
        )

    # ROW_NUMBER() is ANSI SQL:2003 and available on every dialect this
    # project has touched. A new table rather than a DELETE, because deleting
    # duplicates in place needs a row identity (ctid, ROWID) that is
    # dialect-specific — the one thing this module works hardest to avoid.
    deduped = f"{stage}_dedup"
    columns_sql = ", ".join(name for name, _ in _fetch_columns(conn, stage))
    conn.execute(text(f"DROP TABLE IF EXISTS {deduped}"))
    conn.execute(
        text(
            f"CREATE TEMPORARY TABLE {deduped} AS SELECT {columns_sql} FROM ("
            f"SELECT {columns_sql}, ROW_NUMBER() OVER ("
            f"PARTITION BY {key_sql} ORDER BY {dedupe_order}) AS etl_dedupe_rn "
            f"FROM {stage}) AS ranked WHERE etl_dedupe_rn = 1"
        )
    )
    _drop_stage(conn, stage)
    return deduped


ROW_ID_COLUMN = "ROW_ID"


def _sequence_name(target_object: str, database: str) -> str:
    """Fully-qualified name for a target's own ROW_ID sequence (DuckDB only).

    [DEVIATION, 2026-09-21, E2-64] Schema-qualified, and created in the
    target's own schema. This was previously the bare table name, created
    unqualified -- so `staging.orders` and `marts.orders`, two perfectly
    ordinary targets, shared one sequence.

    Probed on a single connection that looks self-limiting: DuckDB refuses the
    `DROP SEQUENCE IF EXISTS` with a dependency error. But every task runs in
    its own process with its own connection, and there the DROP *succeeds
    silently*. Building the second table then reset the shared sequence to 1,
    and the next insert into the first table -- untouched by that run --
    collided with a ROW_ID it already held (Duplicate key ROW_ID: 2 violates
    primary key constraint), leaving it un-writable until someone repaired the
    sequence by hand. Verified at both levels, not reasoned about.
    """
    schema_name, table_name = split_object_ref(target_object)
    return qualify(
        f"{schema_name}.etl_seq_{table_name}_{ROW_ID_COLUMN.lower()}",
        database,
    )


def _row_id_insert_parts(conn: Connection, qualified_target: str) -> tuple[str, str]:
    """Extra (columns, values) an INSERT needs where ROW_ID cannot auto-fill itself.

    [ADDITION, 2026-09-22] Postgres fills ROW_ID from its identity and DuckDB
    from a sequence default, so their INSERTs never mention it. Iceberg has
    neither, so every insert has to supply the value.

    The base is read with its own query *before* the mutating statement runs,
    rather than as a subquery over the target inside the INSERT itself: this
    module's own rule for counts, and here it also avoids referencing a table
    in the same statement that is writing to it -- which engines disagree
    about.
    """
    if not _is_iceberg_backed(conn.dialect.name):
        return "", ""
    base = int(
        conn.execute(
            text(f"SELECT COALESCE(MAX({ROW_ID_COLUMN}), 0) FROM {qualified_target}")
        ).scalar_one()
    )
    return (
        f", {ROW_ID_COLUMN}",
        f", {base} + CAST(ROW_NUMBER() OVER () AS BIGINT)",
    )


def _is_iceberg_backed(dialect_name: str) -> bool:
    """Whether this warehouse stores its tables as Iceberg rather than natively."""
    return dialect_name.split("+", 1)[0] not in NATIVE_STORAGE_DIALECTS


def _add_computed_surrogate_key(conn: Connection, qualified: str) -> None:
    """Give an Iceberg-backed target a ROW_ID without an identity column or a sequence.

    [ADDITION, 2026-09-22] Iceberg has neither. The spec has no constraint
    concept at all -- no primary keys, no identity, no sequences -- so the
    route both other warehouses take (Postgres `GENERATED ALWAYS AS IDENTITY`,
    DuckDB a sequence default) does not exist here.

    ROW_ID is therefore *computed*: the largest value already in the table
    plus a row number over the rows being added. That keeps what ROW_ID is
    actually for -- a stable single-column key per row, which
    AUD_BUSINESS_RULES_RESULTS references -- and is what makes CLAUDE.md's
    single-column-key convention hold on every warehouse.

    **[CHOICE] What is genuinely given up, stated rather than implied:**
    uniqueness is not enforced by the database, because Iceberg cannot enforce
    it. Two concurrent writers to the same target could compute the same
    ROW_ID. That is acceptable here and nowhere near as bad as it sounds --
    `ux_pipeline_run_one_active` already permits only one active run per
    pipeline, and one task writes one target within it -- but it is a real
    difference from the Postgres path and `validate` reports it rather than
    pretending the convention is enforced.

    No `ADD PRIMARY KEY` is issued: Databricks accepts primary keys only as
    informational, unenforced metadata, and Trino/Iceberg rejects the
    statement outright.
    """
    # Rebuilt rather than ALTER-then-UPDATE: a correlated UPDATE assigning a
    # window function is not portable across these engines, and the
    # drop-and-rename shape is one this module already relies on in
    # _evolve_schema.
    rebuild = f"{qualified}__etl_rowid"
    conn.execute(text(f"DROP TABLE IF EXISTS {rebuild}"))
    create_table_as(
        conn,
        rebuild,
        f"SELECT *, CAST(ROW_NUMBER() OVER () AS BIGINT) AS {ROW_ID_COLUMN} FROM {qualified}",
    )
    conn.execute(text(f"DROP TABLE {qualified}"))
    conn.execute(text(f"ALTER TABLE {rebuild} RENAME TO {qualified.rsplit('.', 1)[-1]}"))


def _add_surrogate_key(conn: Connection, target_object: str, database: str) -> None:
    """Add the engine-generated identity primary key to a freshly created target.

    [DEVIATION, 2026-09-20, E2-54] Replaces the `PRIMARY_KEY` parameter, which
    named an existing *business* column. Per explicit correction — "all primary
    keys are basically identity columns. merge keys are natural keys" — the
    engine generates the key instead.

    The parameter version was not merely awkward, it was unusable on an SCD2
    target: SCD2 holds several rows per merge key by design, so declaring the
    natural key as PRIMARY_KEY worked for exactly one run and then failed
    permanently with a unique violation, leaving the target holding only the
    old version of every changed row. Reproduced against real Postgres.

    A surrogate identity key makes CLAUDE.md's single-column-primary-key
    convention satisfiable on *every* target, SCD2 included, with no exemption
    for `validate` to know about. CFG_BUSINESS_RULES.BUSINESS_RULE_KEY_COLUMN
    should name this column.

    [CHOICE] Unprefixed `ROW_ID`, matching the other engine-managed columns
    (PIPELINE_RUN_ID, HASH_KEY, CREATE_DATE) rather than introducing a prefix
    convention for one column.

    [DEVIATION, 2026-09-20] DuckDB takes a different route, verified against a
    real database: it rejects `ALTER TABLE ... ADD COLUMN ... GENERATED ALWAYS
    AS IDENTITY` outright ("Adding generated columns after table creation is
    not supported yet"), but a sequence plus a column `DEFAULT nextval(...)`
    behaves identically — it backfills the rows already there and auto-fills
    every later insert.
    """
    qualified = qualify(target_object, database)
    if _is_iceberg_backed(conn.dialect.name):
        _add_computed_surrogate_key(conn, qualified)
        return
    if conn.dialect.name == "duckdb":
        sequence = _sequence_name(target_object, database)
        conn.execute(text(f"DROP SEQUENCE IF EXISTS {sequence}"))
        conn.execute(text(f"CREATE SEQUENCE {sequence} START 1"))
        conn.execute(
            text(
                f"ALTER TABLE {qualified} ADD COLUMN {ROW_ID_COLUMN} BIGINT "
                f"DEFAULT nextval('{sequence}')"
            )
        )
    else:
        conn.execute(
            text(
                f"ALTER TABLE {qualified} ADD COLUMN {ROW_ID_COLUMN} BIGINT "
                "GENERATED ALWAYS AS IDENTITY"
            )
        )
    conn.execute(text(f"ALTER TABLE {qualified} ADD PRIMARY KEY ({ROW_ID_COLUMN})"))


def _restore_surrogate_key(conn: Connection, target_object: str, database: str) -> None:
    """Re-promote a carried-across ROW_ID column to a primary key after a rebuild."""
    qualified = qualify(target_object, database)
    schema_name, table_name = split_object_ref(target_object)
    has_row_id = any(
        name.lower() == ROW_ID_COLUMN.lower()
        for name, _ in _fetch_columns(conn, table_name, schema=schema_name)
    )
    if not has_row_id:
        # A target that predates the surrogate key: give it one now.
        _add_surrogate_key(conn, target_object, database)
        return

    next_value = int(
        conn.execute(
            text(f"SELECT COALESCE(MAX({ROW_ID_COLUMN}), 0) + 1 FROM {qualified}")
        ).scalar_one()
    )
    if _is_iceberg_backed(conn.dialect.name):
        # Nothing to restore: an Iceberg ROW_ID is a plain BIGINT with no
        # sequence or identity behind it, and the rebuild carried its values
        # across like any other column.
        return
    if conn.dialect.name == "duckdb":
        # The sequence survived the table rebuild (it is a separate object),
        # but its position has to skip whatever the carried-across values
        # already occupy.
        sequence = _sequence_name(target_object, database)
        conn.execute(text(f"DROP SEQUENCE IF EXISTS {sequence}"))
        conn.execute(text(f"CREATE SEQUENCE {sequence} START {next_value}"))
        conn.execute(
            text(
                f"ALTER TABLE {qualified} ALTER COLUMN {ROW_ID_COLUMN} "
                f"SET DEFAULT nextval('{sequence}')"
            )
        )
    else:
        # NOT NULL first: Postgres refuses to attach an identity to a nullable
        # column, and the column arrives nullable because CREATE TABLE AS
        # SELECT does not carry the constraint across.
        conn.execute(text(f"ALTER TABLE {qualified} ALTER COLUMN {ROW_ID_COLUMN} SET NOT NULL"))
        conn.execute(
            text(
                f"ALTER TABLE {qualified} ALTER COLUMN {ROW_ID_COLUMN} "
                "ADD GENERATED ALWAYS AS IDENTITY"
            )
        )
        conn.execute(
            text(
                f"ALTER TABLE {qualified} ALTER COLUMN {ROW_ID_COLUMN} "
                f"RESTART WITH {next_value}"
            )
        )
    conn.execute(text(f"ALTER TABLE {qualified} ADD PRIMARY KEY ({ROW_ID_COLUMN})"))


def _create_target_shape(
    conn: Connection,
    *,
    stage: str,
    target_object: str,
    database: str,
    audit_columns: tuple[str, ...],
) -> None:
    """Create `target_object` empty, shaped from `stage` plus `audit_columns`.

    [ADDITION, 2026-09-20, E2-42] Per explicit instruction: "apart from drop
    and delete, everything should create a table if the target does not exist,
    using select query and adding audit columns". Previously only CREATE_TABLE
    and SETUP_TABLE created anything, so a first run of OVERWRITE_TABLE or
    either SCD merge failed on a target nobody had bootstrapped yet.

    Rows are excluded (`WHERE 1 = 0`) because the caller's own write path —
    TRUNCATE-and-insert, or the merge's NOT EXISTS leg — is what populates it,
    and stamps the audit columns correctly while doing so.
    """
    select_parts = [f"s.{name}" for name, _ in _fetch_columns(conn, stage)]
    select_parts.append("CAST(NULL AS BIGINT) AS PIPELINE_RUN_ID")
    select_parts.extend(
        f"CAST(NULL AS {AUDIT_COLUMN_TYPES[col]}) AS {col}" for col in audit_columns
    )
    create_table_as(
        conn,
        qualify(target_object, database),
        f"SELECT {', '.join(select_parts)} FROM {stage} AS s WHERE 1 = 0",
    )
    _add_surrogate_key(conn, target_object, database)


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
    warehouse = ctx.config.warehouse
    if warehouse is None:  # pragma: no cover - active_database already raised
        raise HandlerError("no [Warehouse] section configured in craft-connector.yml")
    updated_by = warehouse.active.user
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
    create_table_as(
        conn,
        qualified_target,
        f"SELECT s.*, CAST({ctx.pipeline_run_id} AS BIGINT) AS PIPELINE_RUN_ID "
        f"FROM {stage} AS s",
    )
    _add_surrogate_key(conn, target_object, database)
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
    sibling = fetch_sibling_target_writer(cfg_conn, ctx.pipeline_id, ctx.task_id, target_object)
    audit_columns = AUDIT_COLUMNS.get(sibling.sql_action, ()) if sibling else ()

    stage = _build_stage(conn, ctx.task_run_id, select_sql, empty=True)
    stage_columns = _fetch_columns(conn, stage)
    select_parts = [f"s.{name}" for name, _ in stage_columns]
    select_parts.append("CAST(NULL AS BIGINT) AS PIPELINE_RUN_ID")
    for col in audit_columns:
        select_parts.append(f"CAST(NULL AS {AUDIT_COLUMN_TYPES[col]}) AS {col}")

    qualified_target = qualify(target_object, database)
    conn.execute(text(f"DROP TABLE IF EXISTS {qualified_target}"))
    create_table_as(conn, qualified_target, f"SELECT {', '.join(select_parts)} FROM {stage} AS s")
    _add_surrogate_key(conn, target_object, database)
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
        schema_evolution=_schema_evolution_enabled(ctx),
    )
    stage_columns = [name for name, _ in _fetch_columns(conn, stage)]
    qualified_target = qualify(target_object, database)
    conn.execute(text(f"TRUNCATE TABLE {qualified_target}"))
    columns_sql = ", ".join(stage_columns)
    rid_cols, rid_vals = _row_id_insert_parts(conn, qualified_target)
    conn.execute(
        text(
            f"INSERT INTO {qualified_target} "
            f"({columns_sql}, PIPELINE_RUN_ID, UPDATE_DATE{rid_cols}) "
            f"SELECT {columns_sql}, :pipeline_run_id, :now{rid_vals} FROM {stage}"
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
    stage = _dedupe_stage(
        conn,
        stage=stage,
        merge_key=merge_key,
        dedupe_order=ctx.task_params.get("MERGE_DEDUPE_ORDER"),
        target_object=target_object,
    )
    _check_or_evolve_schema(
        conn,
        target_object=target_object,
        database=database,
        action="SCD1_MERGE",
        stage=stage,
        schema_evolution=_schema_evolution_enabled(ctx),
    )
    _add_hash_key(conn, stage, merge_compare_columns)
    stage_columns = [name for name, _ in _fetch_columns(conn, stage)]
    # HASH_KEY is deliberately included here (not treated as a key column) —
    # a matched-and-changed row must have its target-side HASH_KEY refreshed
    # too, or it would permanently compare as "changed" on every future run.
    non_key_columns = [c for c in stage_columns if c.lower() not in {k.lower() for k in merge_key}]
    qualified_target = qualify(target_object, database)

    key_match = " AND ".join(f"t.{k} = s.{k}" for k in merge_key)
    # A single HASH_KEY comparison, not an OR-chain over every compare
    # column — "scd tables should also have hashkey created by
    # merge_compare columns," per explicit instruction.
    changed = "t.HASH_KEY IS DISTINCT FROM s.HASH_KEY"

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
    rid_cols, rid_vals = _row_id_insert_parts(conn, qualified_target)
    conn.execute(
        text(
            f"INSERT INTO {qualified_target} ({columns_sql}, PIPELINE_RUN_ID, CREATE_DATE, "
            f"CREATED_BY, UPDATE_DATE, UPDATED_BY, DELETE_FLAG{rid_cols}) "
            f"SELECT {columns_sql}, :pipeline_run_id, :now, :updated_by, :now, :updated_by, "
            f"'N'{rid_vals} "
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
    stage = _dedupe_stage(
        conn,
        stage=stage,
        merge_key=merge_key,
        dedupe_order=ctx.task_params.get("MERGE_DEDUPE_ORDER"),
        target_object=target_object,
    )
    _check_or_evolve_schema(
        conn,
        target_object=target_object,
        database=database,
        action="SCD2_MERGE",
        stage=stage,
        schema_evolution=_schema_evolution_enabled(ctx),
    )
    _add_hash_key(conn, stage, merge_compare_columns)
    stage_columns = [name for name, _ in _fetch_columns(conn, stage)]
    qualified_target = qualify(target_object, database)

    key_match = " AND ".join(f"t.{k} = s.{k}" for k in merge_key)
    # A single HASH_KEY comparison, not an OR-chain over every compare
    # column — "scd tables should also have hashkey created by
    # merge_compare columns," per explicit instruction. The new version
    # inserted for a changed row carries its own freshly-computed HASH_KEY
    # (via columns_sql/stage below) — no separate refresh needed the way
    # SCD1_MERGE's matched-in-place UPDATE requires.
    changed = "t.HASH_KEY IS DISTINCT FROM s.HASH_KEY"

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
    rid_cols, rid_vals = _row_id_insert_parts(conn, qualified_target)
    conn.execute(
        text(
            f"INSERT INTO {qualified_target} ({columns_sql}, PIPELINE_RUN_ID, CREATE_DATE, "
            f"CREATED_BY, UPDATE_DATE, UPDATED_BY, DELETE_FLAG, ACTIVE_FLAG{rid_cols}) "
            "SELECT "
            f"{columns_sql}, :pipeline_run_id, :now, :updated_by, :now, :updated_by, "
            f"'N', 'Y'{rid_vals} "
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
    rid_cols, rid_vals = _row_id_insert_parts(conn, qualified_target)
    conn.execute(
        text(
            f"INSERT INTO {qualified_target} ({columns_sql}, PIPELINE_RUN_ID, CREATE_DATE, "
            f"CREATED_BY, UPDATE_DATE, UPDATED_BY, DELETE_FLAG, ACTIVE_FLAG{rid_cols}) "
            "SELECT "
            f"{columns_sql}, :pipeline_run_id, :now, :updated_by, :now, :updated_by, "
            f"'N', 'Y'{rid_vals} "
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
    sibling = fetch_sibling_target_writer(cfg_conn, ctx.pipeline_id, ctx.task_id, target_object)
    if sibling is None or sibling.sql_action != "CREATE_TABLE":
        raise HandlerError(
            f"DROP_TABLE refused for {target_object!r}: no other active task in this pipeline "
            "creates it via SQL_ACTION=CREATE_TABLE — DROP_TABLE only ever removes tables this "
            "pipeline itself is responsible for creating"
        )
    # "created by this pipeline using create_table before this drop table
    # step" — per explicit instruction, not just declared in CFG_ somewhere:
    # the CREATE_TABLE sibling must have genuinely already run and succeeded
    # under *this* pipeline_run_id.
    sibling_status = fetch_task_run_status(cfg_conn, sibling.task_id, ctx.pipeline_run_id)
    if sibling_status != "SUCCESS":
        raise HandlerError(
            f"DROP_TABLE refused for {target_object!r}: its CREATE_TABLE task "
            f"(task_id={sibling.task_id}) hasn't completed successfully yet under this run "
            f"(status={sibling_status!r}) — DROP_TABLE requires that task to have already run"
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
        schema_name, table_name = split_object_ref(target_object)
        target_name_set = {
            name.lower() for name, _ in _fetch_columns(conn, table_name, schema=schema_name)
        }
        if "delete_flag" not in target_name_set:
            raise HandlerError(
                f"{qualify(target_object, database)}: target table is "
                "missing the DELETE_FLAG "
                "column that a soft DELETE_ROWS (HARD_DELETE not 'true') requires — run a "
                "SETUP_TABLE task against it first, fix its schema by hand, or set "
                "HARD_DELETE=true for this task"
            )
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
