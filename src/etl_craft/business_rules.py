"""HANDLER=BUSINESS_RULES execution — drives CFG_BUSINESS_RULES rows for one task.

Per explicit instruction, the check itself is an EXISTS-shaped query against
the rule's own TARGET_TABLE (in the warehouse), scoped to the current run:

    SELECT DISTINCT t.<key_column>
    FROM <target_table> t
    WHERE t.PIPELINE_RUN_ID = :pipeline_run_id   -- or 1=1, see below
    AND EXISTS (<business_rule_sql>)

Every key this returns gets flagged (a new AUD_BUSINESS_RULES_RESULTS row,
if not already actively flagged for this rule). The same shape run with
NOT EXISTS instead — "then again not exists on the same query to check if
we can deactivate any history of the results that are passing now" — finds
keys that no longer violate the rule, deactivating whatever active flagged
row they still have.

[ADDITION] BUSINESS_RULE_SQL is a correlated condition, not a standalone
query — the outer target row is aliased `t`, and a rule author's SQL text
must reference it that way (e.g. `SELECT 1 FROM other_table o WHERE
o.some_key = t.some_key AND o.flag = 'BAD'`) for EXISTS/NOT EXISTS to
actually correlate. CLAUDE.md doesn't name this convention anywhere — it's
this module's own, and there's no way to check a rule follows it without a
real SQL parser (ruled out per Non-goals), so a rule that doesn't correlate
just silently matches every row or no rows, same failure mode as a
hand-written EXISTS clause anyone gets wrong.

[CHOICE] "a manual br task execution will run for all data" is read as: a
task run via `--force` (this codebase's existing "bypass normal gating,
just run this standalone" signal, see execution.TaskExecutionContext.force)
scans the whole TARGET_TABLE (WHERE 1=1) instead of scoping to the current
PIPELINE_RUN_ID. A pipeline-triggered run always scopes to PIPELINE_RUN_ID —
which, for a FULL-refresh target, still covers every row anyway, since a
full refresh re-stamps PIPELINE_RUN_ID onto every row it (re)writes.

[ADDITION] BUSINESS_RULE_TYPE (INCOMPLETE/REJECT/REPORT, post-signoff)
classifies what's found, copied verbatim onto AUD_BUSINESS_RULES_RESULTS.STATUS
— execution is identical for all three; this module never branches on it.

Cross-database mechanics: the target data lives in the warehouse, but
AUD_BUSINESS_RULES_RUN_LOG/AUD_BUSINESS_RULES_RESULTS live in the Engine DB
— two separate connections/engines, so there is no single SQL statement that
can join them. Flagged/passing keys are therefore always pulled into Python
first (the EXISTS/NOT EXISTS query, run once against the warehouse), then
written to the Engine DB as a second, deliberate step — the same shape
crosspipe.py already uses for its own cross-connection comparisons.

[Bug caught and fixed before shipping, not after] Each rule gets its own
independently-committed Engine DB transaction (`engine.begin()`, opened
fresh per rule — this module takes an Engine, not a shared Connection),
rather than sharing handlers.py's one outer transaction across every rule
and both databases. A first version passed a single `cfg_conn` through:
when a later rule's warehouse query raised, the whole enclosing
`engine.begin()` block in handlers.py rolled back on the way out —
including the "FAILED" status this module had just written for the *broken*
rule, and any earlier rules' genuinely-succeeded results in the same task.
Found by asserting AUD_BUSINESS_RULES_RUN_LOG.STATUS == 'FAILED' after a
deliberately malformed rule and watching the row not exist at all.

[ADDITION] Sequencing, per explicit instruction ("it sequence would be
like a dense rank. run in waves. every rule sharing same number for a task
can run parallel"): CFG_BUSINESS_RULES.SEQUENCE_NUMBER groups rules into
waves (consecutive equal-SEQUENCE_NUMBER runs, since
cfg.fetch_business_rules_for_task already orders by SEQUENCE_NUMBER); one
wave fully completes — every rule in it, success or failure — before the
next wave starts, and a same-wave rule genuinely runs concurrently with its
wave-mates via a thread pool (these are I/O-bound DB round trips, not CPU
work, so threads — not the fork-based approach runner.py's own crash
detection uses for a different reason). Each thread opens its own warehouse
connection from `warehouse_engine` (never shares one — SQLAlchemy Connections
aren't safe for concurrent use across threads) and, thanks to the
independently-committed-per-rule transaction above, its own Engine DB
transaction too. If any rule in a wave fails, every other rule in that same
wave still runs to completion (parallel means genuinely independent, not
cancel-on-first-failure) — only once the whole wave finishes does the first
failure propagate, stopping any later wave from starting.
"""

from __future__ import annotations

import itertools
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Connection, Engine

from etl_craft.cfg import BusinessRuleDetail, fetch_business_rules_for_task
from etl_craft.execution import HandlerError, HandlerResult, TaskExecutionContext
from etl_craft.sql_actions import active_database, qualify


def _find_or_create_run_log(
    conn: Connection, business_rule_id: int, task_run_id: int
) -> tuple[int, bool]:
    """Resume-not-restart bookkeeping for one CFG_BUSINESS_RULES row under `task_run_id`.

    Returns (business_rule_run_id, already_succeeded).

    [ADDITION, 2026-09-20, E2-40] The second half of the tuple is new.
    CLAUDE.md's "retry resumes, not restarts" held at task level but not
    inside a BUSINESS_RULES task: this reused the existing row but never
    short-circuited on an existing SUCCESS, so a retry re-ran every rule in
    every earlier wave. Idempotent, but wasteful and inconsistent with the
    stated principle — a task with thirty rules that failed on the last one
    re-ran all thirty.
    """
    existing = conn.execute(
        text(
            "SELECT BUSINESS_RULE_RUN_ID AS business_rule_run_id, STATUS AS status "
            "FROM AUD_BUSINESS_RULES_RUN_LOG "
            "WHERE BUSINESS_RULE_ID = :business_rule_id AND TASK_RUN_ID = :task_run_id"
        ),
        {"business_rule_id": business_rule_id, "task_run_id": task_run_id},
    ).one_or_none()
    if existing is not None:
        return existing.business_rule_run_id, existing.status == "SUCCESS"
    return (
        conn.execute(
            text(
                "INSERT INTO AUD_BUSINESS_RULES_RUN_LOG (BUSINESS_RULE_ID, TASK_RUN_ID, STATUS) "
                "VALUES (:business_rule_id, :task_run_id, 'IN-PROGRESS') "
                "RETURNING BUSINESS_RULE_RUN_ID"
            ),
            {"business_rule_id": business_rule_id, "task_run_id": task_run_id},
        ).scalar_one(),
        False,
    )


def _mark_run_log(conn: Connection, business_rule_run_id: int, status: str) -> None:
    conn.execute(
        text(
            "UPDATE AUD_BUSINESS_RULES_RUN_LOG SET STATUS = :status, END_DATE = :now "
            "WHERE BUSINESS_RULE_RUN_ID = :id"
        ),
        {"id": business_rule_run_id, "status": status, "now": datetime.now(UTC)},
    )


def _fetch_keys(warehouse_conn: Connection, sql: str) -> list[str]:
    return [str(row[0]) for row in warehouse_conn.execute(text(sql)).all()]


def _fetch_already_active_keys(
    cfg_conn: Connection, business_rule_id: int, keys: list[str]
) -> set[str]:
    if not keys:
        return set()
    stmt = text(
        "SELECT BUSINESS_RULE_KEY AS business_rule_key FROM AUD_BUSINESS_RULES_RESULTS "
        "WHERE BUSINESS_RULE_ID = :business_rule_id AND ACTIVE_FLAG = 'Y' "
        "AND BUSINESS_RULE_KEY IN :keys"
    ).bindparams(bindparam("keys", expanding=True))
    return set(
        cfg_conn.execute(stmt, {"business_rule_id": business_rule_id, "keys": keys}).scalars().all()
    )


def _run_one_rule(
    warehouse_engine: Engine,
    engine: Engine,
    database: str,
    scope: str,
    ctx: TaskExecutionContext,
    rule: BusinessRuleDetail,
) -> tuple[int, int]:
    """Run one rule to completion; return (newly_flagged_count, deactivated_count)."""
    with engine.begin() as conn:
        business_rule_run_id, already_succeeded = _find_or_create_run_log(
            conn, rule.business_rule_id, ctx.task_run_id
        )
    if already_succeeded and not ctx.force:
        # E2-40: this rule already ran to completion under this task run. Its
        # results are in AUD_BUSINESS_RULES_RESULTS; re-running would re-derive
        # the same answer at full cost.
        #
        # --force deliberately bypasses this, as it bypasses every other gate:
        # a forced business-rule run means "scan all data", which is a
        # different question from the one the previous run answered under its
        # PIPELINE_RUN_ID scope. Caught by an existing test, not by review.
        return 0, 0
    qualified_target = qualify(rule.target_table, database)
    try:
        with warehouse_engine.connect() as warehouse_conn:
            failing_keys = _fetch_keys(
                warehouse_conn,
                f"SELECT DISTINCT t.{rule.business_rule_key_column} FROM {qualified_target} AS t "
                f"WHERE {scope} AND EXISTS ({rule.business_rule_sql})",
            )
            passing_keys = _fetch_keys(
                warehouse_conn,
                f"SELECT DISTINCT t.{rule.business_rule_key_column} FROM {qualified_target} AS t "
                f"WHERE {scope} AND NOT EXISTS ({rule.business_rule_sql})",
            )
    except Exception as exc:
        with engine.begin() as conn:
            _mark_run_log(conn, business_rule_run_id, "FAILED")
        raise HandlerError(
            f"business rule {rule.business_rule_name!r} failed to execute: {exc}"
        ) from exc

    with engine.begin() as conn:
        already_active = _fetch_already_active_keys(conn, rule.business_rule_id, failing_keys)
        new_keys = [key for key in failing_keys if key not in already_active]
        now = datetime.now(UTC)
        if new_keys:
            conn.execute(
                text(
                    "INSERT INTO AUD_BUSINESS_RULES_RESULTS "
                    "(BUSINESS_RULE_RUN_ID, BUSINESS_RULE_ID, BUSINESS_RULE_KEY, TARGET_TABLE, "
                    "STATUS, ACTIVE_FLAG, START_DATE) "
                    "VALUES (:run_id, :business_rule_id, :key, :target_table, :status, "
                    "'Y', :now)"
                ),
                [
                    {
                        "run_id": business_rule_run_id,
                        "business_rule_id": rule.business_rule_id,
                        "key": key,
                        "target_table": rule.target_table,
                        "status": rule.business_rule_type,
                        "now": now,
                    }
                    for key in new_keys
                ],
            )

        # Same "never trust the driver's own rowcount" discipline as
        # sql_actions.py: figure out exactly which passing keys are
        # currently active *before* deactivating them, rather than
        # reading back how many the UPDATE claims to have touched.
        to_deactivate = _fetch_already_active_keys(conn, rule.business_rule_id, passing_keys)
        if to_deactivate:
            stmt = text(
                "UPDATE AUD_BUSINESS_RULES_RESULTS SET ACTIVE_FLAG = 'N', END_DATE = :now "
                "WHERE BUSINESS_RULE_ID = :business_rule_id AND ACTIVE_FLAG = 'Y' "
                "AND BUSINESS_RULE_KEY IN :keys"
            ).bindparams(bindparam("keys", expanding=True))
            conn.execute(
                stmt,
                {
                    "business_rule_id": rule.business_rule_id,
                    "keys": list(to_deactivate),
                    "now": now,
                },
            )

        _mark_run_log(conn, business_rule_run_id, "SUCCESS")

    return len(new_keys), len(to_deactivate)


def _run_wave(
    warehouse_engine: Engine,
    engine: Engine,
    database: str,
    scope: str,
    ctx: TaskExecutionContext,
    wave: list[BusinessRuleDetail],
) -> tuple[int, int]:
    """Run every rule in `wave` concurrently; return summed (flagged, deactivated) counts.

    [DEVIATION, 2026-09-20, E2-19] Capped at `[Execution] Max_parallel_tasks`.
    This used to be `max_workers=len(wave)`, so a wave of thirty rules opened
    thirty warehouse connections at once — a connection budget set by how many
    rules someone happened to give the same SEQUENCE_NUMBER.
    """
    max_workers = max(ctx.config.limits.max_parallel_tasks, 1)
    if len(wave) == 1:
        # Not worth a thread pool for the overwhelmingly common case of one
        # rule per SEQUENCE_NUMBER.
        return _run_one_rule(warehouse_engine, engine, database, scope, ctx, wave[0])

    total_flagged = 0
    total_deactivated = 0
    first_error: HandlerError | None = None
    with ThreadPoolExecutor(max_workers=min(len(wave), max_workers)) as executor:
        futures = {
            executor.submit(
                _run_one_rule, warehouse_engine, engine, database, scope, ctx, rule
            ): rule
            for rule in wave
        }
        for future in as_completed(futures):
            try:
                flagged, deactivated = future.result()
            except HandlerError as exc:
                if first_error is None:
                    first_error = exc
                continue
            total_flagged += flagged
            total_deactivated += deactivated
    if first_error is not None:
        raise first_error
    return total_flagged, total_deactivated


def execute(warehouse_engine: Engine, engine: Engine, ctx: TaskExecutionContext) -> HandlerResult:
    """Run every active CFG_BUSINESS_RULES row for this task, wave by wave; return counts.

    `warehouse_engine`/`engine` are both Engines, not shared Connections — each
    rule opens its own connection to each database, required for genuine
    thread-safe concurrency within a wave (see this module's own "Sequencing"
    note above) and for the independently-committed-per-rule transaction
    ("[Bug caught and fixed]" above).
    """
    with engine.connect() as conn:
        rules = fetch_business_rules_for_task(conn, ctx.task_id)
    database = active_database(ctx.config)
    scope = "1=1" if ctx.force else f"t.PIPELINE_RUN_ID = {ctx.pipeline_run_id}"

    total_flagged = 0
    total_deactivated = 0
    for _sequence_number, wave_iter in itertools.groupby(rules, key=lambda r: r.sequence_number):
        flagged, deactivated = _run_wave(
            warehouse_engine, engine, database, scope, ctx, list(wave_iter)
        )
        total_flagged += flagged
        total_deactivated += deactivated

    return HandlerResult(insert_count=total_flagged, update_count=total_deactivated)
