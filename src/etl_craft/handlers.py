"""Per-HANDLER task execution — dispatches to sql_actions.py / business_rules.py / scripts.py.

The closed vocabulary from CLAUDE.md's Handlers section: PYTHON (ingestion
scripts, scripts.py), SQL (the closed action vocabulary, sql_actions.py),
BUSINESS_RULES (CFG_BUSINESS_RULES sequencing, business_rules.py),
EMAIL_ALERT (SMTP send, email_alert.py — see that module's own docstring for
how CLAUDE.md's open transport/substitution question was resolved).

`HandlerError`/`HandlerResult`/`TaskExecutionContext` live in execution.py,
not here — see that module's own docstring for why (this module dispatches
to sql_actions/business_rules/scripts, each of which needs those same three
names, so defining them here would make this module import the very modules
it dispatches to). Re-exported from here anyway, since runner.py (and
anything else written before this split) reasonably expects to find them on
`etl_craft.handlers`.
"""

from __future__ import annotations

from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_craft import business_rules, email_alert, scripts, sql_actions
from etl_craft.config import ConfigError
from etl_craft.execution import HandlerError, HandlerResult, TaskExecutionContext
from etl_craft.limits import task_timeout_seconds
from etl_craft.warehouse import data_db

__all__ = ["HandlerError", "HandlerResult", "TaskExecutionContext", "dispatch"]

HANDLERS = frozenset({"PYTHON", "SQL", "BUSINESS_RULES", "EMAIL_ALERT"})


def dispatch(engine: Engine, ctx: TaskExecutionContext) -> HandlerResult:
    """Run `ctx.handler`'s body. `engine` is the Engine DB — CFG_/AUD_ tables.

    Runs inside runner.py's crash-detection fork — anything raised here that
    *isn't* a HandlerError would otherwise crash the child process outright,
    losing its real message in favor of the parent's generic "died
    unexpectedly" fallback. So every predictable failure mode (a bad/missing
    [Warehouse] profile, a real SQLAlchemy error from the Data DB — a
    malformed author SELECT, a connection refused, ...) is caught here and
    re-raised as a HandlerError with its original message preserved, same as
    every explicit HandlerError sql_actions.py/business_rules.py/scripts.py
    themselves raise for a known-bad condition.
    """
    if ctx.handler not in HANDLERS:
        raise HandlerError(f"unknown HANDLER: {ctx.handler!r}")
    try:
        if ctx.handler == "PYTHON":
            with engine.begin() as cfg_conn:
                # Slightly under the task's own limit, so a wedged script
                # reports its own stderr rather than the blunter "terminated
                # by the task timeout" from runner.py's fork watchdog.
                task_limit = task_timeout_seconds(ctx)
                script_limit = max(task_limit - 30, 1) if task_limit else 0
                return scripts.execute(cfg_conn, ctx, script_limit)
        if ctx.handler == "EMAIL_ALERT":
            # Read-only against the Engine DB (fetch_failure_watch_messages)
            # -- no AUD_/CFG_ writes of its own, unlike PYTHON's offset-
            # tracker upsert, so a plain connect() suffices.
            with engine.connect() as cfg_conn:
                return email_alert.execute(cfg_conn, ctx)
        # SQL and BUSINESS_RULES both need the Data DB — one engine, disposed
        # after this single task's use, same lifecycle as validate.py's own
        # pairing.
        #
        # [DEVIATION, 2026-09-21, E2-61] Through warehouse.data_db, not a bare
        # build_data_engine: on a single-writer warehouse this queues behind
        # any other task already using it, instead of failing with DuckDB's
        # raw "Could not set lock on file". No-op for Postgres. The wait is
        # bounded by the task's own timeout — a task that would outlive its
        # limit waiting is better off failing with a clear reason.
        with data_db(ctx.config, engine, wait_seconds=task_timeout_seconds(ctx)) as data_engine:
            if ctx.handler == "SQL":
                # sql_actions.py's own writes must be atomic (per explicit
                # instruction) — one Data DB transaction for the whole
                # action. cfg_conn is read-only here (fetch_sibling_target_
                # sql_action), so sharing engine's transaction costs nothing.
                with engine.begin() as cfg_conn, data_engine.begin() as data_conn:
                    return sql_actions.execute(data_conn, cfg_conn, ctx)
            # business_rules.py manages its own per-rule connections to both
            # databases (see its own module docstring's "[Bug caught and
            # fixed]" and "Sequencing" notes) — genuine same-wave
            # parallelism needs a fresh Data DB connection per thread, not
            # one shared Connection, so it takes data_engine directly.
            return business_rules.execute(data_engine, engine, ctx)
    except HandlerError:
        raise
    except (ConfigError, SQLAlchemyError) as exc:
        raise HandlerError(str(exc)) from exc
