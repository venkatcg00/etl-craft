"""``HANDLER=BUSINESS_RULES``: check a task's ``CFG_BUSINESS_RULES`` and flag the rows that fail.

A rule names a warehouse table (``TARGET_TABLE``, ``schema.table`` or ``database.schema.table``),
the column that identifies a row (``BUSINESS_RULE_KEY_COLUMN``) and a condition
(``BUSINESS_RULE_SQL``): a correlated ``SELECT`` that returns a row when the table's row ``t``
breaks the rule, such as ``SELECT 1 FROM sales.customers c WHERE c.id = t.customer_id AND
c.active = 'N'``. For each rule the engine:

1. finds the keys of the rows in scope that break it:
   ``SELECT DISTINCT t.<key> FROM <table> t WHERE <scope> AND EXISTS (<rule>)``;
2. flags each such key not already flagged, as a row of ``AUD_BUSINESS_RULES_RESULTS`` with
   the rule's ``BUSINESS_RULE_TYPE``;
3. clears the flags of keys it flagged before whose rows in scope no longer break it, and of
   keys that no longer have a row in the table at all.

The scope is the rows the current run wrote (``t.PIPELINE_RUN_ID = <run>``), or every row when
the task is forced; on a table with ``ACTIVE_FLAG`` (an ``SCD2_MERGE`` target) only active
versions are judged. A forced run judges the whole table, so every flag it does not find again
is cleared. Rules run in waves by ``SEQUENCE_NUMBER``: the rules of a wave run in
parallel, at most ``Orchestration.Max_parallel_tasks`` at once, and the next wave starts once
every rule of this one has finished. A rule that fails is recorded ``FAILED`` and the rest of
its wave still runs; then the task fails, naming every rule that failed. A rule that already
succeeded under the task run is not run again, unless the task itself is run again after it
succeeded.

Each rule records its outcome in ``AUD_BUSINESS_RULES_RUN_LOG`` in a transaction of its own.
"""

from __future__ import annotations

import contextlib
import contextvars
import itertools
import logging
import time
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.config.targets import active_catalog
from etl_craft.core.errors import ConfigurationError, HandlerError, SqlGuardError
from etl_craft.core.text import (
    as_subquery,
    is_safe_identifier,
    qualify,
    split_object_ref,
    split_statements,
)
from etl_craft.engine.repository.business_rules import (
    BusinessRule,
    begin_rule_run,
    clear_rule_keys,
    fetch_active_rule_keys,
    fetch_business_rules_for_task,
    finish_rule_run,
    flag_rule_keys,
)
from etl_craft.handlers.registry import HandlerResult, TaskContext
from etl_craft.handlers.sql.analysis import read_only_problem
from etl_craft.warehouse.connection import open_warehouse, warehouse_dialect

logger = logging.getLogger(__name__)

KEY_CHUNK = 500
"""How many flagged keys one clearing query checks at a time."""


@dataclass(frozen=True)
class RuleOutcome:
    """What one rule did: the keys it flagged and cleared, or that it had already run."""

    flagged: int = 0
    cleared: int = 0
    skipped: bool = False


@dataclass(frozen=True)
class _Rule:
    """A checked rule, with its table's full name."""

    rule: BusinessRule
    table: str


class _RuleFailedError(Exception):
    """A rule failed; the message names it, the step and the cause."""


def run(context: TaskContext, engine_db: Engine) -> HandlerResult:
    """Run every active business rule of the task, wave by wave; return flagged and cleared."""
    config = context.config
    if config.warehouse is None:
        raise ConfigurationError("a BUSINESS_RULES task needs a Warehouse section")
    with engine_db.connect() as conn:
        rules = fetch_business_rules_for_task(conn, context.task_id)
    if not rules:
        raise HandlerError(
            f"task {context.task_code} has HANDLER=BUSINESS_RULES but no active row in "
            "CFG_BUSINESS_RULES"
        )
    catalog = active_catalog(config)
    sqlglot_dialect = warehouse_dialect(config).sqlglot_dialect
    checked = [check_rule(rule, catalog, sqlglot_dialect) for rule in rules]
    scope = "1=1" if context.force else f"t.PIPELINE_RUN_ID = {int(context.pipeline_run_id)}"
    string_type = warehouse_dialect(config).string_type
    logger.info(
        "%d business rule(s) in %d wave(s), over %s",
        len(checked),
        len({rule.sequence_number for rule in rules}),
        "every row (forced)" if context.force else f"pipeline_run_id={context.pipeline_run_id}",
    )
    flagged = cleared = 0
    with open_warehouse(config, engine_db) as warehouse:
        runner = _RuleRunner(context, engine_db, warehouse, scope, string_type)
        for number, wave in itertools.groupby(checked, key=lambda r: r.rule.sequence_number):
            outcomes = runner.wave(number, list(wave))
            flagged += sum(outcome.flagged for outcome in outcomes)
            cleared += sum(outcome.cleared for outcome in outcomes)
    logger.info("business rules done: %d key(s) flagged, %d cleared", flagged, cleared)
    return HandlerResult(insert_count=flagged, update_count=cleared)


def check_rule(rule: BusinessRule, catalog: str, dialect: str | None = None) -> _Rule:
    """Check a rule's definition before any rule runs; ``SqlGuardError`` naming what is wrong.

    ``dialect`` is the warehouse's sqlglot dialect the rule's SQL is read in.
    """
    name = f"business rule {rule.business_rule_name!r} (BUSINESS_RULE_ID={rule.business_rule_id})"
    if not is_safe_identifier(rule.business_rule_key_column):
        raise SqlGuardError(
            f"{name}: BUSINESS_RULE_KEY_COLUMN={rule.business_rule_key_column!r} is not a plain "
            "column name"
        )
    try:
        split_object_ref(rule.target_table, param_name="TARGET_TABLE")
    except HandlerError as error:
        raise SqlGuardError(f"{name}: {error}") from error
    statements = split_statements(rule.business_rule_sql)
    if len(statements) != 1:
        raise SqlGuardError(
            f"{name}: BUSINESS_RULE_SQL must be one correlated SELECT; it holds "
            f"{len(statements)} statements"
        )
    problem = read_only_problem(statements[0], dialect)
    if problem is not None:
        raise SqlGuardError(f"{name}: BUSINESS_RULE_SQL must be a read-only SELECT; it {problem}")
    return _Rule(rule, qualify(rule.target_table, catalog))


class _RuleRunner:
    """Runs rules against one warehouse engine, recording each in the Engine DB."""

    def __init__(
        self,
        context: TaskContext,
        engine_db: Engine,
        warehouse: Engine,
        scope: str,
        string_type: str,
    ) -> None:
        self.context = context
        self.engine_db = engine_db
        self.warehouse = warehouse
        self.scope = scope
        self.string_type = string_type

    def wave(self, number: int, rules: list[_Rule]) -> list[RuleOutcome]:
        """Run one wave's rules in parallel; ``HandlerError`` naming every rule that failed."""
        names = ", ".join(rule.rule.business_rule_name for rule in rules)
        logger.info("wave SEQUENCE_NUMBER=%d: %s", number, names)
        workers = min(max(self.context.config.limits.max_parallel_tasks, 1), len(rules))
        with ThreadPoolExecutor(workers, thread_name_prefix="etl-craft-rule") as pool:
            futures = [
                pool.submit(contextvars.copy_context().run, self._run_rule, rule) for rule in rules
            ]
        outcomes: list[RuleOutcome] = []
        failures: list[str] = []
        for future in futures:
            try:
                outcomes.append(future.result())
            except _RuleFailedError as failure:
                failures.append(str(failure))
        if failures:
            raise HandlerError(
                f"{len(failures)} of {len(rules)} business rule(s) failed in wave "
                f"SEQUENCE_NUMBER={number}: " + "; ".join(failures)
            )
        return outcomes

    def _run_rule(self, checked: _Rule) -> RuleOutcome:
        rule = checked.rule
        now = datetime.now(UTC)
        with self.engine_db.begin() as conn:
            binding = begin_rule_run(conn, rule.business_rule_id, self.context.task_run_id, now)
        if binding.already_succeeded and not (self.context.force or self.context.rerun):
            logger.info(
                "rule %s already succeeded under this task run; not run again",
                rule.business_rule_name,
            )
            return RuleOutcome(skipped=True)
        started = time.monotonic()
        step = "prepare"
        try:
            step = "read the table's columns"
            scope = self._scope(checked)
            step = "find the keys that break the rule"
            failing = self._keys(checked, scope, "EXISTS")
            with self.engine_db.connect() as conn:
                active = fetch_active_rule_keys(conn, rule.business_rule_id)
            new = sorted(failing - active)
            step = "find the flagged keys that now pass"
            unflagged = sorted(active - failing)
            if self.context.force:
                # The whole table was judged: a flag not found again no longer applies.
                passing = unflagged
            else:
                passing = self._now_passing(checked, scope, unflagged)
                step = "find the flagged keys whose rows are gone"
                passing += self._gone(checked, scope, sorted(set(unflagged) - set(passing)))
            step = "record the flags"
            with self.engine_db.begin() as conn:
                flag_rule_keys(conn, rule, binding.business_rule_run_id, new, now)
                clear_rule_keys(conn, rule.business_rule_id, passing, now)
                finish_rule_run(conn, binding.business_rule_run_id, "SUCCESS", datetime.now(UTC))
        except Exception as error:
            with contextlib.suppress(Exception), self.engine_db.begin() as conn:
                finish_rule_run(conn, binding.business_rule_run_id, "FAILED", datetime.now(UTC))
            cause = getattr(error, "orig", None) or error
            detail = str(cause).strip().splitlines()[0] if str(cause).strip() else repr(cause)
            logger.error(
                "rule %s on %s: %s failed: %s",
                rule.business_rule_name,
                checked.table,
                step,
                detail,
            )
            raise _RuleFailedError(
                f"{rule.business_rule_name} on {checked.table}: {step} failed: "
                f"{type(cause).__name__}: {detail}"
            ) from error
        logger.info(
            "rule %s on %s: %d key(s) break it, %d newly flagged, %d cleared (%.2fs)",
            rule.business_rule_name,
            checked.table,
            len(failing),
            len(new),
            len(passing),
            time.monotonic() - started,
        )
        return RuleOutcome(flagged=len(new), cleared=len(passing))

    def _key(self, checked: _Rule) -> str:
        return f"CAST(t.{checked.rule.business_rule_key_column} AS {self.string_type})"

    def _scope(self, checked: _Rule) -> str:
        """Return the rows the rule judges: the run's scope, and only active versions."""
        rows = self._query(f"SELECT * FROM {checked.table} WHERE 1 = 0", {}, keys=True)
        if any(str(name).lower() == "active_flag" for name in rows):
            return f"{self.scope} AND t.ACTIVE_FLAG = 'Y'"
        return self.scope

    def _keys(self, checked: _Rule, scope: str, test: str) -> set[str]:
        sql = (
            f"SELECT DISTINCT {self._key(checked)} FROM {checked.table} t "
            f"WHERE {scope} AND {test} {as_subquery(checked.rule.business_rule_sql)}"
        )
        return {str(row[0]) for row in self._query(sql, {})}

    def _now_passing(self, checked: _Rule, scope: str, flagged: Sequence[str]) -> list[str]:
        """Return which of the flagged keys have rows in scope that no longer break the rule."""
        return self._chunked(
            checked,
            flagged,
            f"{scope} AND {self._key(checked)} IN :keys "
            f"AND NOT EXISTS {as_subquery(checked.rule.business_rule_sql)}",
        )

    def _gone(self, checked: _Rule, scope: str, flagged: Sequence[str]) -> list[str]:
        """Return which of the flagged keys have no row left in the table (active, if versioned)."""
        active_only = " AND t.ACTIVE_FLAG = 'Y'" if "t.ACTIVE_FLAG" in scope else ""
        present = set(
            self._chunked(checked, flagged, f"{self._key(checked)} IN :keys{active_only}")
        )
        return sorted(key for key in flagged if key not in present)

    def _chunked(self, checked: _Rule, keys: Sequence[str], where: str) -> list[str]:
        """Return which of ``keys`` the rows matching ``where`` have, ``KEY_CHUNK`` at a time.

        ``where`` binds the chunk as ``:keys``.
        """
        found: list[str] = []
        for start in range(0, len(keys), KEY_CHUNK):
            chunk = list(keys[start : start + KEY_CHUNK])
            sql = f"SELECT DISTINCT {self._key(checked)} FROM {checked.table} t WHERE {where}"
            found += [str(row[0]) for row in self._query(sql, {"keys": chunk}, expanding=True)]
        return sorted(found)

    def _query(
        self, sql: str, params: dict[str, object], *, expanding: bool = False, keys: bool = False
    ) -> list[Any]:
        """Return the rows ``sql`` selects, or with ``keys`` the names of its columns."""
        logger.debug("%s", sql)
        statement = text(sql)
        if expanding:
            statement = statement.bindparams(bindparam("keys", expanding=True))
        try:
            with self.warehouse.connect() as conn:
                result = conn.execute(
                    statement,
                    {
                        "pipeline_id": self.context.pipeline_id,
                        "pipeline_run_id": self.context.pipeline_run_id,
                        "task_run_id": self.context.task_run_id,
                        **params,
                    },
                )
                if keys:
                    return list(result.keys())
                return [tuple(row) for row in result]
        except SQLAlchemyError:
            logger.error("the failing statement was:\n%s", sql)
            raise
