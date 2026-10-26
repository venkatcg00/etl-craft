"""Run complete business-rule queries, publishing one serialized flag snapshot per rule.

Normal checks flag and clear current-run active rows only. Forced checks also clear flags
for corrected rows outside the run and rows deleted from the warehouse.
"""

from __future__ import annotations

import contextlib
import contextvars
import itertools
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import QueuePool

from etl_craft.config.targets import active_catalog
from etl_craft.core.enums import RunStatus
from etl_craft.core.errors import ConfigurationError, HandlerError, SqlGuardError
from etl_craft.core.text import (
    as_subquery,
    is_safe_identifier,
    is_safe_object_ref,
    qualify,
    split_object_ref,
)
from etl_craft.dialects.business_rules import (
    expand_scope,
    key_identifier,
    output_key,
    quote_output_key,
    rule_namespace,
    rule_problem,
    scope_cte,
    table_identifier,
)
from etl_craft.engine import locks
from etl_craft.engine.repository.business_rules import BusinessRule
from etl_craft.engine.task_store import RuleEnding, TaskStore
from etl_craft.handlers.registry import HandlerResult, TaskContext, task_store
from etl_craft.warehouse.connection import open_warehouse, warehouse_dialect

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RuleOutcome:
    """What one rule did: the keys it flagged and cleared, or that it had already run."""

    rule_id: int = 0
    returned: int = 0
    dropped: int = 0
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


def run(context: TaskContext, engine_db: Engine | TaskStore) -> HandlerResult:
    """Run every active business rule of the task, wave by wave; return flagged and cleared."""
    config = context.config
    if config.warehouse is None:
        raise ConfigurationError("a BUSINESS_RULES task needs a Warehouse section")
    store = task_store(context, engine_db)
    rules = list(store.state().rules)
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
    values: dict[str, object] = {}
    with open_warehouse(
        config,
        store,
        poolclass=QueuePool,
        pool_size=min(max(config.limits.max_parallel_tasks, 1), len(checked)),
        max_overflow=0,
    ) as warehouse:
        runner = _RuleRunner(context, store, warehouse, scope, string_type)
        for number, wave in itertools.groupby(checked, key=lambda r: r.rule.sequence_number):
            outcomes = runner.wave(number, list(wave))
            for outcome in outcomes:
                if not outcome.skipped:
                    values[f"BUSINESS_RULE_{outcome.rule_id}"] = {
                        "returned": outcome.returned,
                        "flagged": outcome.flagged,
                        "cleared": outcome.cleared,
                        "dropped": outcome.dropped,
                    }
            flagged += sum(outcome.flagged for outcome in outcomes)
            cleared += sum(outcome.cleared for outcome in outcomes)
    logger.info("business rules done: %d key(s) flagged, %d cleared", flagged, cleared)
    return HandlerResult(insert_count=flagged, update_count=cleared, variables=values)


def check_rule(rule: BusinessRule, catalog: str, dialect: str | None = None) -> _Rule:
    """Check a rule's definition before any rule runs; ``SqlGuardError`` naming what is wrong.

    ``dialect`` is the warehouse's sqlglot dialect the rule's SQL is read in.
    """
    name = f"business rule {rule.business_rule_name!r} (BUSINESS_RULE_ID={rule.business_rule_id})"
    if not rule.target_active:
        raise SqlGuardError(
            f"{name}: target contract {rule.target_table!r} has ACTIVE_FLAG=N; "
            "reactivate its CFG_TABLES row or deactivate this rule"
        )
    if not is_safe_identifier(rule.business_rule_key_column):
        raise SqlGuardError(
            f"{name}: BUSINESS_RULE_KEY_COLUMN={rule.business_rule_key_column!r} is not a plain "
            "column name"
        )
    try:
        split_object_ref(rule.target_table, param_name="TARGET_TABLE")
    except HandlerError as error:
        raise SqlGuardError(f"{name}: {error}") from error
    problem = rule_problem(rule.business_rule_sql, rule.business_rule_key_column, dialect)
    if problem is not None:
        raise SqlGuardError(f"{name}: BUSINESS_RULE_SQL {problem}")
    table = qualify(rule.target_table, catalog)
    if not is_safe_object_ref(table):
        raise SqlGuardError(
            f"{name}: TARGET_TABLE={table!r}; expected schema.table or catalog.schema.table "
            "with plain SQL identifiers; correct the CFG_TABLES catalog/schema/table contract"
        )
    return _Rule(rule, table)


def check_output(checked: _Rule, context: TaskContext, warehouse: Engine) -> str:
    """Resolve wildcard or unparsed projections through the warehouse without returning rows."""
    rule = checked.rule
    prefix = rule_namespace(rule.business_rule_sql)
    rows = prefix + "scope"
    query = expand_scope(rule.business_rule_sql, rows)
    dialect = warehouse_dialect(context.config).sqlglot_dialect
    if key := output_key(rule.business_rule_sql, rule.business_rule_key_column, dialect):
        return key
    sql = (
        f"WITH {scope_cte(checked.table, '1=0', rows, dialect)} "
        f"SELECT * FROM {as_subquery(query)} {prefix}output WHERE 1=0"
    )
    try:
        with (
            warehouse.connect() as conn,
            conn.execute(
                text(sql),
                {
                    "pipeline_id": context.pipeline_id,
                    "pipeline_run_id": context.pipeline_run_id,
                    "task_run_id": context.task_run_id,
                },
            ) as result,
        ):
            columns = list(result.keys())
    except SQLAlchemyError as error:
        raise SqlGuardError(
            f"business rule {rule.business_rule_name!r}: could not verify BUSINESS_RULE_SQL "
            f"output for BUSINESS_RULE_KEY_COLUMN={rule.business_rule_key_column!r}; "
            "expected a complete read-only SELECT returning that key; restore the target/warehouse "
            f"connection or correct the rule and run validate again; warehouse reported: {error}"
        ) from error
    found = next(
        (name for name in columns if name.lower() == rule.business_rule_key_column.lower()), None
    )
    if found is None:
        raise SqlGuardError(
            f"business rule {rule.business_rule_name!r}: BUSINESS_RULE_SQL returns {columns!r}; "
            f"expected BUSINESS_RULE_KEY_COLUMN={rule.business_rule_key_column!r}; "
            "select that target row key in BUSINESS_RULE_SQL"
        )
    return quote_output_key(found, dialect)


class _RuleRunner:
    """Runs rules against one warehouse engine, recording each in the Engine DB."""

    def __init__(
        self,
        context: TaskContext,
        store: TaskStore,
        warehouse: Engine,
        scope: str,
        string_type: str,
    ) -> None:
        self.context = context
        self.store = store
        self.warehouse = warehouse
        self.scope = scope
        self.string_type = string_type

    def wave(self, number: int, rules: list[_Rule]) -> list[RuleOutcome]:
        """Run one wave's rules in parallel; ``HandlerError`` naming every rule that failed."""
        names = ", ".join(rule.rule.business_rule_name for rule in rules)
        logger.info("wave SEQUENCE_NUMBER=%d: %s", number, names)
        agents = min(max(self.context.config.limits.max_parallel_tasks, 1), len(rules))
        with ThreadPoolExecutor(agents, thread_name_prefix="etl-craft-rule") as pool:
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
        lock = locks.business_rule(checked.rule.business_rule_id, checked.rule.business_rule_name)
        with lock.hold(self.store):
            try:
                return self._check_locked(checked)
            except _RuleFailedError as error:
                # This failure cannot publish flags; release without cancelling sibling reads.
                failure = error
        raise failure

    def _check_locked(self, checked: _Rule) -> RuleOutcome:
        rule = checked.rule
        binding = self.store.begin_rule(rule.business_rule_id)
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
            failing, in_scope, returned, dropped = self._keys(checked, scope)
            active = self.store.rule_keys(rule.business_rule_id)
            new = sorted(failing - active)
            step = "find the flagged keys that now pass"
            unflagged = sorted(active - failing)
            passing = unflagged if self.context.force else sorted(set(unflagged) & in_scope)
            step = "record the flags"
            self.store.end_rule(
                RuleEnding(
                    rule.business_rule_id,
                    binding.business_rule_run_id,
                    RunStatus.SUCCESS,
                    tuple(new),
                    tuple(passing),
                )
            )
        except Exception as error:
            with contextlib.suppress(Exception):
                self.store.end_rule(
                    RuleEnding(
                        rule.business_rule_id,
                        binding.business_rule_run_id,
                        RunStatus.FAILED,
                    )
                )
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
            "rule %s on %s: %d returned, %d flagged, %d cleared, %d dropped (%.2fs)",
            rule.business_rule_name,
            checked.table,
            returned,
            len(new),
            len(passing),
            dropped,
            time.monotonic() - started,
        )
        return RuleOutcome(rule.business_rule_id, returned, dropped, len(new), len(passing))

    def _scope(self, checked: _Rule) -> str:
        """Return the rows the rule judges: the run's scope, and only active versions."""
        table = table_identifier(
            checked.table, warehouse_dialect(self.context.config).sqlglot_dialect
        )
        rows = self._query(f"SELECT * FROM {table} WHERE 1 = 0", keys=True)
        if any(str(name).lower() == "active_flag" for name in rows):
            return f"{self.scope} AND t.ACTIVE_FLAG = 'Y'"
        return self.scope

    def _keys(self, checked: _Rule, scope: str) -> tuple[set[str], set[str], int, int]:
        """Evaluate the rule once and intersect its distinct text keys with scoped keys."""
        rule = checked.rule
        result_key = check_output(checked, self.context, self.warehouse)
        dialect = warehouse_dialect(self.context.config).sqlglot_dialect
        key = key_identifier(rule.business_rule_key_column, dialect)
        prefix = rule_namespace(rule.business_rule_sql)
        rows = prefix + "scope"
        query = expand_scope(rule.business_rule_sql, rows)

        sql = (
            f"WITH {scope_cte(checked.table, scope, rows, dialect)}, "
            f"{prefix}returned AS {as_subquery(query)}, "
            f"{prefix}keys AS (SELECT CAST({result_key} AS {self.string_type}) AS rule_key, "
            f"COUNT(*) AS occurrences FROM {prefix}returned "
            f"GROUP BY CAST({result_key} AS {self.string_type})), "
            f"{prefix}scope_keys AS (SELECT DISTINCT CAST({key} AS {self.string_type}) "
            f"AS rule_key FROM {rows}) "
            f"SELECT k.rule_key AS returned_key, s.rule_key AS scope_key, k.occurrences "
            f"FROM {prefix}keys k FULL OUTER JOIN {prefix}scope_keys s "
            "ON k.rule_key = s.rule_key"
        )
        failing: set[str] = set()
        in_scope: set[str] = set()
        returned = dropped = nulls = 0
        for row in self._query(sql):
            returned_key, scope_key, occurrences = row
            returned += int(occurrences or 0)
            if occurrences is not None and returned_key is None:
                nulls += int(occurrences)
            if scope_key is not None:
                in_scope.add(str(scope_key))
                if occurrences is not None:
                    failing.add(str(scope_key))
            elif returned_key is not None:
                dropped += int(occurrences)
        if nulls:
            raise HandlerError(
                f"business rule {rule.business_rule_name!r}: returned {nulls} NULL key(s) in "
                f"BUSINESS_RULE_KEY_COLUMN={rule.business_rule_key_column!r}; expected non-NULL "
                "target row keys; correct BUSINESS_RULE_SQL to return a key for every flagged row"
            )
        return failing, in_scope, returned, dropped

    def _query(self, sql: str, *, keys: bool = False) -> list[Any]:
        """Return the rows ``sql`` selects, or with ``keys`` the names of its columns."""
        logger.debug("%s", sql)
        statement = text(sql)
        try:
            with self.warehouse.connect() as conn:
                result = conn.execute(
                    statement,
                    {
                        "pipeline_id": self.context.pipeline_id,
                        "pipeline_run_id": self.context.pipeline_run_id,
                        "task_run_id": self.context.task_run_id,
                    },
                )
                try:
                    if keys:
                        return list(result.keys())
                    return [tuple(row) for row in result]
                finally:
                    result.close()
        except SQLAlchemyError:
            logger.error("the failing statement was:\n%s", sql)
            raise
