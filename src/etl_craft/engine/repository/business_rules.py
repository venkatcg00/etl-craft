"""Business rules: the rules a task runs, their run log and the keys they flag."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import bindparam
from sqlalchemy.engine import Connection

from etl_craft.engine.queries import statement


@dataclass(frozen=True)
class BusinessRule:
    """One active business rule of a task."""

    business_rule_id: int
    business_rule_name: str
    business_rule_sql: str
    business_rule_type: str
    business_rule_key_column: str
    target_table: str
    sequence_number: int


def fetch_business_rules_for_task(conn: Connection, task_id: int) -> list[BusinessRule]:
    """Return the active business rules of ``task_id``, in the order they run."""
    rows = conn.execute(statement(conn, "business_rules_for_task"), {"task_id": task_id})
    return [
        BusinessRule(
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


@dataclass(frozen=True)
class BusinessRuleTarget:
    """An active business rule's warehouse table and the key column it flags rows by."""

    business_rule_name: str
    target_table: str
    key_column: str


def fetch_business_rule_targets(conn: Connection) -> list[BusinessRuleTarget]:
    """Return every active business rule's table and key column, across all pipelines."""
    rows = conn.execute(statement(conn, "business_rule_targets"))
    return [
        BusinessRuleTarget(row.business_rule_name, row.target_table, row.key_column) for row in rows
    ]


@dataclass(frozen=True)
class RuleRunBinding:
    """A rule's run-log row under a task run, and whether it already succeeded there."""

    business_rule_run_id: int
    already_succeeded: bool


def begin_rule_run(
    conn: Connection, business_rule_id: int, task_run_id: int, now: datetime
) -> RuleRunBinding:
    """Bind ``business_rule_id`` to ``task_run_id``: its existing row, or a new one.

    An existing row that did not succeed starts another attempt, ``IN-PROGRESS`` again.
    """
    params = {"business_rule_id": business_rule_id, "task_run_id": task_run_id}
    row = conn.execute(statement(conn, "business_rule_run"), params).one_or_none()
    if row is None:
        run_id = conn.execute(statement(conn, "insert_business_rule_run"), params).scalar_one()
        return RuleRunBinding(int(run_id), False)
    if row.status == "SUCCESS":
        return RuleRunBinding(int(row.business_rule_run_id), True)
    conn.execute(
        statement(conn, "restart_business_rule_run"),
        {"business_rule_run_id": row.business_rule_run_id, "now": now},
    )
    return RuleRunBinding(int(row.business_rule_run_id), False)


def finish_rule_run(
    conn: Connection, business_rule_run_id: int, status: str, now: datetime
) -> None:
    """End a rule's run-log row with ``status``."""
    conn.execute(
        statement(conn, "finish_business_rule_run"),
        {"business_rule_run_id": business_rule_run_id, "status": status, "now": now},
    )


def fetch_active_rule_keys(conn: Connection, business_rule_id: int) -> set[str]:
    """Return the keys ``business_rule_id`` currently flags."""
    rows = conn.execute(statement(conn, "active_rule_keys"), {"business_rule_id": business_rule_id})
    return {str(row.business_rule_key) for row in rows}


def flag_rule_keys(
    conn: Connection,
    rule: BusinessRule,
    business_rule_run_id: int,
    keys: Sequence[str],
    now: datetime,
) -> None:
    """Flag ``keys`` for ``rule``, found by ``business_rule_run_id``."""
    if not keys:
        return
    conn.execute(
        statement(conn, "insert_rule_result"),
        [
            {
                "business_rule_run_id": business_rule_run_id,
                "business_rule_id": rule.business_rule_id,
                "business_rule_key": key,
                "target_table": rule.target_table,
                "status": rule.business_rule_type,
                "now": now,
            }
            for key in keys
        ],
    )


def clear_rule_keys(
    conn: Connection, business_rule_id: int, keys: Sequence[str], now: datetime
) -> None:
    """Clear the active flags of ``business_rule_id`` on ``keys``."""
    if not keys:
        return
    query = statement(conn, "clear_rule_results").bindparams(bindparam("keys", expanding=True))
    conn.execute(query, {"business_rule_id": business_rule_id, "keys": list(keys), "now": now})
