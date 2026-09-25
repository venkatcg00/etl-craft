"""Business rules: the rules a task runs, and every rule's warehouse table."""

from __future__ import annotations

from dataclasses import dataclass

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
