"""Lifecycle writes for business-rule summaries."""

from datetime import datetime

from sqlalchemy.engine import Connection

from etl_craft.engine.queries import statement


def restart_rule_run(conn: Connection, business_rule_run_id: int, now: datetime) -> None:
    """Restart a business-rule summary that has not succeeded."""
    conn.execute(
        statement(conn, "transition_restart_business_rule_run"),
        {"business_rule_run_id": business_rule_run_id, "now": now},
    )


def finish_rule_run(
    conn: Connection, business_rule_run_id: int, status: str, now: datetime
) -> None:
    """Record a business-rule summary's outcome."""
    conn.execute(
        statement(conn, "transition_finish_business_rule_run"),
        {"business_rule_run_id": business_rule_run_id, "status": status, "now": now},
    )
