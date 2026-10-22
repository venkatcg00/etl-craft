"""Capture each attempt's slot and admitted execution flags before it becomes visible."""

from sqlalchemy.engine import Connection

from etl_craft.core.enums import Handler, SlotKind, YesNo
from etl_craft.engine.queries import statement


def inherit_dispatch(conn: Connection, attempt_id: int, task_run_id: int) -> None:
    """Retries retain their task kind and admitted options, without consulting a recent run."""
    row = conn.execute(
        statement(conn, "attempt_dispatch"),
        {
            "attempt_id": attempt_id,
            "task_run_id": task_run_id,
        },
    ).one()
    conn.execute(
        statement(conn, "agent_queue"),
        {
            "attempt_id": attempt_id,
            "kind": SlotKind.PYTHON if row.handler == Handler.PYTHON else SlotKind.WAREHOUSE,
            "force": row.force_flag or YesNo.NO,
            "rerun": row.rerun_flag or YesNo.NO,
            "reason": None,
        },
    )
