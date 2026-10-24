"""Capture each attempt's slot and admitted execution flags before it becomes visible."""

from sqlalchemy.engine import Connection

from etl_craft.core.definitions import current
from etl_craft.core.enums import Handler, SlotKind, YesNo
from etl_craft.engine.placement import claim_parameters
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
    definition = current.get()
    handler = (
        row.handler
        if definition is None
        else definition.row("CFG_TASKS", "task_id", row.task_id)["handler"]
    )
    kind = SlotKind.PYTHON if handler == Handler.PYTHON else SlotKind.WAREHOUSE
    if definition is None and row.previous_kind is not None:
        kind = SlotKind(row.previous_kind)
    conn.execute(
        statement(conn, "agent_queue"),
        {
            **claim_parameters(),
            "attempt_id": attempt_id,
            "kind": kind,
            "force": row.force_flag or YesNo.NO,
            "rerun": row.rerun_flag or YesNo.NO,
            "reason": None,
        },
    )


def dispatch_options(
    conn: Connection, attempt_id: int, handler: str, reason: str | None, options: dict[str, bool]
) -> None:
    """Persist the admitted flags with the task's recorded pool and pipeline priority."""
    kind = SlotKind.PYTHON if handler == Handler.PYTHON else SlotKind.WAREHOUSE

    conn.execute(
        statement(conn, "agent_queue"),
        {
            **claim_parameters(),
            "attempt_id": attempt_id,
            "kind": kind,
            "force": YesNo.YES if options["force"] else YesNo.NO,
            "rerun": YesNo.YES if options["rerun"] else YesNo.NO,
            "reason": reason,
        },
    )
