"""Propagate durable parent interventions to the WAIT children they pinned."""

from etl_craft.core.enums import TERMINAL_STATUSES, ActionName, ControlAction
from etl_craft.core.errors import RunStateError
from etl_craft.engine.queries import statement
from etl_craft.engine.runlog import fetch_pipeline_run_status
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.requests import ControlRequest


def cascade_children(ctx: OperationContext, run_id: int | None = None) -> None:
    """Replay exact child controls after a crash using stable per-parent request keys."""
    from etl_craft.services.operations.actions import invoke

    with ctx.engine.connect() as conn:
        rows = conn.execute(statement(conn, "trigger_child_controls"), {"run_id": run_id}).all()
    for row in rows:
        try:
            invoke(
                ctx,
                ActionName.CONTROL,
                ControlRequest(
                    pipeline_code=row.child_code,
                    run_id=row.child_run_id,
                    action=ControlAction(row.action),
                    reason=(
                        f"parent pipeline_run_id={row.parent_run_id}: {row.action} "
                        f"(control_id={row.control_id})"
                    ),
                    request_key=f"trigger-control:{row.control_id}:{row.child_run_id}",
                ),
            )
        except RunStateError:
            with ctx.engine.connect() as conn:
                if fetch_pipeline_run_status(conn, row.child_run_id) not in TERMINAL_STATUSES:
                    raise
