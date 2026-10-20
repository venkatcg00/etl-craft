"""Controller checks for handed-off trigger attempts; no agent slot is held."""

from datetime import datetime, timedelta

from sqlalchemy.engine import Engine

from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
from etl_craft.core.enums import TERMINAL_STATUSES, ControlAction, RunStatus
from etl_craft.core.time import as_utc
from etl_craft.engine import controls, transitions
from etl_craft.engine.queries import statement
from etl_craft.engine.repository import trackers
from etl_craft.engine.repository.runs import fetch_task_statuses_for_run


@acting_as(SYSTEM_ACTOR)
def check_trigger_waits(engine: Engine, run_id: int, now: datetime) -> list[datetime]:
    """Resume durable waits and publish task outcomes with captured dependency consumption."""
    waiting = []
    with engine.begin() as conn:
        controls.lock_run(conn, run_id)
        fences = controls.blocked(conn, run_id)
        rows = conn.execute(statement(conn, "trigger_waits"), {"pipeline_run_id": run_id}).all()
        for row in rows:
            scope = {"pipeline_run_id": run_id, "task_id": row.task_id}
            if row.task_status in TERMINAL_STATUSES:
                conn.execute(statement(conn, "trigger_wait_close"), scope)
                continue
            fence = fences.get(None) or fences.get(row.task_id)
            stopping = fence in {ControlAction.STOP, ControlAction.ABORT}
            if fence is not None and not stopping:
                waiting.append(now + timedelta(seconds=1))
                continue
            status = RunStatus(row.child_status)
            expired = (
                row.deadline is not None
                and now >= as_utc(row.deadline)
                and (
                    row.child_ended_at is None or as_utc(row.child_ended_at) > as_utc(row.deadline)
                )
                and not stopping
            )
            if status not in TERMINAL_STATUSES and (stopping or not expired):
                waiting.append(now + timedelta(seconds=1))
                continue
            outcome = status if status in TERMINAL_STATUSES and not expired else RunStatus.FAILED
            message = f"child pipeline_run_id={row.child_run_id}: {status}"
            if expired:
                message += "; trigger deadline passed; "
                message += (
                    "child keeps running; inspect it or stop it explicitly"
                    if status not in TERMINAL_STATUSES
                    else "child ended after the deadline; inspect its history or mark this trigger"
                )
            elif outcome in {RunStatus.FAILED, RunStatus.CANCELLED}:
                message += (
                    "; inspect the child history; repair the child "
                    "or explicitly rerun/mark this trigger"
                )
            if outcome in {RunStatus.FAILED, RunStatus.CANCELLED}:
                details = [
                    f"{task.task_code}: {task.status} — {task.error_message}"
                    for task in fetch_task_statuses_for_run(
                        conn, row.child_pipeline_id, row.child_run_id
                    )
                    if task.error_message
                ]
                if details:
                    message += "; " + "; ".join(details)
            transitions.finish_task_run(
                conn,
                row.task_run_id,
                status=outcome,
                error_message=message
                if outcome in {RunStatus.FAILED, RunStatus.CANCELLED}
                else None,
                task_log=message,
            )
            if outcome == RunStatus.SUCCESS:
                trackers.consume_task_decisions(
                    conn, row.task_id, run_id, attempt_id=row.attempt_id
                )
            conn.execute(statement(conn, "trigger_wait_close"), scope)
    return waiting


@acting_as(SYSTEM_ACTOR)
def wake_child_waits(engine: Engine, child_run_id: int) -> None:
    """Bring controller checks forward after a child's outcome has committed."""
    from datetime import UTC

    with engine.begin() as conn:
        conn.execute(
            statement(conn, "trigger_wait_wake"),
            {"child_run_id": child_run_id, "now": datetime.now(UTC)},
        )


def settle_ended_parent_waits(engine: Engine, now: datetime) -> None:
    """Transfer child endings even after a parent intervention retired its supervisor."""
    with engine.connect() as conn:
        runs = conn.execute(statement(conn, "trigger_ended_parents")).scalars().all()
    for run_id in runs:
        check_trigger_waits(engine, run_id, now)
