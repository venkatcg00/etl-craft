"""A run whose EXTRACT task fails once and succeeds on retry, whose SLA lapses, and whose LOAD
is held: the scene the run log and run zip tests read."""

from datetime import UTC, datetime, timedelta

from etl_craft.core.actor import Actor, ActorKind, acting_as
from etl_craft.core.enums import ActionName, ApiRole, AttemptStatus, ControlAction, RunStatus
from etl_craft.engine import transitions
from etl_craft.services.operations import OperationContext
from fixtures.actions import act
from fixtures.metadata import add_dependency, add_pipeline, add_task, start_run

OWNER = Actor("owner", ActorKind.USER)


def attempt(conn, summary, *, fail=None, not_before=None, previous=None):
    attempt_id = transitions.queue_attempt(
        conn, summary, OWNER, not_before=not_before, previous_attempt_id=previous
    )
    lease = datetime.now(UTC) + timedelta(minutes=5)
    transitions.claim_attempt(conn, attempt_id, OWNER, owner="w1", lease_expires_at=lease)
    transitions.start_attempt(conn, attempt_id, OWNER, owner="w1", host="worker-1", pid=41)
    status = AttemptStatus.FAILED if fail else AttemptStatus.SUCCESS
    transitions.finish_attempt(
        conn, attempt_id, status, OWNER, owner="w1", error_message=fail, exit_code=1 if fail else 0
    )
    return attempt_id


def seed(engine, config):
    """EXTRACT fails once and succeeds on retry, the SLA lapses, and LOAD is held."""
    with acting_as(OWNER), engine.begin() as conn:
        pipeline = add_pipeline(conn, "ORDERS")
        extract = add_task(conn, pipeline, "EXTRACT")
        load = add_task(conn, pipeline, "LOAD")
        add_dependency(conn, pipeline, load, extract)
        run = start_run(conn, pipeline)
        summary = transitions.find_or_create_task_run(conn, extract, run).task_run_id
        failed = attempt(conn, summary, fail="connection reset by source")
        retried = attempt(conn, summary, not_before=datetime.now(UTC), previous=failed)
        transitions.finish_task_run(conn, summary, status=RunStatus.SUCCESS)
        transitions.mark_sla_breached(conn, run)
    ctx = OperationContext(engine, config, OWNER, role=ApiRole.ADMIN)
    act(
        ctx,
        ActionName.CONTROL,
        pipeline_code="ORDERS",
        action=ControlAction.HOLD,
        run_id=run,
        task_code="LOAD",
        reason="check the load",
    )
    return ctx, run, (failed, retried)
