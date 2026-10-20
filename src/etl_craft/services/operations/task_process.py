"""Compose the task process with service-owned pipeline admission."""

from sqlalchemy.engine import Engine

from etl_craft.core.actor import Actor
from etl_craft.core.enums import ActionName, ActorKind, ApiRole, YesNo
from etl_craft.engine.queries import statement
from etl_craft.execution.child import run_as_process
from etl_craft.handlers.registry import TaskContext
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.triggers import TriggerRequest


def execute_trigger(engine: Engine, task: TaskContext) -> int:
    """Use the initiating account and the ordinary operation policy in the task process."""
    with engine.connect() as conn:
        row = conn.execute(
            statement(conn, "trigger_context"), {"task_run_id": task.task_run_id}
        ).one()
        account = conn.execute(
            statement(conn, "account_find"), {"name": row.started_by}
        ).one_or_none()
    actor = Actor(row.started_by, ActorKind(row.started_by_kind))
    invoke(
        OperationContext(
            engine,
            task.config,
            actor,
            role=ApiRole(account.role) if account and account.active_flag == YesNo.YES else None,
        ),
        ActionName.TRIGGER_PIPELINE,
        TriggerRequest(task_run_id=task.task_run_id, attempt_id=task.attempt_id, owner=task.owner),
    )
    return 0


if __name__ == "__main__":
    run_as_process(trigger=execute_trigger)
