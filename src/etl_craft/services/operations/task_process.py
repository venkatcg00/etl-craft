"""Compose the task process with service-owned pipeline admission."""

from sqlalchemy.engine import Engine

from etl_craft.core.actor import Actor
from etl_craft.core.enums import ActionName, ActorKind
from etl_craft.engine.queries import statement
from etl_craft.execution.child import run_as_process
from etl_craft.handlers.registry import TaskContext
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.child_admission import initiating_context
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.triggers import TriggerRequest


def execute_trigger(engine: Engine, task: TaskContext) -> int:
    """Use the initiating account and the ordinary operation policy in the task process."""
    with engine.connect() as conn:
        row = conn.execute(
            statement(conn, "trigger_context"), {"task_run_id": task.task_run_id}
        ).one()
        ctx = initiating_context(
            conn,
            OperationContext(
                engine, task.config, Actor(row.started_by, ActorKind(row.started_by_kind))
            ),
            row.pipeline_run_id,
        )
    invoke(
        ctx,
        ActionName.TRIGGER_PIPELINE,
        TriggerRequest(task_run_id=task.task_run_id, attempt_id=task.attempt_id, owner=task.owner),
    )
    return 0


if __name__ == "__main__":
    run_as_process(trigger=execute_trigger)
