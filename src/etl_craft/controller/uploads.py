"""Recover the durable pending upload queue independently of any API process."""

from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
from etl_craft.engine.queries import statement
from etl_craft.engine.uploads import admit_queued
from etl_craft.services.operations.context import OperationContext


def admit_pending(ctx: OperationContext) -> None:
    """Take queued files together after a prior exclusive run ends or the controller restarts."""
    with ctx.engine.connect() as conn:
        pipelines = tuple(conn.execute(statement(conn, "upload_queue_pipelines")).scalars())
    with acting_as(SYSTEM_ACTOR):
        for pipeline_id in pipelines:
            with ctx.engine.begin() as conn:
                admit_queued(conn, pipeline_id)
