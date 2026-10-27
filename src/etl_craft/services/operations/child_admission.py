"""Revalidate retained initiating identities immediately before a queued child starts."""

from dataclasses import replace

from sqlalchemy.engine import Connection, Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.actor import Actor, ActorKind
from etl_craft.core.enums import ActionName, ApiRole, TriggerKind, YesNo
from etl_craft.core.errors import PermissionDeniedError, ResourceNotFoundError
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.group_claims import decode_groups
from etl_craft.execution.attempt_types import ChildOptions
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.permissions import (
    pipeline_access,
    require_child_groups,
    require_project,
)


def initiating_context(conn: Connection, ctx: OperationContext, run_id: int) -> OperationContext:
    """Keep the admitted group ids, but read the initiating account's current enabled role."""
    row = conn.execute(statement(conn, "run_initiator"), {"run_id": run_id}).one()
    account = conn.execute(statement(conn, "account_find"), {"name": row.started_by}).one_or_none()
    return replace(
        ctx,
        actor=Actor(row.started_by, ActorKind(row.started_by_kind)),
        role=ApiRole(account.role) if account and account.active_flag == YesNo.YES else None,
        sso_group_ids=decode_groups(row.sso_group_ids, f"run_id={run_id}"),
        credential_token_id=None,
        project_id=row.project_id,
        project=row.project_code,
    )


def check_child_start(
    engine: Engine, config: ConnectorConfig, conn: Connection, run_id: int
) -> str | None:
    """Return a durable cancellation reason when an admitted child has lost permission."""
    from etl_craft.services.operations.actions import ACTIONS

    row = conn.execute(statement(conn, "run_initiator"), {"run_id": run_id}).one()
    if row.trigger_kind != TriggerKind.PIPELINE_TASK and row.started_by_kind == ActorKind.SYSTEM:
        return None
    ctx = initiating_context(
        conn,
        OperationContext(engine, config, Actor(row.started_by, ActorKind(row.started_by_kind))),
        run_id,
    )
    required = ACTIONS[ActionName.RUN].role
    assert required is not None
    try:
        require_project(conn, ctx)
        require_child_groups(conn, ctx, row.pipeline_code)
        pipeline_access(conn, ctx).require(ctx, row.pipeline_code, required)
    except (PermissionDeniedError, ResourceNotFoundError) as error:
        return str(error)
    return None


def guarded_children(ctx: OperationContext) -> ChildOptions:
    """Carry the same initiating-account check to a local process's claim boundary."""
    from functools import partial

    return replace(ctx.child, before_start=partial(check_child_start, ctx.engine, ctx.config))
