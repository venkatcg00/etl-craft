"""PostgreSQL claims reserve per-agent and shared-host capacity in one transaction."""

import asyncio
import json
import time
from dataclasses import fields, replace

from sqlalchemy.engine import Row

from etl_craft.core.actor import ActorKind, acting_as
from etl_craft.core.agent_resources import AgentResources
from etl_craft.core.enums import AgentStatus, SlotKind, TriggerKind, YesNo
from etl_craft.core.errors import PermissionDeniedError, RunRefusedError
from etl_craft.core.faults import fault_point
from etl_craft.engine import agent_transitions
from etl_craft.engine.notifications import agent_channel, notification_hub, pool_channel
from etl_craft.engine.queries import statement
from etl_craft.execution.attempt_records import refuse_attempt
from etl_craft.execution.context import build_task_context
from etl_craft.execution.limits import task_timeout_seconds
from etl_craft.services.operations.agent_capacity import available, pool_headroom
from etl_craft.services.operations.agent_models import AgentClaim, AgentReply
from etl_craft.services.operations.agents import check_agent
from etl_craft.services.operations.child_admission import check_child_start
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.serialization import from_json


def claim_once(ctx: OperationContext, request: AgentClaim, agent_id: int) -> AgentReply | None:
    """Claim exactly one row; a missing notification never affects correctness."""
    with ctx.engine.begin() as conn:
        agent = conn.execute(statement(conn, "agent_get"), {"agent_id": agent_id}).one_or_none()
        if agent is None or agent.status == AgentStatus.GONE:
            raise RunRefusedError(f"agent {agent_id}: GONE or missing; stop children and register")
        hub = notification_hub(ctx.config, ctx.engine)
        hub.subscribe(pool_channel(agent.pool), agent_channel(agent_id))
        resources = from_json(AgentResources, json.loads(agent.resource_report))
        conn.execute(statement(conn, "agent_host_lock"), {"node_id": resources.node_id}).all()
        agent = conn.execute(statement(conn, "agent_get"), {"agent_id": agent_id}).one()
        if not agent.credential_live:
            raise PermissionDeniedError(
                f"agent {agent_id}: credential is no longer valid; expected a live agent token "
                "and active account; stop children and issue a new agent token"
            )
        resources = from_json(AgentResources, json.loads(agent.resource_report))
        commands = conn.execute(statement(conn, "agent_commands"), {"agent_id": agent_id}).first()
        attempts = conn.execute(
            statement(conn, "agent_control_work"), {"owner": str(agent_id)}
        ).all()
        if commands is not None or any(
            a.cancel_requested_at is not None or a.cleanup for a in attempts
        ):
            return AgentReply(
                {
                    "controls_pending": True,
                    "cleanup": [int(a.attempt_id) for a in attempts if a.cleanup],
                }
            )
        if agent.status != AgentStatus.ACTIVE:
            return None
        free, cpu, memory = available(conn, agent_id, resources)
        claimed = agent_transitions.claim_queued_attempt(
            conn,
            agent_id,
            agent.pool,
            {
                f"{kind}_free": free[kind] > 0 and kind in request.free_kinds
                for kind in (SlotKind.PYTHON, SlotKind.WAREHOUSE)
            }
            | {
                "cpu_free": cpu,
                "memory_free_mb": memory,
                "warehouse_limit": ctx.config.max_warehouse_queries,
            },
        )
        if claimed is None:
            from etl_craft.engine.placement import record_wait_reasons

            record_wait_reasons(
                conn,
                ctx.config.max_warehouse_queries,
                pool=agent.pool,
                resources=pool_headroom(conn, agent.pool),
            )
            return None
        work = conn.execute(statement(conn, "agent_work"), {"attempt_id": claimed.attempt_id}).one()
        refusal = (
            check_child_start(ctx.engine, ctx.config, conn, work.pipeline_run_id)
            if work.started_by_kind != ActorKind.SYSTEM
            or work.trigger_kind == TriggerKind.PIPELINE_TASK
            else None
        )
        if refusal is not None:
            refuse_attempt(conn, claimed.attempt_id, refusal, ctx.config.logs.tail_bytes)
            return None
    return _reply(ctx, claimed, work)


def _reply(
    ctx: OperationContext, claimed: Row[tuple[object, ...]], work: Row[tuple[object, ...]]
) -> AgentReply:
    """Build dispatched task metadata only after the claim and permission check commit."""
    task_run_id = int(work.task_run_id)
    force, rerun = work.force_flag == YesNo.YES, work.rerun_flag == YesNo.YES
    budget = (claimed.lease_expires_at - claimed.db_now).total_seconds()
    fault_point("agent.after_claim")
    context = build_task_context(ctx.engine, ctx.config, task_run_id, force=force, rerun=rerun)
    context = replace(context, attempt_id=int(claimed.attempt_id), owner=str(work.owner_id))
    metadata = {
        f.name: getattr(context, f.name)
        for f in fields(context)
        if f.name != "config" and not f.name.startswith("_")
    }
    return AgentReply(
        {
            "task": metadata,
            "lease_seconds": budget,
            "timeout_seconds": task_timeout_seconds(context.task_params, ctx.config),
        }
    )


async def claim(ctx: OperationContext, request: AgentClaim) -> AgentReply:
    """Long-poll a shared LISTEN session for at most thirty seconds, ending early on shutdown."""
    with acting_as(ctx.actor):
        agent_id = check_agent(ctx, request)
        hub = notification_hub(ctx.config, ctx.engine)
        deadline = time.monotonic() + 30
        while ctx.shutdown is None or not ctx.shutdown.is_set():
            generation = hub.generation
            found = await asyncio.to_thread(claim_once, ctx, request, agent_id)
            if found is not None:
                return found
            if time.monotonic() >= deadline:
                break
            await hub.wait_async(generation, min(10, deadline - time.monotonic()), ctx.shutdown)
    return AgentReply()
