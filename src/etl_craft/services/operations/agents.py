"""Agent registration, resource accounting and operator commands behind one service."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar

from etl_craft import __version__
from etl_craft.core.agent_resources import AgentResources
from etl_craft.core.enums import AgentStatus, DeploymentProfile, SlotKind
from etl_craft.core.errors import ConfigurationError, RunRefusedError, UsageError
from etl_craft.core.resources import size_agent
from etl_craft.core.time import as_utc
from etl_craft.dialects.engine import for_engine
from etl_craft.engine import agent_transitions, resource_locks
from etl_craft.engine.notifications import agent_channel
from etl_craft.engine.queries import statement
from etl_craft.services.operations.agent_capacity import free_slots
from etl_craft.services.operations.agent_models import (
    AgentControl,
    AgentRegister,
    AgentRenew,
    AgentReply,
    AgentRequest,
)
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.requests import NoInput
from etl_craft.services.operations.serialization import encode, from_json


@dataclass(frozen=True)
class AgentView:
    """Registry, pressure, live attempts and command acknowledgements from one read path."""

    agent_id: int
    pool: str
    host: str
    version: str
    status: AgentStatus
    heartbeat_at: datetime
    resources: AgentResources
    used: dict[str, int]
    free: dict[str, int]
    attempts: tuple[int, ...]
    pending_commands: tuple[dict[str, object], ...]


@dataclass(frozen=True)
class AgentsView:
    """One agents document shared with status and doctor."""

    SCHEMA: ClassVar[str] = "etl-craft/agents/1"
    agents: tuple[AgentView, ...]


def check_agent(ctx: OperationContext, request: AgentRequest) -> int:
    """Derive identity from the authenticated credential and refuse incompatible roles."""
    if request.version != __version__:
        raise ConfigurationError(
            f"agent version={request.version}, API version={__version__}; "
            "install the API's etl-craft version on the agent and restart it"
        )
    if ctx.credential_token_id is None:
        raise RunRefusedError("agent token identity is missing; use an issued agent token")
    if (
        ctx.config.deployment != DeploymentProfile.CONTROLLER_AGENTS
        or not for_engine(ctx.engine).supports_api_replicas
    ):
        raise RunRefusedError(
            f"Deployment.Profile={ctx.config.deployment}; agent execution requires "
            "CONTROLLER_AGENTS and PostgreSQL; choose that profile on the API and controller"
        )
    return ctx.credential_token_id


def resource_parameters(report: AgentResources) -> dict[str, object]:
    """Accept only capacities produced by the shared sizing rule, retaining unavailable reasons."""
    measurements = (report.allocation.cpus, report.allocation.memory, *report.pressure.values())
    if any(
        m.value is not None and (not math.isfinite(m.value) or m.value < 0) for m in measurements
    ):
        raise UsageError(
            "agent resources: expected finite nonnegative measurements; restart discovery"
        )
    if any(
        value is not None and value < 0
        for value in (
            report.ceilings.max_parallel_tasks,
            report.ceilings.python_slots,
            report.ceilings.warehouse_slots,
        )
    ):
        raise UsageError("agent resources: slot ceilings must be nonnegative; fix the agent flags")
    expected = size_agent(report.allocation, report.ceilings)
    if report.sizing != expected:
        raise UsageError("agent resources: sizing differs from shared discovery; restart the agent")
    return {
        "report": json.dumps(encode(report)),
        "python_slots": expected.python_slots.value,
        "warehouse_slots": expected.warehouse_slots.value,
    }


def register(ctx: OperationContext, request: AgentRegister) -> AgentReply:
    """Register one token; a duplicate live process cannot replace its capacity."""
    agent_id = check_agent(ctx, request)
    with ctx.engine.begin() as conn:
        row = agent_transitions.register_agent(
            conn,
            {
                "agent_id": agent_id,
                "pool": request.pool,
                "host": request.resources.host,
                "version": request.version,
                **resource_parameters(request.resources),
            },
        )
        if row is None:
            raise RunRefusedError(
                f"agent {agent_id}: registration already live; stop the other process or "
                "wait for reconciliation, and use a distinct token for each agent"
            )
        resource_locks.notify(conn)
    return AgentReply({"agent_id": agent_id, "lease_seconds": 60})


def deregister(ctx: OperationContext, request: AgentRequest) -> AgentReply:
    """Withdraw capacity after the agent has drained all its task processes."""
    agent_id = check_agent(ctx, request)
    with ctx.engine.begin() as conn:
        attempts = conn.execute(statement(conn, "agent_attempts"), {"owner": str(agent_id)}).all()
        if attempts:
            raise RunRefusedError(
                f"agent {agent_id}: still owns live attempts; drain before exiting"
            )
        row = conn.execute(statement(conn, "agent_get"), {"agent_id": agent_id}).one_or_none()
        if row is None:
            raise RunRefusedError(
                f"agent {agent_id}: not registered; register before deregistering"
            )
        agent_transitions.heartbeat_agent(
            conn,
            {
                "agent_id": agent_id,
                "status": AgentStatus.GONE,
                "report": row.resource_report,
                "python_slots": row.python_slots,
                "warehouse_slots": row.warehouse_slots,
            },
        )
        resource_locks.notify(conn)
    return AgentReply()


def renew(ctx: OperationContext, request: AgentRenew) -> AgentReply:
    """Renew the known live attempts using the database clock and return pending controls."""
    agent_id = check_agent(ctx, request)
    with ctx.engine.begin() as conn:
        row = conn.execute(statement(conn, "agent_get"), {"agent_id": agent_id}).one_or_none()
        if row is None or row.status == AgentStatus.GONE:
            raise RunRefusedError(
                f"agent {agent_id}: GONE or unregistered; stop children and restart"
            )
        if request.status == AgentStatus.GONE:
            raise UsageError("renew status GONE is refused; drain and deregister instead")
        previous = from_json(AgentResources, json.loads(row.resource_report))
        if (
            previous.node_id != request.resources.node_id
            or previous.allocation.scope != request.resources.allocation.scope
        ):
            raise RunRefusedError(
                f"agent {agent_id}: host identity changed; stop and register again"
            )
        updated = agent_transitions.heartbeat_agent(
            conn,
            {
                "agent_id": agent_id,
                "status": request.status,
                **resource_parameters(request.resources),
            },
        )
        if updated is None:
            raise RunRefusedError(
                f"agent {agent_id}: heartbeat expired; stop children and register again"
            )
        renewed = []
        for attempt in request.attempts:
            lease = conn.execute(
                statement(conn, "agent_renew_attempt"),
                {
                    "attempt_id": attempt,
                    "owner": str(agent_id),
                },
            ).one_or_none()
            if lease is not None:
                conn.execute(
                    statement(conn, "agent_renew_locks"),
                    {
                        "attempt_id": attempt,
                        "owner": str(agent_id),
                    },
                )
                renewed.append(attempt)
        now = conn.execute(statement(conn, "agent_now")).scalar_one()
        for command in request.acknowledged_commands:
            conn.execute(
                statement(conn, "agent_command_ack"),
                {
                    "command_id": command,
                    "agent_id": agent_id,
                    "now": now,
                },
            )
        commands = tuple(
            dict(r._mapping)
            for r in conn.execute(statement(conn, "agent_commands"), {"agent_id": agent_id})
        )
        cancel = tuple(
            int(r.attempt_id)
            for r in conn.execute(statement(conn, "agent_attempts"), {"owner": str(agent_id)})
            if r.cancel_requested_at is not None
        )
    return AgentReply(
        {"renewed": renewed, "cancel": cancel, "commands": commands, "lease_seconds": 60}
    )


def command(ctx: OperationContext, request: AgentControl) -> AgentReply:
    """Record an operator's command; the agent acknowledges it after applying it."""
    with ctx.engine.begin() as conn:
        row = conn.execute(
            statement(conn, "agent_get"), {"agent_id": request.agent_id}
        ).one_or_none()
        if row is None or row.status == AgentStatus.GONE:
            raise RunRefusedError(f"agent {request.agent_id}: GONE or missing; choose a live agent")
        command_id = conn.execute(
            statement(conn, "agent_command_insert"),
            {
                "agent_id": request.agent_id,
                "command": request.command,
                "actor": ctx.actor.name,
                "kind": ctx.actor.kind,
                "now": conn.execute(statement(conn, "agent_now")).scalar_one(),
            },
        ).scalar_one()
        resource_locks.notify(conn, agent_channel(request.agent_id))
    return AgentReply({"command_id": command_id})


def list_agents(ctx: OperationContext, request: NoInput) -> AgentsView:
    """Read measured resources and live ownership using the same service for every interface."""
    result = []
    with ctx.engine.connect() as conn:
        for row in conn.execute(statement(conn, "agent_list")):
            resources = from_json(AgentResources, json.loads(row.resource_report))
            attempts = tuple(
                conn.execute(statement(conn, "agent_attempts"), {"owner": str(row.agent_id)})
            )
            used: dict[str, int] = {
                kind: sum(a.slot_kind == kind for a in attempts) for kind in SlotKind
            }
            free = free_slots(conn, row.agent_id, resources)
            now = as_utc(conn.execute(statement(conn, "agent_now")).scalar_one())
            stale = (now - as_utc(row.heartbeat_at)).total_seconds() >= 30
            status = AgentStatus.GONE if stale else AgentStatus(row.status)
            if status != AgentStatus.ACTIVE:
                free = dict.fromkeys(SlotKind, 0)
            commands = tuple(
                dict(r._mapping)
                for r in conn.execute(statement(conn, "agent_commands"), {"agent_id": row.agent_id})
            )
            result.append(
                AgentView(
                    row.agent_id,
                    row.pool,
                    row.host,
                    row.version,
                    status,
                    as_utc(row.heartbeat_at),
                    resources,
                    used,
                    free,
                    tuple(int(a.attempt_id) for a in attempts),
                    commands,
                )
            )
    return AgentsView(tuple(result))
