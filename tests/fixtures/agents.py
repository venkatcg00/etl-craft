"""Deterministic resource evidence for the agent and protocol tests."""

from datetime import UTC, datetime

from etl_craft.core.agent_resources import AgentResources
from etl_craft.core.resources import GIB, Allocation, Ceilings, Measurement, size_agent


def resources(cpus=4, memory=2 * GIB, scope="/host", node="node-a", host="agent-host"):
    allocation = Allocation(
        Measurement(cpus, "cpu.max", "test CPU allocation"),
        Measurement(memory, "memory.max", "test memory allocation"),
        4,
        2 * GIB,
        scope,
        datetime(2026, 10, 21, tzinfo=UTC),
    )
    ceilings = Ceilings(2, 2, 2)
    return AgentResources(host, node, allocation, ceilings, size_agent(allocation, ceilings))
