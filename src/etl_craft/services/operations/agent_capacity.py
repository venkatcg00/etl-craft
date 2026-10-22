"""Shared per-agent capacity with reservations counted across overlapping host scopes."""

import json
from pathlib import PurePosixPath

from sqlalchemy.engine import Connection

from etl_craft.core.agent_resources import AgentResources
from etl_craft.core.enums import SlotKind
from etl_craft.core.resources import TASK_COST
from etl_craft.engine.queries import statement
from etl_craft.services.operations.serialization import from_json


def free_slots(conn: Connection, agent_id: int, resources: AgentResources) -> dict[str, int]:
    """Count each live reservation against its agent, host and overlapping scope."""
    used = dict.fromkeys(SlotKind, 0)
    host_cpu = host_memory = scope_cpu = scope_memory = 0.0
    rows = conn.execute(statement(conn, "agent_usage"), {"node_id": resources.node_id}).all()
    for row in rows:
        report = from_json(AgentResources, json.loads(row.resource_report))
        if row.slot_kind is None:
            continue
        cpu, memory = TASK_COST[row.slot_kind]
        host_cpu += row.used * cpu
        host_memory += row.used * memory
        first, second = (
            PurePosixPath(report.allocation.scope),
            PurePosixPath(resources.allocation.scope),
        )
        overlaps = first.is_relative_to(second) or second.is_relative_to(first)
        if overlaps or report.allocation.cpus.source == resources.allocation.cpus.source:
            scope_cpu += row.used * cpu
        if overlaps or report.allocation.memory.source == resources.allocation.memory.source:
            scope_memory += row.used * memory
        if row.agent_id == agent_id:
            used[SlotKind(row.slot_kind)] += int(row.used)
    sizing, allocation = resources.sizing, resources.allocation
    if (
        resources.node_id == "unavailable"
        or allocation.cpus.value is None
        or allocation.memory.value is None
    ):
        return dict.fromkeys(SlotKind, 0)
    cpu_left = min(
        allocation.cpus.value - (sizing.reserve_cpus or 0) - scope_cpu,
        (allocation.host_cpus or allocation.cpus.value) - host_cpu,
    )
    memory_left = min(
        allocation.memory.value - (sizing.reserve_memory or 0) - scope_memory,
        (allocation.host_memory or allocation.memory.value) - host_memory,
    )
    limits = {
        SlotKind.PYTHON: sizing.python_slots.value,
        SlotKind.WAREHOUSE: sizing.warehouse_slots.value,
    }
    return {
        kind: max(
            0,
            min(
                limits[kind] - used[kind],
                sizing.task_slots.value - sum(used.values()),
                int(cpu_left // TASK_COST[kind][0]),
                int(memory_left // TASK_COST[kind][1]),
            ),
        )
        for kind in SlotKind
    }
