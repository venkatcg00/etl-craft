"""Shared per-agent capacity with reservations counted across overlapping host scopes."""

import json
from pathlib import PurePosixPath

from sqlalchemy.engine import Connection

from etl_craft.core.agent_resources import AgentResources, capacity
from etl_craft.core.enums import CapacitySource, SlotKind
from etl_craft.core.resources import MIB
from etl_craft.engine.queries import statement
from etl_craft.services.operations.serialization import from_json


def available(
    conn: Connection, agent_id: int, resources: AgentResources
) -> tuple[dict[str, int], float, int]:
    """Count each live reservation against its agent, host and overlapping scope."""
    used = dict.fromkeys(SlotKind, 0)
    host_cpu = host_memory = scope_cpu = scope_memory = 0.0
    rows = conn.execute(statement(conn, "agent_usage"), {"node_id": resources.node_id}).all()
    for row in rows:
        report = from_json(AgentResources, json.loads(row.resource_report))
        if row.slot_kind is None:
            continue
        if report.capacity_source != resources.capacity_source:
            continue
        if resources.capacity_source == CapacitySource.DECLARED and row.agent_id != agent_id:
            continue
        cpu, memory = float(row.cpu_used), int(row.memory_used_mb) * MIB
        host_cpu += cpu
        host_memory += memory
        first, second = (
            PurePosixPath(report.allocation.scope),
            PurePosixPath(resources.allocation.scope),
        )
        overlaps = first.is_relative_to(second) or second.is_relative_to(first)
        if overlaps or report.allocation.cpus.source == resources.allocation.cpus.source:
            scope_cpu += cpu
        if overlaps or report.allocation.memory.source == resources.allocation.memory.source:
            scope_memory += memory
        if row.agent_id == agent_id:
            used[SlotKind(row.slot_kind)] += int(row.used)
    sizing, allocation = resources.sizing, resources.allocation
    if (
        resources.node_id == "unavailable"
        or allocation.cpus.value is None
        or allocation.memory.value is None
    ):
        return dict.fromkeys(SlotKind, 0), 0, 0
    cpu, memory_mb = capacity(resources)
    cpu_left = cpu - scope_cpu
    memory_left = memory_mb * MIB - scope_memory
    if resources.capacity_source != CapacitySource.DECLARED:
        cpu_left = min(cpu_left, (allocation.host_cpus or allocation.cpus.value) - host_cpu)
        memory_left = min(
            memory_left, (allocation.host_memory or allocation.memory.value) - host_memory
        )
    limits = {
        SlotKind.PYTHON: sizing.python_slots.value,
        SlotKind.WAREHOUSE: sizing.warehouse_slots.value,
    }
    free: dict[str, int] = {
        kind: max(
            0,
            min(
                limits[kind] - used[kind],
                sizing.task_slots.value - sum(used.values()),
            ),
        )
        for kind in SlotKind
    }

    return free, max(0, cpu_left), max(0, int(memory_left / MIB))


def pool_headroom(conn: Connection, pool: str) -> dict[str, object]:
    """Use the admission accountant for queued diagnostics, including shared host limits."""
    row = conn.execute(statement(conn, "placement_largest_agent"), {"pool": pool}).one_or_none()
    if row is None:
        return {}
    resources = from_json(AgentResources, json.loads(row.resource_report))
    slots, cpu_free, memory_free = available(conn, row.agent_id, resources)
    cpu, memory = capacity(resources)
    return {
        "capacity_label": f"largest agent in pool {pool} (agent {row.agent_id})",
        "cpu_free": cpu_free,
        "memory_free_mb": memory_free,
        "cpu_capacity": cpu,
        "memory_capacity_mb": memory,
        "python_free_slots": slots[SlotKind.PYTHON],
        "warehouse_free_slots": slots[SlotKind.WAREHOUSE],
        "python_capacity_slots": resources.sizing.python_slots.value,
        "warehouse_capacity_slots": resources.sizing.warehouse_slots.value,
    }
