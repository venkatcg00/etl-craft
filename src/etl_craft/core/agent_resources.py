"""Agent resource evidence produced by the shared discovery and sizing report."""

import socket
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import ClassVar

import psutil

from etl_craft.core.enums import CapacitySource
from etl_craft.core.errors import ConfigurationError
from etl_craft.core.resources import (
    MIB,
    Allocation,
    Ceilings,
    Measurement,
    Sizing,
    discover,
    resource_pressure,
    size_agent,
)
from etl_craft.core.text import sha256_hex


@dataclass(frozen=True)
class AgentResources:
    """Measured allocation, ceilings and reserves, refreshed on the agent heartbeat."""

    SCHEMA: ClassVar[str] = "etl-craft/agent-resources/1"
    host: str
    node_id: str
    allocation: Allocation
    ceilings: Ceilings
    sizing: Sizing
    pressure: dict[str, Measurement] = field(default_factory=dict)
    schema: str = SCHEMA
    capacity_source: CapacitySource = CapacitySource.MEASURED

    def __post_init__(self) -> None:
        """Refuse an unrecognized resource report before registering capacity."""
        if self.schema != self.SCHEMA:
            raise ConfigurationError(
                f"agent resources schema={self.schema!r}; expected {self.SCHEMA}; upgrade the agent"
            )


@dataclass(frozen=True)
class DeclaredCapacity:
    """Optional local ceilings, or an explicitly declared external runtime quota."""

    cpu: int | None = None
    memory_mb: int | None = None
    external: bool = False

    def allocation(self, measured: Allocation) -> Allocation:
        """Ceilings cannot increase physical capacity; external quotas require both dimensions."""
        if self.external and (self.cpu is None or self.memory_mb is None):
            raise ConfigurationError(
                "agent --declared-capacity needs both --cpu and --memory-mb; "
                "declare the external runtime's quota"
            )
        values = (
            ("cpus", self.cpu),
            ("memory", None if self.memory_mb is None else self.memory_mb * MIB),
        )
        for name, value in values:
            if value is None:
                continue
            if value <= 0:
                raise ConfigurationError(
                    f"agent {name} declaration={value}; expected a positive whole number; "
                    "correct --cpu or --memory-mb"
                )
            current = getattr(measured, name)
            if self.external or (current.value is not None and value < current.value):
                bound = Measurement(value, "declared", "agent quota")
                measured = (
                    replace(measured, cpus=bound)
                    if name == "cpus"
                    else replace(measured, memory=bound)
                )
        return measured


NO_DECLARATION = DeclaredCapacity()


def report(ceilings: Ceilings, declared: DeclaredCapacity = NO_DECLARATION) -> AgentResources:
    """Discover once at startup and on each heartbeat, with no extra polling loop."""
    allocation = declared.allocation(discover())
    try:
        node_id = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip()
    except OSError:
        node_id = sha256_hex(f"{socket.gethostname()}:{psutil.boot_time()}".encode())
    return AgentResources(
        socket.gethostname(),
        node_id,
        allocation,
        ceilings,
        size_agent(allocation, ceilings),
        resource_pressure(allocation),
        capacity_source=CapacitySource.DECLARED if declared.external else CapacitySource.MEASURED,
    )


def capacity(resources: AgentResources) -> tuple[float, int]:
    """Advertise usable resources after the local agent's reserve, or the external quota."""
    external = resources.capacity_source == CapacitySource.DECLARED
    cpu = resources.allocation.cpus.value
    memory = resources.allocation.memory.value
    return (
        max(0.0, (cpu or 0) - (0 if external else resources.sizing.reserve_cpus or 0)),
        max(
            0,
            int(((memory or 0) - (0 if external else resources.sizing.reserve_memory or 0)) / MIB),
        ),
    )
