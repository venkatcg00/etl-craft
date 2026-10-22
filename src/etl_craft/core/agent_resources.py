"""Agent resource evidence produced by the shared discovery and sizing report."""

import socket
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar

import psutil

from etl_craft.core.errors import ConfigurationError
from etl_craft.core.resources import (
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

    def __post_init__(self) -> None:
        """Refuse an unrecognized resource report before registering capacity."""
        if self.schema != self.SCHEMA:
            raise ConfigurationError(
                f"agent resources schema={self.schema!r}; expected {self.SCHEMA}; upgrade the agent"
            )


def report(ceilings: Ceilings) -> AgentResources:
    """Discover once at startup and on each heartbeat, with no extra polling loop."""
    allocation = discover()
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
    )
