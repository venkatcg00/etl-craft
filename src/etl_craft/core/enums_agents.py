"""Closed values for agent ownership, capacity and task reports."""

from enum import StrEnum


class AgentStatus(StrEnum):
    """Whether an agent accepts claims, is draining, or has departed."""

    ACTIVE = "ACTIVE"
    DRAINING = "DRAINING"
    GONE = "GONE"


class AgentCommand(StrEnum):
    """Durable operator requests acknowledged by their agent."""

    DRAIN = "DRAIN"
    RESUME = "RESUME"
    STOP = "STOP"


class SlotKind(StrEnum):
    """The process capacity an entire attempt consumes."""

    PYTHON = "python"
    WAREHOUSE = "warehouse"
