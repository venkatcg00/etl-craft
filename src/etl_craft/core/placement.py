"""Task resource requests and agent capacity share the same measured sizing estimates."""

import math
from collections.abc import Mapping
from dataclasses import dataclass

from etl_craft.core.enums import Handler, SlotKind
from etl_craft.core.errors import MetadataError
from etl_craft.core.resources import MIB, TASK_COST


@dataclass(frozen=True)
class ResourceRequest:
    """CPU cores and memory megabytes reserved for a whole task attempt."""

    cpu: float
    memory_mb: int


def resource_request(handler: str, parameters: Mapping[str, str], task: str) -> ResourceRequest:
    """Resolve defaults and refuse malformed requests before creating a task run."""
    kind = SlotKind.PYTHON if handler == Handler.PYTHON else SlotKind.WAREHOUSE
    cpu, memory = TASK_COST[kind]
    values = {"CPU_REQUEST": cpu, "MEMORY_REQUEST_MB": math.ceil(memory / MIB)}
    for name in values:
        raw = parameters.get(name)
        if raw is None:
            continue
        if not str(raw).isascii() or not str(raw).isdecimal() or int(raw) <= 0:
            raise MetadataError(
                f"task {task}: {name}={raw!r}; expected a positive whole number; "
                f"set {name} to the task's required resources or remove it to use the default"
            )
        values[name] = int(raw)
    return ResourceRequest(values["CPU_REQUEST"], int(values["MEMORY_REQUEST_MB"]))
