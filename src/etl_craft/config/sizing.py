"""Measure the allocation when a role starts, and apply the declared profile's sizes."""

from __future__ import annotations

import logging
from dataclasses import replace

from etl_craft.config.model import ConnectorConfig
from etl_craft.core.enums import DeploymentProfile
from etl_craft.core.errors import ConfigurationError
from etl_craft.core.resources import (
    CONTROLLER_MEMORY,
    MIB,
    TASK_COST,
    Allocation,
    Ceilings,
    Host,
    Sizing,
    describe,
    discover,
    size,
)

logger = logging.getLogger(__name__)


def ceilings(config: ConnectorConfig) -> Ceilings:
    """Return the slot settings the team wrote explicitly; defaults are not ceilings."""
    limits = config.limits
    return Ceilings(
        None,
        limits.local_python_slots,
        limits.local_warehouse_slots,
    )


def start_role(
    config: ConnectorConfig, *, server: bool, host: Host | None = None
) -> ConnectorConfig:
    """Measure and log the allocation; with a profile, size the role or refuse to start."""
    allocation = discover(host)
    logger.info(
        "allocation in %s: CPUs %s; memory %s",
        allocation.scope,
        describe(allocation.cpus, "CPUs"),
        describe(allocation.memory, "bytes"),
    )
    if config.deployment is None:
        return config
    sizing = size(allocation, config.deployment, ceilings(config))
    if server or config.deployment != DeploymentProfile.CONTROLLER_AGENTS:
        _refuse_unaffordable(allocation, sizing, server=server)
    logger.info(
        "%s sizing: %d task slots (%s), %d python, %d warehouse, %d server threads, "
        "Engine DB pool %d%s",
        sizing.profile,
        sizing.task_slots.value,
        sizing.task_slots.reason,
        sizing.python_slots.value,
        sizing.warehouse_slots.value,
        sizing.server_threads.value,
        sizing.engine_pool.value,
        "".join(f"; {fallback}" for fallback in sizing.fallbacks),
    )
    limits = replace(
        config.limits,
        max_parallel_tasks=sizing.task_slots.value,
        local_python_slots=sizing.python_slots.value,
        local_warehouse_slots=sizing.warehouse_slots.value,
    )
    return replace(config, limits=limits, sizing=sizing)


def _refuse_unaffordable(allocation: Allocation, sizing: Sizing, *, server: bool) -> None:
    """Refuse a role the allocation cannot hold, rather than be killed later."""
    memory = allocation.memory.value
    if memory is not None and memory < CONTROLLER_MEMORY:
        raise ConfigurationError(
            f"this allocation has {memory / MIB:.0f} MB ({allocation.memory.source}); the "
            f"etl-craft process needs about {CONTROLLER_MEMORY // MIB} MB. Raise its memory"
        )
    if sizing.task_slots.value:
        return
    cpu_cost, memory_cost = TASK_COST["warehouse"]
    if server:
        logger.warning(
            "starting with no task slots (%s): runs stay queued until a slot fits",
            sizing.task_slots.reason,
        )
        return
    raise ConfigurationError(
        f"this allocation has {describe(allocation.cpus, 'CPUs')} and "
        f"{describe(allocation.memory, 'bytes')}; after the {sizing.profile} reserve and the "
        f"etl-craft process no task slot fits ({sizing.task_slots.reason}; one task needs about "
        f"{cpu_cost:g} CPU and {memory_cost // MIB} MB). Raise the allocation, or choose the "
        "controller profile on a dedicated host"
    )
