"""The recorded deployment profile alone selects where task processes execute."""

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.enums import DeploymentProfile
from etl_craft.execution.attempt_types import ChildOptions
from etl_craft.execution.pools.local import LocalPool
from etl_craft.execution.pools.queue import QueuePool


def create_pool(
    engine: Engine, config: ConnectorConfig, child: ChildOptions
) -> LocalPool | QueuePool:
    """Keep PostgreSQL local unless CONTROLLER_AGENTS was explicitly selected."""
    if config.deployment == DeploymentProfile.CONTROLLER_AGENTS:
        return QueuePool(engine, config)
    return LocalPool(engine, config, child=child)
