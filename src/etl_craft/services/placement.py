"""Validation reads pool and capacity evidence once, never summing distinct allocations."""

from dataclasses import dataclass

from sqlalchemy.engine import Connection

from etl_craft.config import ConnectorConfig
from etl_craft.core.enums import DeploymentProfile
from etl_craft.core.placement import ResourceRequest
from etl_craft.engine.placement import check_pool
from etl_craft.engine.queries import statement
from etl_craft.execution.placement import local_capacity, require_capacity


@dataclass(frozen=True)
class Placement:
    """One validation snapshot of pool state and individual advertised capacities."""

    pools: dict[str, str]
    agents: dict[str, dict[int, ResourceRequest]]
    local: ResourceRequest | None

    def check(self, task: str, request: ResourceRequest, pool: str) -> None:
        """Refuse invalid pool names or a request that fits no available allocation."""
        check_pool(pool, self.pools.get(pool), task)
        if self.local is not None:
            require_capacity(request, self.local.cpu, self.local.memory_mb, task, "local pool")
            return
        agents = self.agents.get(pool, {})
        if not agents or any(
            request.cpu <= a.cpu and request.memory_mb <= a.memory_mb for a in agents.values()
        ):
            return
        largest_id, largest = max(agents.items(), key=lambda item: (item[1].memory_mb, item[1].cpu))
        require_capacity(
            request, largest.cpu, largest.memory_mb, task, f"pool {pool}, agent {largest_id}"
        )


def placement(conn: Connection, config: ConnectorConfig) -> Placement:
    """Read each kind of placement metadata once for the entire validation."""
    pools = {
        row.pool_name: row.active_flag for row in conn.execute(statement(conn, "placement_pools"))
    }
    agents: dict[str, dict[int, ResourceRequest]] = {}
    local = None
    if config.deployment == DeploymentProfile.CONTROLLER_AGENTS:
        for row in conn.execute(statement(conn, "placement_agents")):
            agents.setdefault(row.pool, {})[int(row.agent_id)] = ResourceRequest(
                row.cpu_capacity, row.memory_capacity_mb
            )
    else:
        measured = local_capacity(config)
        local = ResourceRequest(measured.cpu, measured.memory_mb)
    return Placement(pools, agents, local)
