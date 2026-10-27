"""One unsigned, bounded controller scrape for both HTTP listeners."""

import json
from collections import Counter, defaultdict
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy.engine import Connection, Row
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
from etl_craft.core.agent_resources import AgentResources
from etl_craft.core.enums import AgentStatus, AttemptStatus, ConnectionUse, SlotKind
from etl_craft.core.errors import EtlCraftError
from etl_craft.core.metrics import Labels
from etl_craft.core.resources import MIB, TASK_COST
from etl_craft.core.time import as_utc
from etl_craft.dialects.engine import for_engine
from etl_craft.engine.metrics import install, state
from etl_craft.engine.process_reservations import connection_slot
from etl_craft.engine.queries import statement
from etl_craft.services.operations.agent_capacity import AgentUsage, available
from etl_craft.services.operations.agents import registered_agents
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.serialization import from_json

CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


@dataclass(frozen=True)
class Exposition:
    """Already formatted HTTP response; interfaces only copy these fields."""

    body: str
    status: int = 200
    content_type: str = CONTENT_TYPE


def scrape(ctx: OperationContext, *, now: datetime | None = None) -> Exposition:
    """Use a dedicated probe allowance and never write a scrape audit record."""
    metrics = install(ctx.engine)
    if not metrics.scrape_lock.acquire(blocking=False):
        return busy()
    try:
        return _scrape(ctx, now=now)
    finally:
        metrics.scrape_lock.release()


def busy() -> Exposition:
    """Describe saturation without making a work-connection request."""
    return Exposition(
        "metrics connection is busy; expected a free probe session; "
        "retry the next scrape or another controller copy\n",
        503,
    )


@contextmanager
def probe_session(ctx: OperationContext) -> Iterator[Connection]:
    """Keep the scrape outside the work pool without constructing another Engine."""
    if for_engine(ctx.engine).supports_api_replicas:
        with for_engine(ctx.engine).session_connection(ctx.engine, ctx.config) as conn:
            yield conn
        return
    pool = ctx.engine.pool.recreate()
    try:
        with Connection(ctx.engine, connection=pool.connect()) as conn:
            yield conn
    finally:
        pool.dispose()


def _scrape(ctx: OperationContext, *, now: datetime | None = None) -> Exposition:
    metrics = install(ctx.engine)
    moment = now or datetime.now(UTC)
    with acting_as(SYSTEM_ACTOR), connection_slot(ctx.engine, ConnectionUse.PROBE) as free:
        if not free:
            return busy()
        try:
            gauges: dict[str, dict[Labels, float]] = {
                "controller_leader": {(): float(metrics.leader)}
            }
            if metrics.leader:
                with probe_session(ctx) as conn:
                    conn = for_engine(conn).snapshot(conn)
                    with conn.begin():
                        gauges.update(cluster_gauges(conn, moment))
                current = state(ctx.engine)
                if current is None or not current.leader:
                    return Exposition(metrics.render({"controller_leader": {(): 0}}))
                if metrics.heartbeat is not None:
                    gauges["controller_heartbeat_age_seconds"] = {
                        (): max(0, moment.timestamp() - metrics.heartbeat)
                    }
            return Exposition(metrics.render(gauges))
        except (EtlCraftError, SQLAlchemyError):
            return Exposition(
                "metrics Engine DB is unavailable; expected a readable current Engine DB; "
                "check connectivity, server logs and applied migrations, then retry the scrape\n",
                503,
            )


def cluster_gauges(conn: Connection, now: datetime) -> dict[str, dict[Labels, float]]:
    """Four shared reads, independent of project count and terminal history."""
    gauges: dict[str, dict[Labels, float]] = defaultdict(dict)
    for row in conn.execute(statement(conn, "metrics_runs")):
        labels: Labels = (("status", str(row.status)),)
        gauges["runs"][labels] = gauges["runs"].get(labels, 0) + row.value
    attempts = list(conn.execute(statement(conn, "agent_attempts"), {"owner": None}))
    counts: Counter[Labels] = Counter()
    queues: Counter[Labels] = Counter()
    ages: dict[Labels, float] = {}
    for row in attempts:
        counts[(("status", str(row.status)),)] += 1
        if row.status == AttemptStatus.QUEUED:
            labels = (("pool", row.pool), ("slot_kind", row.slot_kind))
            queues[labels] += 1
            ages[labels] = max(
                ages.get(labels, 0), (now - as_utc(row.queued_at)).total_seconds(), 0
            )
    gauges["attempts"] = dict(counts)
    gauges["queue_depth"] = dict(queues)
    gauges["queue_oldest_age_seconds"] = ages
    for row in conn.execute(statement(conn, "metrics_gates")):
        gauges["gate_waits"][()] = gauges["gate_waits"].get((), 0) + row.value
    gauges.update(agent_gauges(conn, now, attempts))
    return dict(gauges)


def agent_gauges(
    conn: Connection, now: datetime, attempts: Sequence[Row[tuple[object, ...]]]
) -> dict[str, dict[Labels, float]]:
    """Calculate current agent pressure through the admission accountant."""
    gauges: dict[str, dict[Labels, float]] = defaultdict(dict)
    agents = registered_agents(conn, current=True)
    reports = {
        row.agent_id: from_json(AgentResources, json.loads(row.resource_report)) for row in agents
    }
    grouped: dict[tuple[int, str], list[Row[tuple[object, ...]]]] = defaultdict(list)
    owners = {str(agent.agent_id): agent.agent_id for agent in agents}
    for row in attempts:
        if (
            row.owner_id in owners
            and row.status != AttemptStatus.QUEUED
            and as_utc(row.lease_expires_at) > now
        ):
            grouped[(owners[row.owner_id], row.slot_kind)].append(row)
    usage = [
        AgentUsage(
            agent.agent_id,
            agent.resource_report,
            kind,
            sum(float(r.reserved_cpu) for r in rows),
            sum(int(r.reserved_memory_mb) for r in rows),
            len(rows),
        )
        for agent in agents
        for kind in SlotKind
        if (rows := grouped.get((agent.agent_id, kind)))
    ]
    for agent in agents:
        labels = (("pool", agent.pool), ("agent", str(agent.agent_id)))
        report = reports[agent.agent_id]
        age = max(0, (now - as_utc(agent.heartbeat_at)).total_seconds())
        active = age < 30 and agent.status == AgentStatus.ACTIVE
        pool = (("pool", agent.pool),)
        gauges["active_agents"][pool] = gauges["active_agents"].get(pool, 0) + active
        free, cpu, memory = available(
            conn,
            agent.agent_id,
            report,
            usage=[u for u in usage if reports[u.agent_id].node_id == report.node_id],
        )
        gauges["agent_heartbeat_age_seconds"][labels] = age
        gauges["agent_running_children"][labels] = sum(
            row.status == AttemptStatus.RUNNING and row.owner_id == str(agent.agent_id)
            for row in attempts
        )
        for kind in SlotKind:
            capacity = agent.python_slots if kind == SlotKind.PYTHON else agent.warehouse_slots
            used = len(grouped.get((agent.agent_id, kind), []))
            left = (
                min(
                    free[kind],
                    int(cpu // TASK_COST[kind][0]),
                    int(memory * MIB // TASK_COST[kind][1]),
                )
                if active
                else 0
            )
            for status, value in (("capacity", capacity), ("used", used), ("free", left)):
                gauges["agent_slots"][(*labels, ("slot_kind", kind), ("status", status))] = value
    return dict(gauges)
