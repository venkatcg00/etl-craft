"""Pool identities and resource admission are read from the same deployment metadata."""

from sqlalchemy.engine import Connection

from etl_craft.core.enums import YesNo
from etl_craft.core.errors import MetadataError
from etl_craft.engine.queries import statement


def require_pool(conn: Connection, pool: str, task: str) -> None:
    """Refuse a missing or inactive pool before admitting work or an agent."""
    row = conn.execute(statement(conn, "placement_pool"), {"pool": pool}).one_or_none()
    check_pool(pool, None if row is None else row.active_flag, task)


def check_pool(pool: str, active: str | None, task: str) -> None:
    """Use a pool state fetched with a task's existing metadata read."""
    if active != YesNo.YES:
        state = "missing" if active is None else f"ACTIVE_FLAG={active}"
        raise MetadataError(
            f"{task}: POOL={pool!r} is {state}; expected an active CFG_POOLS row; "
            "add or activate the pool in config/pools.csv and run etl-craft config apply"
        )


def claim_parameters() -> dict[str, object]:
    """Supply the shared per-kind defaults to both Engine DB claim statements."""
    from etl_craft.core.resources import MIB, TASK_COST

    return {
        "python_cpu": TASK_COST["python"][0],
        "warehouse_cpu": TASK_COST["warehouse"][0],
        "python_memory_mb": TASK_COST["python"][1] // MIB,
        "warehouse_memory_mb": TASK_COST["warehouse"][1] // MIB,
    }


def record_wait_reasons(
    conn: Connection,
    warehouse_limit: int | None,
    *,
    attempt_id: int | None = None,
    pool: str | None = None,
    resources: dict[str, object] | None = None,
) -> None:
    """Update blocked-work diagnostics only when a claim found no eligible work."""
    conn.execute(
        statement(conn, "placement_wait_reasons"),
        {
            **claim_parameters(),
            "capacity_label": "no active agent",
            "cpu_free": 0,
            "cpu_capacity": 0,
            "memory_free_mb": 0,
            "memory_capacity_mb": 0,
            "python_free_slots": 0,
            "warehouse_free_slots": 0,
            "python_capacity_slots": 0,
            "warehouse_capacity_slots": 0,
            **(resources or {}),
            "warehouse_limit": warehouse_limit,
            "attempt_id": attempt_id,
            "pool": pool,
        },
    )
