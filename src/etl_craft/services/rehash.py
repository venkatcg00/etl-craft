"""Upgrade a target's stored change hashes without altering its rows or audit columns."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.config.targets import active_catalog
from etl_craft.core.enums import SqlAction
from etl_craft.core.errors import HandlerError
from etl_craft.core.text import is_safe_identifier, qualify, split_object_ref
from etl_craft.engine import locks
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.hash_versions import save_hash_version
from etl_craft.engine.repository.tasks import fetch_task_parameters
from etl_craft.handlers.resources import LocalLocker
from etl_craft.handlers.sql.protection import refuse_replacement
from etl_craft.handlers.sql.session import Session
from etl_craft.warehouse.connection import open_warehouse, warehouse_dialect


@dataclass(frozen=True)
class RehashResult:
    """The validated target, row count and statement, whether applied or only planned."""

    SCHEMA: ClassVar[str] = "etl-craft/rehash/1"
    target: str
    rows: int
    sql: str
    dry_run: bool


def rehash(
    engine: Engine, config: ConnectorConfig, target: str, *, dry_run: bool = False
) -> RehashResult:
    """Recompute every target version using its active merge tasks' common compare columns."""
    split_object_ref(target)
    catalog = active_catalog(config)
    qualified = qualify(target, catalog)
    with locks.target(target, catalog).hold(LocalLocker(engine, config)):
        with engine.connect() as conn:
            task_ids = conn.execute(statement(conn, "rehash_tasks")).scalars().all()
            tasks = {task_id: fetch_task_parameters(conn, task_id) for task_id in task_ids}
        contracts = set()
        for params in tasks.values():
            if params.get("SQL_ACTION") not in {SqlAction.SCD1_MERGE, SqlAction.SCD2_MERGE}:
                continue
            if (
                not params.get("TARGET_OBJECT")
                or qualify(params["TARGET_OBJECT"], catalog).lower() != qualified.lower()
            ):
                continue
            columns = tuple(
                c.strip().lower() for c in params.get("MERGE_COMPARE_COLUMNS", "").split("|")
            )
            if not columns or any(not is_safe_identifier(c) for c in columns):
                raise HandlerError(
                    f"{qualified}: active merge task has invalid MERGE_COMPARE_COLUMNS; "
                    "fix its metadata"
                )
            contracts.add(columns)
        if len(contracts) != 1:
            raise HandlerError(
                f"{qualified}: expected one common ordered MERGE_COMPARE_COLUMNS contract "
                f"across active merge tasks; found {sorted(contracts)}. "
                "Configure matching compare columns before rehashing"
            )
        columns = next(iter(contracts))
        with (
            open_warehouse(config, LocalLocker(engine, config)) as warehouse,
            warehouse.begin() as conn,
        ):
            session = Session(
                conn,
                warehouse_dialect(config),
                catalog=catalog,
                action="REHASH",
                target_object=target,
                task_run_id=0,
                params={},
            )
            if not dry_run:
                refuse_replacement(session)
            shape = {name.lower() for name, _ in session.target_columns()}
            if "hash_key" not in shape:
                raise HandlerError(
                    f"{qualified} does not exist or lacks HASH_KEY; "
                    "rehash requires an existing merge target"
                )
            expression = session.hash(list(columns), columns)
            sql = f"UPDATE {session.target} SET HASH_KEY = {expression}"
            count = session.count(f"SELECT COUNT(*) FROM {session.target}", step="rows to rehash")
            if not dry_run:
                session.run(sql, step="recompute every stored hash")
        if not dry_run:
            with engine.begin() as conn:
                save_hash_version(conn, qualified, 2)
    return RehashResult(qualified, count, sql, dry_run)
