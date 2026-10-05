"""Actor markers scoped to Engine DB transactions and SQLite connections."""

from __future__ import annotations

from typing import Any

from sqlalchemy import event
from sqlalchemy.engine import Connection, Engine

from etl_craft.core.actor import current_actor, migration, purpose


def mark_connections(engine: Engine) -> None:
    """Register the engine's identity functions or transaction-local PostgreSQL settings."""
    if engine.dialect.name == "sqlite":

        def connected(driver: Any, record: Any) -> None:
            inserting: set[str] = set()
            record.info["etl_craft_inserting"] = inserting

            def enter(table: str) -> int:
                inserting.add(table)
                return 1

            def leave(table: str) -> int:
                inserting.discard(table)
                return 1

            driver.create_function("etl_craft_enter_insert", 1, enter)
            driver.create_function("etl_craft_exit_insert", 1, leave)
            driver.create_function("etl_craft_inserting", 1, lambda table: table in inserting)
            driver.create_function(
                "etl_craft_actor", 0, lambda: current_actor().name, deterministic=True
            )
            driver.create_function(
                "etl_craft_actor_kind", 0, lambda: current_actor().kind.value, deterministic=True
            )
            driver.create_function("etl_craft_purpose", 0, purpose.get, deterministic=True)
            driver.create_function("etl_craft_migration", 0, migration.get, deterministic=True)

        def reset_inserts(
            conn: Connection,
            cursor: Any,
            statement: str,
            parameters: Any,
            context: Any,
            executemany: bool,
        ) -> None:
            conn.info["etl_craft_inserting"].clear()

        event.listen(engine, "connect", connected)
        event.listen(engine, "before_cursor_execute", reset_inserts)
    else:

        def begun(conn: Connection) -> None:
            actor = current_actor()
            conn.exec_driver_sql(
                "SELECT set_config('etl_craft.actor', %s, true) AS actor, "
                "set_config('etl_craft.actor_kind', %s, true) AS actor_kind, "
                "set_config('etl_craft.purpose', %s, true) AS purpose, "
                "set_config('etl_craft.migration', %s, true) AS migration",
                (actor.name, actor.kind.value, purpose.get(), migration.get()),
            )

        event.listen(engine, "begin", begun)
