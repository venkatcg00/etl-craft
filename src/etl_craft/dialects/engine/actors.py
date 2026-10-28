"""Actor markers scoped to Engine DB transactions and SQLite connections."""

from __future__ import annotations

from typing import Any

from sqlalchemy import event
from sqlalchemy.engine import Connection, Engine

from etl_craft.core.actor import migration, purpose, scoped_actor


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
                "etl_craft_actor",
                0,
                lambda: actor.name if (actor := scoped_actor()) is not None else None,
                deterministic=True,
            )
            driver.create_function(
                "etl_craft_actor_kind",
                0,
                lambda: actor.kind.value if (actor := scoped_actor()) is not None else None,
                deterministic=True,
            )
            driver.create_function("etl_craft_purpose", 0, purpose.get, deterministic=True)
            driver.create_function("etl_craft_migration", 0, migration.get, deterministic=True)

            from etl_craft.dialects.engine.guards import sqlite_authorizer

            driver.set_authorizer(sqlite_authorizer)

        def reset_inserts(
            conn: Connection,
            cursor: Any,
            statement: str,
            parameters: Any,
            context: Any,
            executemany: bool,
        ) -> None:
            conn.info["etl_craft_inserting"].clear()
            trusted = scoped_actor() is not None
            if conn.info.get("etl_craft_ddl_actor") != trusted:
                from etl_craft.dialects.engine.guards import sqlite_authorizer

                # Changing the authorizer invalidates SQLite's cached authorization decisions.
                driver = conn.connection.driver_connection
                assert driver is not None
                driver.set_authorizer(sqlite_authorizer)
                conn.info["etl_craft_ddl_actor"] = trusted

        event.listen(engine, "connect", connected)
        event.listen(engine, "before_cursor_execute", reset_inserts)
    else:

        def begun(conn: Connection) -> None:
            actor = scoped_actor()
            conn.exec_driver_sql(
                "SELECT set_config('etl_craft.actor', %s, true) AS actor, "
                "set_config('etl_craft.actor_kind', %s, true) AS actor_kind, "
                "set_config('etl_craft.purpose', %s, true) AS purpose, "
                "set_config('etl_craft.migration', %s, true) AS migration",
                (
                    "" if actor is None else actor.name,
                    "" if actor is None else actor.kind.value,
                    purpose.get(),
                    migration.get(),
                ),
            )

        event.listen(engine, "begin", begun)
