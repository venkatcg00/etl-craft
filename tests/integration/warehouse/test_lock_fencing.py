"""A lock passes on only after the expired holder's warehouse statements are confirmed gone.

A statement keeps running on the server after its client is cut off, and may still commit.
These tests leave one running under an expired lock and check that the next holder stops it
before writing, so the target holds exactly one writer's result.
"""

import pytest

from etl_craft.core.errors import LockTimeoutError
from etl_craft.dialects.warehouse.postgres import PostgresWarehouse
from etl_craft.engine.resource_locks import holder_tag
from etl_craft.handlers.resources import LocalLocker
from etl_craft.warehouse import fencing
from etl_craft.warehouse.connection import warehouse_dialect
from fixtures.lock_fencing import KEY, OLD, expired_lock, lock_holder, start_old_write
from fixtures.sql_warehouse import warehouses

# A statement that runs well past the test's own deadline unless it is stopped.
SLOW_WRITE = {
    "postgres": "INSERT INTO {table} SELECT 'old' FROM pg_sleep(60)",
    "trino_iceberg": (
        "INSERT INTO {table} SELECT 'old' FROM (SELECT count(*) AS n "
        "FROM UNNEST(sequence(1, 10000)) a CROSS JOIN UNNEST(sequence(1, 10000)) b "
        "CROSS JOIN UNNEST(sequence(1, 10000)) c) "
        "WHERE n > 0"
    ),
}


@warehouses("postgres", "trino_iceberg")
def test_the_next_holder_stops_the_expired_holders_statement_before_writing(sql_world):
    table = sql_world.name("fenced")
    sql_world.execute(f"CREATE TABLE {table} (writer VARCHAR(10))")
    expired_lock(sql_world)
    engine, thread, errors = start_old_write(
        sql_world, SLOW_WRITE[sql_world.kind].format(table=table)
    )
    try:
        with LocalLocker(sql_world.engine_db, sql_world.config).hold(KEY, "fenced target", 30):
            assert lock_holder(sql_world) != OLD
            sql_world.execute(f"INSERT INTO {table} VALUES ('new')")
        thread.join(30)
        assert not thread.is_alive()
        assert errors, "the expired holder's statement finished instead of being stopped"
        assert sql_world.rows(f"SELECT writer FROM {table}") == [("new",)]
    finally:
        engine.dispose()


@warehouses("postgres")
def test_an_unconfirmed_stop_leaves_the_lock_with_its_holder(sql_world, monkeypatch):
    table = sql_world.name("fenced")
    sql_world.execute(f"CREATE TABLE {table} (writer VARCHAR(10))")
    expired_lock(sql_world)
    engine, thread, _ = start_old_write(sql_world, SLOW_WRITE[sql_world.kind].format(table=table))
    stop = PostgresWarehouse.stop_tagged
    monkeypatch.setattr(PostgresWarehouse, "stop_tagged", lambda self, conn, tag: None)
    monkeypatch.setattr(fencing, "STOP_WAIT_SECONDS", 1)
    try:
        with (
            pytest.raises(LockTimeoutError) as error,
            LocalLocker(sql_world.engine_db, sql_world.config).hold(KEY, "fenced target", 5),
        ):
            pytest.fail("the lock passed while the expired holder's statement still ran")
        message = str(error.value)
        assert "fenced target" in message
        assert f"the lock stays with {OLD}" in message
        assert f"tagged {holder_tag(OLD)} still run" in message
        assert lock_holder(sql_world) == OLD
    finally:
        with sql_world.warehouse.begin() as conn:
            stop(warehouse_dialect(sql_world.config), conn, holder_tag(OLD))
        thread.join(30)
        engine.dispose()
