"""Operational warehouse retention preserves current data and project boundaries."""

from fixtures.sql_warehouse import warehouses
from fixtures.warehouse_retention import check_retention


def test_reject_retention_uses_ingestion_time_and_project_scope(sql_world):
    check_retention(sql_world)


def test_purge_and_merge_hold_the_same_target_lock(sql_world, monkeypatch):
    import threading
    from contextlib import contextmanager

    from etl_craft.core.enums import ActionName
    from etl_craft.engine import locks, resource_locks
    from etl_craft.handlers import sql
    from etl_craft.services.operations.actions import invoke
    from etl_craft.services.operations.retention import PurgeInput
    from fixtures.races import two_at_once
    from fixtures.warehouse_retention import configure, prepare

    world = sql_world
    ctx = prepare(world)
    configure(world, REJECT_RETENTION_DAYS=30)
    context = world.task(
        "merge",
        SQL_ACTION="SCD1_MERGE",
        TARGET_OBJECT="retained",
        MERGE_KEY="id",
        MERGE_COMPARE_COLUMNS="val",
        SELECT_QUERY="SELECT 1 AS id, 'updated' AS val",
    )
    target_key = locks.target(world.name("retained"), world.catalog).key
    blocked = threading.Event()
    holders = set()
    reserve, heartbeat = resource_locks.reserve, resource_locks._heartbeat

    def observed_reserve(conn, resources, holder, confirmed):
        budgets = reserve(conn, resources, holder, confirmed)
        if target_key in dict(resources) and target_key not in budgets:
            blocked.set()
        return budgets

    @contextmanager
    def observed_heartbeat(engine, key, name, holder, started, budget):
        keys = key if isinstance(key, tuple) else (key,)
        with heartbeat(engine, key, name, holder, started, budget):
            if target_key in keys:
                holders.add(holder.holder_id)
                if len(holders) == 1:
                    assert blocked.wait(timeout=10), (
                        "competing writer did not wait for the target lock"
                    )
            yield

    monkeypatch.setattr(resource_locks, "reserve", observed_reserve)
    monkeypatch.setattr(resource_locks, "_heartbeat", observed_heartbeat)
    two_at_once(
        lambda: invoke(ctx, ActionName.PURGE, PurgeInput()),
        lambda: sql.run(context, world.engine_db),
        at="etl_craft.engine.locks.target",
    )
    assert len(holders) == 2
    assert blocked.is_set()
    with world.warehouse.connect() as conn:
        from sqlalchemy import text

        assert (
            conn.execute(text(f"SELECT val AS val FROM {world.name('retained')}")).scalar_one()
            == "updated"
        )


def test_a_registered_business_target_cannot_be_purged_as_a_reject_ledger(sql_world):
    import pytest

    from etl_craft.core.enums import ActionName
    from etl_craft.core.errors import MetadataError
    from etl_craft.services.operations.actions import invoke
    from etl_craft.services.operations.retention import PurgeInput
    from fixtures.metadata import add_table
    from fixtures.warehouse_retention import configure, prepare, rows

    world = sql_world
    ctx = prepare(world)
    configure(world, REJECT_RETENTION_DAYS=30)
    with world.engine_db.begin() as conn:
        add_table(conn, world.name("retained__rejects"))
    with pytest.raises(MetadataError, match="exclusively owned maintenance object"):
        invoke(ctx, ActionName.PURGE, PurgeInput())
    assert len(rows(world)) == 3


@warehouses("duckdb", "postgres", "duckdb_iceberg")
def test_unsupported_snapshot_policy_fails_before_any_rows_are_removed(sql_world):
    from dataclasses import replace
    from datetime import UTC, datetime

    import pytest
    from sqlalchemy import text

    from etl_craft.config.model import Retention
    from etl_craft.core.enums import ActionName
    from etl_craft.core.errors import MetadataError
    from etl_craft.services.operations.actions import invoke
    from etl_craft.services.operations.retention import PurgeInput
    from fixtures.warehouse_retention import configure, prepare, rows

    world = sql_world
    ctx = prepare(world)
    ctx = replace(ctx, config=replace(ctx.config, retention=Retention(run_history_days=1)))
    with world.engine_db.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO AUD_ACTIONS (STARTED_AT,ENDED_AT,ACTOR,ACTOR_KIND,"
                "HOST,COMMAND,ARGUMENTS,"
                "OUTCOME,PROJECT_ID) VALUES (:old,:old,'test','SYSTEM','test',"
                "'keep_on_refusal','{}',"
                "'SUCCESS',1)"
            ),
            {"old": datetime(2000, 1, 1, tzinfo=UTC)},
        )
    configure(world, REJECT_RETENTION_DAYS=30, SNAPSHOT_RETENTION_DAYS=7)
    with pytest.raises(MetadataError, match=r"unsupported.*expected"):
        invoke(ctx, ActionName.PURGE, PurgeInput())
    assert len(rows(world)) == 3

    with world.engine_db.connect() as conn:
        assert (
            conn.execute(
                text("SELECT COUNT(*) AS count FROM AUD_ACTIONS WHERE COMMAND='keep_on_refusal'")
            ).scalar_one()
            == 1
        )


@warehouses("trino_iceberg")
def test_supported_snapshot_cleanup_keeps_current_data_and_has_a_read_only_plan(sql_world):
    from sqlalchemy import text

    from etl_craft.core.enums import ActionName
    from etl_craft.services.operations.actions import invoke
    from etl_craft.services.operations.retention import PurgeInput
    from fixtures.warehouse_retention import configure, prepare

    world = sql_world
    ctx = prepare(world)
    configure(world, SNAPSHOT_RETENTION_DAYS=7, ORPHAN_RETENTION_DAYS=7)
    snapshot = invoke(ctx, ActionName.PURGE, PurgeInput(dry_run=True))
    assert snapshot.counts[world.name("retained") + ":expire_snapshots"] == 1
    assert snapshot.counts[world.name("retained") + ":remove_orphan_files"] == 1
    done = invoke(ctx, ActionName.PURGE, PurgeInput())
    for key, value in snapshot.counts.items():
        assert done.counts[key] == value
    with world.warehouse.connect() as conn:
        row = conn.execute(text(f"SELECT id AS id,val AS val FROM {world.name('retained')}")).one()
    assert (row.id, row.val) == (1, "kept")


def test_a_delete_that_keeps_eligible_rows_is_not_reported_as_success(sql_world, monkeypatch):
    import pytest

    from etl_craft.core.enums import ActionName
    from etl_craft.core.errors import MetadataError
    from etl_craft.services import warehouse_retention
    from etl_craft.services.operations.actions import invoke
    from etl_craft.services.operations.retention import PurgeInput
    from fixtures.warehouse_retention import configure, prepare, rows

    world = sql_world
    ctx = prepare(world)
    configure(world, REJECT_RETENTION_DAYS=30)
    original = warehouse_retention.rejects_statement

    def blocked_delete(*args, **kwargs):
        query = original(*args, **kwargs)
        if kwargs["delete"]:
            query.text += " AND 1=0"
        return query

    monkeypatch.setattr(warehouse_retention, "rejects_statement", blocked_delete)
    with pytest.raises(MetadataError, match="1 eligible rows remain; expected zero"):
        invoke(ctx, ActionName.PURGE, PurgeInput())
    assert len(rows(world)) == 3


@warehouses("duckdb")
def test_engine_history_cleanup_holds_no_warehouse_reservation(sql_world, monkeypatch):
    from etl_craft.core.enums import ActionName
    from etl_craft.engine.queries import statement
    from etl_craft.services.operations import retention
    from etl_craft.services.operations.actions import invoke
    from etl_craft.services.operations.retention import PurgeInput
    from etl_craft.warehouse.connection import WAREHOUSE_LOCK_KEY
    from fixtures.warehouse_retention import configure, prepare

    world = sql_world
    ctx = prepare(world)
    configure(world, REJECT_RETENTION_DAYS=30)
    original = retention.purge_history
    observed = []

    def inspect_reservation(engine, *args, **kwargs):
        with engine.connect() as conn:
            assert (
                conn.execute(
                    statement(conn, "resource_lock_read"), {"key": WAREHOUSE_LOCK_KEY}
                ).one_or_none()
                is None
            )
        observed.append(True)
        return original(engine, *args, **kwargs)

    monkeypatch.setattr(retention, "purge_history", inspect_reservation)
    invoke(ctx, ActionName.PURGE, PurgeInput())
    assert observed == [True]
