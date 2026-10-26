"""Incremental mirrors equal retained source rows with bounded changed-row copy volume."""

import json
import logging
from dataclasses import replace

import pytest
from sqlalchemy import text

from etl_craft.core.actor import purpose
from etl_craft.core.errors import CloningError, ConfigurationError, InjectedFaultError
from etl_craft.services.cloning import clone
from fixtures.cloning import configured, insert_pause, only_table

TABLE = "AUD_PIPELINE_PAUSES"


def assert_source(world):
    with world.engine_db.connect() as conn:
        expected = (
            conn.execute(
                text(
                    "SELECT PIPELINE_PAUSE_ID AS pause_id, REASON AS reason "
                    "FROM AUD_PIPELINE_PAUSES "
                    "ORDER BY PIPELINE_PAUSE_ID"
                )
            )
            .mappings()
            .all()
        )
    with world.warehouse.connect() as conn:
        actual = (
            conn.execute(
                text(
                    "SELECT PIPELINE_PAUSE_ID AS pause_id, REASON AS reason "
                    f"FROM {world.name(TABLE)} "
                    "ORDER BY PIPELINE_PAUSE_ID"
                )
            )
            .mappings()
            .all()
        )
    assert actual == expected


def test_incremental_matches_full_after_changes_and_retention(sql_world, monkeypatch):
    world = sql_world
    only_table(monkeypatch, TABLE)
    with world.engine_db.begin() as conn:
        keys = [insert_pause(conn, world.pipeline_id, f"row {i}") for i in range(501)]
    config = configured(world)
    assert clone(world.engine_db, config)[0].rows == 501
    assert clone(world.engine_db, config)[0].rows == 0
    with world.engine_db.begin() as conn:
        conn.execute(
            text("UPDATE AUD_PIPELINE_PAUSES SET REASON=:r WHERE PIPELINE_PAUSE_ID=:p"),
            {"r": "a bound ' quote", "p": keys[0]},
        )
        insert_pause(conn, world.pipeline_id, "new")
    assert clone(world.engine_db, config)[0].rows == 2
    token = purpose.set("retention")
    try:
        with world.engine_db.begin() as conn:
            conn.execute(
                text("DELETE FROM AUD_PIPELINE_PAUSES WHERE PIPELINE_PAUSE_ID=:p"), {"p": keys[-1]}
            )
    finally:
        purpose.reset(token)
    assert clone(world.engine_db, config)[0].rows == 0
    assert_source(world)
    assert clone(world.engine_db, config)[0].rows == 0


def test_mirror_commit_without_acknowledgement_replays_idempotently(sql_world, monkeypatch):
    world = sql_world
    only_table(monkeypatch, TABLE)
    config = configured(world)
    clone(world.engine_db, config)
    with world.engine_db.begin() as conn:
        insert_pause(conn, world.pipeline_id, "pending")
    monkeypatch.setenv("ETL_CRAFT_FAULT", "clone.after_mirror_commit")
    with pytest.raises(InjectedFaultError):
        clone(world.engine_db, config)
    monkeypatch.delenv("ETL_CRAFT_FAULT")
    clone(world.engine_db, config)
    assert_source(world)
    assert clone(world.engine_db, config)[0].rows == 0


def test_warehouse_outage_keeps_changes_pending_for_recovery(sql_world, monkeypatch):
    world = sql_world
    only_table(monkeypatch, TABLE)
    config = configured(world)
    clone(world.engine_db, config)
    with world.engine_db.begin() as conn:
        insert_pause(conn, world.pipeline_id, "warehouse retry")
    unavailable = replace(
        config, warehouse=replace(config.warehouse, schema="missing_clone_schema")
    )
    with pytest.raises((CloningError, ConfigurationError), match="missing_clone_schema"):
        clone(world.engine_db, unavailable)
    assert clone(world.engine_db, config)[0].rows == 1
    assert_source(world)


def test_clone_waits_then_records_a_skip_without_touching_the_warehouse(
    sql_world, monkeypatch, caplog
):
    from contextlib import contextmanager

    from etl_craft.core.errors import LockTimeoutError
    from etl_craft.engine import locks

    world = sql_world
    waited = []

    @contextmanager
    def busy(self, engine, wait_seconds=0):
        waited.append(wait_seconds)
        raise LockTimeoutError("busy")
        yield

    monkeypatch.setattr(locks.EngineLock, "hold", busy)
    config = configured(world)
    config = replace(config, cloning=replace(config.cloning, lock_wait_seconds=2))
    with caplog.at_level(logging.WARNING):
        assert clone(world.engine_db, config) == []
    assert waited == [2]
    assert "clone skipped" in caplog.text and "next scheduled cycle will retry" in caplog.text
    with world.engine_db.connect() as conn:
        arguments = (
            conn.execute(
                text("SELECT ARGUMENTS AS arguments FROM AUD_ACTIONS WHERE COMMAND='clone'")
            )
            .scalars()
            .all()
        )
    assert any("clone skipped" in json.loads(value).get("skip", "") for value in arguments)
