"""Cloning the Engine DB tables into each warehouse, by hand and at the end of a run."""

import json
import logging
from dataclasses import replace

import pytest
from sqlalchemy import text

from etl_craft.config import CloningConfig
from etl_craft.core.enums import CloningScope
from etl_craft.core.errors import CloningError, ConfigurationError
from etl_craft.execution.pipeline import finalize_active_run
from etl_craft.services.cloning import clone, run_hooks
from fixtures.metadata import add_pipeline, add_task


def cloning(world, scope=CloningScope.ALL):
    return replace(world.config, cloning=CloningConfig(enabled=True, scope=scope))


def engine_tables(world, prefixes=("CFG_", "AUD_")):
    with world.engine_db.connect() as conn:
        names = conn.execute(text("SELECT name FROM sqlite_master WHERE type = 'table'")).scalars()
        return sorted(n.upper() for n in names if n.upper().startswith(prefixes))


def count(world, table):
    return world.rows(f"SELECT COUNT(*) FROM {world.name(table)}")[0][0]


def test_every_table_is_mirrored_and_replaced_on_the_next_clone(sql_world):
    world = sql_world
    with world.engine_db.begin() as conn:
        add_task(conn, world.pipeline_id, "load", SQL_ACTION="CREATE_TABLE")
        conn.execute(
            text("UPDATE CFG_PIPELINES SET PIPELINE_PARAMETERS = :p, SLA_IN_HOURS = 1.5"),
            {"p": json.dumps({"RETRIES": 2, "TAGS": ["sales"]})},
        )
    config = cloning(world)
    first = clone(world.engine_db, config)
    assert [t.table for t in first] == engine_tables(world)
    assert all(t.created for t in first)
    assert {t.mirror for t in first} == {world.name(t.table) for t in first}
    assert {t.table: t.rows for t in first}["CFG_TASK_PARAMETERS"] == 1
    params, sla, created = world.rows(
        f"SELECT PIPELINE_PARAMETERS, SLA_IN_HOURS, CREATE_DATE FROM {world.name('CFG_PIPELINES')}"
    )[0]
    assert json.loads(params) == {"RETRIES": 2, "TAGS": ["sales"]}
    assert float(sla) == 1.5
    assert created is not None
    assert count(world, "AUD_PIPELINES_RUN_LOG") == 1

    with world.engine_db.begin() as conn:
        add_pipeline(conn, "Q")
    second = {t.table: t for t in clone(world.engine_db, config)}
    assert not any(t.created for t in second.values())
    assert second["CFG_PIPELINES"].rows == 2
    assert count(world, "CFG_PIPELINES") == 2


def test_a_mirror_gains_new_columns_and_keeps_old_ones(sql_world):
    world = sql_world
    world.execute(
        f"CREATE TABLE {world.name('CFG_PIPELINES')} (PIPELINE_ID BIGINT, RETIRED BIGINT)"
    )
    world.execute(f"INSERT INTO {world.name('CFG_PIPELINES')} VALUES (99, 1)")
    tables = {t.table: t for t in clone(world.engine_db, cloning(world, CloningScope.CFG))}
    pipelines = tables["CFG_PIPELINES"]
    assert not pipelines.created
    assert pipelines.added[:2] == ("PIPELINE_CODE", "PIPELINE_NAME")
    assert "PIPELINE_ID" not in pipelines.added
    columns = world.columns("CFG_PIPELINES")
    assert columns[:2] == ["pipeline_id", "retired"] and "pipeline_code" in columns
    assert world.rows(f"SELECT PIPELINE_CODE, RETIRED FROM {world.name('CFG_PIPELINES')}") == [
        ("P", None)
    ]
    again = {t.table: t for t in clone(world.engine_db, cloning(world, CloningScope.CFG))}
    assert again["CFG_PIPELINES"].added == ()
    assert set(tables) == set(engine_tables(world, ("CFG_",)))
    assert not any(name.startswith("aud_") for name in world.tables())


def test_a_clone_runs_when_a_run_ends(sql_world):
    world = sql_world
    config = cloning(world, CloningScope.AUD)
    ended = finalize_active_run(
        world.engine_db, config, "P", hooks=run_hooks(config, world.engine_db)
    )
    assert ended.status == "SUCCESS"
    assert world.rows(f"SELECT STATUS FROM {world.name('AUD_PIPELINES_RUN_LOG')}") == [("SUCCESS",)]


def test_a_failed_clone_names_the_table_and_leaves_the_run_as_it_ended(sql_world, caplog):
    world = sql_world
    profile = replace(world.config.warehouse.active, schema="t_missing_schema")
    config = replace(
        cloning(world), warehouse=replace(world.config.warehouse, profiles={"dev": profile})
    )
    # DuckDB over Iceberg finds the schema missing as it connects; the others at the first table.
    with pytest.raises((CloningError, ConfigurationError), match="t_missing_schema"):
        clone(world.engine_db, config)
    with caplog.at_level(logging.ERROR, logger="etl_craft"):
        ended = finalize_active_run(
            world.engine_db, config, "P", hooks=run_hooks(config, world.engine_db)
        )
    assert ended.status == "SUCCESS"
    assert "the on_finalized hook failed; the run's outcome is unchanged" in caplog.text
    assert "t_missing_schema" in caplog.text
