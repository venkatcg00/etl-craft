"""Focused mirror fixtures and independently read source rows."""

from dataclasses import replace
from datetime import UTC, datetime

from sqlalchemy import text

from etl_craft.config import CloningConfig
from etl_craft.core.enums import CloningScope
from etl_craft.services import cloning


def only_table(monkeypatch, table):
    inventory = cloning._engine_tables
    monkeypatch.setattr(
        cloning,
        "_engine_tables",
        lambda *args: [(name, columns) for name, columns in inventory(*args) if name == table],
    )


def configured(world):
    return replace(world.config, cloning=CloningConfig(enabled=True, scope=CloningScope.AUD))


def insert_pause(conn, pipeline, reason, *, closed=True):
    now = datetime.now(UTC)
    return conn.execute(
        text(
            "INSERT INTO AUD_PIPELINE_PAUSES "
            "(PIPELINE_ID,REASON,PAUSED_BY,PAUSED_BY_KIND,PAUSED_AT,RESUMED_AT) "
            "VALUES (:p,:r,'tester','SYSTEM',:now,:resumed) "
            "RETURNING PIPELINE_PAUSE_ID AS pause_id"
        ),
        {"p": pipeline, "r": reason, "now": now, "resumed": now if closed else None},
    ).scalar_one()
