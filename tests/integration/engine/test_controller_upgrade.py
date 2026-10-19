"""Controller history survives the name change from every released Engine DB."""

from dataclasses import replace
from datetime import UTC, datetime

import pytest
from sqlalchemy import inspect, text

from etl_craft.engine import migrations
from etl_craft.engine.repository import controllers
from fixtures.released_schema import SCHEMAS, install


@pytest.mark.parametrize("release", sorted(p.name for p in SCHEMAS.iterdir() if p.is_dir()))
def test_controller_upgrade_preserves_closed_and_unclosed_history(
    empty_engine_db, release, monkeypatch
):
    db = empty_engine_db
    monkeypatch.delenv("ETL_CRAFT_MIGRATIONS_DIR", raising=False)
    install(db, release)
    original = migrations.migration_streams
    with monkeypatch.context() as patch:
        patch.setattr(
            migrations,
            "migration_streams",
            lambda *args, **kwargs: [
                replace(stream, files=tuple(f for f in stream.files if f.version < "0042"))
                for stream in original(*args, **kwargs)
            ],
        )
        migrations.apply_pending_migrations(db.engine)
    now = datetime(2026, 1, 2, tzinfo=UTC)
    with db.engine.begin() as conn:
        for stopped in (now, None):
            conn.execute(
                text(
                    "INSERT INTO AUD_OVERSEERS (HOST,PID,VERSION,STARTED_AT,HEARTBEAT_AT,"
                    "STOPPED_AT,STARTED_BY,STARTED_BY_KIND,STOPPED_BY,STOPPED_BY_KIND) "
                    "VALUES ('original-host',42,'0.3.0',:now,:now,:stopped,'original-person',"
                    "'HUMAN',:actor,:kind)"
                ),
                {
                    "now": now,
                    "stopped": stopped,
                    "actor": "original-person" if stopped else None,
                    "kind": "HUMAN" if stopped else None,
                },
            )
        before = (
            conn.execute(
                text(
                    "SELECT OVERSEER_ID AS controller_id, HOST AS host, "
                    "PID AS pid, VERSION AS version, "
                    "STARTED_AT AS started_at, HEARTBEAT_AT AS heartbeat_at, "
                    "STOPPED_AT AS stopped_at, "
                    "STARTED_BY AS started_by, STARTED_BY_KIND AS started_by_kind, "
                    "STOPPED_BY AS stopped_by, STOPPED_BY_KIND AS stopped_by_kind "
                    "FROM AUD_OVERSEERS ORDER BY OVERSEER_ID"
                )
            )
            .mappings()
            .all()
        )
    assert "0042_controllers.sql" in migrations.apply_pending_migrations(db.engine)
    with db.engine.connect() as conn:
        names = {name.upper() for name in inspect(conn).get_table_names()}
        assert "AUD_CONTROLLERS" in names and "AUD_OVERSEERS" not in names
        after = (
            conn.execute(
                text(
                    "SELECT CONTROLLER_ID AS controller_id, HOST AS host, "
                    "PID AS pid, VERSION AS version, "
                    "STARTED_AT AS started_at, HEARTBEAT_AT AS heartbeat_at, "
                    "STOPPED_AT AS stopped_at, "
                    "STARTED_BY AS started_by, STARTED_BY_KIND AS started_by_kind, "
                    "STOPPED_BY AS stopped_by, STOPPED_BY_KIND AS stopped_by_kind "
                    "FROM AUD_CONTROLLERS ORDER BY CONTROLLER_ID"
                )
            )
            .mappings()
            .all()
        )
    assert after == before
    current = controllers.start(db.engine)
    assert current > max(row["controller_id"] for row in before)
    controllers.heartbeat(db.engine, current, stopped=True)
    assert migrations.apply_pending_migrations(db.engine) == []
