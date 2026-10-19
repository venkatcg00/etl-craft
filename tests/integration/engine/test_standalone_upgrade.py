"""Pipeline parameter cleanup retains alert settings and all existing execution evidence."""

import json
from dataclasses import replace

import pytest
from sqlalchemy import text

from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.engine import migrations
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.requests import HistoryInput, RunId
from fixtures.metadata import add_pipeline
from fixtures.released_schema import SCHEMAS, install

OBSOLETE = {
    "CATCHUP": True,
    "DEPENDS_ON_PAST": True,
    "EMAIL_ON_FAILURE": True,
    "RETRIES": 3,
    "RETRY_DELAY_MINUTES": 2,
}


@pytest.mark.parametrize("release", sorted(p.name for p in SCHEMAS.iterdir() if p.is_dir()))
def test_pipeline_parameter_upgrade_retains_history(
    empty_engine_db, release, monkeypatch, tmp_path
):
    db = empty_engine_db
    install(db, release)
    original = migrations.migration_streams
    monkeypatch.delenv("ETL_CRAFT_MIGRATIONS_DIR", raising=False)
    monkeypatch.chdir(tmp_path)
    with monkeypatch.context() as patch:
        patch.setattr(
            migrations,
            "migration_streams",
            lambda *args, **kwargs: [
                replace(stream, files=tuple(f for f in stream.files if f.version < "0043"))
                for stream in original(*args, **kwargs)
            ],
        )
        migrations.apply_pending_migrations(db.engine)
    retained = {
        "TAGS": ["sales"],
        "EMAIL_RECIPIENTS": ["ops@example.com"],
        "TEAM_KEY": {"nested": 1},
    }
    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        untouched = add_pipeline(conn, "UNCHANGED")
        conn.execute(
            text("UPDATE CFG_PIPELINES SET PIPELINE_PARAMETERS=:value WHERE PIPELINE_ID=:id"),
            {"value": json.dumps({**OBSOLETE, **retained}), "id": pipeline},
        )
        conn.execute(
            text(
                "INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS, RUN_KEY, RUN_DATE, "
                "TRIGGER_KIND, STARTED_BY, STARTED_BY_KIND) VALUES (:id, 'SUCCESS', "
                "'orchestrator:original', '2026-01-02', 'ORCHESTRATOR', 'original', "
                "'ORCHESTRATOR')"
            ),
            {"id": pipeline},
        )
        before = (
            conn.execute(
                text(
                    "SELECT PIPELINE_PARAMETERS AS params, CHANGE_ID AS id FROM "
                    "AUD_CFG_PIPELINES_HISTORY WHERE PIPELINE_ID=:id ORDER BY CHANGE_ID"
                ),
                {"id": pipeline},
            )
            .mappings()
            .all()
        )
        audit = (
            conn.execute(
                text(
                    "SELECT PIPELINE_RUN_ID AS id, RUN_KEY AS key, TRIGGER_KIND AS trigger, "
                    "STARTED_BY_KIND AS kind FROM AUD_PIPELINES_RUN_LOG"
                )
            )
            .mappings()
            .all()
        )
    assert "0043_pipeline_parameters.sql" in migrations.apply_pending_migrations(db.engine)
    with db.engine.connect() as conn:
        stored = conn.execute(
            text(
                "SELECT PIPELINE_PARAMETERS AS params, UPDATED_BY AS actor FROM CFG_PIPELINES "
                "WHERE PIPELINE_ID=:id"
            ),
            {"id": pipeline},
        ).one()
        assert (
            json.loads(stored.params) if isinstance(stored.params, str) else stored.params
        ) == retained
        assert stored.actor == "etl-craft"
        after = (
            conn.execute(
                text(
                    "SELECT PIPELINE_PARAMETERS AS params, CHANGE_ID AS id FROM "
                    "AUD_CFG_PIPELINES_HISTORY WHERE PIPELINE_ID=:id ORDER BY CHANGE_ID"
                ),
                {"id": pipeline},
            )
            .mappings()
            .all()
        )
        assert after[: len(before)] == before
        assert len(after) == len(before) + 1
        assert (
            conn.execute(
                text("SELECT PIPELINE_PARAMETERS FROM CFG_PIPELINES WHERE PIPELINE_ID=:id"),
                {"id": untouched},
            ).scalar_one()
            is None
        )
        assert (
            conn.execute(
                text(
                    "SELECT PIPELINE_RUN_ID AS id, RUN_KEY AS key, TRIGGER_KIND AS trigger, "
                    "STARTED_BY_KIND AS kind FROM AUD_PIPELINES_RUN_LOG"
                )
            )
            .mappings()
            .all()
            == audit
        )
    assert migrations.apply_pending_migrations(db.engine) == []
    ctx = OperationContext(db.engine, db.config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
    history = invoke(
        ctx, ActionName.HISTORY, HistoryInput(pipeline_code="P", run_key="orchestrator:original")
    )
    assert history.entries[0].run_key == "orchestrator:original"
    assert history.entries[0].started_by_kind == "ORCHESTRATOR"
    run = invoke(ctx, ActionName.RUN_DETAIL, RunId(run_id=audit[0]["id"]))
    assert run.run_key == "orchestrator:original"
