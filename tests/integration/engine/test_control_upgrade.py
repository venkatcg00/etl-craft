"""Upgrade populated scoped controls without losing fences, identities or audit guards."""

from dataclasses import replace
from datetime import UTC, datetime

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from etl_craft.core.actor import Actor, ActorKind, acting_as
from etl_craft.core.enums import ActionName, ApiRole, ControlAction, ControlStatus
from etl_craft.engine import controls, migrations
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.controls import ControlRequest
from fixtures.actions import act
from fixtures.metadata import add_pipeline, add_task, insert
from fixtures.released_schema import install


def test_migrating_populated_controls_preserves_requests_fences_and_immutability(
    empty_engine_db, monkeypatch
):
    db = empty_engine_db
    install(db, "0.3.0")
    streams = migrations.migration_streams

    def before_recovery(*args, **kwargs):
        return [
            replace(s, files=tuple(f for f in s.files if f.version < "0039"))
            for s in streams(*args, **kwargs)
        ]

    with monkeypatch.context() as previous:
        previous.setattr(migrations, "migration_streams", before_recovery)
        migrations.apply_pending_migrations(db.engine)
    ctx = OperationContext(
        db.engine, db.config, Actor("operator", ActorKind.USER), role=ApiRole.OPERATOR
    )
    with acting_as(ctx.actor), db.engine.begin() as conn:
        p = add_pipeline(conn, "P")
        a = add_task(conn, p, "a")
        run = insert(
            conn,
            "INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID,STATUS,RUN_KEY,TRIGGER_KIND,RUN_DATE) "
            "VALUES (:pipeline,'IN-PROGRESS','retained-run','MANUAL','2026-01-01')",
            "PIPELINE_RUN_ID",
            pipeline=p,
        )
        control_id = insert(
            conn,
            "INSERT INTO AUD_CONTROL_REQUESTS "
            "(REQUEST_KEY,PIPELINE_RUN_ID,TASK_ID,ACTION,STATUS,ATTEMPT_IDS,"
            "REQUESTED_BY,REQUESTED_BY_KIND,REASON,REQUESTED_AT,ACKNOWLEDGED_AT,"
            "COMPLETED_AT,MESSAGE) VALUES "
            "(:key,:run,:task,:action,:status,'[]',:actor,:kind,:reason,:now,:now,:now,:message)",
            "CONTROL_ID",
            key="retained-key",
            run=run,
            task=a,
            action=ControlAction.HOLD,
            status=ControlStatus.COMPLETED,
            actor=ctx.actor.name,
            kind=ctx.actor.kind,
            reason="retained reason",
            now=datetime(2026, 1, 1, tzinfo=UTC),
            message="HOLD: task dispatch held",
        )
        held = controls.get(conn, control_id)
    assert migrations.apply_pending_migrations(db.engine)
    assert act(ctx, ActionName.CONTROL_REQUEST, control_id=held.control_id) == held
    with db.engine.connect() as conn:
        assert controls.blocked(conn, run) == {a: ControlAction.HOLD}
    with pytest.raises(DBAPIError, match="immutable"), db.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_CONTROL_REQUESTS SET REASON='replacement' WHERE CONTROL_ID=:id"),
            {"id": held.control_id},
        )
    permission = invoke(
        ctx,
        ActionName.CONTROL,
        ControlRequest(
            pipeline_code="P", action=ControlAction.RUN_PAUSED, reason="one run", run_id=run
        ),
    )
    assert permission.control_id > held.control_id
    assert permission.execution_run_id == run
    assert permission.reason == "one run"
    assert migrations.apply_pending_migrations(db.engine) == []
