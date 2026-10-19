"""Upgrade populated scoped controls without losing fences, identities or audit guards."""

from dataclasses import replace

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from etl_craft.core.actor import Actor, ActorKind
from etl_craft.core.enums import ActionName, ApiRole, ControlAction
from etl_craft.engine import controls, migrations
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.controls import ControlRequest
from fixtures.actions import act
from fixtures.metadata import add_pipeline, add_task, start_run
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
    with db.engine.begin() as conn:
        p = add_pipeline(conn, "P")
        a = add_task(conn, p, "a")
        run = start_run(conn, p)
    held = invoke(
        ctx,
        ActionName.CONTROL,
        ControlRequest(
            pipeline_code="P",
            action=ControlAction.HOLD,
            reason="retained reason",
            run_id=run,
            task_code="a",
            request_key="retained-key",
        ),
    )
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
