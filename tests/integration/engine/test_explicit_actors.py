"""Account kinds, durable writers and historical attribution across upgrades."""

import threading
from contextvars import Context
from dataclasses import replace

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from etl_craft.controller.supervisor import serve
from etl_craft.core.actor import SYSTEM_ACTOR, Actor, ActorKind, acting_as, current_actor
from etl_craft.core.enums import ActionName, ApiRole, FailureClass
from etl_craft.core.errors import ConfigurationError
from etl_craft.core.failures import Failure
from etl_craft.engine import migrations, transitions
from etl_craft.execution.context import build_task_context
from etl_craft.services.operations import OperationContext, tokens
from fixtures.actions import act
from fixtures.metadata import add_pipeline, add_task
from fixtures.released_schema import SCHEMAS, install


def test_account_proof_selects_user_or_service_kind(engine_db):
    db = engine_db
    ctx = OperationContext(db.engine, db.config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
    act(
        ctx,
        ActionName.ACCOUNT_ADD,
        name="person",
        role=ApiRole.ADMIN,
        password="correct horse battery",
    )
    with db.engine.begin() as conn:
        migrations.mark_packaged_migrations_applied(conn)
    person = act(ctx, ActionName.LOGIN, name="person", password="correct horse battery")
    service = act(ctx, ActionName.TOKEN_CREATE, name="scheduler", role=ApiRole.ADMIN)
    for credential, kind in (
        (person.access_token, ActorKind.USER),
        (service.token, ActorKind.SERVICE),
    ):
        identity = tokens.authenticate(ctx, credential)
        assert identity is not None
        assert identity.actor.kind == kind
        with acting_as(identity.actor), db.engine.begin() as conn:
            pipeline = add_pipeline(conn, identity.name.upper())
            task = add_task(conn, pipeline, "load", "PYTHON")
            run = transitions.create_active_run(conn, pipeline)
            task_run = transitions.find_or_create_task_run(conn, task, run).task_run_id
        context = build_task_context(db.engine, db.config, task_run)
        assert context.writer == "etl-craft"
        with db.engine.connect() as conn:
            recorded = conn.execute(
                text(
                    "SELECT STARTED_BY AS name, STARTED_BY_KIND AS kind, WRITER AS writer "
                    "FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID=:run"
                ),
                {"run": run},
            ).one()
        assert recorded.name == identity.name
        assert recorded.kind == kind
        assert recorded.writer == "etl-craft"


def test_unscoped_connection_can_read_but_cannot_write(engine_db):
    db = engine_db

    def unattributed():
        with pytest.raises(ConfigurationError, match="actor=None"):
            current_actor()
        with db.engine.connect() as conn:
            assert conn.execute(text("SELECT 1 AS readable")).scalar_one() == 1
        with (
            pytest.raises(DBAPIError, match="written only by etl-craft"),
            db.engine.begin() as conn,
        ):
            add_pipeline(conn, "UNATTRIBUTED")

    Context().run(unattributed)


def test_server_refuses_a_user_account(engine_db):
    db = engine_db
    ctx = OperationContext(
        db.engine, db.config, Actor("person", ActorKind.USER), role=ApiRole.ADMIN
    )
    with pytest.raises(ConfigurationError, match=r"person.*USER.*SERVICE.*ETL_CRAFT_TOKEN"):
        serve(ctx, threading.Event())


@pytest.mark.parametrize("release", sorted(p.name for p in SCHEMAS.iterdir() if p.is_dir()))
def test_upgrade_keeps_legacy_actor_and_adds_run_writer(
    empty_engine_db, release, monkeypatch, tmp_path
):
    db = empty_engine_db
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ETL_CRAFT_MIGRATIONS_DIR", raising=False)
    install(db, release)
    original = migrations.migration_streams
    with monkeypatch.context() as patch:
        patch.setattr(
            migrations,
            "migration_streams",
            lambda *args, **kwargs: [
                replace(stream, files=tuple(f for f in stream.files if f.version < "0035"))
                for stream in original(*args, **kwargs)
            ],
        )
        migrations.apply_pending_migrations(db.engine)
    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "LEGACY")
        run = conn.execute(
            text(
                "INSERT INTO AUD_PIPELINES_RUN_LOG "
                "(PIPELINE_ID, STATUS, STARTED_BY, STARTED_BY_KIND) "
                "VALUES (:p, 'SUCCESS', 'original-person', 'HUMAN') RETURNING PIPELINE_RUN_ID AS id"
            ),
            {"p": pipeline},
        ).scalar_one()
        conn.execute(
            text(
                "INSERT INTO AUD_ACTIONS "
                "(ACTOR, ACTOR_KIND, HOST, COMMAND, ARGUMENTS, OUTCOME) "
                "VALUES ('original-agent', 'WORKER', 'host', 'run', '{}', 'REQUESTED')"
            )
        )
    assert "0035_explicit_actors.sql" in migrations.apply_pending_migrations(db.engine)
    with db.engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT STARTED_BY AS name, STARTED_BY_KIND AS kind, "
                "WRITER AS writer FROM AUD_PIPELINES_RUN_LOG "
                "WHERE PIPELINE_RUN_ID=:run"
            ),
            {"run": run},
        ).one()
        action = conn.execute(
            text(
                "SELECT ACTOR AS name, ACTOR_KIND AS kind FROM AUD_ACTIONS "
                "WHERE ACTOR='original-agent'"
            )
        ).one()
    assert row.name == "original-person"
    assert row.kind == "HUMAN"
    assert row.writer == "etl-craft"
    assert action.name == "original-agent"
    assert action.kind == "WORKER"
    with acting_as(Actor("new-person", ActorKind.USER)), db.engine.begin() as conn:
        new_run = transitions.create_active_run(conn, pipeline)
    assert new_run > run


def test_automatic_retry_uses_system_account_and_restores_requester(engine_db):
    from datetime import UTC, datetime, timedelta

    from etl_craft.execution.retries import refresh_retries

    db = engine_db
    requester = Actor("person", ActorKind.USER)
    with acting_as(requester):
        with db.engine.begin() as conn:
            pipeline = add_pipeline(conn, "RETRY")
            task = add_task(conn, pipeline, "load", "PYTHON", RETRIES="1")
            run = transitions.create_active_run(conn, pipeline)
            summary = transitions.find_or_create_task_run(conn, task, run).task_run_id
            attempt = transitions.queue_attempt(conn, summary, requester)
            transitions.claim_attempt(
                conn,
                attempt,
                requester,
                owner="initial",
                lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
            )
            transitions.record_attempt_failure(
                conn, attempt, Failure(FailureClass.TRANSIENT, "TransientError")
            )
            transitions.finish_attempt(conn, attempt, "FAILED", requester, owner="initial")
        waiting, exhausted = refresh_retries(db.engine, db.config, run, {task})
        assert task in waiting and not exhausted
        assert current_actor() == requester
    with db.engine.connect() as conn:
        retry = conn.execute(
            text(
                "SELECT REQUESTED_BY AS name, REQUESTED_BY_KIND AS kind FROM AUD_TASK_ATTEMPTS "
                "WHERE TASK_RUN_ID=:task AND ATTEMPT_NUMBER=2"
            ),
            {"task": summary},
        ).one()
    assert retry.name == "etl-craft"
    assert retry.kind == "SYSTEM"
