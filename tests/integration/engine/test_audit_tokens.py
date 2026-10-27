"""Pool binding, token lifetimes and best-effort usage telemetry on both Engine DBs."""

import threading
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.core.errors import PermissionDeniedError, UsageError
from etl_craft.engine.placement import require_pool
from etl_craft.services.operations import OperationContext, tokens
from etl_craft.services.operations.actions import invoke


def test_agent_token_pool_binding(engine_db):
    ctx = OperationContext(engine_db.engine, engine_db.config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
    with pytest.raises(UsageError, match="--pool P is required"):
        tokens.TokenCreate(name="unbound", role=ApiRole.AGENT)
    with pytest.raises(UsageError, match="omit it for other roles"):
        tokens.TokenCreate(name="service", role=ApiRole.VIEWER, pool="default")
    bound = invoke(
        ctx,
        ActionName.TOKEN_CREATE,
        tokens.TokenCreate(name="worker", role=ApiRole.AGENT, pool="default"),
    )
    with engine_db.engine.begin() as conn:
        conn.execute(text("INSERT INTO CFG_POOLS (POOL_NAME) VALUES ('other')"))
        require_pool(conn, "default", "worker", token_id=bound.metadata.token_id)
        with pytest.raises(
            PermissionDeniedError, match="token pool='default', requested pool='other'"
        ):
            require_pool(conn, "other", "worker", token_id=bound.metadata.token_id)
    assert tokens.authenticate(ctx, bound.token).pool == "default"


def test_service_token_expiry_and_last_used(engine_db, monkeypatch, caplog):
    ctx = OperationContext(engine_db.engine, engine_db.config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
    token = invoke(
        ctx, ActionName.TOKEN_CREATE, tokens.TokenCreate(name="service", role=ApiRole.VIEWER)
    )
    assert (
        datetime.now(UTC) + timedelta(days=364)
        < token.metadata.expires_at
        < datetime.now(UTC) + timedelta(days=366)
    )
    done = threading.Event()
    original = tokens._write_use

    def write(*args):
        try:
            original(*args)
        finally:
            done.set()

    monkeypatch.setattr(tokens, "_write_use", write)
    assert tokens.authenticate(ctx, token.token)
    assert done.wait(5)
    used = tokens.find_token(ctx, token.token).last_used_at
    assert used is not None
    assert tokens.authenticate(ctx, token.token)
    assert tokens.find_token(ctx, token.token).last_used_at == used
    # A blocked writer never holds an authentication request.
    ready, release = threading.Event(), threading.Event()

    def blocked(*args):
        ready.set()
        assert release.wait(5)
        original(*args)

    monkeypatch.setattr(tokens, "_write_use", blocked)
    with engine_db.engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_API_TOKENS SET LAST_USED_AT=NULL WHERE TOKEN_ID=:id"),
            {"id": token.metadata.token_id},
        )
    try:
        assert tokens.authenticate(ctx, token.token)
        assert ready.wait(5)
        assert tokens.authenticate(ctx, token.token)
    finally:
        release.set()


def test_token_usage_failure_never_changes_authorization(engine_db, monkeypatch, caplog):
    ctx = OperationContext(engine_db.engine, engine_db.config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
    token = invoke(
        ctx, ActionName.TOKEN_CREATE, tokens.TokenCreate(name="service", role=ApiRole.VIEWER)
    )

    def unavailable(*args):
        raise OperationalError("unavailable", {}, Exception("offline"))

    monkeypatch.setattr(tokens, "statement", unavailable)
    tokens._USAGE_SLOT.acquire()
    tokens._write_use(ctx, token.metadata, datetime.now(UTC))
    assert "LAST_USED_AT update failed; request continues" in caplog.text


def test_token_telemetry_never_takes_a_request_connection(engine_db, monkeypatch):
    from etl_craft.engine.connection import engine_db as connect

    ctx = OperationContext(engine_db.engine, engine_db.config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
    token = invoke(
        ctx, ActionName.TOKEN_CREATE, tokens.TokenCreate(name="locked", role=ApiRole.VIEWER)
    )
    # Authentication has exactly one connection; the competing writer is a different session.
    request_engine = connect(ctx.config, pool_size=1, max_overflow=0, pool_timeout=1)
    request_ctx = replace(ctx, engine=request_engine)
    reached, done = threading.Event(), threading.Event()
    from sqlalchemy import event

    original_connect, original_write = tokens.engine_db, tokens._write_use

    def connect_writer(*args, **kwargs):
        writer = original_connect(*args, **kwargs)
        event.listen(writer, "checkout", lambda *_: reached.set())
        return writer

    def write(*args):
        try:
            original_write(*args)
        finally:
            done.set()

    monkeypatch.setattr(tokens, "engine_db", connect_writer)
    monkeypatch.setattr(tokens, "_write_use", write)
    try:
        with ctx.engine.begin() as held:
            held.execute(
                text("UPDATE CFG_API_TOKENS SET NAME=NAME WHERE TOKEN_ID=:id"),
                {"id": token.metadata.token_id},
            )
            assert tokens.authenticate(request_ctx, token.token)
            assert reached.wait(5)
            assert tokens.authenticate(request_ctx, token.token)
        assert done.wait(5)
    finally:
        request_engine.dispose()
