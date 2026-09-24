"""Connecting to and locking each Engine DB, against a real database."""

import threading

import pytest
from sqlalchemy import text

from etl_craft.core.errors import ConfigurationError, LockTimeoutError
from etl_craft.dialects.engine import build_engine
from fixtures.engine_db import SECRET_VAR


def test_the_engine_url_names_the_database_but_never_the_secret(engine_db):
    url = engine_db.engine.url
    assert url.password is None
    assert "password" not in url.render_as_string(hide_password=False)
    with engine_db.engine.connect() as conn:
        assert conn.execute(text("SELECT 1 AS one")).scalar_one() == 1


def test_a_lock_excludes_other_holders_and_times_out(engine_db):
    dialect, engine = engine_db.dialect, engine_db.engine
    holding, release = threading.Event(), threading.Event()

    def hold():
        with dialect.lock(engine, 42, "migrate"):
            holding.set()
            release.wait(10)

    holder = threading.Thread(target=hold)
    holder.start()
    assert holding.wait(10)
    try:
        with (
            pytest.raises(LockTimeoutError, match="waiting for migrate"),
            dialect.lock(engine, 42, "migrate", wait_seconds=0.3),
        ):
            pass  # pragma: no cover - the lock is never acquired
        # A different lock is independent.
        with dialect.lock(engine, 43, "other", wait_seconds=1):
            pass
    finally:
        release.set()
        holder.join()
    with dialect.lock(engine, 42, "migrate", wait_seconds=5):
        pass


def test_a_lock_is_released_when_its_holder_raises(engine_db):
    dialect, engine = engine_db.dialect, engine_db.engine
    with pytest.raises(RuntimeError), dialect.lock(engine, 7, "unit"):
        raise RuntimeError
    with dialect.lock(engine, 7, "unit", wait_seconds=1):
        pass


@pytest.mark.engine_postgres
def test_a_wrong_password_fails_when_connecting(postgres_database, monkeypatch):
    monkeypatch.setenv(SECRET_VAR, "not-the-password")
    engine = build_engine(postgres_database.config)
    try:
        with pytest.raises(Exception, match="password authentication failed"):
            engine.connect()
    finally:
        engine.dispose()


@pytest.mark.engine_postgres
def test_a_secret_removed_after_loading_is_a_configuration_error(postgres_database, monkeypatch):
    monkeypatch.delenv(SECRET_VAR)
    with pytest.raises(ConfigurationError, match=f"'{SECRET_VAR}' not found"):
        build_engine(postgres_database.config)
