"""Schema templates save preparation without sharing writable databases between tests."""

import pytest
from sqlalchemy import text

from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
from fixtures.engine_db import postgres_engine_db, prepared_sqlite_engine_db
from fixtures.metadata import add_pipeline


def check_isolation(first, second):
    with acting_as(SYSTEM_ACTOR), first.engine.begin() as conn:
        add_pipeline(conn, "TEMPLATE_ISOLATION")
    with second.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) AS count FROM CFG_PIPELINES")).scalar_one() == 0
    with first.engine.connect() as conn:
        row = conn.execute(text("SELECT CREATED_BY AS created_by FROM CFG_PIPELINES")).one()
        assert row.created_by == SYSTEM_ACTOR.name


@pytest.mark.engine_sqlite
def test_sqlite_template_copies_have_independent_rows_and_working_audit_guards(
    sqlite_schema_template, tmp_path
):
    directories = [tmp_path / "first", tmp_path / "second"]
    for directory in directories:
        directory.mkdir()
    first, second = [
        prepared_sqlite_engine_db(directory, sqlite_schema_template) for directory in directories
    ]
    try:
        check_isolation(first, second)
    finally:
        first.engine.dispose()
        second.engine.dispose()


@pytest.mark.engine_postgres
def test_postgres_template_clones_have_independent_rows_and_working_audit_guards(
    postgres_schema_template, monkeypatch
):
    with (
        postgres_engine_db(monkeypatch, postgres_schema_template) as first,
        postgres_engine_db(monkeypatch, postgres_schema_template) as second,
    ):
        check_isolation(first, second)


@pytest.mark.engine_postgres
def test_database_cleanup_finishes_telemetry_before_dropping_its_database(
    postgres_schema_template, monkeypatch
):
    import threading

    from fixtures.engine_db import postgres_admin
    from fixtures.races import paused_after

    ready, release, joining, finished = (threading.Event() for _ in range(4))
    state = {}
    original_join = threading.Thread.join

    def join(worker, *args, **kwargs):
        if worker.name == "token-usage":
            joining.set()
        return original_join(worker, *args, **kwargs)

    def write():
        ready.set()
        assert release.wait(10)
        with postgres_admin() as conn:
            state["database_alive"] = conn.execute(
                "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=%s)", (state["name"],)
            ).fetchone()[0]
        finished.set()

    def lifecycle():
        with postgres_engine_db(monkeypatch, postgres_schema_template) as db:
            state["name"] = db.engine.url.database
            state["worker"] = threading.Thread(target=write, name="token-usage", daemon=True)
            state["worker"].start()

    monkeypatch.setattr(threading.Thread, "join", join)
    try:
        with paused_after(
            lifecycle,
            owner=threading.Thread,
            name="start",
            when=lambda worker: worker.name == "token-usage",
        ) as continue_cleanup:
            assert ready.wait(5)
            continue_cleanup.set()
            assert joining.wait(5), "database teardown did not wait for its telemetry writer"
            assert not finished.is_set()
            release.set()
        assert finished.is_set() and state["database_alive"]
    finally:
        release.set()
        if "worker" in state:
            original_join(state["worker"], timeout=5)
