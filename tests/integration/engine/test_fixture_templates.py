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
