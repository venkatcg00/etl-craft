"""The Engine profile's schema: where the Engine DB's tables live."""

from dataclasses import replace

import pytest
from sqlalchemy import text

from etl_craft.core.errors import ConfigurationError
from etl_craft.dialects.engine import build_engine
from etl_craft.engine.connection import check_reachable
from etl_craft.engine.schema import init_db
from fixtures.engine_db import engine_config, sqlite_engine_db


def with_schema(db, schema):
    profile = replace(db.config.engine.active, schema=schema)
    return build_engine(engine_config(profile, db.config.config_path))


@pytest.mark.engine_postgres
def test_postgres_tables_live_in_the_engine_schema(postgres_database):
    with postgres_database.engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA etl_meta"))
    engine = with_schema(postgres_database, "etl_meta")
    try:
        check_reachable(engine, "etl_meta")
        init_db(engine)
        with engine.connect() as conn:
            schemas = set(
                conn.execute(
                    text(
                        "SELECT DISTINCT table_schema FROM information_schema.tables "
                        "WHERE table_name = 'cfg_pipelines'"
                    )
                ).scalars()
            )
            assert schemas == {"etl_meta"}
            # Unqualified names resolve there, so every query works unchanged.
            assert conn.execute(text("SELECT COUNT(*) FROM CFG_PIPELINES")).scalar_one() == 0
    finally:
        engine.dispose()


@pytest.mark.engine_postgres
def test_a_missing_postgres_schema_is_refused_with_the_remedy(postgres_database):
    engine = with_schema(postgres_database, "Missing_Schema")
    try:
        with pytest.raises(
            ConfigurationError,
            match=r"the Engine schema 'Missing_Schema' does not exist in database .* \(CREATE "
            r"SCHEMA missing_schema\) — etl-craft does not create PostgreSQL schemas",
        ):
            check_reachable(engine, "Missing_Schema")
    finally:
        engine.dispose()


@pytest.mark.engine_sqlite
def test_sqlite_needs_no_schema_of_its_own(tmp_path):
    db = sqlite_engine_db(tmp_path)
    try:
        check_reachable(db.engine, "main")
        assert (tmp_path / "engine.db").exists()
    finally:
        db.engine.dispose()
