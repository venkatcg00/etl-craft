"""An Engine DB made by an earlier release upgrades to exactly the schema a new one starts with.

``tests/fixtures/schemas/<version>/`` holds each release's packaged schema, as it was tagged.
Each is applied to an empty database, then ``migrate`` runs the packaged migrations; the result
must match a fresh initialization in columns, constraints, indexes and triggers, and retain
representative rows by named column.
"""

from dataclasses import replace
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from etl_craft.dialects.engine import build_engine
from etl_craft.engine.migrations import apply_pending_migrations
from etl_craft.engine.schema import init_db
from fixtures.catalog import snapshot
from fixtures.engine_db import engine_config, sqlite_engine_db
from fixtures.released_schema import install
from fixtures.upgrade_rows import assert_retained, seed

SCHEMAS = Path(__file__).parents[2] / "fixtures" / "schemas"
RELEASES = sorted(path.name for path in SCHEMAS.iterdir() if path.is_dir())


@pytest.mark.parametrize("release", RELEASES)
def test_an_earlier_engine_db_upgrades_to_the_current_schema(
    empty_engine_db, release, monkeypatch, tmp_path
):
    engine = empty_engine_db.engine
    folder = "postgres" if engine.dialect.name == "postgresql" else "sqlite"
    install(empty_engine_db, release)
    monkeypatch.delenv("ETL_CRAFT_MIGRATIONS_DIR", raising=False)
    monkeypatch.chdir(tmp_path)
    retained = seed(engine, tmp_path)

    apply_pending_migrations(engine, project_default=tmp_path / "migrations")

    if folder == "postgres":
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA fresh"))
        fresh = build_engine(engine_config(replace(empty_engine_db.config.engine, schema="fresh")))
    else:
        directory = tmp_path / "fresh"
        directory.mkdir()
        fresh = sqlite_engine_db(directory).engine
    try:
        init_db(fresh)
        upgraded = snapshot(engine)
        for section, expected in snapshot(fresh).items():
            assert upgraded[section] == expected, section
    finally:
        fresh.dispose()
    assert_retained(engine, retained)
    # The partial unique indexes came along too: one open pause per pipeline.
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
                "VALUES ('P', 'P', 'FULL')"
            )
        )
    insert = text(
        "INSERT INTO AUD_PIPELINE_PAUSES (PIPELINE_ID, PAUSED_BY, REASON) "
        "SELECT PIPELINE_ID, 'op', 'why' FROM CFG_PIPELINES WHERE PIPELINE_CODE = 'P'"
    )
    with engine.begin() as conn:
        conn.execute(insert)
    with pytest.raises(IntegrityError), engine.begin() as conn:
        conn.execute(insert)
    # A second migrate has nothing left to do.
    assert apply_pending_migrations(engine, project_default=tmp_path / "migrations") == []
