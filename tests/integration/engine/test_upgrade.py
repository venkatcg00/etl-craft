"""An Engine DB made by an earlier release upgrades to exactly the schema a new one starts with.

``tests/fixtures/schemas/<version>/`` holds each release's packaged schema, as it was tagged.
Each is applied to an empty database, then ``migrate`` runs the packaged migrations; the result
must have every table and column of today's ``schema.sql``, in order.
"""

import re
from pathlib import Path

import pytest
from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError

from etl_craft.dialects.engine import for_engine
from etl_craft.engine.migrations import apply_pending_migrations
from etl_craft.engine.queries import run_script

SCHEMAS = Path(__file__).parents[2] / "fixtures" / "schemas"
CURRENT = Path(__file__).parents[3] / "src" / "etl_craft" / "dialects" / "engine"
RELEASES = sorted(path.name for path in SCHEMAS.iterdir() if path.is_dir())


def declared(schema: str) -> dict[str, list[str]]:
    tables: dict[str, list[str]] = {}
    for match in re.finditer(r"CREATE TABLE (\w+) \((.*?)\n\);", schema, re.DOTALL):
        tables[match[1].lower()] = [
            line.split()[0].lower()
            for line in match[2].splitlines()
            if re.match(r"^\s+[A-Z_]+\s", line)
            and line.split()[0] not in {"CONSTRAINT", "CHECK", "OR", "AND"}
        ]
    return tables


@pytest.mark.parametrize("release", RELEASES)
def test_an_earlier_engine_db_upgrades_to_the_current_schema(
    empty_engine_db, release, monkeypatch, tmp_path
):
    engine = empty_engine_db.engine
    dialect = for_engine(engine)
    folder = "postgres" if engine.dialect.name == "postgresql" else "sqlite"
    old = (SCHEMAS / release / f"{folder}.sql").read_text(encoding="utf-8")
    with engine.begin() as conn:
        run_script(conn, dialect.split_statements(old))
    monkeypatch.delenv("ETL_CRAFT_MIGRATIONS_DIR", raising=False)
    monkeypatch.chdir(tmp_path)
    carried = release == "0.1.0" and _seed_a_tracker_row(engine)

    apply_pending_migrations(engine)

    expected = declared((CURRENT / folder / "schema.sql").read_text(encoding="utf-8"))
    inspector = inspect(engine)
    schema = None if folder == "sqlite" else inspector.default_schema_name
    found = {
        name.lower(): [c["name"].lower() for c in inspector.get_columns(name, schema=schema)]
        for name in inspector.get_table_names(schema=schema)
    }
    assert found == expected
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
    if carried:
        # The tracker's last consumed run became the consumption log's first row.
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT PIPELINE_DEPENDENCY_ID AS dependency, PIPELINE_RUN_ID AS run_id, "
                    "CONSUMED_PIPELINE_RUN_ID AS consumed FROM AUD_DEPENDENCY_CONSUMPTION"
                )
            ).all()
        assert [tuple(row) for row in rows] == [carried]
    # A second migrate has nothing left to do.
    assert apply_pending_migrations(engine) == []


def _seed_a_tracker_row(engine):
    """In a 0.1.0 Engine DB, a pipeline dependency whose tracker consumed an upstream run."""
    with engine.begin() as conn:
        for code in ("UP", "DOWN"):
            conn.execute(
                text(
                    "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
                    "VALUES (:c, :c, 'FULL')"
                ),
                {"c": code},
            )
        ids = dict(conn.execute(text("SELECT PIPELINE_CODE, PIPELINE_ID FROM CFG_PIPELINES")).all())
        dependency = conn.execute(
            text(
                "INSERT INTO CFG_PIPELINE_DEPENDENCY (PIPELINE_ID, DEPENDS_ON_PIPELINE_ID, "
                "DEPENDENCY_TYPE) VALUES (:d, :u, 'SUCCESS') RETURNING PIPELINE_DEPENDENCY_ID"
            ),
            {"d": ids["DOWN"], "u": ids["UP"]},
        ).scalar_one()
        run_id = conn.execute(
            text(
                "INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS) VALUES (:u, 'SUCCESS') "
                "RETURNING PIPELINE_RUN_ID"
            ),
            {"u": ids["UP"]},
        ).scalar_one()
        conn.execute(
            text(
                "INSERT INTO AUD_PIPELINE_DEPENDENCY_TRACKER (PIPELINE_DEPENDENCY_ID, PIPELINE_ID, "
                "DEPENDS_ON_PIPELINE_ID, LAST_CONSUMED_PIPELINE_RUN_ID) VALUES (:d, :down, :up, :r)"
            ),
            {"d": dependency, "down": ids["DOWN"], "up": ids["UP"], "r": run_id},
        )
    return (dependency, None, run_id)
