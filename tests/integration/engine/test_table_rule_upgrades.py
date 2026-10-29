"""Released row rules and their run evidence survive the additive table-result upgrade."""

import pytest
from sqlalchemy import text

from etl_craft.core.time import as_utc
from etl_craft.engine.migrations import apply_pending_migrations
from fixtures.released_schema import SCHEMAS, install
from fixtures.upgrade_rows import seed


@pytest.mark.parametrize("release", sorted(p.name for p in SCHEMAS.iterdir() if p.is_dir()))
def test_table_rule_upgrade_preserves_row_rules_and_run_log(
    empty_engine_db, release, tmp_path, monkeypatch
):
    db = empty_engine_db
    install(db, release)
    seed(db.engine, tmp_path)
    query = text(
        "SELECT BUSINESS_RULE_RUN_ID AS id,BUSINESS_RULE_ID AS rule,TASK_RUN_ID AS task,"
        "STATUS AS status,START_DATE AS started,END_DATE AS ended FROM AUD_BUSINESS_RULES_RUN_LOG"
    )

    def rows():
        with db.engine.connect() as conn:
            return [
                dict(r._mapping)
                | {
                    "started": as_utc(r.started),
                    "ended": None if r.ended is None else as_utc(r.ended),
                }
                for r in conn.execute(query)
            ]

    before = rows()
    assert before
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ETL_CRAFT_MIGRATIONS_DIR", raising=False)
    apply_pending_migrations(
        db.engine, project_default=tmp_path / "migrations", warehouse_dialect="duckdb"
    )
    assert rows() == before
    with db.engine.connect() as conn:
        levels = (
            conn.execute(text("SELECT BUSINESS_RULE_LEVEL AS level FROM CFG_BUSINESS_RULES"))
            .scalars()
            .all()
        )
        assert levels and all(level == "ROW" for level in levels)
        for row in conn.execute(
            text(
                "SELECT PASSED AS passed,OBSERVED AS observed,DETAIL AS detail FROM "
                "AUD_BUSINESS_RULES_RUN_LOG"
            )
        ):
            assert row.passed is None and row.observed is None and row.detail is None
