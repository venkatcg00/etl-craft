"""Rule target references preserve identities and evidence through conversion and edits."""

import time
from datetime import UTC, datetime

import pytest
from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError

from etl_craft.core.errors import MigrationError
from etl_craft.engine import migrations
from etl_craft.engine.repository.business_rules import fetch_business_rules_for_task
from etl_craft.services.config_files import export_config, read_config_files
from fixtures.catalog import snapshot
from fixtures.metadata import add_pipeline, add_table, add_task
from fixtures.released_schema import install


@pytest.fixture
def legacy_rules_db(empty_engine_db, monkeypatch):
    db = empty_engine_db
    install(db, "0.2.0")
    monkeypatch.delenv("ETL_CRAFT_MIGRATIONS_DIR", raising=False)
    for file in migrations.migration_streams(db.engine)[0].files:
        if "0007" <= file.version < "0029":
            migrations._apply(db.engine, file)
    return db


def legacy_rule(conn, target="raw.orders", name="check"):
    pipeline = add_pipeline(conn, name)
    task = add_task(conn, pipeline, "rules", "BUSINESS_RULES")
    rule = conn.execute(
        text(
            "INSERT INTO CFG_BUSINESS_RULES (BUSINESS_RULE_NAME,PIPELINE_ID,TASK_ID,"
            "BUSINESS_RULE_SQL,BUSINESS_RULE_TYPE,BUSINESS_RULE_KEY_COLUMN,TARGET_TABLE,"
            "SEQUENCE_NUMBER) VALUES (:name,:pipeline,:task,'SELECT 1','REPORT','id',:target,1) "
            "RETURNING BUSINESS_RULE_ID AS business_rule_id"
        ),
        {"name": name, "pipeline": pipeline, "task": task, "target": target},
    ).scalar_one()
    return rule, task


def test_conversion_reuses_contract_preserves_stamps_history_and_old_names(
    legacy_rules_db, tmp_path
):
    db = legacy_rules_db
    with db.engine.begin() as conn:
        parent = add_table(conn, "analytics.raw.orders")
        first, task = legacy_rule(conn)
        second, _ = legacy_rule(conn, "analytics.raw.orders", "other")
        before = conn.execute(
            text("SELECT * FROM CFG_BUSINESS_RULES ORDER BY BUSINESS_RULE_ID")
        ).all()
        history = conn.execute(
            text(
                "SELECT CHANGE_ID,TARGET_TABLE,CREATED_BY,CREATE_DATE,UPDATED_BY,UPDATED_DATE "
                "FROM AUD_CFG_BUSINESS_RULES_HISTORY ORDER BY CHANGE_ID"
            )
        ).all()
    moment = datetime.now(UTC)
    # SQLite history uses millisecond boundaries.
    time.sleep(0.01)
    applied = migrations.apply_pending_migrations(
        db.engine, catalog="analytics", warehouse_dialect="duckdb"
    )
    assert {"0029_rule_target_contracts.sql", "0030_task_target_contracts.sql"} <= set(applied)
    assert applied == sorted(applied)
    with db.engine.connect() as conn:
        columns = [
            column["name"].upper()
            for column in inspect(conn).get_columns(
                "cfg_business_rules" if conn.dialect.name == "postgresql" else "CFG_BUSINESS_RULES"
            )
        ]
        assert "TARGET_TABLE" not in columns
        assert "TARGET_TABLE_ID" in columns
        after = conn.execute(
            text("SELECT * FROM CFG_BUSINESS_RULES ORDER BY BUSINESS_RULE_ID")
        ).all()
        assert [tuple(row) for row in after] == [(*row[:7], *row[8:], parent) for row in before]
        assert [
            row[:6]
            for row in conn.execute(
                text(
                    "SELECT CHANGE_ID,TARGET_TABLE,CREATED_BY,CREATE_DATE,UPDATED_BY,UPDATED_DATE "
                    "FROM AUD_CFG_BUSINESS_RULES_HISTORY ORDER BY CHANGE_ID"
                )
            )
        ][: len(history)] == history
        assert fetch_business_rules_for_task(conn, task)[0].target_table == "analytics.raw.orders"
        assert conn.execute(text("SELECT COUNT(*) FROM CFG_TABLES")).scalar_one() == 1
    export_config(db.engine, tmp_path / "past", as_of=moment)
    old = read_config_files(tmp_path / "past")
    assert {row.values["CATALOG_NAME"] for row in old.rows["business_rules.csv"].values()} == {
        None,
        "analytics",
    }
    with db.engine.begin() as conn:
        target = add_table(conn, "elsewhere.raw.orders")
        conn.execute(
            text(
                "UPDATE CFG_BUSINESS_RULES SET TARGET_TABLE_ID=:target WHERE BUSINESS_RULE_ID=:id"
            ),
            {"target": target, "id": first},
        )
    export_config(db.engine, tmp_path / "past_again", as_of=moment)
    assert (tmp_path / "past/business_rules.csv").read_bytes() == (
        tmp_path / "past_again/business_rules.csv"
    ).read_bytes()
    with pytest.raises(IntegrityError), db.engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE CFG_BUSINESS_RULES SET TARGET_TABLE_ID=987654321 WHERE BUSINESS_RULE_ID=:id"
            ),
            {"id": second},
        )


@pytest.mark.parametrize("target", ["orders", "four.part.table.name", 'raw."order.with.dot"'])
def test_invalid_target_refuses_before_any_pending_change(legacy_rules_db, target):
    db = legacy_rules_db
    with db.engine.begin() as conn:
        legacy_rule(conn, target)
    before = snapshot(db.engine)
    with pytest.raises(MigrationError, match=r"BUSINESS_RULE_ID=.*previous package"):
        migrations.apply_pending_migrations(
            db.engine, catalog="analytics", warehouse_dialect="duckdb"
        )
    assert snapshot(db.engine) == before
    with db.engine.connect() as conn:
        assert (
            conn.execute(text("SELECT TARGET_TABLE FROM CFG_BUSINESS_RULES")).scalar_one() == target
        )


def test_custom_data_refused_without_discarding_it(legacy_rules_db):
    db = legacy_rules_db
    with db.engine.begin() as conn:
        legacy_rule(conn)
        conn.exec_driver_sql("ALTER TABLE CFG_BUSINESS_RULES ADD COLUMN CUSTOM_NOTE VARCHAR")
        conn.exec_driver_sql("UPDATE CFG_BUSINESS_RULES SET CUSTOM_NOTE='keep this'")
    before = snapshot(db.engine)
    with pytest.raises(MigrationError, match=r"custom columns.*CUSTOM_NOTE"):
        migrations.apply_pending_migrations(db.engine)
    assert snapshot(db.engine) == before


def test_deleted_rule_reconstructs_original_target(legacy_rules_db, tmp_path):
    db = legacy_rules_db
    with db.engine.begin() as conn:
        rule, _ = legacy_rule(conn)
    moment = datetime.now(UTC)
    time.sleep(0.01)
    with db.engine.begin() as conn:
        conn.execute(
            text("DELETE FROM CFG_BUSINESS_RULES WHERE BUSINESS_RULE_ID=:id"), {"id": rule}
        )
    migrations.apply_pending_migrations(db.engine, catalog="analytics", warehouse_dialect="duckdb")
    export_config(db.engine, tmp_path / "past", as_of=moment)
    old = read_config_files(tmp_path / "past")
    assert next(iter(old.rows["business_rules.csv"].values())).values["CATALOG_NAME"] is None
    assert (None, "raw", "orders") in old.rows["tables.csv"]


def test_late_conversion_failure_rolls_back_schema_rows_history_and_ledger(
    legacy_rules_db, monkeypatch
):
    from etl_craft.engine.repository import target_contracts

    db = legacy_rules_db
    with db.engine.begin() as conn:
        legacy_rule(conn)
    before = snapshot(db.engine)
    with db.engine.connect() as conn:
        rows = conn.execute(text("SELECT * FROM CFG_BUSINESS_RULES")).all()
        history = conn.execute(text("SELECT * FROM AUD_CFG_BUSINESS_RULES_HISTORY")).all()
        ledger = conn.execute(text("SELECT * FROM SCHEMA_MIGRATIONS")).all()
    original = target_contracts.convert_targets

    def fail_after_copy(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("conversion fault")

    monkeypatch.setattr(target_contracts, "convert_targets", fail_after_copy)
    with pytest.raises(MigrationError, match="conversion fault"):
        migrations.apply_pending_migrations(
            db.engine, catalog="analytics", warehouse_dialect="duckdb"
        )
    assert snapshot(db.engine) == before
    with db.engine.connect() as conn:
        assert conn.execute(text("SELECT * FROM CFG_BUSINESS_RULES")).all() == rows
        assert conn.execute(text("SELECT * FROM AUD_CFG_BUSINESS_RULES_HISTORY")).all() == history
        assert conn.execute(text("SELECT * FROM SCHEMA_MIGRATIONS")).all() == ledger


def test_reserved_rule_counter_survives_conversion(legacy_rules_db):
    db = legacy_rules_db
    with db.engine.begin() as conn:
        rule, task = legacy_rule(conn)
        if conn.dialect.name == "sqlite":
            conn.exec_driver_sql(
                "UPDATE sqlite_sequence SET seq=100 WHERE name='CFG_BUSINESS_RULES'"
            )
        else:
            conn.exec_driver_sql(
                "SELECT setval(pg_get_serial_sequence("
                "'cfg_business_rules','business_rule_id'),100,true)"
            )
    migrations.apply_pending_migrations(db.engine)
    with db.engine.begin() as conn:
        created = conn.execute(
            text(
                "INSERT INTO CFG_BUSINESS_RULES (BUSINESS_RULE_NAME,PIPELINE_ID,TASK_ID,"
                "BUSINESS_RULE_SQL,BUSINESS_RULE_TYPE,BUSINESS_RULE_KEY_COLUMN,TARGET_TABLE_ID,"
                "SEQUENCE_NUMBER) SELECT 'another',PIPELINE_ID,TASK_ID,BUSINESS_RULE_SQL,"
                "BUSINESS_RULE_TYPE,BUSINESS_RULE_KEY_COLUMN,TARGET_TABLE_ID,2 "
                "FROM CFG_BUSINESS_RULES "
                "WHERE BUSINESS_RULE_ID=:id RETURNING BUSINESS_RULE_ID AS business_rule_id"
            ),
            {"id": rule},
        ).scalar_one()
        assert created == 101
        conn.execute(text("UPDATE CFG_TABLES SET ACTIVE_FLAG='N'"))
        from etl_craft.core.errors import SqlGuardError
        from etl_craft.handlers.business_rules import check_rule

        current = fetch_business_rules_for_task(conn, task)[0]
        with pytest.raises(SqlGuardError, match=r"target contract.*ACTIVE_FLAG=N"):
            check_rule(current, "analytics")


@pytest.mark.parametrize(
    "ddl",
    [
        "CREATE INDEX custom_target ON CFG_BUSINESS_RULES (TARGET_TABLE)",
        "CREATE VIEW custom_target AS SELECT TARGET_TABLE FROM CFG_BUSINESS_RULES",
    ],
)
def test_target_dependent_schema_is_refused_before_conversion(legacy_rules_db, ddl):
    db = legacy_rules_db
    with db.engine.begin() as conn:
        legacy_rule(conn)
        conn.exec_driver_sql(ddl)
    before = snapshot(db.engine)
    with pytest.raises(MigrationError, match="target-dependent schema objects"):
        migrations.apply_pending_migrations(db.engine)
    assert snapshot(db.engine) == before


def test_unversioned_rule_conversion_snapshot_keeps_earlier_export(legacy_rules_db, tmp_path):
    from etl_craft.core.actor import purpose

    db = legacy_rules_db
    with db.engine.begin() as conn:
        legacy_rule(conn)
    token = purpose.set("retention")
    try:
        with db.engine.begin() as conn:
            conn.execute(text("DELETE FROM AUD_CFG_BUSINESS_RULES_HISTORY"))
    finally:
        purpose.reset(token)
    moment = datetime.now(UTC)
    time.sleep(0.01)
    migrations.apply_pending_migrations(db.engine, catalog="analytics", warehouse_dialect="duckdb")
    export_config(db.engine, tmp_path / "past", as_of=moment)
    old = read_config_files(tmp_path / "past")
    assert next(iter(old.rows["business_rules.csv"].values())).values["CATALOG_NAME"] is None
