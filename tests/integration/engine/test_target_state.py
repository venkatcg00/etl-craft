"""Target state and rule results keep their rows and times as separate name components."""

from datetime import UTC, datetime

import pytest
from sqlalchemy import inspect, text

from etl_craft.core.errors import MigrationError
from etl_craft.engine import locks, migrations
from fixtures.metadata import add_pipeline, add_table, add_task, start_run, task_run
from fixtures.released_schema import install

RECORDED = datetime(2026, 10, 1, 12, 30, tzinfo=UTC)
COMBINED = {"SOURCE_OBJECT", "TARGET_OBJECT", "TARGET_TABLE"}


def pending_target_state(db):
    """The Engine DB just before migration 0033, from the 0.2.0 release."""
    install(db, "0.2.0")
    files = migrations.migration_streams(db.engine)[0].files
    for file in files:
        if "0007" <= file.version < "0033":
            migrations._apply(db.engine, file)
    return next(file for file in files if file.version.startswith("0033"))


def record_legacy_state(conn, *, hash_name, view_name, rule_names):
    conn.execute(
        text(
            "INSERT INTO AUD_TARGET_HASH_VERSION (TARGET_OBJECT, HASH_VERSION, RECOMPUTED_AT) "
            "VALUES (:name, 2, :at)"
        ),
        {"name": hash_name, "at": RECORDED},
    )
    conn.execute(
        text(
            "INSERT INTO AUD_TARGET_VIEW_STATEMENT (TARGET_OBJECT, STATEMENT_SHA256, RECORDED_AT) "
            "VALUES (:name, :sha, :at)"
        ),
        {"name": view_name, "sha": "a" * 64, "at": RECORDED},
    )
    pipeline = add_pipeline(conn, "P")
    task = add_task(conn, pipeline, "rules", "BUSINESS_RULES")
    rule = conn.execute(
        text(
            "INSERT INTO CFG_BUSINESS_RULES (BUSINESS_RULE_NAME, PIPELINE_ID, TASK_ID, "
            "BUSINESS_RULE_SQL, BUSINESS_RULE_TYPE, BUSINESS_RULE_KEY_COLUMN, SEQUENCE_NUMBER, "
            "TARGET_TABLE_ID) VALUES ('check', :pipeline, :task, 'SELECT 1', 'REJECT', 'ROW_ID', "
            "1, :table) RETURNING BUSINESS_RULE_ID AS id"
        ),
        {"pipeline": pipeline, "task": task, "table": add_table(conn, "raw.orders")},
    ).scalar_one()
    rule_run = conn.execute(
        text(
            "INSERT INTO AUD_BUSINESS_RULES_RUN_LOG (BUSINESS_RULE_ID, TASK_RUN_ID, STATUS) "
            "VALUES (:rule, :task_run, 'SUCCESS') RETURNING BUSINESS_RULE_RUN_ID AS id"
        ),
        {"rule": rule, "task_run": task_run(conn, task, start_run(conn, pipeline))},
    ).scalar_one()
    for key, name in enumerate(rule_names):
        conn.execute(
            text(
                "INSERT INTO AUD_BUSINESS_RULES_RESULTS (BUSINESS_RULE_RUN_ID, BUSINESS_RULE_ID, "
                "BUSINESS_RULE_KEY, TARGET_TABLE, STATUS, ACTIVE_FLAG, START_DATE, END_DATE) "
                "VALUES (:run, :rule, :key, :name, 'REJECT', 'N', :at, :at)"
            ),
            {"run": rule_run, "rule": rule, "key": str(key), "name": name, "at": RECORDED},
        )


def rows(engine, sql):
    with engine.connect() as conn:
        return [dict(row._mapping) for row in conn.execute(text(sql))]


def test_upgrade_splits_recorded_names_and_keeps_values_and_times(empty_engine_db):
    db = empty_engine_db
    target_state = pending_target_state(db)
    with db.engine.begin() as conn:
        record_legacy_state(
            conn,
            hash_name="analytics.raw.orders",
            view_name="analytics.mart.daily",
            rule_names=["raw.orders", "lake.raw.orders"],
        )
    migrations._apply(db.engine, target_state)
    (hashed,) = rows(
        db.engine,
        "SELECT TARGET_CATALOG_NAME AS catalog, TARGET_SCHEMA_NAME AS schema_name, "
        "TARGET_TABLE_NAME AS table_name, HASH_VERSION AS version, RECOMPUTED_AT AS at "
        "FROM AUD_TARGET_HASH_VERSION",
    )
    assert (hashed["catalog"], hashed["schema_name"], hashed["table_name"]) == (
        "analytics",
        "raw",
        "orders",
    )
    assert hashed["version"] == 2
    (view,) = rows(
        db.engine,
        "SELECT TARGET_CATALOG_NAME AS catalog, TARGET_SCHEMA_NAME AS schema_name, "
        "TARGET_TABLE_NAME AS table_name, STATEMENT_SHA256 AS sha FROM AUD_TARGET_VIEW_STATEMENT",
    )
    assert (view["catalog"], view["schema_name"], view["table_name"], view["sha"]) == (
        "analytics",
        "mart",
        "daily",
        "a" * 64,
    )
    flags = rows(
        db.engine,
        "SELECT BUSINESS_RULE_KEY AS key, TARGET_CATALOG_NAME AS catalog, "
        "TARGET_SCHEMA_NAME AS schema_name, TARGET_TABLE_NAME AS table_name, "
        "ACTIVE_FLAG AS active, START_DATE AS started FROM AUD_BUSINESS_RULES_RESULTS "
        "ORDER BY BUSINESS_RULE_KEY",
    )
    assert [(f["catalog"], f["schema_name"], f["table_name"], f["active"]) for f in flags] == [
        (None, "raw", "orders", "N"),
        ("lake", "raw", "orders", "N"),
    ]
    assert str(hashed["at"])[:16] == str(flags[0]["started"])[:16] == "2026-10-01 12:30"


def test_an_unsplittable_recorded_name_refuses_the_upgrade_with_nothing_changed(empty_engine_db):
    db = empty_engine_db
    target_state = pending_target_state(db)
    with db.engine.begin() as conn:
        record_legacy_state(
            conn, hash_name="orders", view_name="analytics.mart.daily", rule_names=["raw.orders"]
        )
    with pytest.raises(MigrationError, match=r"AUD_TARGET_HASH_VERSION 'orders' cannot be split"):
        migrations.apply_pending_migrations(db.engine)
    assert rows(db.engine, "SELECT TARGET_OBJECT AS name FROM AUD_TARGET_HASH_VERSION") == [
        {"name": "orders"}
    ]
    assert target_state.version not in {
        row["version"]
        for row in rows(db.engine, "SELECT VERSION AS version FROM SCHEMA_MIGRATIONS")
    }


@pytest.mark.parametrize(
    "spelling",
    [("raw.orders", "analytics"), ("analytics.raw.orders", "other"), ("ANALYTICS.Raw.ORDERS", "")],
)
def test_one_target_takes_one_lock_however_its_name_is_written(engine_db, spelling):
    assert locks.target(*spelling).key == locks.target("raw.orders", "analytics").key


def combined_name_columns(engine):
    tables = inspect(engine).get_table_names()
    return sorted(
        f"{table.upper()}.{column['name'].upper()}"
        for table in tables
        if not (table.upper().startswith("AUD_CFG_") and table.upper().endswith("_HISTORY"))
        for column in inspect(engine).get_columns(table)
        if column["name"].upper() in COMBINED
    )


def test_no_table_outside_the_config_histories_stores_a_combined_object_name(engine_db):
    assert combined_name_columns(engine_db.engine) == []


def test_an_upgraded_engine_db_stores_no_combined_object_name(empty_engine_db):
    db = empty_engine_db
    install(db, "0.2.0")
    migrations.apply_pending_migrations(db.engine)
    assert combined_name_columns(db.engine) == []
