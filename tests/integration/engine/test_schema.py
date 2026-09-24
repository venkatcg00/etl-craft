"""The packaged Engine DB schema enforces the same rules on SQLite and PostgreSQL.

Every case runs on both dialects (``engine_db`` is parametrized). A statement the schema must
refuse is expected to raise ``IntegrityError``; everything else must succeed.
"""

import time
from datetime import datetime

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError

from fixtures.engine_db import apply_schema

TABLES = (
    "cfg_pipelines",
    "cfg_pipeline_dependency",
    "cfg_tasks",
    "cfg_task_dependency",
    "cfg_task_parameters",
    "cfg_business_rules",
    "aud_pipelines_run_log",
    "aud_task_run_log",
    "aud_business_rules_run_log",
    "aud_business_rules_results",
    "aud_task_offset_tracker",
    "aud_column_lineage",
    "aud_task_documentation",
    "aud_pipeline_dependency_tracker",
    "aud_task_dependency_tracker",
    "schema_migrations",
)


def run(db, sql, **params):
    with db.engine.begin() as conn:
        return conn.execute(text(sql), params)


def scalar(db, sql, **params):
    with db.engine.connect() as conn:
        return conn.execute(text(sql), params).scalar_one()


def refused(db, sql, **params):
    with pytest.raises(IntegrityError), db.engine.begin() as conn:
        conn.execute(text(sql), params)


def insert_id(db, sql, id_column, **params):
    with db.engine.begin() as conn:
        return conn.execute(text(f"{sql} RETURNING {id_column}"), params).scalar_one()


@pytest.fixture
def seeded(engine_db):
    """Two pipelines; pipeline A has tasks `extract` and `load`."""
    db = engine_db
    ids = {
        "a": insert_id(
            db,
            "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
            "VALUES ('PL_A', 'Pipeline A', 'INCREMENTAL')",
            "PIPELINE_ID",
        ),
        "b": insert_id(
            db,
            "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
            "VALUES ('PL_B', 'Pipeline B', 'FULL')",
            "PIPELINE_ID",
        ),
    }
    for code, task_type, handler in (("extract", "INGESTION", "PYTHON"), ("load", "ETL", "SQL")):
        ids[code] = insert_id(
            db,
            "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
            "VALUES (:code, :task_type, :pipeline, :handler)",
            "TASK_ID",
            code=code,
            task_type=task_type,
            pipeline=ids["a"],
            handler=handler,
        )
    return db, ids


def test_the_schema_creates_every_table(engine_db):
    assert engine_db.dialect.existing_tables(engine_db.engine, TABLES) == sorted(TABLES)


def test_existing_tables_is_empty_before_the_schema(empty_engine_db):
    db = empty_engine_db
    assert db.dialect.existing_tables(db.engine, TABLES) == []
    assert apply_schema(db.engine) > len(TABLES)
    assert db.dialect.existing_tables(db.engine, ("CFG_TASKS", "missing")) == ["cfg_tasks"]


def test_the_schema_is_all_or_nothing(empty_engine_db):
    db = empty_engine_db
    apply_schema(db.engine)
    # A second application fails on the first table and leaves nothing half-done behind.
    with pytest.raises(DBAPIError):
        apply_schema(db.engine)
    assert db.dialect.existing_tables(db.engine, TABLES) == sorted(TABLES)


# CFG_ rules


@pytest.mark.parametrize(
    ("column", "value"),
    [("HANDLER", "BOGUS"), ("TASK_TYPE", "LOAD"), ("ACTIVE_FLAG", "X")],
)
def test_task_values_outside_their_lists_are_refused(seeded, column, value):
    db, ids = seeded
    row = {
        "TASK_CODE": "bad",
        "TASK_TYPE": "ETL",
        "HANDLER": "SQL",
        "ACTIVE_FLAG": "Y",
        column: value,
    }
    columns = ", ".join(row)
    placeholders = ", ".join(f":{name.lower()}" for name in row)
    refused(
        db,
        f"INSERT INTO CFG_TASKS (PIPELINE_ID, {columns}) VALUES (:p, {placeholders})",
        p=ids["a"],
        **{name.lower(): v for name, v in row.items()},
    )


def test_every_handler_is_accepted(seeded):
    db, ids = seeded
    for handler in ("PYTHON", "SQL", "BUSINESS_RULES", "EMAIL_ALERT"):
        run(
            db,
            "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
            "VALUES (:code, 'ETL', :p, :handler)",
            code=f"t_{handler.lower()}",
            p=ids["a"],
            handler=handler,
        )


@pytest.mark.parametrize(
    ("condition", "count", "accepted"),
    [
        (None, None, True),
        ("ALL", None, True),
        ("ANY", None, True),
        ("N", 2, True),
        ("MOST", None, False),
        ("N", None, False),
        ("ANY", 2, False),
        ("N", 0, False),
        (None, 2, False),
    ],
)
def test_run_condition_and_count(seeded, condition, count, accepted):
    db, ids = seeded
    statement = (
        "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER, RUN_CONDITION, "
        "RUN_CONDITION_COUNT) VALUES ('rc', 'ETL', :p, 'SQL', :condition, :count)"
    )
    params = {"p": ids["a"], "condition": condition, "count": count}
    if accepted:
        run(db, statement, **params)
    else:
        refused(db, statement, **params)


def test_a_retired_code_can_be_reused_but_not_two_active_ones(seeded):
    db, ids = seeded
    insert = (
        "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
        "VALUES ('reuse_me', 'ETL', :p, 'SQL')"
    )
    run(db, insert, p=ids["a"])
    run(db, "UPDATE CFG_TASKS SET ACTIVE_FLAG = 'N' WHERE TASK_CODE = 'reuse_me'")
    run(db, insert, p=ids["a"])
    refused(db, insert, p=ids["a"])
    # The same code in another pipeline is a different task.
    run(db, insert, p=ids["b"])
    refused(
        db,
        "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
        "VALUES ('PL_A', 'duplicate', 'FULL')",
    )


def test_a_same_pipeline_dependency_fills_in_its_pipeline(seeded):
    db, ids = seeded
    dependency = insert_id(
        db,
        "INSERT INTO CFG_TASK_DEPENDENCY (PIPELINE_ID, TASK_ID, DEPENDS_ON_TASK_ID, "
        "DEPENDENCY_TYPE) VALUES (:p, :load, :extract, 'SUCCESS')",
        "TASK_DEPENDENCY_ID",
        p=ids["a"],
        load=ids["load"],
        extract=ids["extract"],
    )
    assert (
        scalar(
            db,
            "SELECT DEPENDS_ON_PIPELINE_ID AS depends_on FROM CFG_TASK_DEPENDENCY "
            "WHERE TASK_DEPENDENCY_ID = :d",
            d=dependency,
        )
        == ids["a"]
    )


def test_self_dependencies_are_refused(seeded):
    db, ids = seeded
    refused(
        db,
        "INSERT INTO CFG_TASK_DEPENDENCY (PIPELINE_ID, TASK_ID, DEPENDS_ON_TASK_ID, "
        "DEPENDENCY_TYPE) VALUES (:p, :t, :t, 'SUCCESS')",
        p=ids["a"],
        t=ids["extract"],
    )
    refused(
        db,
        "INSERT INTO CFG_PIPELINE_DEPENDENCY (PIPELINE_ID, DEPENDS_ON_PIPELINE_ID, "
        "DEPENDENCY_TYPE) VALUES (:p, :p, 'SUCCESS')",
        p=ids["a"],
    )
    run(
        db,
        "INSERT INTO CFG_PIPELINE_DEPENDENCY (PIPELINE_ID, DEPENDS_ON_PIPELINE_ID, "
        "DEPENDENCY_TYPE) VALUES (:a, :b, 'HAS_DATA')",
        a=ids["a"],
        b=ids["b"],
    )
    refused(
        db,
        "INSERT INTO CFG_PIPELINE_DEPENDENCY (PIPELINE_ID, DEPENDS_ON_PIPELINE_ID, "
        "DEPENDENCY_TYPE) VALUES (:a, :b, 'SOMETIMES')",
        a=ids["b"],
        b=ids["a"],
    )


def test_foreign_keys_are_enforced(seeded):
    db, _ = seeded
    refused(
        db,
        "INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) "
        "VALUES ('orphan', 'ETL', 999999, 'SQL')",
    )


def test_refresh_type_and_business_rule_type_lists(seeded):
    db, ids = seeded
    refused(
        db,
        "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
        "VALUES ('PL_X', 'x', 'WEEKLY')",
    )
    rule = (
        "INSERT INTO CFG_BUSINESS_RULES (BUSINESS_RULE_NAME, PIPELINE_ID, TASK_ID, "
        "BUSINESS_RULE_SQL, BUSINESS_RULE_TYPE, BUSINESS_RULE_KEY_COLUMN, TARGET_TABLE, "
        "SEQUENCE_NUMBER) VALUES (:name, :p, :t, 'SELECT 1', :type, 'id', 'public.t', 1)"
    )
    for rule_type in ("INCOMPLETE", "REJECT", "REPORT"):
        run(db, rule, name=rule_type.lower(), p=ids["a"], t=ids["load"], type=rule_type)
    refused(db, rule, name="bogus", p=ids["a"], t=ids["load"], type="BOGUS")


def test_pipeline_parameters_hold_json(seeded):
    db, _ = seeded
    run(
        db,
        "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE, "
        "PIPELINE_PARAMETERS) VALUES ('PL_J', 'json', 'FULL', :params)",
        params='{"RETRIES": 2, "TAGS": ["daily"]}',
    )
    with pytest.raises(DBAPIError), db.engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE, "
                "PIPELINE_PARAMETERS) VALUES ('PL_K', 'bad json', 'FULL', :params)"
            ),
            {"params": "{not json"},
        )


def test_audit_columns_are_stamped_and_creation_never_changes(seeded):
    db, ids = seeded
    select = (
        "SELECT CREATED_BY AS created_by, CREATE_DATE AS create_date, "
        "UPDATED_DATE AS updated_date FROM CFG_PIPELINES WHERE PIPELINE_ID = :p"
    )
    with db.engine.connect() as conn:
        before = conn.execute(text(select), {"p": ids["a"]}).one()
    assert before.created_by
    assert isinstance(before.create_date, datetime)
    assert before.create_date.tzinfo is not None
    time.sleep(0.02)
    run(
        db,
        "UPDATE CFG_PIPELINES SET CREATED_BY = 'someone', DESCRIPTION = 'x' WHERE PIPELINE_ID = :p",
        p=ids["a"],
    )
    with db.engine.connect() as conn:
        after = conn.execute(text(select), {"p": ids["a"]}).one()
    assert after.created_by == before.created_by
    assert after.create_date == before.create_date
    assert after.updated_date > before.updated_date


# AUD_ rules


def test_one_in_progress_run_per_pipeline(seeded):
    db, ids = seeded
    insert = "INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS) VALUES (:p, :status)"
    run(db, insert, p=ids["a"], status="IN-PROGRESS")
    refused(db, insert, p=ids["a"], status="IN-PROGRESS")
    # Another pipeline, and finished runs of the same one, are unaffected.
    run(db, insert, p=ids["b"], status="IN-PROGRESS")
    run(db, insert, p=ids["a"], status="SUCCESS")
    run(db, insert, p=ids["a"], status="FAILED")
    refused(db, insert, p=ids["a"], status="RUNNING")


def test_one_task_run_row_per_task_per_pipeline_run(seeded):
    db, ids = seeded
    run_id = insert_id(
        db,
        "INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS) VALUES (:p, 'IN-PROGRESS')",
        "PIPELINE_RUN_ID",
        p=ids["a"],
    )
    insert = (
        "INSERT INTO AUD_TASK_RUN_LOG (TASK_ID, PIPELINE_RUN_ID, STATUS) VALUES (:t, :r, :status)"
    )
    run(db, insert, t=ids["extract"], r=run_id, status="FAILED")
    # A retry updates the row in place; a second row is refused.
    refused(db, insert, t=ids["extract"], r=run_id, status="IN-PROGRESS")
    run(
        db,
        "UPDATE AUD_TASK_RUN_LOG SET STATUS = 'SUCCESS', ATTEMPT_COUNT = ATTEMPT_COUNT + 1 "
        "WHERE TASK_ID = :t AND PIPELINE_RUN_ID = :r",
        t=ids["extract"],
        r=run_id,
    )
    assert (
        scalar(
            db,
            "SELECT ATTEMPT_COUNT AS attempts FROM AUD_TASK_RUN_LOG WHERE TASK_ID = :t",
            t=ids["extract"],
        )
        == 2
    )


def test_sla_status_is_met_or_breached(seeded):
    db, ids = seeded
    run(
        db,
        "INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS, SLA_STATUS) "
        "VALUES (:p, 'SUCCESS', 'BREACHED')",
        p=ids["a"],
    )
    refused(db, "UPDATE AUD_PIPELINES_RUN_LOG SET SLA_STATUS = 'LATE'")


def test_trackers_share_their_dependency_row_key(seeded):
    db, ids = seeded
    dependency = insert_id(
        db,
        "INSERT INTO CFG_PIPELINE_DEPENDENCY (PIPELINE_ID, DEPENDS_ON_PIPELINE_ID, "
        "DEPENDENCY_TYPE) VALUES (:a, :b, 'SUCCESS')",
        "PIPELINE_DEPENDENCY_ID",
        a=ids["a"],
        b=ids["b"],
    )
    insert = (
        "INSERT INTO AUD_PIPELINE_DEPENDENCY_TRACKER (PIPELINE_DEPENDENCY_ID, PIPELINE_ID, "
        "DEPENDS_ON_PIPELINE_ID) VALUES (:d, :a, :b)"
    )
    run(db, insert, d=dependency, a=ids["a"], b=ids["b"])
    refused(db, insert, d=dependency, a=ids["a"], b=ids["b"])
    refused(db, insert, d=999999, a=ids["a"], b=ids["b"])


def test_the_offset_type_list(seeded):
    db, ids = seeded
    insert = (
        "INSERT INTO AUD_TASK_OFFSET_TRACKER (TASK_ID, OFFSET_TYPE, OFFSET_VALUE) "
        "VALUES (:t, :type, '42')"
    )
    refused(db, insert, t=ids["extract"], type="DATE")
    run(db, insert, t=ids["extract"], type="NUMBER")


@pytest.mark.parametrize(
    ("source", "checksum", "accepted"),
    [
        ("ENGINE", "a" * 64, True),
        ("PROJECT", "0123456789abcdef" * 4, True),
        ("LEGACY", "a" * 64, False),
        ("ENGINE", "A" * 64, False),
        ("ENGINE", "a" * 63, False),
        ("ENGINE", None, False),
    ],
)
def test_the_migration_ledger(engine_db, source, checksum, accepted):
    statement = (
        "INSERT INTO SCHEMA_MIGRATIONS (SOURCE, VERSION, CHECKSUM) "
        "VALUES (:source, '0001_x.sql', :checksum)"
    )
    params = {"source": source, "checksum": checksum}
    if accepted:
        run(engine_db, statement, **params)
        with engine_db.engine.connect() as conn:
            row = conn.execute(text(engine_db.dialect.query("applied_migrations"))).one()
        assert (row.source, row.version, row.checksum) == (source, "0001_x.sql", checksum)
        assert row.applied_at.tzinfo is not None
    else:
        refused(engine_db, statement, **params)


def test_duration_seconds(seeded):
    db, ids = seeded
    run(
        db,
        "INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS, START_DATE, END_DATE) "
        "VALUES (:p, 'SUCCESS', :start, :end)",
        p=ids["a"],
        start=datetime(2026, 1, 1, 10, 0, 0).astimezone(),
        end=datetime(2026, 1, 1, 10, 1, 30).astimezone(),
    )
    seconds = scalar(
        db,
        f"SELECT {db.dialect.duration_seconds_sql()} AS seconds FROM AUD_PIPELINES_RUN_LOG",
    )
    assert float(seconds) == pytest.approx(90.0)
