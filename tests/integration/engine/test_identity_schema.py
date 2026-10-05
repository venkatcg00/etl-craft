"""Identity constraints and historical attempts on both Engine DB dialects."""

from dataclasses import replace

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from test_catalog_upgrade import assert_history, seed_run_history

from etl_craft.core.errors import MigrationError, RunStateError
from etl_craft.engine import migrations, transitions
from fixtures.catalog import snapshot
from fixtures.metadata import add_pipeline, add_pipeline_dependency, add_task
from fixtures.released_schema import install


@pytest.fixture
def released(empty_engine_db, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ETL_CRAFT_MIGRATIONS_DIR", raising=False)
    install(empty_engine_db, "0.2.0")
    original = migrations.migration_streams
    monkeypatch.setattr(
        migrations,
        "migration_streams",
        lambda *args, **kwargs: [
            replace(stream, files=tuple(f for f in stream.files if f.version < "0008"))
            if stream.source == migrations.ENGINE
            else stream
            for stream in original(*args, **kwargs)
        ],
    )
    return empty_engine_db


def test_migration_preserves_attempt_values_and_skipped_summaries(released):
    db = released
    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        run = conn.execute(
            text(
                "INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS, BACKFILL) "
                "VALUES (:p, 'SUCCESS', 'Y') RETURNING PIPELINE_RUN_ID"
            ),
            {"p": pipeline},
        ).scalar_one()
        for status in ("IN-PROGRESS", "SUCCESS", "FAILED", "CANCELLED", "SKIPPED"):
            task = add_task(conn, pipeline, status.replace("-", "_"))
            conn.execute(
                text(
                    "INSERT INTO AUD_TASK_RUN_LOG (TASK_ID, PIPELINE_RUN_ID, STATUS, "
                    "ATTEMPT_COUNT, SOURCE_COUNT, TARGET_COUNT, INSERT_COUNT, UPDATE_COUNT, "
                    "DELETE_COUNT, ROWS_WRITTEN, ERROR_MESSAGE, TASK_LOG) "
                    "VALUES (:t, :r, :s, 3, 10, 9, 8, 7, 6, 5, 'why', 'log')"
                ),
                {"t": task, "r": run, "s": status},
            )
    assert migrations.apply_pending_migrations(db.engine) == ["0007_identity.sql"]
    with db.engine.connect() as conn:
        assert tuple(
            conn.execute(
                text(
                    "SELECT RUN_KEY, TRIGGER_KIND, OUTPUT_REVISION, OWNER_ID, "
                    "LEASE_EXPIRES_AT, CONFIG_SHA256 FROM AUD_PIPELINES_RUN_LOG"
                )
            ).one()
        ) == (f"legacy:{run}", "BACKFILL", 1, None, None, None)
        rows = conn.execute(
            text(
                "SELECT t.STATUS AS summary, a.STATUS AS attempt, a.ATTEMPT_NUMBER, "
                "a.SOURCE_COUNT, a.TARGET_COUNT, a.INSERT_COUNT, a.UPDATE_COUNT, "
                "a.DELETE_COUNT, a.ROWS_WRITTEN, a.ERROR_MESSAGE, a.TASK_LOG, "
                "a.STARTED_AT = t.START_DATE AS same_start, a.ENDED_AT "
                "FROM AUD_TASK_RUN_LOG t LEFT JOIN AUD_TASK_ATTEMPTS a "
                "ON a.TASK_RUN_ID=t.TASK_RUN_ID ORDER BY t.TASK_RUN_ID"
            )
        ).all()
        for row in rows:
            if row.summary == "SKIPPED":
                assert row.attempt is None
            else:
                assert tuple(row)[1:] == (
                    "RUNNING" if row.summary == "IN-PROGRESS" else row.summary,
                    3,
                    10,
                    9,
                    8,
                    7,
                    6,
                    5,
                    "why",
                    "log",
                    True,
                    None,
                )
    assert migrations.apply_pending_migrations(db.engine) == []


def test_identity_migration_preserves_references_objects_and_counter(released):
    history = seed_run_history(released)
    migrations.apply_pending_migrations(released.engine)
    assert_history(released, *history)


def test_identity_migration_failure_rolls_back_schema_and_history(released, monkeypatch):
    seed_run_history(released)
    before = snapshot(released.engine)
    original = migrations._record

    def refuse(conn, migration):
        original(conn, migration)
        raise MigrationError("ledger refused")

    monkeypatch.setattr(migrations, "_record", refuse)
    with pytest.raises(MigrationError, match="ledger refused"):
        migrations.apply_pending_migrations(released.engine)
    assert snapshot(released.engine) == before
    with released.engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT COUNT(*) FROM SCHEMA_MIGRATIONS WHERE VERSION='0007_identity.sql'")
            ).scalar_one()
            == 0
        )
    monkeypatch.setattr(migrations, "_record", original)
    assert migrations.apply_pending_migrations(released.engine) == ["0007_identity.sql"]


def test_attempt_identity_and_active_uniqueness(engine_db):
    with engine_db.engine.begin() as conn:
        p = add_pipeline(conn, "P")
        t = add_task(conn, p, "load")
        r = transitions.find_or_create_active_run(conn, p)
        task_run = transitions.find_or_create_task_run(conn, t, r).task_run_id
    insert = text(
        "INSERT INTO AUD_TASK_ATTEMPTS (TASK_RUN_ID, ATTEMPT_NUMBER, STATUS) VALUES (:t, :n, :s)"
    )
    with engine_db.engine.begin() as conn:
        conn.execute(insert, {"t": task_run, "n": 1, "s": "SUCCESS"})
        conn.execute(insert, {"t": task_run, "n": 2, "s": "QUEUED"})
    for number, status in ((1, "FAILED"), (3, "CLAIMED"), (3, "RUNNING"), (3, "SKIPPED")):
        with pytest.raises(IntegrityError), engine_db.engine.begin() as conn:
            conn.execute(insert, {"t": task_run, "n": number, "s": status})
    with pytest.raises(IntegrityError), engine_db.engine.begin() as conn:
        conn.execute(insert, {"t": task_run + 100, "n": 1, "s": "SUCCESS"})


def test_run_keys_and_trigger_kinds(engine_db):
    with engine_db.engine.begin() as conn:
        p = add_pipeline(conn, "P")
        r = transitions.create_active_run(conn, p, backfill=True)
        key, kind = conn.execute(
            text(
                "SELECT RUN_KEY, TRIGGER_KIND FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID=:r"
            ),
            {"r": r},
        ).one()
        assert key.startswith("backfill:") and kind == "BACKFILL"
        transitions.end_run_if(conn, r, "IN-PROGRESS", "SUCCESS")
    for value, trigger in ((key, "MANUAL"), ("other", "INVALID")):
        with pytest.raises(IntegrityError), engine_db.engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO AUD_PIPELINES_RUN_LOG "
                    "(PIPELINE_ID, STATUS, RUN_KEY, TRIGGER_KIND) "
                    "VALUES (:p, 'SUCCESS', :k, :t)"
                ),
                {"p": p, "k": value, "t": trigger},
            )
    with engine_db.engine.begin() as conn:
        p2 = add_pipeline(conn, "OTHER")
        conn.execute(
            text(
                "INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS, RUN_KEY) "
                "VALUES (:p, 'SUCCESS', :k)"
            ),
            {"p": p2, "k": key},
        )


def test_gate_decision_dependency_and_result_constraints(engine_db):
    with engine_db.engine.begin() as conn:
        p = add_pipeline(conn, "DOWN")
        up = add_pipeline(conn, "UP")
        dep = add_pipeline_dependency(conn, p, up)
        r = transitions.find_or_create_active_run(conn, p)
        assert (
            conn.execute(text("SELECT CONSUME_REPAIRS FROM CFG_PIPELINE_DEPENDENCY")).scalar_one()
            == "Y"
        )
    insert = text(
        "INSERT INTO AUD_GATE_DECISIONS (PIPELINE_RUN_ID, PIPELINE_DEPENDENCY_ID, "
        "RESULT, REASON) VALUES (:r, :d, :s, 'gate reason')"
    )
    for dependency, result in ((None, "UNSATISFIED"), (dep, "INVALID"), (dep + 100, "SATISFIED")):
        with pytest.raises(IntegrityError), engine_db.engine.begin() as conn:
            conn.execute(insert, {"r": r, "d": dependency, "s": result})
    with engine_db.engine.begin() as conn:
        for result in ("SATISFIED", "UNSATISFIED", "BYPASSED"):
            conn.execute(insert, {"r": r, "d": dep, "s": result})
    with pytest.raises(IntegrityError), engine_db.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_PIPELINE_DEPENDENCY SET CONSUME_REPAIRS='X'"))


def test_identity_migration_refuses_or_preserves_project_columns(released):
    seed_run_history(released)
    with released.engine.begin() as conn:
        conn.exec_driver_sql("ALTER TABLE AUD_PIPELINES_RUN_LOG ADD COLUMN PROJECT_OWNER VARCHAR")
        conn.exec_driver_sql("UPDATE AUD_PIPELINES_RUN_LOG SET PROJECT_OWNER='team'")
    if released.engine.dialect.name == "sqlite":
        with pytest.raises(MigrationError, match=r"project_owner.*would discard"):
            migrations.apply_pending_migrations(released.engine)
    else:
        migrations.apply_pending_migrations(released.engine)
    with released.engine.connect() as conn:
        assert (
            conn.execute(text("SELECT PROJECT_OWNER FROM AUD_PIPELINES_RUN_LOG")).scalar_one()
            == "team"
        )


def test_skipped_summary_can_be_reset_after_migration(released):
    from etl_craft.engine.transitions import delete_skipped_task_run

    with released.engine.begin() as conn:
        p = add_pipeline(conn, "P")
        t = add_task(conn, p, "load")
        r = conn.execute(
            text(
                "INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS) "
                "VALUES (:p, 'SUCCESS') RETURNING PIPELINE_RUN_ID"
            ),
            {"p": p},
        ).scalar_one()
        skipped = conn.execute(
            text(
                "INSERT INTO AUD_TASK_RUN_LOG (TASK_ID, PIPELINE_RUN_ID, STATUS) "
                "VALUES (:t, :r, 'SKIPPED') RETURNING TASK_RUN_ID"
            ),
            {"t": t, "r": r},
        ).scalar_one()
    migrations.apply_pending_migrations(released.engine)
    with released.engine.begin() as conn:
        delete_skipped_task_run(conn, skipped)
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_TASK_RUN_LOG")).scalar_one() == 0
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS")).scalar_one() == 0


@pytest.mark.parametrize("trigger_kind,prefix", [("MANUAL", "manual:"), ("STAND_IN", "stand-in:")])
def test_new_run_identity_kind(engine_db, trigger_kind, prefix):
    with engine_db.engine.begin() as conn:
        p = add_pipeline(conn, "P")
        transitions.create_active_run(conn, p, trigger_kind=trigger_kind)
        key, kind = conn.execute(
            text("SELECT RUN_KEY, TRIGGER_KIND FROM AUD_PIPELINES_RUN_LOG")
        ).one()
        assert key.startswith(prefix) and kind == trigger_kind


def test_inconsistent_trigger_kind_is_refused_before_inserting(engine_db):
    with engine_db.engine.begin() as conn:
        p = add_pipeline(conn, "P")
        with pytest.raises(RunStateError, match="expected MANUAL, BACKFILL or STAND_IN"):
            transitions.create_active_run(conn, p, backfill=True, trigger_kind="STAND_IN")
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_PIPELINES_RUN_LOG")).scalar_one() == 0
