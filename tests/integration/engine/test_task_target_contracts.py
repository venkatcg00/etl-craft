"""Task references preserve stamps, inactive intent and earlier target definitions."""

import time
from datetime import UTC, datetime

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from etl_craft.core.actor import purpose
from etl_craft.core.errors import MigrationError
from etl_craft.engine import migrations
from etl_craft.engine.repository.tasks import fetch_task_parameters
from etl_craft.services.config_files import export_config, read_config_files
from fixtures.catalog import snapshot
from fixtures.metadata import add_pipeline, add_table, add_task
from fixtures.released_schema import install


@pytest.fixture
def legacy_targets_db(empty_engine_db, monkeypatch):
    db = empty_engine_db
    install(db, "0.2.0")
    monkeypatch.delenv("ETL_CRAFT_MIGRATIONS_DIR", raising=False)
    for file in migrations.migration_streams(db.engine)[0].files:
        if "0007" <= file.version < "0030":
            migrations._apply(db.engine, file)
    return db


@pytest.mark.parametrize("unversioned", [False, True])
def test_conversion_preserves_targets_stamps_disabled_intent_and_past_exports(
    legacy_targets_db, tmp_path, unversioned
):
    db = legacy_targets_db
    with db.engine.begin() as conn:
        parent = add_table(conn, "analytics.raw.orders")
        pipeline = add_pipeline(conn, "P")
        first = add_task(
            conn, pipeline, "load", TARGET_OBJECT="raw.orders", SQL_ACTION="SCD1_MERGE"
        )
        disabled = add_task(conn, pipeline, "disabled", TARGET_OBJECT="analytics.raw.orders")
        add_task(conn, pipeline, "email", "EMAIL_ALERT")
        conn.execute(
            text("UPDATE CFG_TASK_PARAMETERS SET ACTIVE_FLAG='N' WHERE TASK_ID=:task"),
            {"task": disabled},
        )
        before = conn.execute(text("SELECT * FROM CFG_TASKS ORDER BY TASK_ID")).all()
        old_params = conn.execute(
            text(
                "SELECT TASK_PARAMETER_ID,PARAMETER_VALUE,ACTIVE_FLAG,CREATED_BY,CREATE_D"
                "ATE,UPDATED_BY,UPDATED_DATE FROM CFG_TASK_PARAMETERS WHERE "
                "PARAMETER_NAME='TARGET_OBJECT' ORDER BY TASK_PARAMETER_ID"
            )
        ).all()
    if unversioned:
        token = purpose.set("retention")
        try:
            with db.engine.begin() as conn:
                conn.execute(text("DELETE FROM AUD_CFG_TASKS_HISTORY"))
                conn.execute(text("DELETE FROM AUD_CFG_TASK_PARAMETERS_HISTORY"))
        finally:
            purpose.reset(token)
    moment = datetime.now(UTC)
    time.sleep(0.01)
    assert migrations.apply_pending_migrations(
        db.engine, catalog="analytics", warehouse_dialect="duckdb"
    ) == ["0030_task_target_contracts.sql"]
    with db.engine.connect() as conn:
        after = conn.execute(text("SELECT * FROM CFG_TASKS ORDER BY TASK_ID")).all()
        assert [row[:-2] for row in after] == before
        assert [row[-2:] for row in after] == [(parent, "Y"), (parent, "N"), (None, "Y")]
        assert fetch_task_parameters(conn, first)["TARGET_OBJECT"] == "analytics.raw.orders"
        assert "TARGET_OBJECT" not in fetch_task_parameters(conn, disabled)
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) FROM CFG_TASK_PARAMETERS WHERE PARAMETER_NAME='TARGET_OBJECT'"
                )
            ).scalar_one()
            == 0
        )
        assert (
            conn.execute(
                text(
                    "SELECT TASK_PARAMETER_ID,PARAMETER_VALUE,ACTIVE_FLAG,CREATED_BY,CREA"
                    "TE_DATE,UPDATED_BY,UPDATED_DATE FROM AUD_CFG_TASK_PARAMETERS_HISTORY"
                    " WHERE OPERATION='DELETE' AND "
                    "MIGRATION='engine/0030_task_target_contracts.sql' ORDER BY "
                    "TASK_PARAMETER_ID"
                )
            ).all()
            == old_params
        )
        changes = conn.execute(text("SELECT CHANGE_ID FROM AUD_METADATA_CHANGES")).scalars().all()
        assert len(changes) == len(set(changes))
    export_config(db.engine, tmp_path / "past", as_of=moment)
    past = read_config_files(tmp_path / "past")
    assert past.rows["tasks.csv"][("P", "load")].values["TARGET_CATALOG_NAME"] is None
    assert past.rows["tasks.csv"][("P", "disabled")].values["TARGET_ACTIVE_FLAG"] == "N"
    assert not any(key[-1] == "TARGET_OBJECT" for key in past.rows["task_parameters.csv"])
    with db.engine.begin() as conn:
        other = add_table(conn, "other.raw.orders")
        conn.execute(
            text("UPDATE CFG_TASKS SET TARGET_TABLE_ID=:target WHERE TASK_ID=:id"),
            {"target": other, "id": first},
        )
    export_config(db.engine, tmp_path / "past-again", as_of=moment)
    assert (tmp_path / "past/tasks.csv").read_bytes() == (
        tmp_path / "past-again/tasks.csv"
    ).read_bytes()


@pytest.mark.parametrize(
    ("name", "target"),
    [
        ("TARGET_OBJECT", None),
        ("TARGET_OBJECT", ""),
        ("TARGET_OBJECT", "orders"),
        ("TARGET_OBJECT", "four.part.table.name"),
        ("target_object", "raw.orders"),
        (" TARGET_OBJECT ", "raw.orders"),
    ],
)
def test_invalid_legacy_targets_refuse_all_pending_changes(legacy_targets_db, name, target):
    db = legacy_targets_db
    with db.engine.begin() as conn:
        task = add_task(conn, add_pipeline(conn, "P"), "load")
        conn.execute(
            text(
                "INSERT INTO CFG_TASK_PARAMETERS "
                "(TASK_ID,PARAMETER_NAME,PARAMETER_VALUE,ACTIVE_FLAG) VALUES "
                "(:id,:name,:target,'N')"
            ),
            {"id": task, "name": name, "target": target},
        )
    before = snapshot(db.engine)
    with pytest.raises(MigrationError, match="previous package"):
        migrations.apply_pending_migrations(db.engine)
    assert snapshot(db.engine) == before


def test_late_failure_rolls_back_schema_data_history_and_ledger(legacy_targets_db, monkeypatch):
    from etl_craft.engine.repository import task_targets

    db = legacy_targets_db
    with db.engine.begin() as conn:
        add_task(conn, add_pipeline(conn, "P"), "load", TARGET_OBJECT="raw.orders")
    before = snapshot(db.engine)
    original = task_targets.convert_task_targets

    def fail(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("conversion fault")

    monkeypatch.setattr(task_targets, "convert_task_targets", fail)
    with pytest.raises(MigrationError, match="conversion fault"):
        migrations.apply_pending_migrations(db.engine)
    assert snapshot(db.engine) == before


def test_parameter_counter_and_fk_guards_survive_conversion(legacy_targets_db):
    db = legacy_targets_db
    with db.engine.begin() as conn:
        task = add_task(conn, add_pipeline(conn, "P"), "load", TARGET_OBJECT="raw.orders")
        if conn.dialect.name == "sqlite":
            conn.exec_driver_sql(
                "UPDATE sqlite_sequence SET seq=100 WHERE name='CFG_TASK_PARAMETERS'"
            )
        else:
            conn.exec_driver_sql(
                "SELECT setval(pg_get_serial_sequence('cfg_task_parameters','task_paramet"
                "er_id'),100,true)"
            )
    migrations.apply_pending_migrations(db.engine)
    with db.engine.begin() as conn:
        assert (
            conn.execute(
                text(
                    "INSERT INTO CFG_TASK_PARAMETERS "
                    "(TASK_ID,PARAMETER_NAME,PARAMETER_VALUE) VALUES "
                    "(:id,'SQL_ACTION','CREATE_TABLE') RETURNING TASK_PARAMETER_ID AS id"
                ),
                {"id": task},
            ).scalar_one()
            == 101
        )
    with pytest.raises(IntegrityError), db.engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_TASKS SET TARGET_TABLE_ID=987654321 WHERE TASK_ID=:id"), {"id": task}
        )
    with pytest.raises(IntegrityError), db.engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO CFG_TASK_PARAMETERS (TASK_ID,PARAMETER_NAME,PARAMETER_VALUE)"
                " VALUES (:id,'target_object','raw.other')"
            ),
            {"id": task},
        )


def test_disabled_table_contract_requires_explicit_target_disable(engine_db):
    from etl_craft.core.errors import MetadataError

    with engine_db.engine.begin() as conn:
        task = add_task(
            conn, add_pipeline(conn, "P"), "script", "PYTHON", TARGET_OBJECT="raw.orders"
        )
        conn.execute(text("UPDATE CFG_TABLES SET ACTIVE_FLAG='N'"))
        with pytest.raises(MetadataError, match=r"table contract.*ACTIVE_FLAG=N"):
            fetch_task_parameters(conn, task)
        conn.execute(
            text("UPDATE CFG_TASKS SET TARGET_ACTIVE_FLAG='N' WHERE TASK_ID=:id"), {"id": task}
        )
        assert "TARGET_OBJECT" not in fetch_task_parameters(conn, task)


def test_custom_parameter_data_refuses_before_upgrade(legacy_targets_db):
    db = legacy_targets_db
    with db.engine.begin() as conn:
        add_task(conn, add_pipeline(conn, "P"), "script", TARGET_OBJECT="raw.orders")
        conn.exec_driver_sql("ALTER TABLE CFG_TASK_PARAMETERS ADD COLUMN CUSTOM_NOTE VARCHAR")
        conn.exec_driver_sql("UPDATE CFG_TASK_PARAMETERS SET CUSTOM_NOTE='keep this'")
    before = snapshot(db.engine)
    with pytest.raises(MigrationError, match=r"custom columns.*CUSTOM_NOTE"):
        migrations.apply_pending_migrations(db.engine)
    assert snapshot(db.engine) == before
