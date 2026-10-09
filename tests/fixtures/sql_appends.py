"""Append retry, legacy and target-upgrade assertions shared by warehouse suites."""

import pytest

from etl_craft.core.errors import InjectedFaultError, SqlGuardError
from etl_craft.services.upgrade_targets import upgrade_targets
from etl_craft.warehouse.connection import warehouse_dialect


def check_append_retries(w, target):
    """Keep another load, replace a retried batch and retain both task runs across runs."""
    params = {"SQL_ACTION": "APPEND_TABLE", "TARGET_OBJECT": target}
    w.setup(target, "SELECT CAST(1 AS BIGINT) AS id", "APPEND_TABLE")
    w.run("seed_append", SOURCE_SQL="SELECT CAST(10 AS BIGINT) AS id", **params)
    for point in ("after_insert", "after_delete"):
        with pytest.MonkeyPatch.context() as patch:
            patch.setenv("ETL_CRAFT_FAULT", f"sql.append.{point}")
            with pytest.raises(InjectedFaultError):
                w.run(
                    "retry_append",
                    SOURCE_SQL="SELECT CAST(1 AS BIGINT) AS id UNION ALL SELECT 2",
                    **params,
                )
        result = w.run(
            "retry_append", SOURCE_SQL="SELECT CAST(1 AS BIGINT) AS id UNION ALL SELECT 2", **params
        )
        assert (result.source_count, result.insert_count, result.target_count) == (2, 2, 3)
        assert sorted(w.rows(f"SELECT id FROM {w.name(target)}")) == [(1,), (2,), (10,)]
    loads = w.rows(f"SELECT task_run_id FROM {w.name(target)}")
    assert all(row[0] is not None for row in loads)
    assert len({row[0] for row in loads}) == 2
    keys = w.rows(f"SELECT row_id FROM {w.name(target)}")
    assert len(set(keys)) == 3
    w.new_run()
    w.run("retry_append", SOURCE_SQL="SELECT CAST(3 AS BIGINT) AS id", **params)
    assert sorted(w.rows(f"SELECT id FROM {w.name(target)}")) == [(1,), (2,), (3,), (10,)]


def create_legacy_append_target(w, target, loads=0):
    """Build the pre-TASK_RUN_ID append shape, retaining each warehouse's ROW_ID strategy.

    ``loads`` rows of ``id`` 1 stand for appends made before the target had TASK_RUN_ID.
    """
    from datetime import UTC, datetime

    from etl_craft.handlers.sql.session import Session
    from etl_craft.handlers.sql.tables import build_stage, create_target_shape

    with w.warehouse.begin() as conn:
        session = Session(
            conn,
            warehouse_dialect(w.config),
            catalog=w.catalog,
            action="SETUP_TABLE",
            target_object=f"{w.schema}.{target}",
            task_run_id=0,
            params={},
        )
        try:
            stage = build_stage(session, "SELECT CAST(1 AS BIGINT) AS id", empty=True)
            create_target_shape(session, stage, ("CREATE_DATE",))
            if loads:
                rows = " UNION ALL ".join(["SELECT CAST(1 AS BIGINT) AS id"] * loads)
                history = build_stage(session, rows)
                row_id_columns, row_id_values = session.row_id_insert_parts()
                session.run(
                    f"INSERT INTO {session.target} (id, PIPELINE_RUN_ID, CREATE_DATE"
                    f"{row_id_columns}) SELECT id, 0, :now{row_id_values} FROM {history}",
                    {"now": datetime.now(UTC)},
                    step="seed earlier appends",
                )
        finally:
            session.sweep()


def check_legacy_upgrade(w, target):
    """Appends refuse a target without TASK_RUN_ID; upgrading keeps earlier loads unassigned."""
    create_legacy_append_target(w, target, loads=2)
    params = {
        "SQL_ACTION": "APPEND_TABLE",
        "TARGET_OBJECT": target,
        "SOURCE_SQL": "SELECT CAST(1 AS BIGINT) AS id",
    }
    with pytest.raises(
        SqlGuardError, match=r"no TASK_RUN_ID column.*upgrade-targets --action APPEND_TABLE"
    ):
        w.run("legacy_append", **params)
    assert w.rows(f"SELECT COUNT(*) FROM {w.name(target)}") == [(2,)]
    before = w.rows(f"SELECT * FROM {w.name(target)}")
    before_names = w.columns(target)
    selected = f"{w.schema}.{target}"
    result = upgrade_targets(
        w.engine_db, w.config, action="APPEND_TABLE", target=selected, dry_run=True
    )
    assert len(result) == 1 and result[0].changed and result[0].dry_run
    assert "task_run_id" not in w.columns(target)
    result = upgrade_targets(w.engine_db, w.config, action="APPEND_TABLE", target=selected)
    assert result[0].changed
    names = w.columns(target)
    index = names.index("task_run_id")
    after = w.rows(f"SELECT * FROM {w.name(target)}")
    positions = [names.index(name) for name in before_names]
    assert sorted(tuple(row[i] for i in positions) for row in after) == sorted(before)
    assert all(row[index] is None for row in after)
    assert all(row[names.index("pipeline_id")] is None for row in after)
    assert not upgrade_targets(w.engine_db, w.config, action="APPEND_TABLE", target=selected)[
        0
    ].changed
    w.run("legacy_append", **params)
    w.run("legacy_append", **params)
    assert w.rows(f"SELECT COUNT(*) FROM {w.name(target)}") == [(3,)]
    assert w.rows(f"SELECT COUNT(*) FROM {w.name(target)} WHERE task_run_id IS NULL") == [(2,)]


def run_killed_append(context):
    """Exit without cleanup after a warehouse insert, using a fresh child connection."""
    import os

    from etl_craft.dialects.engine import build_engine
    from etl_craft.handlers import sql

    os.environ["ETL_CRAFT_FAULT"] = "sql.append.after_insert:kill"
    engine = build_engine(context.config)
    sql.run(context, engine)


def check_identity_inputs(w, target):
    """Resolve named SQL inputs and persist the same identities in the target's audit columns."""
    from etl_craft.handlers import sql

    for code in ("first_dummy", "second_dummy"):
        w.task(code, SQL_ACTION="CREATE_TABLE", TARGET_OBJECT=target, SOURCE_SQL="SELECT 1 AS id")
    w.new_run()
    context = w.task(
        "identity_inputs",
        SQL_ACTION="CREATE_TABLE",
        TARGET_OBJECT=target,
        SOURCE_SQL="SELECT CAST($$pipeline_id AS BIGINT) AS source_pipeline_id, "
        "CAST($$pipeline_run_id AS BIGINT) AS source_pipeline_run_id, "
        "CAST($$task_run_id AS BIGINT) AS source_task_run_id",
        PIPELINE_ID_SUBSTITUTION="true",
        PIPELINE_RUN_ID_SUBSTITUTION="true",
        TASK_RUN_ID_SUBSTITUTION="true",
    )
    expected = (context.pipeline_id, context.pipeline_run_id, context.task_run_id)
    assert len(set(expected)) == 3
    sql.run(context, w.engine_db)
    assert w.rows(
        f"SELECT source_pipeline_id, source_pipeline_run_id, source_task_run_id, "
        f"pipeline_id, pipeline_run_id, task_run_id FROM {w.name(target)}"
    ) == [expected + expected]


def check_ingestion_target_upgrade(w, target):
    """Upgrade a script-owned native target even when SQL defaults to Iceberg."""
    from sqlalchemy import text

    w.execute(f"CREATE TABLE {w.name(target)} AS SELECT CAST('old' AS VARCHAR(20)) AS name")
    context = w.task("ingestion_upgrade", TARGET_OBJECT=target)
    with w.engine_db.begin() as conn:
        conn.execute(
            text("UPDATE CFG_TASKS SET HANDLER = 'PYTHON' WHERE TASK_ID = :t"),
            {"t": context.task_id},
        )
    with w.warehouse.connect() as conn:
        actual = warehouse_dialect(w.config).existing_table_format(conn, w.name(target))
    result = upgrade_targets(w.engine_db, w.config)
    assert len(result) == 1 and result[0].columns == (
        "PIPELINE_ID",
        "PIPELINE_RUN_ID",
        "TASK_RUN_ID",
    )
    assert w.rows(
        f"SELECT name, pipeline_id, pipeline_run_id, task_run_id FROM {w.name(target)}"
    ) == [("old", None, None, None)]
    with w.warehouse.connect() as conn:
        assert warehouse_dialect(w.config).existing_table_format(conn, w.name(target)) == actual
    assert not upgrade_targets(w.engine_db, w.config)[0].changed
