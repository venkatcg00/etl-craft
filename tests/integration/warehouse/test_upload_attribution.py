"""The production SQL actions retain the writer of each admitted upload."""

from dataclasses import replace

import pytest

from etl_craft.core.errors import SqlGuardError
from etl_craft.core.uploads import UploadWriter
from etl_craft.handlers import sql
from fixtures.sql_warehouse import warehouses


def apply_upload_action(w, context):
    from etl_craft.dialects.warehouse.uploads import writer_parameters
    from etl_craft.handlers.registry import task_store
    from etl_craft.handlers.sql.actions import ACTIONS
    from etl_craft.handlers.sql.context import ActionContext
    from etl_craft.handlers.sql.session import Session
    from etl_craft.handlers.sql.spec import read_sql_task

    task = read_sql_task(context)
    store = task_store(context, w.engine_db)
    state = store.state(task.action)
    with w.warehouse.begin() as conn:
        session = Session(
            conn,
            sql.task_dialect(context),
            catalog=w.catalog,
            action=task.action,
            target_object=task.target_object,
            task_run_id=context.task_run_id,
            params=context.task_params,
        )
        session.uploads = context.uploads
        session.bindings = writer_parameters(context.uploads)
        try:
            ACTIONS[task.action](
                session,
                ActionContext(
                    task,
                    context,
                    store,
                    context.writer,
                    context.started_at,
                    state=state,
                    uploads=context.uploads,
                ),
            )
        finally:
            session.sweep()


@pytest.mark.parametrize("kind", ["SCD1_MERGE", "SCD2_MERGE"])
def test_upload_rows_name_their_own_writer(sql_world, kind):
    w = sql_world
    source = "SELECT 1 AS id, 'first' AS name, CAST(10 AS BIGINT) AS upload_id"
    w.setup("uploaded", source, kind)
    context = w.task(
        "upload_merge",
        SQL_ACTION=kind,
        TARGET_OBJECT="uploaded",
        SELECT_QUERY=source + " UNION ALL SELECT 2, 'second', 11",
        MERGE_KEY="id",
        MERGE_COMPARE_COLUMNS="name",
    )
    uploads = (UploadWriter(10, "alice"), UploadWriter(11, "bob"))
    apply_upload_action(w, replace(context, uploads=uploads))
    with w.warehouse.connect() as conn:
        from sqlalchemy import text

        rows = (
            conn.execute(
                text(
                    f"SELECT id AS id, created_by AS creator, updated_by AS writer "
                    f"FROM {w.name('uploaded')} WHERE ACTIVE_FLAG='Y' ORDER BY id"
                )
            )
            .mappings()
            .all()
        )
    assert [(row["id"], row["creator"], row["writer"]) for row in rows] == [
        (1, "alice", "alice"),
        (2, "bob", "bob"),
    ]

    updated = w.task(
        "upload_merge",
        SQL_ACTION=kind,
        TARGET_OBJECT="uploaded",
        SELECT_QUERY="SELECT 1 AS id,'corrected first' AS name,20 AS upload_id "
        "UNION ALL SELECT 2,'corrected second',21",
        MERGE_KEY="id",
        MERGE_COMPARE_COLUMNS="name",
    )
    apply_upload_action(
        w,
        replace(
            updated,
            uploads=(
                UploadWriter(20, "bob"),
                UploadWriter(21, "alice"),
            ),
        ),
    )
    with w.warehouse.connect() as conn:
        rows = (
            conn.execute(
                text(
                    f"SELECT id AS id,CREATED_BY AS creator,UPDATED_BY AS writer "
                    f"FROM {w.name('uploaded')} WHERE ACTIVE_FLAG='Y' ORDER BY id"
                )
            )
            .mappings()
            .all()
        )
    assert [(r["id"], r["creator"], r["writer"]) for r in rows] == [
        (1, "bob", "bob"),
        (2, "alice", "alice"),
    ]


@pytest.mark.parametrize("value", ["NULL", "99"])
def test_unowned_upload_rows_fail_before_target_changes(sql_world, value):
    w = sql_world
    source = "SELECT 1 AS id, 'first' AS name, CAST(10 AS BIGINT) AS upload_id"
    w.setup("unowned", source, "SCD2_MERGE")
    context = w.task(
        "upload_merge",
        SQL_ACTION="SCD2_MERGE",
        TARGET_OBJECT="unowned",
        SELECT_QUERY=f"SELECT 1 AS id, 'first' AS name, CAST({value} AS BIGINT) AS upload_id",
        MERGE_KEY="id",
        MERGE_COMPARE_COLUMNS="name",
    )
    with pytest.raises(SqlGuardError) as failure:
        apply_upload_action(w, replace(context, uploads=(UploadWriter(10, "alice"),)))
    for expected in (w.name("unowned"), "1 staged rows", "sample=", "(10,)", "corrected file"):
        assert expected in str(failure.value)
    assert w.rows(f"SELECT COUNT(*) FROM {w.name('unowned')}") == [(0,)]


def test_append_rows_name_each_upload_account(sql_world):
    w = sql_world
    source = "SELECT 1 AS id, CAST(10 AS BIGINT) AS upload_id"
    uploads = (UploadWriter(10, "alice"), UploadWriter(11, "bob"))
    setup = w.task(
        "upload_setup",
        SQL_ACTION="SETUP_TABLE",
        SETUP_FOR="APPEND_TABLE",
        TARGET_OBJECT="uploaded_append",
        SELECT_QUERY=source,
    )
    sql.run(replace(setup, uploads=uploads), w.engine_db)
    context = w.task(
        "upload_append",
        SQL_ACTION="APPEND_TABLE",
        TARGET_OBJECT="uploaded_append",
        SELECT_QUERY=source + " UNION ALL SELECT 2, 11",
    )
    sql.run(replace(context, uploads=uploads), w.engine_db)
    from sqlalchemy import text

    with w.warehouse.connect() as conn:
        rows = (
            conn.execute(
                text(
                    f"SELECT id AS id, created_by AS creator, updated_by AS writer "
                    f"FROM {w.name('uploaded_append')} ORDER BY id"
                )
            )
            .mappings()
            .all()
        )
    assert [(row["id"], row["creator"], row["writer"]) for row in rows] == [
        (1, "alice", "alice"),
        (2, "bob", "bob"),
    ]


@warehouses("duckdb_iceberg", "trino_iceberg")
@pytest.mark.parametrize("kind", ["SCD1_MERGE", "SCD2_MERGE"])
def test_unsupported_stewarded_scd_is_refused_before_run_and_at_runtime(sql_world, kind):
    from sqlalchemy import text

    from etl_craft.handlers.sql.protection import Protection
    from etl_craft.handlers.sql.session import Session
    from etl_craft.handlers.sql.spec import read_sql_task
    from etl_craft.services.stewardship_checks import sql_problems

    w = sql_world
    context = w.task(
        "protected_upload",
        SQL_ACTION=kind,
        TARGET_OBJECT="protected",
        SELECT_QUERY="SELECT 1 AS id,'x' AS name,1 AS UPLOAD_ID",
        MERGE_KEY="id",
        MERGE_COMPARE_COLUMNS="name",
        MERGE_DEDUPE_ORDER="UPLOAD_ID ASC",
    )
    with w.engine_db.begin() as conn:
        conn.execute(text("UPDATE CFG_PIPELINES SET STEWARDABLE_FLAG='Y'"))
        problems = sql_problems(conn, context, read_sql_task(context))
    message = " ".join(problems)
    for expected in ["P.protected_upload", "protected", "unsupported", "non-protected target"]:
        assert expected in message
    from etl_craft.core.uploads import UploadInfo

    upload = UploadInfo(1, "alice", "data.csv", 0, "0" * 64)
    with pytest.raises(SqlGuardError) as failure:
        sql.run(replace(context, uploads=(upload,), upload_inputs=(upload,)), w.engine_db)
    for expected in ["pipeline P", "task protected_upload", "protected", "non-protected target"]:
        assert expected in str(failure.value)
    with w.warehouse.begin() as conn:
        session = Session(
            conn,
            sql.task_dialect(context),
            catalog=w.catalog,
            action=kind,
            target_object=read_sql_task(context).target_object,
            task_run_id=context.task_run_id,
            params=context.task_params,
        )
        try:
            with pytest.raises(SqlGuardError, match="protected writes unsupported") as failure:
                Protection(session, read_sql_task(context)).acquire()
            assert "native PostgreSQL or DuckDB" in str(failure.value)
            assert "protected" in str(failure.value)
        finally:
            session.sweep()
