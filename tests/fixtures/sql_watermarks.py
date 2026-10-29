"""SQL watermark batches and retries, shared by local and cloud warehouses."""

from datetime import datetime

import pytest

from etl_craft.core.actor import current_actor
from etl_craft.core.enums import ActionName, ApiRole, RunStatus
from etl_craft.core.errors import InjectedFaultError
from etl_craft.engine import transitions
from etl_craft.engine.repository.offsets import fetch_task_offset
from etl_craft.execution.child import run_handler
from etl_craft.scripting import Offset
from etl_craft.services.catalog.collect import build_catalog
from etl_craft.services.operations.context import OperationContext
from etl_craft.warehouse.connection import warehouse_dialect
from fixtures.actions import act


def check_watermarks(w, target):
    """Two batches, a failed write and retry, then an empty batch and an audited reset."""
    w.setup(target, "SELECT CAST(1 AS BIGINT) AS id", "APPEND_TABLE")
    params = {
        "SQL_ACTION": "APPEND_TABLE",
        "TARGET_OBJECT": target,
        "SELECT_QUERY": (
            "SELECT id FROM (SELECT CAST(1 AS BIGINT) AS id UNION ALL SELECT 2) s "
            "WHERE id > $$watermark"
        ),
        "WATERMARK_COLUMN": "id",
        "WATERMARK_TYPE": "NUMBER",
        "WATERMARK_START": "0",
    }
    context = w.task("watermark_load", **params)
    result = w.run("watermark_load", **params)
    with w.engine_db.connect() as conn:
        assert fetch_task_offset(conn, context.task_id) is None
    with w.engine_db.begin() as conn:
        transitions.finish_task_run(
            conn, context.task_run_id, status=RunStatus.SUCCESS, counts=result, offset=result.offset
        )
    assert result.offset.offset_value == "2"
    w.new_run()
    params["SELECT_QUERY"] = (
        "SELECT id FROM (SELECT CAST(1 AS BIGINT) AS id UNION ALL SELECT 2 "
        "UNION ALL SELECT 3) s WHERE id > $$watermark"
    )
    context = w.task("watermark_load", **params)
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("ETL_CRAFT_FAULT", "sql.append.after_insert")
        with pytest.raises(InjectedFaultError):
            w.run("watermark_load", **params)
    with w.engine_db.connect() as conn:
        assert fetch_task_offset(conn, context.task_id).offset_value == "2"
    assert run_handler(w.engine_db, context) == 0
    assert sorted(w.rows(f"SELECT id FROM {w.name(target)}")) == [(1,), (2,), (3,)]
    ctx = OperationContext(w.engine_db, w.config, current_actor(), role=ApiRole.ADMIN)
    explanation = act(
        ctx,
        ActionName.EXPLAIN,
        pipeline_code="P",
        task_code="watermark_load",
        run_id=w.pipeline_run_id,
    )
    assert (explanation.task.watermark_from, explanation.task.watermark_to) == ("2", "3")
    catalog = build_catalog(w.engine_db, w.config)
    assert any(run.watermark_to == "3" for task in catalog.tasks.values() for run in task.runs)
    w.new_run()
    context = w.task("watermark_load", **params)
    result = w.run("watermark_load", **params)
    assert result.source_count == 0 and result.offset is None
    with w.engine_db.begin() as conn:
        transitions.finish_task_run(
            conn, context.task_run_id, status=RunStatus.SUCCESS, counts=result, offset=result.offset
        )
    done = act(
        ctx,
        ActionName.MARK,
        pipeline_code="P",
        reason="reload source",
        task_code="watermark_load",
        watermark="1",
        run_id=w.pipeline_run_id,
    )
    assert "3 -> 1 (NUMBER)" in done.message
    with w.engine_db.connect() as conn:
        assert fetch_task_offset(conn, context.task_id).offset_value == "1"


def check_watermark_literals(w, target):
    """TEXT literals keep quotes and bind characters; TIMESTAMP positions keep microseconds."""
    dialect = warehouse_dialect(w.config)
    string_type = dialect.string_type
    w.setup(target, f"SELECT CAST('' AS {string_type}) AS position_value", "APPEND_TABLE")
    for start in (":cursor", "O'Reilly", "\\:cursor"):
        result = w.run(
            "text_watermark",
            SQL_ACTION="APPEND_TABLE",
            TARGET_OBJECT=target,
            SELECT_QUERY="SELECT $$watermark AS position_value",
            WATERMARK_COLUMN="position_value",
            WATERMARK_TYPE="TEXT",
            WATERMARK_START=start,
        )
        assert result.offset.offset_value == start
        assert w.rows(f"SELECT position_value FROM {w.name(target)}") == [(start,)]
    target += "_ts"
    instant = "TIMESTAMP '2026-01-02 12:30:00.123456'"
    if dialect.spec.key.startswith("snowflake"):
        instant = f"CAST({instant} AS TIMESTAMP_NTZ(6))"
    select = f"SELECT {instant} AS position_value"
    w.setup(target, select, "APPEND_TABLE")
    result = w.run(
        "timestamp_watermark",
        SQL_ACTION="APPEND_TABLE",
        TARGET_OBJECT=target,
        SELECT_QUERY=f"SELECT position_value FROM ({select}) s WHERE position_value > $$watermark",
        WATERMARK_COLUMN="position_value",
        WATERMARK_TYPE="TIMESTAMP",
        WATERMARK_START="2026-01-01T00:00:00",
    )
    position = w.rows(f"SELECT position_value FROM {w.name(target)}")[0][0]
    assert position.replace(tzinfo=None) == datetime(2026, 1, 2, 12, 30, 0, 123456)
    assert result.offset.offset_value == Offset.timestamp(position).stored()


def check_timestamp_stamps(w, target):
    """Created audit columns preserve microseconds, and writes use their recorded start."""
    from etl_craft.handlers import sql

    source = (
        "SELECT CAST(1 AS BIGINT) AS id, CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) AS warehouse_clock"
    )
    w.setup(target, source, "SCD1_MERGE")
    dialect = warehouse_dialect(w.config)
    with w.warehouse.connect() as conn:
        kinds = dialect.full_column_types(conn, w.name(target))
        assert dialect.timestamp_precision(kinds["update_date"]) >= 6
    context = w.task(
        "timestamp_write",
        SQL_ACTION="SCD1_MERGE",
        TARGET_OBJECT=target,
        SELECT_QUERY=source,
        MERGE_KEY="id",
        MERGE_COMPARE_COLUMNS="id",
    )
    with w.engine_db.connect() as conn:
        from sqlalchemy import text

        expected = conn.execute(
            text("SELECT START_DATE AS started_at FROM AUD_TASK_RUN_LOG WHERE TASK_RUN_ID=:id"),
            {"id": context.task_run_id},
        ).scalar_one()
    sql.run(context, w.engine_db)
    from etl_craft.core.time import as_utc

    with w.warehouse.connect() as conn:
        row = (
            conn.execute(text(f"SELECT id AS id, UPDATE_DATE AS update_date FROM {w.name(target)}"))
            .mappings()
            .one()
        )
        assert row["id"] == 1 and as_utc(row["update_date"]) == expected


def check_bounded_race(w, db):
    """An admitted source pins the half-open bound until its writes are visible."""
    from dataclasses import replace
    from datetime import UTC, datetime, timedelta

    from sqlalchemy import text

    from etl_craft.core.enums import AttemptStatus
    from etl_craft.engine import resource_locks
    from etl_craft.handlers import sql
    from fixtures.metadata import add_pipeline, start_run
    from fixtures.races import paused_after

    w.engine_db = db.engine
    w.config = replace(w.config, engine=db.config.engine)
    with db.engine.begin() as conn:
        pipelines = {code: add_pipeline(conn, code) for code in ("ALPHA", "BETA", "GAMMA", "DM")}
        runs = {code: start_run(conn, pipeline) for code, pipeline in pipelines.items()}
    w.pipeline_id, w.pipeline_run_id = pipelines["DM"], runs["DM"]
    source = "SELECT CAST(1 AS BIGINT) AS id, 'initial' AS value"
    for name in ("alpha", "beta"):
        w.setup(name, source, "SCD1_MERGE")
    combined = " UNION ALL ".join(
        f"SELECT id, value, UPDATE_DATE AS changed_at FROM {w.name(name)}"
        for name in ("alpha", "beta")
    )
    w.setup("dm", combined, "SCD2_MERGE")

    def context(code, target, select, *, bounded=False):
        w.pipeline_id, w.pipeline_run_id = pipelines[code], runs[code]
        params = {
            "SQL_ACTION": "SCD2_MERGE" if bounded else "SCD1_MERGE",
            "TARGET_OBJECT": target,
            "SELECT_QUERY": select,
            "MERGE_KEY": "id",
            "MERGE_COMPARE_COLUMNS": "id|value",
        }
        if bounded:
            params.update(
                WATERMARK_COLUMN="changed_at",
                WATERMARK_TYPE="TIMESTAMP",
                WATERMARK_START="2000-01-01T00:00:00+00:00",
                WATERMARK_SOURCE_PIPELINES='["ALPHA","BETA","GAMMA"]',
            )
        task = w.task(code.lower(), **params)
        with db.engine.begin() as conn:
            attempt = transitions.queue_attempt(conn, task.task_run_id, current_actor())
            transitions.claim_attempt(
                conn,
                attempt,
                current_actor(),
                owner=code,
                lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
            )
            transitions.start_attempt(conn, attempt, current_actor(), owner=code)
            started_at = conn.execute(
                text("SELECT STARTED_AT AS started_at FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"),
                {"id": attempt},
            ).scalar_one()
        return replace(task, attempt_id=attempt, owner=code, started_at=started_at)

    def execute(task):
        with resource_locks.attempt_scope(task.attempt_id, task.owner):
            result = sql.run(task, db.engine)
            with db.engine.begin() as conn:
                transitions.finish_attempt(
                    conn,
                    task.attempt_id,
                    AttemptStatus.SUCCESS,
                    current_actor(),
                    owner=task.owner,
                    counts=result,
                    offset=result.offset,
                )
            return result

    alpha = context("ALPHA", "alpha", source)
    beta = context("BETA", "beta", source.replace("AS id", "+ 1 AS id"))
    with paused_after(
        lambda: execute(alpha),
        owner=sql,
        name="task_dialect",
        when=lambda task: task.task_code == "alpha",
    ):
        execute(beta)
        read = context(
            "DM",
            "dm",
            f"SELECT * FROM ({combined}) s "
            "WHERE changed_at >= $$watermark AND changed_at < $$watermark_max",
            bounded=True,
        )
        assert execute(read).source_count == 0
        with db.engine.connect() as conn:
            assert (
                fetch_task_offset(conn, read.task_id).offset_value
                == conn.execute(
                    text(
                        "SELECT STARTED_AT AS started_at FROM AUD_TASK_ATTEMPTS "
                        "WHERE ATTEMPT_ID=:id"
                    ),
                    {"id": alpha.attempt_id},
                )
                .scalar_one()
                .isoformat()
            )
    with db.engine.begin() as conn:
        # Client runs stay open; Gamma has no write attempt and cannot hold a bound.
        transitions.finish_run(conn, runs["DM"], RunStatus.SUCCESS, current_actor())
        runs["DM"] = start_run(conn, pipelines["DM"])
    read = context("DM", "dm", read.task_params["SELECT_QUERY"], bounded=True)
    assert execute(read).source_count == 2
    with w.warehouse.connect() as conn:
        rows = (
            conn.execute(
                text(f"SELECT id, value, ACTIVE_FLAG AS active FROM {w.name('dm')} ORDER BY id")
            )
            .mappings()
            .all()
        )
        assert [dict(row) for row in rows] == [
            {"id": 1, "value": "initial", "active": "Y"},
            {"id": 2, "value": "initial", "active": "Y"},
        ]

    repaired = context("ALPHA", "alpha", source.replace("'initial'", "'corrected'"))
    execute(repaired)
    with db.engine.begin() as conn:
        transitions.finish_run(conn, runs["DM"], RunStatus.SUCCESS, current_actor())
        runs["DM"] = start_run(conn, pipelines["DM"])
    latest = context("DM", "dm", read.task_params["SELECT_QUERY"], bounded=True)
    assert execute(latest).source_count == 1
    with w.warehouse.connect() as conn:
        versions = (
            conn.execute(
                text(
                    f"SELECT id AS id, value AS value, ACTIVE_FLAG AS active FROM {w.name('dm')} "
                    "WHERE id=1 ORDER BY value"
                )
            )
            .mappings()
            .all()
        )
        assert [dict(row) for row in versions] == [
            {"id": 1, "value": "corrected", "active": "Y"},
            {"id": 1, "value": "initial", "active": "N"},
        ]


def check_empty_bounded_window(w, target):
    """An empty bounded batch commits its admitted high, on local and cloud warehouses."""
    from etl_craft.core.time import as_utc
    from etl_craft.handlers import sql

    select = "SELECT CAST(TIMESTAMP '2000-01-01 00:00:00' AS TIMESTAMP(6)) AS changed WHERE 1=0"
    w.setup(target, select, "APPEND_TABLE")
    context = w.task(
        "bounded_read",
        SQL_ACTION="APPEND_TABLE",
        TARGET_OBJECT=target,
        SELECT_QUERY=f"SELECT changed FROM ({select}) s "
        "WHERE changed >= $$watermark AND changed < $$watermark_max",
        WATERMARK_COLUMN="changed",
        WATERMARK_TYPE="TIMESTAMP",
        WATERMARK_START="2000-01-01T00:00:00+00:00",
    )
    result = sql.run(context, w.engine_db)
    assert result.source_count == 0 and result.offset is not None
    with w.engine_db.begin() as conn:
        before = fetch_task_offset(conn, context.task_id)
        high = before.pending_upper_bound
        assert as_utc(result.offset.offset_value) == high
        from etl_craft.engine import transitions

        transitions.finish_task_run(
            conn, context.task_run_id, status=RunStatus.SUCCESS, counts=result, offset=result.offset
        )
        after = fetch_task_offset(conn, context.task_id)
        assert as_utc(after.offset_value) == high and after.pending_task_run_id is None
