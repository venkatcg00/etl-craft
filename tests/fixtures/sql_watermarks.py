"""SQL watermark batches and retries, shared by local and cloud warehouses."""

from datetime import datetime

import pytest

from etl_craft.core.actor import current_actor
from etl_craft.core.enums import ApiRole
from etl_craft.core.errors import InjectedFaultError
from etl_craft.engine import transitions
from etl_craft.engine.repository.offsets import fetch_task_offset
from etl_craft.engine.runlog import RunSelector
from etl_craft.execution.child import run_handler
from etl_craft.scripting import Offset
from etl_craft.services.catalog import build_catalog
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.runs import mark
from etl_craft.services.operations.status import explain_task
from etl_craft.warehouse.connection import warehouse_dialect


def check_watermarks(w, target):
    """Two batches, a failed write and retry, then an empty batch and an audited reset."""
    w.setup(target, "SELECT CAST(1 AS BIGINT) AS id", "APPEND_TABLE")
    params = {
        "SQL_ACTION": "APPEND_TABLE",
        "TARGET_OBJECT": target,
        "SOURCE_SQL": (
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
            conn, context.task_run_id, status="SUCCESS", counts=result, offset=result.offset
        )
    assert result.offset.offset_value == "2"
    w.new_run()
    params["SOURCE_SQL"] = (
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
    explanation = explain_task(
        ctx, "P", "watermark_load", selector=RunSelector(run_id=w.pipeline_run_id)
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
            conn, context.task_run_id, status="SUCCESS", counts=result, offset=result.offset
        )
    done = mark(
        ctx,
        "P",
        None,
        "reload source",
        task_code="watermark_load",
        watermark="1",
        selector=RunSelector(run_id=w.pipeline_run_id),
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
            SOURCE_SQL="SELECT $$watermark AS position_value",
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
        SOURCE_SQL=f"SELECT position_value FROM ({select}) s WHERE position_value > $$watermark",
        WATERMARK_COLUMN="position_value",
        WATERMARK_TYPE="TIMESTAMP",
        WATERMARK_START="2026-01-01T00:00:00",
    )
    position = w.rows(f"SELECT position_value FROM {w.name(target)}")[0][0]
    assert position.replace(tzinfo=None) == datetime(2026, 1, 2, 12, 30, 0, 123456)
    assert result.offset.offset_value == Offset.timestamp(position).stored()
