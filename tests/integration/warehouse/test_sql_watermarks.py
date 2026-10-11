"""SQL offsets advance with success, and retries read the same source range."""

from dataclasses import replace

import pytest
from sqlalchemy import text

from etl_craft.core.errors import SqlGuardError
from etl_craft.engine.repository.offsets import fetch_task_offset
from etl_craft.execution.child import run_handler
from fixtures.sql_warehouse import warehouses
from fixtures.sql_watermarks import check_watermarks


def test_watermark_batches_and_retry(sql_world):
    check_watermarks(sql_world, "watermarked_events")


@pytest.mark.parametrize(
    ("kind", "start", "select", "expected"),
    [
        ("TEXT", "a", "SELECT 'b' AS cursor", "b"),
        (
            "TIMESTAMP",
            "2026-01-01T00:00:00",
            "SELECT TIMESTAMP '2026-01-02 12:30:00.123456' AS cursor",
            "2026-01-02T12:30:00.123456",
        ),
    ],
)
def test_typed_watermarks(sql_world, kind, start, select, expected):
    w = sql_world
    w.setup("typed", select, "APPEND_TABLE")
    result = w.run(
        "typed_load",
        SQL_ACTION="APPEND_TABLE",
        TARGET_OBJECT="typed",
        SOURCE_SQL=f"SELECT cursor FROM ({select}) s WHERE cursor > $$watermark",
        WATERMARK_COLUMN="cursor",
        WATERMARK_TYPE=kind,
        WATERMARK_START=start,
    )
    assert result.offset.offset_type == kind and result.offset.offset_value == expected


@pytest.mark.parametrize(
    ("column", "select", "match"),
    [
        ("missing", "SELECT CAST(1 AS BIGINT) AS id", "absent from the SELECT"),
        ("id", "SELECT CAST(NULL AS BIGINT) AS id", "contains NULL"),
        ("id", "SELECT 'two' AS id", "expected NUMBER"),
    ],
)
def test_bad_staged_watermarks_do_not_write(sql_world, column, select, match):
    w = sql_world
    w.setup("bad_watermark", "SELECT CAST(1 AS BIGINT) AS id", "APPEND_TABLE")
    # The token is used without filtering the invalid values out of this batch.
    with pytest.raises(SqlGuardError, match=match):
        w.run(
            "bad_load",
            SQL_ACTION="APPEND_TABLE",
            TARGET_OBJECT="bad_watermark",
            SOURCE_SQL=f"SELECT * FROM ({select}) s WHERE $$watermark = 0",
            WATERMARK_COLUMN=column,
            WATERMARK_TYPE="NUMBER",
            WATERMARK_START="0",
        )
    assert w.rows(f"SELECT COUNT(*) FROM {w.name('bad_watermark')}") == [(0,)]


@warehouses("postgres")
@pytest.mark.parametrize("value", ["NaN", "Infinity"])
def test_nonfinite_numeric_watermarks_do_not_write(sql_world, value):
    w = sql_world
    w.setup("nonfinite_watermark", "SELECT CAST(1 AS NUMERIC) AS id", "APPEND_TABLE")
    with pytest.raises(SqlGuardError, match="expected NUMBER"):
        w.run(
            "nonfinite_load",
            SQL_ACTION="APPEND_TABLE",
            TARGET_OBJECT="nonfinite_watermark",
            SOURCE_SQL=f"SELECT CAST('{value}' AS NUMERIC) AS id WHERE $$watermark = 0",
            WATERMARK_COLUMN="id",
            WATERMARK_TYPE="NUMBER",
            WATERMARK_START="0",
        )
    assert w.rows(f"SELECT COUNT(*) FROM {w.name('nonfinite_watermark')}") == [(0,)]


def test_backfill_does_not_advance_the_sql_watermark(sql_world):
    w = sql_world
    w.setup("backfill_watermark", "SELECT CAST(1 AS BIGINT) AS id", "APPEND_TABLE")
    context = w.task(
        "backfill_load",
        SQL_ACTION="APPEND_TABLE",
        TARGET_OBJECT="backfill_watermark",
        SOURCE_SQL="SELECT CAST(1 AS BIGINT) AS id WHERE 1 > $$watermark",
        WATERMARK_COLUMN="id",
        WATERMARK_TYPE="NUMBER",
        WATERMARK_START="0",
    )
    assert run_handler(w.engine_db, replace(context, backfill=True)) == 0
    with w.engine_db.connect() as conn:
        assert fetch_task_offset(conn, context.task_id) is None
        row = conn.execute(
            text("SELECT WATERMARK_FROM, WATERMARK_TO FROM AUD_TASK_RUN_LOG WHERE TASK_RUN_ID=:id"),
            {"id": context.task_run_id},
        ).one()
        assert tuple(row) == ("0", "1")


def test_a_reset_reloads_through_a_merge_without_duplicates(sql_world):
    from etl_craft.engine.runlog import RunSelector
    from etl_craft.execution.interventions import reset_watermark

    w = sql_world
    w.setup("merge_watermark", "SELECT CAST(1 AS BIGINT) AS id", "SCD1_MERGE")
    params = {
        "SQL_ACTION": "SCD1_MERGE",
        "TARGET_OBJECT": "merge_watermark",
        "SOURCE_SQL": (
            "SELECT id FROM (SELECT CAST(1 AS BIGINT) AS id UNION ALL SELECT 2) s "
            "WHERE id > $$watermark"
        ),
        "MERGE_KEY": "id",
        "MERGE_COMPARE_COLUMNS": "id",
        "WATERMARK_COLUMN": "id",
        "WATERMARK_TYPE": "NUMBER",
        "WATERMARK_START": "0",
    }
    context = w.task("merge_load", **params)
    assert run_handler(w.engine_db, context) == 0
    reset_watermark(
        w.engine_db,
        w.config,
        "P",
        "merge_load",
        "0",
        "reload",
        selector=RunSelector(run_id=w.pipeline_run_id),
    )
    w.new_run()
    assert run_handler(w.engine_db, w.task("merge_load", **params)) == 0
    assert sorted(w.rows(f"SELECT id FROM {w.name('merge_watermark')}")) == [(1,), (2,)]


def test_text_watermarks_are_literals_even_with_sql_and_bind_characters(sql_world):
    w = sql_world
    w.setup("text_cursor", "SELECT 'z' AS cursor", "APPEND_TABLE")
    for start in (":cursor", "'; DROP TABLE anything; --", "\\:cursor"):
        result = w.run(
            "text_load",
            SQL_ACTION="APPEND_TABLE",
            TARGET_OBJECT="text_cursor",
            SOURCE_SQL="SELECT 'z' AS cursor WHERE 'z' > $$watermark",
            WATERMARK_COLUMN="cursor",
            WATERMARK_TYPE="TEXT",
            WATERMARK_START=start,
        )
        assert result.offset.offset_value == "z"


def test_watermark_literals_keep_their_values(sql_world):
    from fixtures.sql_watermarks import check_watermark_literals

    check_watermark_literals(sql_world, "literal_watermark")
