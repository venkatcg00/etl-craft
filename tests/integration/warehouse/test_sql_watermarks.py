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
        SELECT_QUERY=f"SELECT cursor FROM ({select}) s WHERE cursor > $$watermark",
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
            SELECT_QUERY=f"SELECT * FROM ({select}) s WHERE $$watermark = 0",
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
            SELECT_QUERY=f"SELECT CAST('{value}' AS NUMERIC) AS id WHERE $$watermark = 0",
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
        SELECT_QUERY="SELECT CAST(1 AS BIGINT) AS id WHERE 1 > $$watermark",
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
        "SELECT_QUERY": (
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
            SELECT_QUERY="SELECT 'z' AS cursor WHERE 'z' > $$watermark",
            WATERMARK_COLUMN="cursor",
            WATERMARK_TYPE="TEXT",
            WATERMARK_START=start,
        )
        assert result.offset.offset_value == "z"


def test_watermark_literals_keep_their_values(sql_world):
    from fixtures.sql_watermarks import check_watermark_literals

    check_watermark_literals(sql_world, "literal_watermark")


def test_created_merge_columns_and_write_stamps_preserve_engine_time(sql_world, monkeypatch):
    from datetime import UTC, datetime

    from etl_craft.handlers import sql
    from etl_craft.handlers.sql.session import Session
    from fixtures.sql_watermarks import check_timestamp_stamps

    class AgentTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2099, 1, 1, tzinfo=UTC)

    original = Session.run

    def warehouse_time(self, query, *args, **kwargs):
        return original(
            self,
            query.replace("CURRENT_TIMESTAMP", "TIMESTAMP '1900-01-01 00:00:00'"),
            *args,
            **kwargs,
        )

    monkeypatch.setattr(sql, "datetime", AgentTime, raising=False)
    monkeypatch.setattr(Session, "run", warehouse_time)
    check_timestamp_stamps(sql_world, "microsecond_stamps")


@warehouses("postgres")
@pytest.mark.parametrize("action", ["SCD1_MERGE", "SCD2_MERGE", "OVERWRITE_TABLE", "DELETE_ROWS"])
def test_a_coarse_update_date_is_refused_before_writing(sql_world, action):
    w = sql_world
    writer = "SCD2_MERGE" if action == "DELETE_ROWS" else action
    w.setup("coarse_stamps", "SELECT CAST(1 AS BIGINT) AS id", writer)
    w.execute(f"ALTER TABLE {w.name('coarse_stamps')} ALTER COLUMN UPDATE_DATE TYPE TIMESTAMPTZ(0)")
    params = {
        "SQL_ACTION": action,
        "TARGET_OBJECT": "coarse_stamps",
        "SELECT_QUERY": "SELECT CAST(1 AS BIGINT) AS id",
    }
    if action != "OVERWRITE_TABLE":
        params["MERGE_KEY"] = "id"
    if action in {"SCD1_MERGE", "SCD2_MERGE"}:
        params["MERGE_COMPARE_COLUMNS"] = "id"
    with pytest.raises(
        SqlGuardError, match=r"coarse_stamps.UPDATE_DATE.*precision 0.*6.*migration"
    ):
        w.run("coarse_write", **params)
    with w.warehouse.connect() as conn:
        row = conn.execute(text(f"SELECT COUNT(*) AS count FROM {w.name('coarse_stamps')}"))
        assert row.mappings().one()["count"] == 0


def test_empty_bounded_windows_advance_to_the_admitted_high(sql_world):
    from fixtures.sql_watermarks import check_empty_bounded_window

    check_empty_bounded_window(sql_world, "bounded_empty")


def test_bounded_timestamp_source_race_on_each_engine_and_warehouse(sql_world, engine_db):
    from fixtures.sql_watermarks import check_bounded_race

    check_bounded_race(sql_world, engine_db)
