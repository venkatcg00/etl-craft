"""Unmanaged SQL uses native sessions; earlier committed statements survive a failure."""

import logging
from dataclasses import replace

import pytest

from etl_craft.core.enums import Handler
from etl_craft.core.errors import HandlerError
from etl_craft.handlers.registry import resolve_handler
from etl_craft.handlers.sql_script import run
from fixtures.sql_warehouse import warehouses


def script(w, body, **params):
    folder = w.project / "sql_scripts"
    folder.mkdir(exist_ok=True)
    (folder / "maintenance.sql").write_text(body, encoding="utf-8")
    return replace(
        w.task("maintenance", SCRIPT_FILE="sql_scripts/maintenance.sql", **params),
        handler=Handler.SQL_SCRIPT,
    )


def test_order_binds_and_statement_audit(sql_world, caplog):
    w = sql_world
    context = script(
        w,
        f"""
        CREATE TABLE {w.name("script_rows")} (id BIGINT, label VARCHAR);
        INSERT INTO {w.name("script_rows")} VALUES (:pipeline_id, :label);
        INSERT INTO {w.name("script_rows")} VALUES (:pipeline_run_id, ':literal; untouched 10%');
        SELECT :task_run_id, :run_date;
    """,
        INPUT_PARAMS='{"label": "bound; SELECT secret"}',
    )
    with caplog.at_level(logging.INFO, logger="etl_craft.handlers.sql_script"):
        result = resolve_handler(Handler.SQL_SCRIPT)(context, w.engine_db)
    with w.warehouse.connect() as conn:
        rows = (
            conn.exec_driver_sql(f"SELECT id, label FROM {w.name('script_rows')}").mappings().all()
        )
    assert sorted((r["id"], r["label"]) for r in rows) == sorted(
        [
            (context.pipeline_id, "bound; SELECT secret"),
            (context.pipeline_run_id, ":literal; untouched 10%"),
        ]
    )
    assert result.variables["STATEMENTS"] == 4
    for index in range(1, 5):
        assert f"statement {index}:" in caplog.text
    assert "rows affected=" in caplog.text
    assert "duration=" in caplog.text
    assert "bound; SELECT secret" not in caplog.text
    assert "untouched" not in caplog.text
    assert w.columns("script_rows") == ["id", "label"]


def test_failure_stops_the_file_and_keeps_prior_commits(sql_world, caplog):
    w = sql_world
    context = script(
        w,
        f"""
        CREATE TABLE {w.name("partial")} (id BIGINT);
        INSERT INTO {w.name("partial")} VALUES (7);
        INSERT INTO {w.name("missing_table")} VALUES (8);
        INSERT INTO {w.name("partial")} VALUES (9);
    """,
    )
    with (
        caplog.at_level(logging.INFO, logger="etl_craft.handlers.sql_script"),
        pytest.raises(HandlerError) as raised,
    ):
        run(context, w.engine_db)
    message = str(raised.value)
    assert "maintenance.sql" in message
    assert "statement 3" in message
    assert "missing_table" in message
    assert "correct the file" in message
    with w.warehouse.connect() as conn:
        rows = conn.exec_driver_sql(f"SELECT id FROM {w.name('partial')}").mappings().all()
    assert [row["id"] for row in rows] == [7]
    assert "statement 4 starting" not in caplog.text


@warehouses("duckdb", "postgres")
def test_explicit_file_transaction_can_roll_back(sql_world):
    w = sql_world
    context = script(
        w,
        f"""
        CREATE TABLE {w.name("explicit_tx")} (id BIGINT);
        BEGIN;
        INSERT INTO {w.name("explicit_tx")} VALUES (1);
        ROLLBACK;
        INSERT INTO {w.name("explicit_tx")} VALUES (2);
    """,
    )
    run(context, w.engine_db)
    with w.warehouse.connect() as conn:
        rows = conn.exec_driver_sql(f"SELECT id FROM {w.name('explicit_tx')}").mappings().all()
    assert [row["id"] for row in rows] == [2]


@warehouses("postgres")
def test_vacuum_runs_outside_a_transaction(sql_world):
    w = sql_world
    run(
        script(w, f"CREATE TABLE {w.name('vacuumed')} (id BIGINT); VACUUM {w.name('vacuumed')};"),
        w.engine_db,
    )
