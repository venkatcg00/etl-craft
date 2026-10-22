"""End-to-end attempt costs include the fresh child's SQL, not only its supervisor."""

import json
import os
from collections import Counter
from pathlib import Path

import pytest
import yaml
from sqlalchemy import text

from etl_craft.config import load_config
from etl_craft.core.enums import RunStatus
from etl_craft.execution.attempt_types import ChildOptions
from etl_craft.execution.runner import run_task
from fixtures.metadata import add_pipeline, add_task, start_run
from fixtures.round_trips import count_round_trips, database

pytestmark = [pytest.mark.engine_sqlite, pytest.mark.usefixtures("engine_actor")]


def test_sql_attempt_round_trips_include_supervisor_and_child(
    sqlite_schema_template, tmp_path, monkeypatch
):
    from fixtures.engine_db import prepared_sqlite_engine_db

    db = prepared_sqlite_engine_db(tmp_path, sqlite_schema_template)
    config_path = tmp_path / "craft-connector.yml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "Secrets": {"Source_type": "ENVIRONMENT"},
                "Engine": {"dev": {"Jdbc_url": "jdbc:sqlite:engine.db", "Schema": "main"}},
                "Warehouse": {
                    "dev": {"Jdbc_url": "jdbc:duckdb:warehouse.duckdb", "Schema": "main"}
                },
            },
            sort_keys=False,
        )
    )
    config = load_config(config_path)
    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        add_task(
            conn,
            pipeline,
            "create",
            "SQL",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT="main.created",
            SELECT_QUERY="SELECT 1 AS id",
        )
        start_run(conn, pipeline)
    instrumentation = tmp_path / "instrumentation"
    instrumentation.mkdir()
    (instrumentation / "sitecustomize.py").write_text(
        "from fixtures.round_trips import install_child_counter\ninstall_child_counter()\n"
    )
    records = tmp_path / "round-trips.jsonl"
    tests = Path(__file__).parents[2]
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join((str(instrumentation), str(tests))))
    monkeypatch.setenv("ETL_CRAFT_TEST_ROUND_TRIPS", str(records))
    monkeypatch.setenv("ETL_CRAFT_TEST_ROUND_TRIPS_ENGINE", json.dumps(database(db.engine.url)))
    with count_round_trips(db.engine, path=records, include_threads=True):
        result = run_task(
            db.engine, config, "P", "create", child=ChildOptions(cancel_poll_seconds=60)
        )
    assert result.status == RunStatus.SUCCESS, result.message
    events = [json.loads(line) for line in records.read_text().splitlines()]
    assert len({row["pid"] for row in events}) == 2
    counts = Counter(f"{row['kind']}_{row['measure']}" for row in events)
    observed = {
        name: counts[name]
        for name in (
            "engine_statements",
            "engine_transactions",
            "engine_connections",
            "warehouse_statements",
            "warehouse_connections",
        )
    }
    budget_path = Path(__file__).parents[2] / "fixtures" / "sql-statement-budgets.json"
    budgets = json.loads(budget_path.read_text())
    if os.environ.get("ETL_CRAFT_RECORD_QUERY_BUDGET") == "1":
        budgets["attempt"] = observed
        budget_path.write_text(json.dumps(budgets, indent=2, sort_keys=True) + "\n")
    queries = Counter(
        row["query"] for row in events if row["kind"] == "engine" and row["measure"] == "statements"
    )
    assert observed == budgets["attempt"], (
        "One CREATE_TABLE attempt's round trips changed; investigate growth or record the "
        f"improvement with ETL_CRAFT_RECORD_QUERY_BUDGET=1. Observed: {observed}; by query: "
        + json.dumps(dict(queries), sort_keys=True)
    )
    with db.engine.connect() as conn:
        assert (
            conn.execute(text("SELECT STATUS AS status FROM AUD_TASK_ATTEMPTS")).scalar_one()
            == RunStatus.SUCCESS
        )
    db.engine.dispose()
