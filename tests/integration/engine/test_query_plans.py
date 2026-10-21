"""Every resolved PostgreSQL catalog query has bounded reads of growing tables."""

import pytest
from sqlalchemy import inspect, text

from etl_craft.engine.queries import statement
from etl_craft.engine.repository.intervention_rows import fetch_task_rows
from fixtures.engine_db import apply_schema
from fixtures.query_plan_data import seed_query_plans
from fixtures.query_plans import FULL_READS, UTILITIES, catalog_plans, full_scans

pytestmark = pytest.mark.engine_postgres


def test_catalog_queries_do_not_scan_growing_tables(postgres_database):
    scan = {"Node Type": "Index Only Scan", "Relation Name": "aud_example"}
    assert list(full_scans({"Node Type": "Limit", "Plans": [scan]})) == []
    assert list(
        full_scans({"Node Type": "Limit", "Plans": [{"Node Type": "Aggregate", "Plans": [scan]}]})
    ) == [scan]
    db = postgres_database
    apply_schema(db.engine)
    with db.engine.begin() as conn:
        seed_query_plans(conn)
        inspector = inspect(conn)
        tables = inspector.get_table_names()
        growing = {name for name in tables if name.startswith("aud_") or name.endswith("_history")}
        growing.add("cfg_pipelines")
        populated = set(
            conn.execute(
                text(
                    "SELECT c.relname AS table_name FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid=c.relnamespace "
                    "WHERE n.nspname=current_schema() AND c.reltuples>0"
                )
            ).scalars()
        )
        assert growing <= populated, growing - populated
        partial = {
            index["name"]
            for table in growing
            for index in inspector.get_indexes(table)
            if index.get("dialect_options", {}).get("postgresql_where")
        }
        explained, failures = set(), []
        for name, plan in catalog_plans(conn, db.dialect):
            explained.add(name)
            for node in full_scans(plan, partial):
                table = node.get("Relation Name")
                if table in growing and (name, table) not in FULL_READS:
                    failures.append(f"{name}: {table}: {node['Node Type']} {node}")
        assert explained | UTILITIES.keys() == db.dialect.query_names()
        assert not failures, "\n".join(failures)

        tasks = fetch_task_rows(conn, 1)
        assert len(tasks) == 20
        assert {row.task_code for row in tasks if row.has_rule_runs} == {"task_1"}
        assert all(row.has_attempts and not row.marked for row in tasks)
        assert {row.consumed_by for row in tasks if row.consumed_by} == {"PLAN_1.task_2 run 1"}
        catalog_tasks = conn.execute(statement(conn, "catalog_tasks")).mappings().all()
        assert len(catalog_tasks) == 20000
        assert all(row["has_successful_run"] for row in catalog_tasks)
        versions = conn.execute(statement(conn, "catalog_documentation_versions")).mappings()
        assert {row["task_id"]: row["version"] for row in versions} == {
            (pipeline - 1) * 20 + 1: 10 for pipeline in range(1, 1001)
        }
        consumption = conn.execute(statement(conn, "catalog_consumption"), {"limit": 1})
        actual = [
            (
                row["pipeline_code"],
                row["pipeline_run_id"],
                row["task_code"],
                row["upstream_pipeline"],
                row["upstream_run_id"],
                row["upstream_task"],
            )
            for row in consumption.mappings()
        ]
        expected = {
            (f"PLAN_{p}", p * 10, "task_2", f"PLAN_{p}", p * 10, "task_1") for p in range(1, 1001)
        } | {
            (f"PLAN_{p}", p * 10, None, f"PLAN_{p - 1}", (p - 1) * 10, None) for p in range(2, 1001)
        }
        assert len(actual) == len(expected)
        assert set(actual) == expected
