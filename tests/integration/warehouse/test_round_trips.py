"""Task-scoped connections, metadata freshness and exact action round-trip budgets."""

import json
import os
from pathlib import Path

import pytest
from sqlalchemy import text

from etl_craft.handlers.sql.session import Session
from etl_craft.warehouse.connection import open_warehouse, warehouse_dialect
from fixtures.round_trips import count_round_trips
from fixtures.sql_warehouse import warehouses

BUDGET = Path(__file__).parents[2] / "fixtures" / "sql-statement-budgets.json"


def test_action_round_trip_budgets(sql_world):
    w = sql_world
    observed = {}

    def run(code, action, target, **params):
        context = w.task(code, SQL_ACTION=action, TARGET_OBJECT=target, **params)
        from etl_craft.handlers import sql

        with count_round_trips(w.engine_db) as counts:
            result = sql.run(context, w.engine_db)
        observed[action] = counts.result()
        return result

    shape = "SELECT 1 AS id, CAST('a' AS VARCHAR(20)) AS name"
    assert run("create", "CREATE_TABLE", "created", SELECT_QUERY=shape).insert_count == 1
    run("setup", "SETUP_TABLE", "merged", SELECT_QUERY=shape, SETUP_FOR="SCD1_MERGE")
    w.setup("replaced", shape, "OVERWRITE_TABLE")
    assert run("overwrite", "OVERWRITE_TABLE", "replaced", SELECT_QUERY=shape).insert_count == 1
    w.setup("appended", shape, "APPEND_TABLE")
    assert run("append", "APPEND_TABLE", "appended", SELECT_QUERY=shape).insert_count == 1
    merge = {"SELECT_QUERY": shape, "MERGE_KEY": "id", "MERGE_COMPARE_COLUMNS": "name"}
    assert run("scd1", "SCD1_MERGE", "merged", **merge).insert_count == 1
    w.setup("versions", shape, "SCD2_MERGE")
    assert run("scd2", "SCD2_MERGE", "versions", **merge).insert_count == 1
    assert (
        run(
            "delete", "DELETE_ROWS", "merged", MERGE_KEY="id", SELECT_QUERY="SELECT 1 AS id"
        ).delete_count
        == 1
    )
    if warehouse_dialect(w.config).views:
        run("view", "CREATE_VIEW", "visible", SELECT_QUERY=f"SELECT id FROM {w.name('merged')}")
    w.finish("create")
    run("drop", "DROP_TABLE", "created")
    budgets = json.loads(BUDGET.read_text())
    if os.environ.get("ETL_CRAFT_RECORD_QUERY_BUDGET") == "1":
        budgets[w.kind] = observed
        BUDGET.write_text(json.dumps(budgets, indent=2, sort_keys=True) + "\n")
    assert observed == budgets[w.kind], (
        "Round trips changed; investigate growth or record the improvement with "
        "ETL_CRAFT_RECORD_QUERY_BUDGET=1. Observed: " + json.dumps(observed, sort_keys=True)
    )


@pytest.mark.parametrize("ddl", ["create", "replace", "add", "drop", "rename"])
def test_task_metadata_is_reread_after_ddl(sql_world, ddl):
    w = sql_world
    dialect = warehouse_dialect(w.config)
    name = w.name("shape")
    with w.warehouse.begin() as conn:
        session = Session(
            conn,
            dialect,
            catalog=w.catalog,
            action="CREATE_TABLE",
            target_object=name,
            task_run_id=1,
            params={},
        )
        try:
            assert session.columns(name) == []
            session.create_table_as(name, "SELECT CAST(1 AS BIGINT) AS id", step="create")
            assert [c.lower() for c, _ in session.columns(name)] == ["id"]
            before = session.column_types(name)
            with count_round_trips(w.engine_db) as counts:
                assert session.columns(name) == session.columns(name)
                assert session.hash_types(name) == session.hash_types(name)
                assert session.not_null_columns(name) == session.not_null_columns(name)
                assert session.column_types(name) == before
            assert counts.counts["warehouse_statements"] == 0
            if ddl in {"create", "replace"}:
                session.drop(name)
                session.create_table_as(name, "SELECT CAST('x' AS VARCHAR(20)) AS value", step=ddl)
                assert [c.lower() for c, _ in session.columns(name)] == ["value"]
                assert session.column_types(name) != before
            elif ddl == "add":
                # Dialect DDL can bypass Session.run; its connection event still invalidates.
                conn.execute(
                    text(f"{dialect.alter_table_keyword()} {name} ADD COLUMN value VARCHAR(20)")
                )
                assert [c.lower() for c, _ in session.columns(name)] == ["id", "value"]
                assert "value" in session.column_types(name)
            elif ddl == "drop":
                session.drop(name)
                assert session.columns(name) == []
            else:
                renamed = w.name("renamed")
                assert session.columns(renamed) == []
                session.rename(name, renamed)
                assert session.columns(name) == []
                assert [c.lower() for c, _ in session.columns(renamed)] == ["id"]
        finally:
            session.sweep()


@warehouses("duckdb")
def test_one_task_reuses_its_engine_but_the_next_task_does_not(sql_world):
    from etl_craft.warehouse.connection import warehouse_task

    with warehouse_task():
        with open_warehouse(sql_world.config) as first, first.begin() as conn:
            conn.execute(text("CREATE TEMPORARY TABLE same_session AS SELECT 1 AS id"))
        with open_warehouse(sql_world.config) as second, second.connect() as conn:
            assert second is first
            assert conn.execute(text("SELECT id FROM same_session")).scalar_one() == 1
    with warehouse_task(), open_warehouse(sql_world.config) as third:
        assert third is not first


@warehouses("duckdb")
def test_a_reused_task_engine_refreshes_an_expired_minted_credential(sql_world, monkeypatch):
    from dataclasses import replace

    from etl_craft.core.enums import AuthMode
    from etl_craft.dialects import credentials
    from etl_craft.dialects.warehouse.duckdb import DuckDBWarehouse
    from etl_craft.warehouse.connection import warehouse_task

    minted = []
    present = DuckDBWarehouse.present

    def mint(self, profile, secret, url):
        minted.append(len(minted) + 1)
        return present(self, replace(profile, auth_mode=AuthMode.NONE), secret, url)

    monkeypatch.setattr(DuckDBWarehouse, "present", mint)
    profile = replace(sql_world.config.warehouse, auth_mode=AuthMode.OAUTH)
    config = replace(sql_world.config, warehouse=profile)
    monkeypatch.setenv(profile.secret_var, "test-mint-secret")
    with warehouse_task():
        with open_warehouse(config) as first, first.connect() as conn:
            assert conn.execute(text("SELECT 1 AS value")).scalar_one() == 1
            record = conn.connection._connection_record
            record.starttime -= credentials.MINTED_CREDENTIAL_POOL_RECYCLE_SECONDS + 1
        with open_warehouse(config) as second, second.connect() as conn:
            assert second is first
            assert conn.execute(text("SELECT 2 AS value")).scalar_one() == 2
        assert minted == [1, 2]


@warehouses("postgres", "duckdb")
def test_query_description_is_exact_or_defers_to_ctas(sql_world):
    w = sql_world
    dialect = warehouse_dialect(w.config)
    with w.warehouse.connect() as conn, count_round_trips(w.engine_db) as counter:
        described = dialect.describe_select(
            conn,
            "SELECT 1 AS id, CAST('Ann' AS VARCHAR(20)) AS name, "
            "CAST(12.34 AS NUMERIC(12,4)) AS amount",
        )
    expected = {
        "postgres": [("id", "INTEGER"), ("name", "varchar(20)"), ("amount", "NUMERIC(12,4)")],
        "duckdb": [("id", "INTEGER"), ("name", "VARCHAR"), ("amount", "DECIMAL(12,4)")],
    }
    assert described == expected[w.kind]
    assert counter.result()["warehouse_statements"] == 1
    collation = '"C"' if w.kind == "postgres" else "NOCASE"
    source = w.name("collated_source")
    w.execute(f"CREATE TABLE {source} (name VARCHAR(20) COLLATE {collation})")
    w.execute(f"INSERT INTO {source} VALUES ('Ann')")
    select = f"SELECT name FROM {source}"
    for action in ("CREATE_TABLE", "SETUP_TABLE"):
        target = action.lower()
        options = {"SETUP_FOR": "APPEND_TABLE"} if action == "SETUP_TABLE" else {}
        w.run(target, SQL_ACTION=action, TARGET_OBJECT=target, SELECT_QUERY=select, **options)
        if action == "SETUP_TABLE":
            w.execute(f"INSERT INTO {w.name(target)} (name) VALUES ('Ann')")
        with w.warehouse.connect() as conn:
            if w.kind == "postgres":
                actual = conn.execute(
                    text(
                        "SELECT attcollation::regcollation::text AS collation FROM pg_attribute "
                        "WHERE attrelid=to_regclass(:name) AND attname='name'"
                    ),
                    {"name": f"{w.schema}.{target}"},
                ).scalar_one()
                assert actual == '"C"'
            else:
                actual = conn.execute(
                    text(f"SELECT COUNT(*) AS matching FROM {w.name(target)} WHERE name='ann'")
                ).scalar_one()
                assert actual == 1
    with w.warehouse.connect() as conn, count_round_trips(w.engine_db) as counter:
        assert dialect.describe_select(conn, select) is None
        assert (
            dialect.describe_select(
                conn, f"SELECT CAST('Ann' AS VARCHAR(20)) COLLATE {collation} AS name"
            )
            is None
        )
    assert counter.result()["warehouse_statements"] == 0


@warehouses("trino_iceberg")
def test_trino_merges_compute_identifiers_in_one_mutation(sql_world):
    w = sql_world
    target = "computed_merge"
    w.setup(target, "SELECT 1 AS id, CAST('old' AS VARCHAR(20)) AS name", "SCD1_MERGE")
    w.execute(
        f"INSERT INTO {w.name(target)} (id,name,hash_key,row_id,active_flag,delete_flag) "
        "VALUES (1,'old','old',1,'Y','N'),(2,'stay','old',2,'Y','N'),"
        "(1,'duplicate','old',5,'Y','N')"
    )
    context = w.task(
        "merge",
        SQL_ACTION="SCD1_MERGE",
        TARGET_OBJECT=target,
        MERGE_KEY="id",
        MERGE_COMPARE_COLUMNS="name",
        SELECT_QUERY="SELECT 1 AS id, CAST('new' AS VARCHAR(20)) AS name "
        "UNION ALL SELECT 3, 'x' UNION ALL SELECT 4, 'y'",
    )
    from etl_craft.handlers import sql

    with count_round_trips(w.engine_db) as counter:
        result = sql.run(context, w.engine_db)
    assert (result.insert_count, result.update_count) == (2, 2)
    with w.warehouse.connect() as conn:
        rows = conn.execute(text(f"SELECT id,name,row_id FROM {w.name(target)}")).mappings().all()
    assert {row["row_id"] for row in rows} == {1, 2, 5, 6, 7}
    assert sorted((row["id"], row["name"]) for row in rows) == [
        (1, "new"),
        (1, "new"),
        (2, "stay"),
        (3, "x"),
        (4, "y"),
    ]
    assert (
        sum(n for query, n in counter.queries.items() if query.startswith("other: MERGE INTO")) == 1
    )
    assert (
        sum(n for query, n in counter.queries.items() if query.startswith("other: INSERT INTO"))
        == 0
    )
    w.execute(f"UPDATE {w.name(target)} SET ROW_ID=9223372036854775807 WHERE id=2")
    matched = w.run(
        "matched",
        SQL_ACTION="SCD1_MERGE",
        TARGET_OBJECT=target,
        MERGE_KEY="id",
        MERGE_COMPARE_COLUMNS="name",
        SELECT_QUERY="SELECT 2 AS id, CAST('changed' AS VARCHAR(20)) AS name",
    )
    assert (matched.insert_count, matched.update_count) == (0, 1)
