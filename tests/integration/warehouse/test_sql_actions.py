"""The seven SQL actions on real warehouses: DuckDB, PostgreSQL, Trino, DuckDB over Iceberg."""

from dataclasses import replace
from datetime import date

import pytest

from etl_craft.core.errors import HandlerError
from etl_craft.handlers import sql


def sorted_rows(world, sql):
    return sorted(world.rows(sql))


def test_create_table_stamps_the_run_and_numbers_the_rows(sql_world):
    w = sql_world
    result = w.run(
        "create",
        SQL_ACTION="CREATE_TABLE",
        TARGET_OBJECT="orders",
        SOURCE_SQL="SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2, 'b'",
    )
    assert (result.source_count, result.target_count, result.insert_count) == (2, 2, 2)
    assert w.columns("orders") == ["id", "name", "pipeline_run_id", "row_id"]
    assert sorted_rows(w, f"SELECT id, name, pipeline_run_id FROM {w.name('orders')}") == [
        (1, "a", w.pipeline_run_id),
        (2, "b", w.pipeline_run_id),
    ]
    assert sorted_rows(w, f"SELECT row_id FROM {w.name('orders')}") == [(1,), (2,)]
    # The stage is gone.
    assert w.tables() == ["orders"]


def test_overwrite_table_needs_its_target_then_replaces_the_rows(sql_world):
    w = sql_world
    params = {"SQL_ACTION": "OVERWRITE_TABLE", "TARGET_OBJECT": "daily"}
    with pytest.raises(HandlerError, match=r"does not exist\. Only CREATE_TABLE and SETUP_TABLE"):
        w.run("overwrite", SOURCE_SQL="SELECT 1 AS id", **params)
    w.setup("daily", "SELECT 1 AS id", "OVERWRITE_TABLE")
    assert w.columns("daily") == ["id", "pipeline_run_id", "update_date", "row_id"]
    first = w.run("overwrite", SOURCE_SQL="SELECT 1 AS id UNION ALL SELECT 2", **params)
    assert first.insert_count == 2
    second = w.run("overwrite", SOURCE_SQL="SELECT 3 AS id", **params)
    assert (second.source_count, second.target_count) == (1, 1)
    assert w.rows(f"SELECT id FROM {w.name('daily')}") == [(3,)]
    row_ids = w.rows(f"SELECT row_id, update_date FROM {w.name('daily')}")
    assert row_ids[0][0] is not None and row_ids[0][1] is not None


def test_append_table_adds_rows_on_every_run(sql_world):
    w = sql_world
    params = {"SQL_ACTION": "APPEND_TABLE", "TARGET_OBJECT": "events"}
    with pytest.raises(HandlerError, match=r"the target .* does not exist"):
        w.run("append", SOURCE_SQL="SELECT 1 AS id", **params)
    w.setup("events", "SELECT 1 AS id", "APPEND_TABLE")
    assert w.columns("events") == ["id", "pipeline_run_id", "create_date", "row_id"]
    first = w.run("append", SOURCE_SQL="SELECT 1 AS id UNION ALL SELECT 2", **params)
    w.new_run()
    second = w.run("append", SOURCE_SQL="SELECT 3 AS id", **params)
    assert (first.insert_count, second.insert_count, second.target_count) == (2, 1, 3)
    rows = sorted_rows(w, f"SELECT id, pipeline_run_id FROM {w.name('events')}")
    assert rows == [(1, w.pipeline_run_id - 1), (2, w.pipeline_run_id - 1), (3, w.pipeline_run_id)]
    assert sorted_rows(w, f"SELECT row_id FROM {w.name('events')}") == [(1,), (2,), (3,)]
    # No shape check: a column the target lacks fails the insert with the database's message.
    with pytest.raises(HandlerError, match="append the SELECT's rows failed"):
        w.run("append", SOURCE_SQL="SELECT 4 AS id, 'x' AS extra", **params)


SCD = {"TARGET_OBJECT": "customers", "MERGE_KEY": "id", "MERGE_COMPARE_COLUMNS": "name|city"}
SHAPE = "SELECT 1 AS id, CAST('x' AS VARCHAR(20)) AS name, CAST('x' AS VARCHAR(20)) AS city"


@pytest.fixture
def customers(sql_world):
    """The customers table, set up for a merge by the action named in the test's parameter."""

    def setup(action):
        sql_world.setup("customers", SHAPE, action)
        return sql_world

    return setup


def test_scd1_merge_inserts_updates_and_leaves_unchanged_rows(customers):
    w = customers("SCD1_MERGE")
    first = w.run(
        "scd1",
        SQL_ACTION="SCD1_MERGE",
        SOURCE_SQL="SELECT 1 AS id, 'Ann' AS name, 'Oslo' AS city UNION ALL SELECT 2, 'Bo', 'Rome'",
        **SCD,
    )
    assert (first.insert_count, first.update_count, first.target_count) == (2, 0, 2)
    w.new_run()
    second = w.run(
        "scd1",
        SQL_ACTION="SCD1_MERGE",
        SOURCE_SQL="SELECT 1 AS id, 'Ann' AS name, 'Oslo' AS city "
        "UNION ALL SELECT 2, 'Bo', 'Lima' UNION ALL SELECT 3, 'Cy', NULL",
        **SCD,
    )
    assert (second.source_count, second.insert_count, second.update_count) == (3, 1, 1)
    rows = sorted_rows(
        w, f"SELECT id, city, pipeline_run_id, delete_flag FROM {w.name('customers')}"
    )
    run1, run2 = w.pipeline_run_id - 1, w.pipeline_run_id
    assert rows == [(1, "Oslo", run1, "N"), (2, "Lima", run2, "N"), (3, None, run2, "N")]
    # Unchanged input changes nothing.
    w.new_run()
    third = w.run(
        "scd1",
        SQL_ACTION="SCD1_MERGE",
        SOURCE_SQL="SELECT 1 AS id, 'Ann' AS name, 'Oslo' AS city",
        **SCD,
    )
    assert (third.insert_count, third.update_count, third.target_count) == (0, 0, 3)


def test_scd1_preserve_target_keeps_values_where_the_source_is_null(customers):
    w = customers("SCD1_MERGE")
    params = {"SQL_ACTION": "SCD1_MERGE", "PRESERVE_TARGET": "true", **SCD}
    w.run("keep", SOURCE_SQL="SELECT 1 AS id, 'Ann' AS name, 'Oslo' AS city", **params)
    changed = w.run(
        "keep", SOURCE_SQL="SELECT 1 AS id, CAST(NULL AS VARCHAR) AS name, 'Rome' AS city", **params
    )
    assert changed.update_count == 1
    assert w.rows(f"SELECT name, city FROM {w.name('customers')}") == [("Ann", "Rome")]
    again = w.run(
        "keep", SOURCE_SQL="SELECT 1 AS id, CAST(NULL AS VARCHAR) AS name, 'Rome' AS city", **params
    )
    assert again.update_count == 0


def test_scd2_merge_keeps_history(customers):
    w = customers("SCD2_MERGE")
    params = {"SQL_ACTION": "SCD2_MERGE", **SCD}
    w.run("scd2", SOURCE_SQL="SELECT 1 AS id, 'Ann' AS name, 'Oslo' AS city", **params)
    changed = w.run(
        "scd2",
        SOURCE_SQL="SELECT 1 AS id, 'Ann' AS name, 'Rome' AS city UNION ALL SELECT 2, 'Bo', 'Lima'",
        **params,
    )
    assert (changed.update_count, changed.insert_count, changed.target_count) == (1, 2, 3)
    assert sorted_rows(w, f"SELECT id, city, active_flag FROM {w.name('customers')}") == [
        (1, "Oslo", "N"),
        (1, "Rome", "Y"),
        (2, "Lima", "Y"),
    ]
    # A key left with no active version, as an interrupted run would leave it, gets one again.
    w.execute(f"UPDATE {w.name('customers')} SET active_flag = 'N' WHERE id = 2")
    healed = w.run(
        "scd2",
        SOURCE_SQL="SELECT 1 AS id, 'Ann' AS name, 'Rome' AS city UNION ALL SELECT 2, 'Bo', 'Lima'",
        **params,
    )
    assert (healed.update_count, healed.insert_count) == (0, 1)
    assert w.rows(
        f"SELECT COUNT(*) FROM {w.name('customers')} WHERE id = 2 AND active_flag = 'Y'"
    ) == [(1,)]


def test_duplicate_merge_keys_fail_unless_an_order_chooses(customers):
    w = customers("SCD1_MERGE")
    source = "SELECT 1 AS id, 'old' AS name, 'x' AS city UNION ALL SELECT 1, 'new', 'y'"
    with pytest.raises(HandlerError, match=r"returns 1 MERGE_KEY value\(s\) \(id\) more than once"):
        w.run("dupes", SQL_ACTION="SCD1_MERGE", SOURCE_SQL=source, **SCD)
    assert w.rows(f"SELECT COUNT(*) FROM {w.name('customers')}") == [(0,)]
    result = w.run(
        "dupes", SQL_ACTION="SCD1_MERGE", SOURCE_SQL=source, MERGE_DEDUPE_ORDER="city DESC", **SCD
    )
    assert (result.source_count, result.insert_count) == (2, 1)
    assert w.rows(f"SELECT name FROM {w.name('customers')}") == [("new",)]


def test_schema_checks_and_evolution(sql_world):
    w = sql_world
    params = {"SQL_ACTION": "OVERWRITE_TABLE", "TARGET_OBJECT": "wide"}
    w.setup("wide", "SELECT 1 AS id, CAST('a' AS VARCHAR(10)) AS name", "OVERWRITE_TABLE")
    w.run("evolve", SOURCE_SQL="SELECT 1 AS id, 'a' AS name", **params)
    with pytest.raises(HandlerError, match=r"new column\(s\) score .* set SCHEMA_EVOLUTION=true"):
        w.run("evolve", SOURCE_SQL="SELECT 1 AS id, 7 AS score, 'a' AS name", **params)
    with pytest.raises(HandlerError, match=r"has column\(s\) name that the SELECT no longer"):
        w.run("evolve", SOURCE_SQL="SELECT 1 AS id", **params)
    w.run(
        "evolve",
        SOURCE_SQL="SELECT 2 AS id, 7 AS score, 'b' AS name",
        SCHEMA_EVOLUTION="true",
        **params,
    )
    assert w.columns("wide") == ["id", "score", "name", "pipeline_run_id", "update_date", "row_id"]
    assert w.rows(f"SELECT id, score, name FROM {w.name('wide')}") == [(2, 7, "b")]


def test_a_target_missing_audit_columns_is_refused(sql_world):
    w = sql_world
    w.execute(f"CREATE TABLE {w.name('bare')} AS SELECT 1 AS id, 'a' AS name, 'b' AS city")
    with pytest.raises(HandlerError, match="lacks PIPELINE_RUN_ID, HASH_KEY, CREATE_DATE"):
        w.run(
            "bare",
            SQL_ACTION="SCD1_MERGE",
            SOURCE_SQL="SELECT 1 AS id, 'a' AS name, 'b' AS city",
            TARGET_OBJECT="bare",
            MERGE_KEY="id",
            MERGE_COMPARE_COLUMNS="name",
        )


def test_delete_rows_soft_and_hard(customers):
    w = customers("SCD1_MERGE")
    w.run(
        "load",
        SQL_ACTION="SCD1_MERGE",
        SOURCE_SQL="SELECT 1 AS id, 'a' AS name, 'x' AS city UNION ALL SELECT 2, 'b', 'y' "
        "UNION ALL SELECT 3, 'c', 'z'",
        **SCD,
    )
    soft = w.run(
        "soft",
        SQL_ACTION="DELETE_ROWS",
        TARGET_OBJECT="customers",
        MERGE_KEY="id",
        SOURCE_SQL="SELECT 1 AS id",
    )
    assert (soft.source_count, soft.delete_count) == (1, 1)
    assert sorted_rows(w, f"SELECT id, delete_flag FROM {w.name('customers')}") == [
        (1, "Y"),
        (2, "N"),
        (3, "N"),
    ]
    hard = w.run(
        "hard",
        SQL_ACTION="DELETE_ROWS",
        TARGET_OBJECT="customers",
        MERGE_KEY="id",
        HARD_DELETE="true",
        SOURCE_SQL="SELECT 2 AS id UNION ALL SELECT 9",
    )
    assert hard.delete_count == 1
    assert sorted_rows(w, f"SELECT id FROM {w.name('customers')}") == [(1,), (3,)]


def test_drop_table_needs_this_pipelines_create_table_to_have_run(sql_world):
    w = sql_world
    with pytest.raises(HandlerError, match="no other active task in this pipeline creates it"):
        w.run("drop", SQL_ACTION="DROP_TABLE", TARGET_OBJECT="scratch")
    w.task("make", SQL_ACTION="CREATE_TABLE", TARGET_OBJECT="scratch", SOURCE_SQL="SELECT 1 AS id")
    with pytest.raises(HandlerError, match=r"is IN-PROGRESS under pipeline_run_id=.*not SUCCESS"):
        w.run("drop", SQL_ACTION="DROP_TABLE", TARGET_OBJECT="scratch")
    w.run("make", SQL_ACTION="CREATE_TABLE", TARGET_OBJECT="scratch", SOURCE_SQL="SELECT 1 AS id")
    w.finish("make")
    w.run("drop", SQL_ACTION="DROP_TABLE", TARGET_OBJECT="scratch")
    assert "scratch" not in w.tables()
    # Dropping it again finds nothing to drop, which is fine.
    w.run("drop", SQL_ACTION="DROP_TABLE", TARGET_OBJECT="scratch")


def test_setup_table_takes_the_audit_columns_of_the_real_writer(sql_world):
    w = sql_world
    select = "SELECT 1 AS id, 'a' AS name, 'b' AS city"
    setup = {"SQL_ACTION": "SETUP_TABLE", "TARGET_OBJECT": "customers", "SOURCE_SQL": select}
    with pytest.raises(HandlerError, match=r"no task in this pipeline writes .* set SETUP_FOR"):
        w.run("setup", **setup)
    w.task("writer", SQL_ACTION="SCD2_MERGE", SOURCE_SQL="SELECT 1 AS id", **SCD)
    w.task("remover", SQL_ACTION="DELETE_ROWS", TARGET_OBJECT="customers", MERGE_KEY="id")
    result = w.run("setup", **setup)
    assert result.insert_count == 0
    assert w.columns("customers") == [
        "id",
        "name",
        "city",
        "pipeline_run_id",
        "hash_key",
        "create_date",
        "created_by",
        "update_date",
        "updated_by",
        "delete_flag",
        "active_flag",
        "row_id",
    ]
    # An existing table is left as it is, rows and all.
    w.run("writer", SQL_ACTION="SCD2_MERGE", SOURCE_SQL=select, **SCD)
    w.run("setup", **setup)
    assert w.rows(f"SELECT COUNT(*) FROM {w.name('customers')}") == [(1,)]


def test_setup_table_refuses_writers_that_need_different_audit_columns(sql_world):
    w = sql_world
    w.task("merge", SQL_ACTION="SCD1_MERGE", SOURCE_SQL="SELECT 1 AS id", **SCD)
    w.task("append", SQL_ACTION="APPEND_TABLE", TARGET_OBJECT="customers", SOURCE_SQL="SELECT 1")
    setup = {
        "SQL_ACTION": "SETUP_TABLE",
        "TARGET_OBJECT": "customers",
        "SOURCE_SQL": "SELECT 1 AS id",
    }
    with pytest.raises(HandlerError, match=r"merge \(SCD1_MERGE\), append \(APPEND_TABLE\)"):
        w.run("setup", **setup)
    with pytest.raises(HandlerError, match="SETUP_FOR=SCD1_MERGE, but tasks in this pipeline"):
        w.run("setup", SETUP_FOR="SCD1_MERGE", **setup)
    assert "customers" not in w.tables()


def test_a_sql_file_with_the_pipeline_id_filter_reads_this_runs_rows(sql_world):
    w = sql_world
    w.run("seed", SQL_ACTION="CREATE_TABLE", TARGET_OBJECT="events", SOURCE_SQL="SELECT 1 AS id")
    other_run = w.pipeline_run_id
    w.new_run()
    w.execute(
        f"INSERT INTO {w.name('events')} (id, pipeline_run_id) VALUES (2, {w.pipeline_run_id})"
    )
    sql_file = w.project / "sql_files" / "loads" / "events.sql"
    sql_file.parent.mkdir(parents=True)
    sql_file.write_text(
        f"-- this run's events\nSELECT id, $$pipeline_id AS loaded_by\nFROM {w.name('events')}\n"
        "WHERE $$pipeline_id_filter;\n",
        encoding="utf-8",
    )
    result = w.run(
        "incremental",
        SQL_ACTION="CREATE_TABLE",
        TARGET_OBJECT="loaded",
        SOURCE_SQL_FILE="loads/events.sql",
        PIPELINE_ID_SUBSTITUTION="true",
        PIPELINE_ID_FILTER="true",
    )
    assert result.source_count == 1
    assert w.rows(f"SELECT id, loaded_by FROM {w.name('loaded')}") == [(2, w.pipeline_run_id)]
    assert other_run != w.pipeline_run_id
    # A FULL refresh reads every row.
    w.refresh_type = "FULL"
    full = w.run(
        "incremental",
        SQL_ACTION="CREATE_TABLE",
        TARGET_OBJECT="loaded",
        SOURCE_SQL_FILE="loads/events.sql",
        PIPELINE_ID_SUBSTITUTION="true",
        PIPELINE_ID_FILTER="true",
    )
    assert full.source_count == 2


def test_a_select_names_its_tables_as_schema_table(sql_world):
    # The environment's database comes from the Warehouse profile; SQL never names it.
    w = sql_world
    w.execute(f"CREATE TABLE {w.name('source')} AS SELECT 7 AS id")
    result = w.run(
        "two_part",
        SQL_ACTION="CREATE_TABLE",
        TARGET_OBJECT="copied",
        SOURCE_SQL=f"SELECT id FROM {w.schema}.source",
    )
    assert result.insert_count == 1
    assert w.rows(f"SELECT id FROM {w.schema}.copied") == [(7,)]


def test_a_target_may_name_its_database(sql_world):
    w = sql_world
    target = w.name("named")
    select = "SELECT 1 AS id, CAST('a' AS VARCHAR(20)) AS name"
    w.run(
        "setup_named",
        SQL_ACTION="SETUP_TABLE",
        TARGET_OBJECT=target,
        SOURCE_SQL=select,
        SETUP_FOR="SCD1_MERGE",
    )
    result = w.run(
        "merge_named",
        SQL_ACTION="SCD1_MERGE",
        TARGET_OBJECT=target,
        SOURCE_SQL=select,
        MERGE_KEY="id",
        MERGE_COMPARE_COLUMNS="name",
    )
    assert (result.insert_count, result.target_count) == (1, 1)
    assert w.rows(f"SELECT id, name FROM {target}") == [(1, "a")]


def test_a_failing_statement_names_its_step_and_leaves_no_scratch_tables(sql_world, caplog):
    w = sql_world
    with pytest.raises(HandlerError) as error:
        w.run(
            "broken",
            SQL_ACTION="OVERWRITE_TABLE",
            TARGET_OBJECT="broken",
            SOURCE_SQL=f"SELECT id FROM {w.name('does_not_exist')}",
        )
    assert str(error.value).startswith(
        f"OVERWRITE_TABLE {w.name('broken')}: stage the SELECT failed: "
    )
    assert "stage the SELECT failed; the statement was:" in caplog.text
    assert not [t for t in w.tables() if t.startswith("etl_")]


def test_a_storage_location_is_used_where_it_applies_and_refused_elsewhere(sql_world):
    w = sql_world
    location = f"s3://warehouse/custom/{w.schema}/located"
    params = {
        "SQL_ACTION": "CREATE_TABLE",
        "TARGET_OBJECT": "located",
        "SOURCE_SQL": "SELECT 1 AS id",
        "EXTERNAL_LOCATION": location,
    }
    if w.kind != "trino_iceberg":
        with pytest.raises(HandlerError, match=r"EXTERNAL_LOCATION does not apply to .* ignored"):
            w.run("located", **params)
        assert "located" not in w.tables()
        return
    w.run("located", **params)
    ddl = w.rows(f"SHOW CREATE TABLE {w.name('located')}")[0][0]
    assert f"location = '{location}'" in ddl
    # Schema evolution rebuilds the table, which would lose its location: refused.
    placed = {"EXTERNAL_LOCATION": f"s3://warehouse/custom/{w.schema}/placed"}
    w.run(
        "setup_placed",
        SQL_ACTION="SETUP_TABLE",
        TARGET_OBJECT="placed",
        SOURCE_SQL="SELECT 1 AS id",
        SETUP_FOR="OVERWRITE_TABLE",
        **placed,
    )
    with pytest.raises(HandlerError, match=r"cannot keep its EXTERNAL_LOCATION .* ADD COLUMN"):
        w.run(
            "evolve_placed",
            SQL_ACTION="OVERWRITE_TABLE",
            TARGET_OBJECT="placed",
            SOURCE_SQL="SELECT 1 AS id, 2 AS extra",
            SCHEMA_EVOLUTION="true",
            **placed,
        )


def test_run_date_reads_the_rows_of_the_date_the_run_runs_as_of(sql_world):
    w = sql_world
    w.execute(
        f"CREATE TABLE {w.name('sales')} AS SELECT 1 AS id, DATE '2026-08-31' AS sold_on "
        "UNION ALL SELECT 2, DATE '2026-09-01'"
    )
    context = w.task(
        "daily",
        SQL_ACTION="CREATE_TABLE",
        TARGET_OBJECT="daily",
        SOURCE_SQL=f"SELECT id, $$run_date AS as_of FROM {w.name('sales')} "
        "WHERE sold_on = $$run_date",
        RUN_DATE_SUBSTITUTION="true",
    )
    result = sql.run(replace(context, run_date=date(2026, 9, 1)), w.engine_db)
    assert result.source_count == 1
    assert [(i, str(d)[:10]) for i, d in w.rows(f"SELECT id, as_of FROM {w.name('daily')}")] == [
        (2, "2026-09-01")
    ]
