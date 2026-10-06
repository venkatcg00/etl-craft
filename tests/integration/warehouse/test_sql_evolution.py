"""ALTER-based evolution retains types, dependencies and the existing table definition."""

import pytest

from etl_craft.core.errors import HandlerError
from etl_craft.warehouse.connection import warehouse_dialect
from fixtures.sql_evolution import check_evolution, check_interrupted_evolution


def test_evolution_retains_full_types_rows_and_keys(sql_world):
    check_evolution(sql_world, "evolved")


def test_interrupted_evolution_preserves_rows_and_retry_completes(sql_world):
    check_interrupted_evolution(sql_world, "interrupted")


def test_full_column_type_retains_modifiers_and_names_missing_column(sql_world):
    w = sql_world
    w.setup("typed", "SELECT CAST(123.45 AS DECIMAL(12,2)) AS amount", "OVERWRITE_TABLE")
    with w.warehouse.connect() as conn:
        dialect = warehouse_dialect(w.config)
        kind = dialect.full_column_type(conn, w.name("typed"), "AMOUNT")
        assert "(12,2)" in kind.replace(" ", "")
        with pytest.raises(HandlerError, match=r"typed.missing: cannot read the complete"):
            dialect.full_column_type(conn, w.name("typed"), "missing")


@pytest.mark.parametrize(
    "sql_world",
    [pytest.param("duckdb_iceberg", marks=pytest.mark.warehouse_duckdb_iceberg)],
    indirect=True,
)
def test_duckdb_iceberg_refuses_nested_additions_before_any_alter(sql_world):
    w = sql_world
    w.setup("people", "SELECT 1 AS id", "OVERWRITE_TABLE")
    params = {"SQL_ACTION": "OVERWRITE_TABLE", "TARGET_OBJECT": "people"}
    w.run("people", SOURCE_SQL="SELECT 1 AS id", **params)
    before = w.rows(f"SELECT * FROM {w.name('people')}")
    columns = w.columns("people")
    with pytest.raises(HandlerError, match=r"cannot add nested type.*such as Trino"):
        w.run(
            "people",
            SOURCE_SQL="SELECT 1 AS id, CAST(1 AS DECIMAL(12,2)) AS scalar_added, "
            "ARRAY[1,2] AS nested_added",
            SCHEMA_EVOLUTION="true",
            **params,
        )
    assert w.columns("people") == columns
    assert w.rows(f"SELECT * FROM {w.name('people')}") == before


@pytest.mark.parametrize(
    "sql_world", [pytest.param("postgres", marks=pytest.mark.warehouse_postgres)], indirect=True
)
def test_postgres_evolution_preserves_dependent_view_index_comments_and_identity(sql_world):
    w = sql_world
    shape = "SELECT 1 AS id, CAST('Ann' AS VARCHAR(20)) AS name"
    w.setup("people", shape, "SCD1_MERGE")
    target = w.name("people")
    params = {
        "SQL_ACTION": "SCD1_MERGE",
        "TARGET_OBJECT": "people",
        "MERGE_KEY": "id",
        "MERGE_COMPARE_COLUMNS": "name",
    }
    w.run("people", SOURCE_SQL=shape, **params)
    w.execute(f"CREATE VIEW {w.name('people_view')} AS SELECT id, row_id FROM {target}")
    w.execute(f"CREATE INDEX people_name_idx ON {target} (name)")
    w.execute(f"COMMENT ON TABLE {target} IS 'people table'")
    w.execute(f"COMMENT ON COLUMN {target}.name IS 'person name'")
    before = w.rows(f"SELECT id, row_id FROM {w.name('people_view')}")
    source = f"SELECT id, name, CAST(123.45 AS DECIMAL(12,2)) AS amount FROM {target} WHERE 1 = 0"
    w.run("people", SOURCE_SQL=source, SCHEMA_EVOLUTION="true", **params)
    assert w.rows(f"SELECT id, row_id FROM {w.name('people_view')}") == before
    assert set(
        w.rows(
            "SELECT indexname FROM pg_indexes WHERE schemaname = :schema AND tablename = 'people'",
            schema=w.schema,
        )
    ) == {("people_pkey",), ("people_name_idx",)}
    assert w.rows(
        "SELECT obj_description(CAST(:table AS regclass))", table=f"{w.schema}.people"
    ) == [("people table",)]
    assert w.rows(
        "SELECT col_description(attrelid, attnum) FROM pg_attribute "
        "WHERE attrelid = CAST(:table AS regclass) AND attname = 'name'",
        table=f"{w.schema}.people",
    ) == [("person name",)]
    w.run(
        "people",
        SOURCE_SQL="SELECT 2 AS id, CAST('Bo' AS VARCHAR(20)) AS name, "
        "CAST(12.34 AS DECIMAL(12,2)) AS amount",
        **params,
    )
    assert sorted(w.rows(f"SELECT row_id FROM {target}")) == [(1,), (2,)]


@pytest.mark.parametrize(
    "sql_world",
    [pytest.param("trino_iceberg", marks=pytest.mark.warehouse_trino_iceberg)],
    indirect=True,
)
def test_trino_evolution_preserves_partitioning_comments_and_snapshot_history(sql_world):
    w = sql_world
    shape = "SELECT 1 AS id, CAST('Ann' AS VARCHAR(20)) AS name"
    w.setup("people", shape, "SCD1_MERGE")
    target = w.name("people")
    prepared = w.name("partitioned")
    w.execute(
        f"CREATE TABLE {prepared} WITH (partitioning = ARRAY['id']) AS SELECT * FROM {target}"
    )
    w.execute(f"DROP TABLE {target}")
    w.execute(f"ALTER TABLE {prepared} RENAME TO {target}")
    w.execute(f"COMMENT ON TABLE {target} IS 'people table'")
    w.execute(f"COMMENT ON COLUMN {target}.name IS 'person name'")
    params = {
        "SQL_ACTION": "SCD1_MERGE",
        "TARGET_OBJECT": "people",
        "MERGE_KEY": "id",
        "MERGE_COMPARE_COLUMNS": "name",
    }
    w.run("people", SOURCE_SQL=shape, **params)
    snapshots = f'{w.catalog}.{w.schema}."people$snapshots"'
    before = {row[0] for row in w.rows(f"SELECT snapshot_id FROM {snapshots}")}
    assert before
    populated_snapshot = w.rows(f"SELECT snapshot_id FROM {snapshots} ORDER BY committed_at DESC")[
        0
    ][0]
    source = f"SELECT id, name, CAST(123.45 AS DECIMAL(12,2)) AS amount FROM {target} WHERE 1 = 0"
    w.run("people", SOURCE_SQL=source, SCHEMA_EVOLUTION="true", **params)
    ddl = w.rows(f"SHOW CREATE TABLE {target}")[0][0]
    assert "partitioning = ARRAY['id']" in ddl
    assert "people table" in ddl and "person name" in ddl
    assert before <= {row[0] for row in w.rows(f"SELECT snapshot_id FROM {snapshots}")}
    snapshot = populated_snapshot
    assert w.rows(f"SELECT id, name FROM {target} FOR VERSION AS OF {snapshot}") == [(1, "Ann")]
