"""The Warehouse profile's schema, where cloning writes, must already exist."""

from dataclasses import replace

from etl_craft.config import ConnectionSection
from etl_craft.execution.connections import probe_warehouse
from etl_craft.warehouse.connection import warehouse_schema_problem


def with_schema(world, schema):
    profile = replace(world.config.warehouse.active, schema=schema)
    return replace(world.config, warehouse=ConnectionSection("dev", {"dev": profile}))


def test_an_existing_schema_passes_and_a_missing_one_is_named(sql_world):
    w = sql_world
    assert warehouse_schema_problem(with_schema(w, w.schema.upper()), w.warehouse) is None
    missing = with_schema(w, "no_such_schema")
    assert warehouse_schema_problem(missing, w.warehouse) == (
        f"the Warehouse schema {w.catalog}.no_such_schema does not exist; create it first — "
        "etl-craft does not create warehouse schemas"
    )
    assert "no_such_schema does not exist" in probe_warehouse(missing, w.engine_db, schema=True)
    if w.kind == "duckdb_iceberg":
        # DuckDB over Iceberg works in the profile's schema, so it cannot connect without it.
        assert "no_such_schema does not exist" in probe_warehouse(missing, w.engine_db)
    else:
        assert probe_warehouse(missing, w.engine_db) is None
    assert warehouse_schema_problem(with_schema(w, ""), w.warehouse) == (
        "the Warehouse profile names no schema"
    )
