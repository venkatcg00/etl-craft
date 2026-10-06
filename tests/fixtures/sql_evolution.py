"""Schema evolution checks shared by local and live warehouses."""

from decimal import Decimal

import pytest

from etl_craft.core.errors import HandlerError
from etl_craft.handlers.sql.session import Session
from etl_craft.warehouse.connection import warehouse_dialect


def types(w, table):
    """Read complete types using the warehouse's metadata contract."""
    with w.warehouse.connect() as conn:
        return warehouse_dialect(w.config).full_column_types(conn, w.name(table))


def check_evolution(w, target):
    """Add exact types, retain old rows and keys, and refuse type changes before DDL."""
    dialect = warehouse_dialect(w.config)
    snowflake = dialect.key.startswith("snowflake")
    array = (
        "CAST(ARRAY_CONSTRUCT(1, 2) AS ARRAY(INTEGER))"
        if snowflake
        else "ARRAY(1, 2)"
        if dialect.key.startswith("databricks")
        else "ARRAY[1, 2]"
    )
    timestamp = (
        "TIMESTAMP_NTZ(6)"
        if snowflake
        else "TIMESTAMP"
        if dialect.key.startswith("databricks")
        else "TIMESTAMP(6)"
    )
    shape = "SELECT CAST(1 AS BIGINT) AS id, CAST('Ann' AS VARCHAR(20)) AS name"
    w.setup(target, shape, "SCD1_MERGE")
    params = {
        "SQL_ACTION": "SCD1_MERGE",
        "TARGET_OBJECT": target,
        "MERGE_KEY": "id",
        "MERGE_COMPARE_COLUMNS": "name",
    }
    w.run(target, SOURCE_SQL=shape, **params)
    before = types(w, target)
    key = w.rows(f"SELECT id, row_id FROM {w.name(target)}")
    has_arrays = dialect.key != "duckdb_iceberg"
    array_column = f", {array} AS numbers" if has_arrays else ""
    new_columns = ["amount", "code", "label", "happened_at"]
    if has_arrays:
        new_columns.insert(3, "numbers")
    extra = (
        "CAST(123.45 AS DECIMAL(12,2)) AS amount, CAST('abc' AS CHAR(3)) AS code, "
        f"CAST('label' AS VARCHAR(20)) AS label{array_column}, "
        f"CAST('2026-10-06 12:34:56.123456' AS {timestamp}) AS happened_at"
    )
    source = f"SELECT id, name, {extra} FROM {w.name(target)} WHERE 1 = 0"
    w.run(target, SOURCE_SQL=source, SCHEMA_EVOLUTION="true", **params)
    after = types(w, target)
    assert {name: after[name] for name in before} == before
    assert w.rows(f"SELECT id, row_id FROM {w.name(target)}") == key
    assert w.rows(f"SELECT {', '.join(new_columns)} FROM {w.name(target)}") == [
        tuple(None for _ in new_columns)
    ]
    assert "12" in after["amount"] and "2" in after["amount"]
    if has_arrays:
        assert "ARRAY" in after["numbers"].upper() or "[]" in after["numbers"]
    if dialect.key == "postgres":
        assert after["code"] == "character(3)"
        assert after["label"] == "character varying(20)"
        assert after["happened_at"] == "timestamp(6) without time zone"
    elif dialect.key == "snowflake":
        assert after["code"] == "VARCHAR(3)"
        assert after["label"] == "VARCHAR(20)"
        assert after["happened_at"] == "TIMESTAMP_NTZ(6)"
    # Read existing columns from the target, preserving its warehouse-reported types on retry.
    retry = f"SELECT id, name, {', '.join(new_columns)} FROM {w.name(target)} WHERE 1 = 0"
    w.run(target, SOURCE_SQL=retry, SCHEMA_EVOLUTION="true", **params)
    assert types(w, target) == after
    bad = retry.replace("amount,", "CAST(amount AS DECIMAL(12,3)) AS amount,").replace(
        "happened_at ", "happened_at, CAST(1 AS INTEGER) AS refused "
    )
    with pytest.raises(HandlerError, match="cannot change the existing type") as error:
        w.run(target, SOURCE_SQL=bad, SCHEMA_EVOLUTION="true", **params)
    assert "12,2" in str(error.value).replace(" ", "")
    assert "12,3" in str(error.value).replace(" ", "")
    assert types(w, target) == after
    assert w.rows(f"SELECT id, row_id FROM {w.name(target)}") == key
    listed = ", ".join(new_columns).replace("amount,", "CAST(123.45 AS DECIMAL(12,2)) AS amount,")
    write = f"SELECT id, name, {listed} FROM {w.name(target)}"
    w.run(
        target,
        SOURCE_SQL=write,
        MERGE_COMPARE_COLUMNS="name|amount",
        **{name: value for name, value in params.items() if name != "MERGE_COMPARE_COLUMNS"},
    )
    assert w.rows(f"SELECT amount FROM {w.name(target)}") == [(Decimal("123.45"),)]


def check_interrupted_evolution(w, target):
    """A failed addition preserves rows; a retry completes from the persisted column set."""
    shape = "SELECT CAST(1 AS BIGINT) AS id, CAST('Ann' AS VARCHAR(20)) AS name"
    w.setup(target, shape, "SCD1_MERGE")
    params = {
        "SQL_ACTION": "SCD1_MERGE",
        "TARGET_OBJECT": target,
        "MERGE_KEY": "id",
        "MERGE_COMPARE_COLUMNS": "name",
    }
    w.run(target, SOURCE_SQL=shape, **params)
    before = w.rows(f"SELECT * FROM {w.name(target)}")
    source = (
        f"SELECT id, name, CAST(1 AS DECIMAL(12,2)) AS first_added, "
        f"CAST(2 AS BIGINT) AS second_added FROM {w.name(target)} WHERE 1 = 0"
    )
    with pytest.MonkeyPatch.context() as patch:
        original = Session.run

        def fail_second(session, sql, params=None, *, step):
            if step.lower().startswith("add column second_added"):
                raise HandlerError("column addition unavailable")
            return original(session, sql, params, step=step)

        patch.setattr(Session, "run", fail_second)
        with pytest.raises(HandlerError, match="column addition unavailable"):
            w.run(target, SOURCE_SQL=source, SCHEMA_EVOLUTION="true", **params)
    columns = w.columns(target)
    assert "second_added" not in columns
    original_columns = [name for name in columns if name != "first_added"]
    assert w.rows(f"SELECT {', '.join(original_columns)} FROM {w.name(target)}") == before
    transactional = warehouse_dialect(w.config).replace_strategy == "transactional"
    assert ("first_added" in columns) != transactional
    w.run(target, SOURCE_SQL=source, SCHEMA_EVOLUTION="true", **params)
    assert {"first_added", "second_added"} <= set(w.columns(target))
