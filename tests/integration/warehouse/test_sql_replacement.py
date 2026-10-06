"""Replacement failures retain old rows and definitions on every local warehouse."""

import pytest

from fixtures.sql_replacement import check_replacement_failure


@pytest.mark.parametrize("action", ["CREATE_TABLE", "OVERWRITE_TABLE"])
def test_replacement_faults_and_write_errors_retain_the_original(sql_world, action):
    check_replacement_failure(sql_world, "daily", action)


@pytest.mark.parametrize("action", ["CREATE_TABLE", "OVERWRITE_TABLE"])
def test_failed_restore_leaves_a_durable_backup(sql_world, action, monkeypatch):
    from etl_craft.core.errors import HandlerError
    from etl_craft.handlers.sql.session import Session

    w = sql_world
    if w.kind != "duckdb_iceberg":
        pytest.skip("durable recovery applies to the copy-and-restore warehouse")
    if action == "OVERWRITE_TABLE":
        w.setup("daily", "SELECT 1 AS id", action)
    params = {"SQL_ACTION": action, "TARGET_OBJECT": "daily"}
    w.run("daily", SOURCE_SQL="SELECT 1 AS id", **params)
    before = w.rows(f"SELECT * FROM {w.name('daily')}")
    original_run = Session.run
    original_rename = Session.rename

    def refuse_restore(session, sql, params=None, *, step):
        if step == "restore original rows":
            raise HandlerError("restore write unavailable")
        return original_run(session, sql, params, step=step)

    def refuse_rename(session, source, target):
        if "__etl_keep_" in source:
            raise HandlerError("restore rename unavailable")
        return original_rename(session, source, target)

    with monkeypatch.context() as patch:
        patch.setenv("ETL_CRAFT_FAULT", "sql.replace.after_publish")
        patch.setattr(Session, "run", refuse_restore)
        patch.setattr(Session, "rename", refuse_rename)
        with pytest.raises(HandlerError, match=r"restoration failed.*retained at.*Restore"):
            w.run("daily", SOURCE_SQL="SELECT 2 AS id", **params)
    keep = [table for table in w.tables() if "__etl_keep_" in table]
    assert len(keep) == 1
    assert w.rows(f"SELECT * FROM {w.name(keep[0])}") == before


def test_cleanup_failure_cannot_undo_nontransactional_publication(sql_world, monkeypatch):
    from etl_craft.core.errors import HandlerError
    from etl_craft.handlers.sql.session import Session
    from etl_craft.warehouse.connection import warehouse_dialect

    w = sql_world
    if warehouse_dialect(w.config).replace_strategy == "transactional":
        pytest.skip("cleanup remains inside the transaction on native warehouses")
    params = {"SQL_ACTION": "CREATE_TABLE", "TARGET_OBJECT": "daily"}
    w.run("daily", SOURCE_SQL="SELECT 1 AS id", **params)
    original = Session.drop

    def unavailable_cleanup(session, name):
        if "etl_stage_" in name or "__etl_keep_" in name:
            raise HandlerError("cleanup unavailable")
        return original(session, name)

    with monkeypatch.context() as patch:
        patch.setattr(Session, "drop", unavailable_cleanup)
        result = w.run("daily", SOURCE_SQL="SELECT 2 AS id", **params)
    assert result.insert_count == 1
    assert w.rows(f"SELECT id FROM {w.name('daily')}") == [(2,)]


@pytest.mark.parametrize("action", ["CREATE_TABLE", "OVERWRITE_TABLE"])
def test_partitioned_iceberg_replacement_preserves_or_refuses(sql_world, action):
    from sqlalchemy import create_engine, text

    from etl_craft.core.errors import HandlerError
    from fixtures.services import require
    from fixtures.sql_replacement import definition

    w = sql_world
    if w.kind not in {"trino_iceberg", "duckdb_iceberg"}:
        pytest.skip("partition specifications belong to the Iceberg catalog")
    if action == "OVERWRITE_TABLE":
        w.setup("daily", "SELECT 1 AS id", action)
    params = {"SQL_ACTION": action, "TARGET_OBJECT": "daily"}
    w.run("daily", SOURCE_SQL="SELECT 1 AS id", **params)
    before_rows = w.rows(f"SELECT * FROM {w.name('daily')}")
    trino = require("trino")
    engine = create_engine(f"trino://etl@{trino.address}/iceberg/{w.schema}")
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    f"CREATE TABLE iceberg.{w.schema}.partitioned "
                    f"WITH (partitioning = ARRAY['id']) AS SELECT * FROM iceberg.{w.schema}.daily"
                )
            )
            conn.execute(text(f"DROP TABLE iceberg.{w.schema}.daily"))
            conn.execute(text(f"ALTER TABLE iceberg.{w.schema}.partitioned RENAME TO daily"))
        before = definition(w, "daily", comment=True)
        assert "partitioning" in before
        if w.kind == "duckdb_iceberg" and action == "CREATE_TABLE":
            with pytest.raises(HandlerError, match="cannot preserve partitioning"):
                w.run("daily", SOURCE_SQL="SELECT 2 AS id", **params)
            assert w.rows(f"SELECT * FROM {w.name('daily')}") == before_rows
        else:
            w.run("daily", SOURCE_SQL="SELECT 2 AS id", **params)
            assert w.rows(f"SELECT id FROM {w.name('daily')}") == [(2,)]
        assert definition(w, "daily") == before
    finally:
        engine.dispose()


def test_transactional_cleanup_failure_rolls_back_replacement(sql_world, monkeypatch):
    from etl_craft.core.errors import HandlerError
    from etl_craft.handlers.sql.session import Session

    w = sql_world
    if w.kind not in {"duckdb", "postgres"}:
        pytest.skip("native warehouses include cleanup in the publication transaction")
    params = {"SQL_ACTION": "CREATE_TABLE", "TARGET_OBJECT": "daily"}
    w.run("daily", SOURCE_SQL="SELECT 1 AS id", **params)
    original = Session.drop

    def unavailable_cleanup(session, name):
        if "etl_stage_" in name:
            raise HandlerError("cleanup unavailable")
        return original(session, name)

    with monkeypatch.context() as patch:
        patch.setattr(Session, "drop", unavailable_cleanup)
        with pytest.raises(HandlerError, match="cleanup unavailable"):
            w.run("daily", SOURCE_SQL="SELECT 2 AS id", **params)
    assert w.rows(f"SELECT id FROM {w.name('daily')}") == [(1,)]


def test_replacement_uses_target_schema_instead_of_connection_default(sql_world, monkeypatch):
    from etl_craft.core.errors import InjectedFaultError
    from etl_craft.warehouse.connection import warehouse_dialect

    w = sql_world
    other = f"{w.schema}_other"
    schema = other if w.kind == "postgres" else f"{w.catalog}.{other}"
    target = f"{w.catalog}.{other}.outside"
    w.execute(f"CREATE SCHEMA {schema}")
    params = {"SQL_ACTION": "CREATE_TABLE", "TARGET_OBJECT": f"{other}.outside"}
    try:
        w.run("outside", SOURCE_SQL="SELECT 1 AS id", **params)
        assert w.rows(f"SELECT id FROM {target}") == [(1,)]
        w.run("outside", SOURCE_SQL="SELECT 2 AS id", **params)
        assert w.rows(f"SELECT id FROM {target}") == [(2,)]
        assert "outside" not in w.tables()
        point = (
            "before_publish"
            if warehouse_dialect(w.config).replace_strategy == "create_or_replace"
            else "after_publish"
        )
        with monkeypatch.context() as patch:
            patch.setenv("ETL_CRAFT_FAULT", f"sql.replace.{point}")
            with pytest.raises(InjectedFaultError):
                w.run("outside", SOURCE_SQL="SELECT 3 AS id", **params)
        assert w.rows(f"SELECT id FROM {target}") == [(2,)]
    finally:
        tables = w.rows(
            "SELECT table_name FROM information_schema.tables WHERE lower(table_schema) = "
            "lower(:schema)",
            schema=other,
        )
        for (table,) in tables:
            w.execute(f"DROP TABLE IF EXISTS {w.catalog}.{other}.{table}")
        cascade = " CASCADE" if w.kind in {"postgres", "duckdb"} else ""
        w.execute(f"DROP SCHEMA {schema}{cascade}")
