"""Failure assertions shared by local and live-cloud replacement acceptance tests."""

import pytest

from etl_craft.core.errors import HandlerError, InjectedFaultError
from etl_craft.handlers.sql.session import Session
from etl_craft.warehouse.connection import warehouse_dialect


def check_replacement_failure(w, target, action):
    params = {"SQL_ACTION": action, "TARGET_OBJECT": target}
    if action == "OVERWRITE_TABLE":
        w.setup(target, "SELECT 1 AS id", action)
    w.run(target, SOURCE_SQL="SELECT 1 AS id UNION ALL SELECT 2", **params)
    before_definition = definition(w, target, comment=True)
    before = sorted(w.rows(f"SELECT * FROM {w.name(target)}"))
    columns = w.columns(target)
    dialect = warehouse_dialect(w.config)
    points = ["before_publish"]
    if dialect.replace_strategy != "create_or_replace":
        points += ["after_clear", "after_publish"]
    for point in points:
        with pytest.MonkeyPatch.context() as patch:
            patch.setenv("ETL_CRAFT_FAULT", f"sql.replace.{point}")
            with pytest.raises(InjectedFaultError):
                w.run(target, SOURCE_SQL="SELECT 3 AS id", **params)
        assert sorted(w.rows(f"SELECT * FROM {w.name(target)}")) == before
        assert w.columns(target) == columns
        assert definition(w, target) == before_definition

    with pytest.MonkeyPatch.context() as patch:
        original = Session.run

        def invalid_publish(session, sql, params=None, *, step):
            if step in {"atomically replace the target", "atomically overwrite the target"}:
                sql += " !!! invalid replacement"
            return original(session, sql, params, step=step)

        if dialect.replace_strategy == "create_or_replace":
            patch.setattr(Session, "run", invalid_publish)
            with pytest.raises(HandlerError):
                w.run(target, SOURCE_SQL="SELECT 3 AS id", **params)
            assert sorted(w.rows(f"SELECT * FROM {w.name(target)}")) == before
            assert definition(w, target) == before_definition
    if action == "OVERWRITE_TABLE":
        with pytest.raises(HandlerError):
            w.run(
                target, SOURCE_SQL="SELECT CAST('invalid_integer' AS VARCHAR(40)) AS id", **params
            )
        assert sorted(w.rows(f"SELECT * FROM {w.name(target)}")) == before
    assert definition(w, target) == before_definition
    result = w.run(target, SOURCE_SQL="SELECT 3 AS id", **params)
    assert result.insert_count == 1 and w.rows(f"SELECT id FROM {w.name(target)}") == [(3,)]
    assert "replacement metadata must survive" in str(definition(w, target))
    assert not any(table.startswith(f"{target.lower()}__etl_keep_") for table in w.tables())


def definition(w, target, *, comment=False):
    """Read definition metadata through the catalog that owns the table."""
    from sqlalchemy import create_engine, text

    from fixtures.services import require

    dialect = warehouse_dialect(w.config)
    engine = w.warehouse
    name = w.name(target)
    extra_engine = None
    if dialect.spec.key == "duckdb_iceberg":
        trino = require("trino")
        extra_engine = create_engine(f"trino://etl@{trino.address}/iceberg/{w.schema}")
        engine = extra_engine
        name = f"iceberg.{w.schema}.{target}"
    try:
        with engine.begin() as conn:
            if comment:
                statement = (
                    f"ALTER ICEBERG TABLE {name} SET COMMENT = 'replacement metadata must survive'"
                    if dialect.spec.key == "snowflake_iceberg"
                    else f"COMMENT ON TABLE {name} IS 'replacement metadata must survive'"
                )
                conn.execute(text(statement))
            if dialect.spec.key == "postgres":
                return conn.execute(
                    text("SELECT obj_description(CAST(:name AS regclass))"),
                    {"name": f"{w.schema}.{target}"},
                ).scalar_one()
            if dialect.spec.key == "duckdb":
                return conn.execute(
                    text(
                        "SELECT comment FROM duckdb_tables() WHERE schema_name = :schema "
                        "AND table_name = :table"
                    ),
                    {"schema": w.schema, "table": target},
                ).scalar_one()
            if dialect.spec.key.startswith("snowflake"):
                return conn.execute(
                    text("SELECT GET_DDL('TABLE', :name)"), {"name": name}
                ).scalar_one()
            return conn.execute(text(f"SHOW CREATE TABLE {name}")).scalar_one()
    finally:
        if extra_engine is not None:
            extra_engine.dispose()
