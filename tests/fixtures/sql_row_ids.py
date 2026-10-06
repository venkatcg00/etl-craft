"""Identity and computed ROW_ID assertions on local and cloud warehouses."""

import pytest
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.handlers.sql.session import Session
from etl_craft.warehouse.connection import warehouse_dialect


def check_row_id_generation(w, target):
    """Create, append, overwrite and evolve while keeping every generated key distinct."""
    from fixtures.sql_evolution import types

    select = "SELECT CAST(1 AS BIGINT) AS id"
    w.setup(target, select, "OVERWRITE_TABLE")
    dialect = warehouse_dialect(w.config)
    with w.warehouse.connect() as conn:
        assert dialect.row_id_generated(conn, w.name(target)) == (
            dialect.surrogate_key != "computed"
        )
    params = {"SQL_ACTION": "OVERWRITE_TABLE", "TARGET_OBJECT": target}
    w.run(target, SOURCE_SQL=select + " UNION ALL SELECT 2", **params)
    initial = w.rows(f"SELECT row_id FROM {w.name(target)}")
    assert len(initial) == 2 and len(set(initial)) == 2
    assert all(row[0] is not None and row[0] > 0 for row in initial)
    # Keep the target's stored types, including cloud-specific decimal precision.
    source = f"SELECT id, CAST(4.25 AS DECIMAL(12,2)) AS amount FROM {w.name(target)}"
    w.run(target, SOURCE_SQL=source, SCHEMA_EVOLUTION="true", **params)
    assert types(w, target)["amount"].lower().replace(" ", "") in {
        "decimal(12,2)",
        "numeric(12,2)",
        "number(12,2)",
    }
    after = w.rows(f"SELECT row_id FROM {w.name(target)}")
    assert len(after) == 2 and len(set(after)) == 2

    created = target + "_create"
    create = {"SQL_ACTION": "CREATE_TABLE", "TARGET_OBJECT": created}
    w.run(created, SOURCE_SQL=select + " UNION ALL SELECT 2", **create)
    with w.warehouse.connect() as conn:
        assert dialect.row_id_generated(conn, w.name(created)) == (
            dialect.surrogate_key != "computed"
        )
    w.run(created, SOURCE_SQL="SELECT CAST(3 AS BIGINT) AS id", **create)
    assert w.rows(f"SELECT id FROM {w.name(created)}") == [(3,)]

    appended = target + "_append"
    w.setup(appended, select, "APPEND_TABLE")
    append = {"SQL_ACTION": "APPEND_TABLE", "TARGET_OBJECT": appended}
    w.run(appended, SOURCE_SQL=select + " UNION ALL SELECT 2", **append)
    w.new_run()
    w.run(appended, SOURCE_SQL="SELECT CAST(3 AS BIGINT) AS id", **append)
    keys = w.rows(f"SELECT row_id FROM {w.name(appended)}")
    assert len(keys) == 3 and len(set(keys)) == 3 and all(row[0] is not None for row in keys)
    if dialect.identity_in_create:
        ddl = "SHOW CREATE TABLE" if dialect.key.startswith("databricks") else "DESCRIBE TABLE"
        assert w.rows(f"{ddl} {w.name(appended)}")
        if dialect.key.startswith("databricks"):
            with pytest.raises(SQLAlchemyError):
                w.execute(f"INSERT INTO {w.name(appended)} (id, row_id) SELECT 4, 4")


def run_paused_append(context, pause, attempted, allocated, release, results):
    """Run a task in a fresh process, optionally pausing after reading its ROW_ID base."""
    from etl_craft.dialects.engine import build_engine
    from etl_craft.engine.locks import EngineLock
    from etl_craft.handlers import sql

    engine = build_engine(context.config)
    original_count = Session.count
    original_hold = EngineLock.hold

    def count(session, statement, *, step):
        value = original_count(session, statement, step=step)
        if step == "largest ROW_ID":
            allocated.set()
            if pause and not release.wait(30):
                raise RuntimeError("the test did not release the paused ROW_ID allocation")
        return value

    def hold(lock, *args, **kwargs):
        attempted.set()
        return original_hold(lock, *args, **kwargs)

    try:
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(Session, "count", count)
            patch.setattr(EngineLock, "hold", hold)
            result = sql.run(context, engine)
        results.put(("ok", result.insert_count))
    except Exception as error:
        results.put(("error", str(error)))
    finally:
        engine.dispose()
