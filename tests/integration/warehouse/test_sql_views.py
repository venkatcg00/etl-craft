"""CREATE_VIEW on real warehouses: a view over the SELECT, replaced each run, never a table."""

import pytest

from etl_craft.core.errors import HandlerError
from etl_craft.warehouse.connection import warehouse_dialect


@pytest.fixture
def orders(sql_world):
    sql_world.run(
        "create",
        SQL_ACTION="CREATE_TABLE",
        TARGET_OBJECT="orders",
        SOURCE_SQL="SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2, 'b'",
    )
    return sql_world


def test_create_view_builds_a_view_and_replaces_it_on_the_next_run(orders):
    w = orders
    view = {"SQL_ACTION": "CREATE_VIEW", "TARGET_OBJECT": "named_orders"}
    if not warehouse_dialect(w.config).views:
        with pytest.raises(HandlerError, match="cannot create views"):
            w.run("view", SOURCE_SQL=f"SELECT id FROM {w.name('orders')}", **view)
        return
    result = w.run("view", SOURCE_SQL=f"SELECT id FROM {w.name('orders')} WHERE id > 1", **view)
    assert (result.source_count, result.target_count, result.rows_written) == (None, None, None)
    assert w.rows(f"SELECT id FROM {w.name('named_orders')}") == [(2,)]
    w.run("view", SOURCE_SQL=f"SELECT id, name FROM {w.name('orders')}", **view)
    assert sorted(w.rows(f"SELECT id, name FROM {w.name('named_orders')}")) == [
        (1, "a"),
        (2, "b"),
    ]


def test_create_view_never_replaces_a_table(orders):
    w = orders
    with pytest.raises(HandlerError, match="CREATE_VIEW replaces only views"):
        w.run("view", SQL_ACTION="CREATE_VIEW", TARGET_OBJECT="orders", SOURCE_SQL="SELECT 1 AS id")
    assert len(w.rows(f"SELECT id FROM {w.name('orders')}")) == 2


def test_a_secure_view_where_the_warehouse_has_them_and_a_refusal_where_it_does_not(orders):
    w = orders
    params = {
        "SQL_ACTION": "CREATE_VIEW",
        "TARGET_OBJECT": "secure_orders",
        "SOURCE_SQL": f"SELECT id FROM {w.name('orders')}",
        "SECURE_VIEW": "true",
    }
    dialect = warehouse_dialect(w.config)
    if not dialect.views or dialect.secure_view is None:
        refusal = "has no secure views" if dialect.views else "cannot create views"
        with pytest.raises(HandlerError, match=refusal):
            w.run("secure", **params)
        return
    w.run("secure", **params)
    options = w.rows("SELECT reloptions FROM pg_class WHERE relname = 'secure_orders'")
    assert "security_barrier=true" in options[0][0]
