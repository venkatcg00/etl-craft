"""Catalog page names, and the lineage graph's size limit."""

from datetime import UTC, datetime

import pytest

from etl_craft.services import catalog_graph
from etl_craft.services.catalog import Catalog, TableAsset, TableEdge
from etl_craft.services.catalog_graph import lineage_drawing, render_svg
from etl_craft.services.catalog_site import slug

pytestmark = pytest.mark.unit


def test_a_page_name_is_the_key_when_safe_and_hashed_when_not():
    assert slug("sales.orders") == "sales.orders"
    assert slug("SALES.load-2") == "SALES.load-2"
    made_safe = slug("crm/orders.py")
    assert made_safe.startswith("crm_orders.py-") and len(made_safe) == len("crm_orders.py-") + 8
    assert slug("crm/orders.py") != slug("crm_orders.py")
    assert not slug("../up").startswith(".")


def chain(length):
    tables = {f"s.t{i}": TableAsset(f"s.t{i}") for i in range(length)}
    edges = [TableEdge(f"s.t{i}", f"s.t{i + 1}", f"P.task{i}") for i in range(length - 1)]
    return Catalog(datetime.now(UTC), {}, {}, tables, {}, {}, [], edges)


def test_a_graph_past_its_size_limit_stops_and_says_where(monkeypatch):
    monkeypatch.setattr(catalog_graph, "MAX_TABLES", 5)
    drawing = lineage_drawing(chain(10), "s.t5")
    assert sorted(node.level for node in drawing.nodes.values()) == [-2, -1, 0, 1, 2]
    assert drawing.truncated_at is not None
    whole = lineage_drawing(chain(3), "s.t0")
    assert whole.truncated_at is None and whole.levels == (0, 2)


def test_names_are_escaped_in_the_drawing():
    catalog = chain(2)
    catalog.tables['s.t0"><script>'] = TableAsset('s.t0"><script>')
    catalog.table_edges.append(TableEdge('s.t0"><script>', "s.t1", "P.x"))
    svg = render_svg(lineage_drawing(catalog, "s.t1"), lambda name: f"{name}.html")
    assert "<script>" not in svg and "&lt;script&gt;" in svg
