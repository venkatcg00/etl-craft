"""Warehouse identity parsing preserves quoted names and refuses ambiguous references."""

import pytest

from etl_craft.engine.repository.normalization import object_identity

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "value,catalog,dialect,expected",
    [
        ("Raw.Orders", None, "postgres", (None, "raw", "orders")),
        ("Raw.Orders", "Analytics", "snowflake", ("ANALYTICS", "RAW", "ORDERS")),
        ("other.Raw.Orders", "analytics", "postgres", ("other", "raw", "orders")),
        ('"Raw.Schema"."Order.Table"', None, "postgres", (None, "Raw.Schema", "Order.Table")),
        ('"Mixed".Raw."Order"', "analytics", "snowflake", ("Mixed", "RAW", "Order")),
        ("`Raw.Schema`.`Order.Table`", None, "databricks", (None, "raw.schema", "order.table")),
        ("raw.orders", None, None, (None, "raw", "orders")),
    ],
)
def test_object_components(value, catalog, dialect, expected):
    assert object_identity(value, catalog, dialect) == expected


@pytest.mark.parametrize(
    "value",
    [
        "",
        "orders",
        "a.b.c.d",
        "raw.orders alias",
        "raw..orders",
        "raw.orders; SELECT 1",
        "raw.orders -- comment",
        "raw.orders /* comment */",
        '"raw.orders',
        "raw.orders()",
    ],
)
def test_ambiguous_objects_name_the_original_and_remedy(value):
    with pytest.raises(ValueError) as error:
        object_identity(value, None, "postgres")
    assert repr(value) in str(error.value)
    assert "separate catalog, schema and table" in str(error.value)
