"""Validate persisted policy values and derived identifiers before SQL construction."""

import pytest

from etl_craft.core.errors import MetadataError
from etl_craft.services.warehouse_retention import AuxiliaryTarget

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("catalog", ["bad name", "db; DROP TABLE customers", "db.other"])
def test_derived_catalog_requires_one_identifier(catalog):
    target = AuxiliaryTarget(7, None, "main", "orders", "TABLE", 30, None, None)
    with pytest.raises(MetadataError, match="expected separate SQL identifiers") as error:
        target.name(catalog)
    assert "TABLE_ID=7" in str(error.value)
    assert repr(catalog) in str(error.value)
    assert "correct tables.csv" in str(error.value)


@pytest.mark.parametrize("days", [0, 7.5, "30", 36501])
def test_persisted_periods_are_whole_bounded_days(days):
    with pytest.raises(MetadataError, match=r"REJECT_RETENTION_DAYS=.*expected whole days"):
        AuxiliaryTarget(7, None, "main", "orders", "TABLE", days, None, None)
