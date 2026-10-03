import pytest
from databricks.sql.types import Row as DatabricksRow
from sqlalchemy.engine.result import SimpleResultMetaData
from sqlalchemy.engine.row import Row

pytestmark = pytest.mark.unit


def test_sqlalchemy_accepts_databricks_connector_rows():
    raw = DatabricksRow(id=1, value="ready")
    row = Row(SimpleResultMetaData(["id", "value"]), None, {"id": 0, "value": 1}, raw)

    assert tuple(row) == (1, "ready")
    assert dict(row._mapping) == {"id": 1, "value": "ready"}
