"""Warehouse metadata distinguishes table formats independently of the requested dialect."""

import json
from unittest.mock import Mock

import pytest
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.core.enums import TableFormat
from etl_craft.core.errors import HandlerError
from etl_craft.dialects.warehouse import for_key
from etl_craft.handlers.sql.session import Session

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("key", ["databricks", "databricks_iceberg"])
@pytest.mark.parametrize("serialized", ["mapping", "json", "pairs"])
@pytest.mark.parametrize(
    "enabled,expected",
    [("", "native"), ("iceberg", "iceberg"), ("hudi, ICEBERG", "iceberg"), ("hudi", "native")],
)
def test_delta_uniform_is_read_from_existing_properties(key, serialized, enabled, expected):
    props = {"delta.universalFormat.enabledFormats": enabled}
    conn = Mock()
    conn.execute.return_value.mappings.return_value.one.return_value = {
        "format": "delta",
        "properties": json.dumps(props)
        if serialized == "json"
        else list(props.items())
        if serialized == "pairs"
        else props,
    }
    assert for_key(key).existing_table_format(conn, "cat.s.t") == expected


def test_unsupported_delta_provider_is_refused():
    conn = Mock()
    conn.execute.return_value.mappings.return_value.one.return_value = {"format": "parquet"}
    with pytest.raises(HandlerError, match="existing storage format is 'parquet'"):
        for_key("databricks").existing_table_format(conn, "cat.s.t")


@pytest.mark.parametrize("key", ["snowflake", "snowflake_iceberg"])
@pytest.mark.parametrize("flag,expected", [("YES", "iceberg"), ("NO", "native")])
def test_snowflake_uses_the_targets_database(key, flag, expected):
    conn = Mock()
    conn.execute.return_value.scalar_one.return_value = flag
    assert for_key(key).existing_table_format(conn, "other.S.T") == expected
    sql, params = conn.execute.call_args.args
    assert "other.information_schema.tables" in str(sql)
    assert params == {"schema": "S", "table": "T"}


def test_unknown_snowflake_metadata_is_not_assumed_native():
    conn = Mock()
    conn.execute.return_value.scalar_one.return_value = None
    with pytest.raises(HandlerError, match="IS_ICEBERG='NONE'"):
        for_key("snowflake").existing_table_format(conn, "cat.s.t")


def test_format_metadata_errors_name_the_action_and_target():
    dialect = Mock()
    dialect.existing_table_format.side_effect = SQLAlchemyError("no access")
    session = Session(
        Mock(),
        dialect,
        catalog="cat",
        action="CREATE_TABLE",
        target_object="s.t",
        task_run_id=1,
        params={},
    )
    with pytest.raises(
        HandlerError, match=r"CREATE_TABLE cat\.s\.t: read the existing table format failed"
    ):
        session.check_target_format()


@pytest.mark.parametrize(
    "key,expected",
    [
        ("postgres", TableFormat.NATIVE),
        ("duckdb", TableFormat.NATIVE),
        ("trino_iceberg", TableFormat.ICEBERG),
        ("duckdb_iceberg", TableFormat.ICEBERG),
    ],
)
def test_fixed_format_warehouses_need_no_metadata_lookup(key, expected):
    conn = Mock()
    assert for_key(key).existing_table_format(conn, "cat.s.t") == expected
    conn.execute.assert_not_called()


@pytest.mark.parametrize("properties", [None, "{", [1]])
def test_unreadable_delta_metadata_is_refused(properties):
    conn = Mock()
    conn.execute.return_value.mappings.return_value.one.return_value = {
        "format": "delta",
        "properties": properties,
    }
    with pytest.raises(HandlerError, match="cannot read table properties"):
        for_key("databricks").existing_table_format(conn, "cat.s.t")


@pytest.mark.parametrize(
    "key, keyword",
    [
        ("snowflake", "ALTER TABLE"),
        ("snowflake_iceberg", "ALTER ICEBERG TABLE"),
    ],
)
def test_snowflake_rename_keeps_the_explicit_destination_namespace(key, keyword):
    conn = Mock()
    session = Session(
        conn,
        for_key(key),
        catalog="active",
        action="CREATE_TABLE",
        target_object="other.schema.target",
        task_run_id=1,
        params={},
    )
    session.rename("other.schema.candidate", "other.schema.target")
    assert str(conn.execute.call_args.args[0]) == (
        f"{keyword} other.schema.candidate RENAME TO other.schema.target"
    )
