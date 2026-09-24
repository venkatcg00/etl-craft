import pytest

from etl_craft.config.model import (
    ConnectionProfile,
    ConnectionSection,
    ConnectorConfig,
    SourceConfig,
)
from etl_craft.config.targets import (
    WarehouseUrl,
    active_catalog,
    active_warehouse,
    parse_warehouse_url,
    preferred_connection_url,
    strip_databricks_credentials,
)
from etl_craft.core.enums import Mode, TableFormat
from etl_craft.core.errors import ConfigurationError

pytestmark = pytest.mark.unit


# Generic URLs


def test_a_postgres_url_selects_psycopg():
    assert parse_warehouse_url("jdbc:postgresql://myhost/mydb") == WarehouseUrl(
        dialect="postgresql+psycopg", host="myhost", port=None, database="mydb", catalog="mydb"
    )


def test_a_mysql_url_selects_pymysql_and_keeps_its_query():
    url = parse_warehouse_url("jdbc:mysql://myhost:3306/mydb?useSSL=true")
    assert (url.dialect, url.port, url.query) == ("mysql+pymysql", 3306, {"useSSL": "true"})


def test_an_unmapped_scheme_is_its_own_dialect():
    assert parse_warehouse_url("jdbc:oracle://myhost:1521/mydb").dialect == "oracle"


def test_trino_takes_the_generic_parser_with_a_catalog_schema_path():
    url = parse_warehouse_url("jdbc:trino://trino.internal:8080/iceberg/analytics")
    assert (url.dialect, url.database, url.catalog) == ("trino", "iceberg/analytics", "iceberg")


@pytest.mark.parametrize("jdbc_url", ["not-a-jdbc-url", "postgresql://h/db"])
def test_a_malformed_url_is_refused(jdbc_url):
    with pytest.raises(ConfigurationError, match="not a recognized JDBC URL"):
        parse_warehouse_url(jdbc_url)


# DuckDB


def test_a_duckdb_file_names_its_catalog_after_the_file():
    url = parse_warehouse_url("jdbc:duckdb:/data/warehouse.duckdb")
    assert url == WarehouseUrl(
        dialect="duckdb",
        host=None,
        port=None,
        database="warehouse",
        catalog="warehouse",
        path="/data/warehouse.duckdb",
    )


def test_bare_duckdb_is_genuinely_in_memory():
    # Not a file named `memory` in whatever directory the process started in.
    url = parse_warehouse_url("jdbc:duckdb:")
    assert (url.path, url.database, url.catalog) == (":memory:", "memory", "memory")


@pytest.mark.parametrize("filename", ["my-warehouse.duckdb", "2024_wh.duckdb", "etl craft.duckdb"])
def test_a_duckdb_file_whose_name_is_not_an_identifier_is_refused(filename):
    # The stem is written unquoted into catalog.schema.table.
    with pytest.raises(ConfigurationError, match="not a usable SQL identifier"):
        parse_warehouse_url(f"jdbc:duckdb:/data/{filename}")


def test_parse_duckdb_refuses_another_shape():
    from etl_craft.config import targets

    with pytest.raises(ConfigurationError, match="not a recognized DuckDB JDBC URL"):
        targets._parse_duckdb("jdbc:postgresql://h/db")


# Databricks


def test_databricks_semicolon_parameters():
    url = parse_warehouse_url(
        "jdbc:databricks://dbc-a1b2.cloud.databricks.com:443/default;"
        "httpPath=/sql/1.0/warehouses/abc123;ConnCatalog=main;ConnSchema=analytics;AuthMech=3"
    )
    assert url.dialect == "databricks"
    assert (url.host, url.port) == ("dbc-a1b2.cloud.databricks.com", 443)
    assert url.query == {
        "http_path": "/sql/1.0/warehouses/abc123",
        "catalog": "main",
        "schema": "analytics",
    }
    assert (url.database, url.catalog) == ("main", "main")


def test_databricks_schema_from_the_path_unless_it_is_default():
    url = parse_warehouse_url("jdbc:databricks://h/sales;httpPath=/p")
    assert url.query == {"http_path": "/p", "schema": "sales"}
    assert url.catalog == ""
    assert parse_warehouse_url("jdbc:databricks://h/default;httpPath=/p").query == {
        "http_path": "/p"
    }


def test_databricks_without_http_path_is_refused():
    with pytest.raises(ConfigurationError, match="no httpPath"):
        parse_warehouse_url("jdbc:databricks://host:443/default;ConnCatalog=main")


def test_a_malformed_databricks_url_is_refused():
    with pytest.raises(ConfigurationError, match="not a recognized Databricks JDBC URL"):
        parse_warehouse_url("jdbc:databricks:/nohost")


def test_strip_databricks_credentials_keeps_only_public_parameters():
    url = "jdbc:databricks://h:443/default;httpPath=/p;AuthMech=3;UID=token;PWD=dapi-x;ssl=1"
    assert strip_databricks_credentials(url) == "jdbc:databricks://h:443/default;httpPath=/p;ssl=1"
    assert strip_databricks_credentials("jdbc:databricks://h:443/default") == (
        "jdbc:databricks://h:443/default"
    )
    assert strip_databricks_credentials("jdbc:databricks://h/d;PWD=x") == "jdbc:databricks://h/d"


# Snowflake


def test_snowflake_account_host_form():
    url = parse_warehouse_url(
        "jdbc:snowflake://myacct.snowflakecomputing.com/"
        "?db=ANALYTICS&schema=PUBLIC&warehouse=COMPUTE_WH&role=SYSADMIN"
    )
    assert url.dialect == "snowflake"
    assert url.host == "myacct.snowflakecomputing.com"
    # The dialect splits database/schema itself; the catalog is the database alone.
    assert (url.database, url.catalog) == ("ANALYTICS/PUBLIC", "ANALYTICS")
    assert url.query == {"warehouse": "COMPUTE_WH", "role": "SYSADMIN", "account": "myacct"}


def test_snowflake_database_without_a_schema():
    url = parse_warehouse_url("jdbc:snowflake://acct.snowflakecomputing.com:443/?database=DB")
    assert (url.database, url.port) == ("DB", 443)


def test_snowflake_without_a_database_is_refused():
    with pytest.raises(ConfigurationError, match="no db="):
        parse_warehouse_url("jdbc:snowflake://myacct.snowflakecomputing.com/?warehouse=WH")


def test_a_malformed_snowflake_url_is_refused():
    with pytest.raises(ConfigurationError, match="not a recognized Snowflake JDBC URL"):
        parse_warehouse_url("jdbc:snowflake:acct")


# Separate connection fields

DATABRICKS_FIELDS = {
    "jdbc_url": "jdbc:databricks://adb.example.net:443/default;httpPath=/sql/1.0/warehouses/x;"
    "AuthMech=3;UID=token;PWD=dapi-leaked",
    "catalog": "main",
    "schema": "analytics",
}
SNOWFLAKE_FIELDS = {
    "user": "u",
    "account": "myorg-myacct",
    "database": "ANALYTICS",
    "schema": "PUBLIC",
    "warehouse": "WH",
    "role": "R",
}


def test_databricks_fields_build_a_credential_free_url():
    url = preferred_connection_url("Databricks", DATABRICKS_FIELDS)
    assert url == (
        "jdbc:databricks://adb.example.net:443/default;httpPath=/sql/1.0/warehouses/x;"
        "ConnCatalog=main;ConnSchema=analytics"
    )


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"catalog": "my-catalog"}, "catalog must be an unquoted SQL identifier"),
        ({"schema": "a b"}, "schema must be an unquoted SQL identifier"),
        ({"jdbc_url": "jdbc:snowflake://x"}, "must start with jdbc:databricks://"),
        ({"catalog": ""}, "Databricks connection requires catalog"),
    ],
)
def test_databricks_fields_are_checked(change, message):
    with pytest.raises(ConfigurationError, match=message):
        preferred_connection_url("databricks", {**DATABRICKS_FIELDS, **change})


def test_snowflake_fields_build_a_url_and_accept_the_full_host():
    expected = (
        "jdbc:snowflake://myorg-myacct.snowflakecomputing.com/"
        "?db=ANALYTICS&schema=PUBLIC&warehouse=WH&role=R"
    )
    assert preferred_connection_url("Snowflake", SNOWFLAKE_FIELDS) == expected
    full_host = {**SNOWFLAKE_FIELDS, "account": "myorg-myacct.snowflakecomputing.com"}
    assert preferred_connection_url("Snowflake", full_host) == expected


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"account": "https://x.snowflakecomputing.com"}, "account identifier, not a URL"),
        ({"role": ""}, "Snowflake connection requires role"),
    ],
)
def test_snowflake_fields_are_checked(change, message):
    with pytest.raises(ConfigurationError, match=message):
        preferred_connection_url("Snowflake", {**SNOWFLAKE_FIELDS, **change})


@pytest.mark.parametrize("name", ["Postgres", "Oracle"])
def test_other_warehouses_take_no_separate_fields(name):
    with pytest.raises(ConfigurationError, match="require Databricks or Snowflake"):
        preferred_connection_url(name, {})


# The active warehouse


def config_for(
    jdbc_url: str | None, *, table_format=TableFormat.NATIVE, **extra
) -> ConnectorConfig:
    engine = ConnectionProfile("ENGINE", "dev", "jdbc:sqlite:e.db", "", "none")
    warehouse = None
    if jdbc_url is not None:
        profile = ConnectionProfile("WAREHOUSE", "dev", jdbc_url, "", "none", extra=extra)
        warehouse = ConnectionSection("dev", {"dev": profile})
    return ConnectorConfig(
        mode=Mode.LOCAL,
        source=SourceConfig(type="environment"),
        engine=ConnectionSection("dev", {"dev": engine}),
        warehouse=warehouse,
        warehouse_table_format=table_format,
    )


@pytest.mark.parametrize(
    ("jdbc_url", "table_format", "key", "catalog"),
    [
        ("jdbc:postgresql://h/wh", TableFormat.NATIVE, "postgres", "wh"),
        ("jdbc:duckdb:/data/warehouse.duckdb", TableFormat.NATIVE, "duckdb", "warehouse"),
        ("jdbc:trino://t:8080/iceberg/analytics", TableFormat.NATIVE, "trino_iceberg", "iceberg"),
        ("jdbc:databricks://h/default;httpPath=/p;ConnCatalog=main", "iceberg",
         "databricks_iceberg", "main"),
        ("jdbc:snowflake://a.snowflakecomputing.com/?db=DB&schema=S", "native", "snowflake", "DB"),
    ],
)  # fmt: skip
def test_the_active_warehouse_and_its_catalog(jdbc_url, table_format, key, catalog):
    config = config_for(jdbc_url, table_format=table_format)
    assert active_warehouse(config).key == key
    assert active_catalog(config) == catalog


def test_duckdb_iceberg_names_its_catalog_in_the_profile():
    config = config_for("jdbc:duckdb:", table_format=TableFormat.ICEBERG, catalog="lake")
    assert active_warehouse(config).key == "duckdb_iceberg"
    assert active_catalog(config) == "lake"


@pytest.mark.parametrize("catalog", ["", "my-lake"])
def test_duckdb_iceberg_needs_a_plain_catalog_name(catalog):
    config = config_for("jdbc:duckdb:", table_format=TableFormat.ICEBERG, catalog=catalog)
    with pytest.raises(ConfigurationError, match="needs `catalog`"):
        active_catalog(config)


def test_a_url_that_names_no_catalog_is_refused():
    config = config_for("jdbc:databricks://h/default;httpPath=/p")
    with pytest.raises(ConfigurationError, match="names no catalog/database"):
        active_catalog(config)


def test_no_warehouse_section():
    with pytest.raises(ConfigurationError, match="no Warehouse section"):
        active_warehouse(config_for(None))
