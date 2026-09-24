import pytest

from etl_craft.config import auth
from etl_craft.core.enums import AuthMode, TableFormat
from etl_craft.core.errors import ConfigurationError

pytestmark = pytest.mark.unit

ALL_MODES = {member.value for member in AuthMode}


@pytest.mark.parametrize(
    ("jdbc_url", "name"),
    [
        ("jdbc:postgresql://h/db", "postgresql"),
        ("  JDBC:PostgreSQL://h/db", "postgresql"),
        ("jdbc:sqlite:engine.db", "sqlite"),
    ],
)
def test_engine_for_jdbc_url(jdbc_url, name):
    assert auth.engine_for_jdbc_url(jdbc_url).name == name


def test_any_other_engine_db_is_refused():
    with pytest.raises(ConfigurationError, match="not a supported Engine DB URL"):
        auth.engine_for_jdbc_url("jdbc:mysql://h/db")


@pytest.mark.parametrize(
    "spec",
    [*auth.ENGINES, *auth.WAREHOUSES],
    ids=lambda spec: spec.key if hasattr(spec, "key") else spec.name,
)
def test_every_target_accepts_only_known_modes_and_verified_ones_are_accepted(spec):
    assert spec.auth_modes <= ALL_MODES
    assert spec.verified_auth_modes <= spec.auth_modes
    for fields in spec.auth_fields.values():
        assert set(fields) <= {"user", "secret", *auth.AUTH_EXTRA_FIELDS}


def test_email_auth_tables_agree():
    assert set(auth.EMAIL_AUTH_FIELDS) <= ALL_MODES
    assert set(auth.EMAIL_AUTH_FIELDS) >= auth.EMAIL_VERIFIED_AUTH_MODES


def test_the_postgres_warehouse_authenticates_like_the_engine_db():
    postgres_engine = auth.engine_for_jdbc_url("jdbc:postgresql://h/db")
    assert auth.warehouse_by_key("postgres").auth_fields == postgres_engine.auth_fields


@pytest.mark.parametrize(
    ("name", "table_format", "key"),
    [
        ("postgresql+psycopg", "native", "postgres"),
        ("duckdb", "native", "duckdb"),
        ("duckdb", "iceberg", "duckdb_iceberg"),
        ("trino", "native", "trino_iceberg"),  # the catalog decides; always Iceberg
        ("trino", "iceberg", "trino_iceberg"),
        ("databricks", "native", "databricks"),
        ("databricks", "iceberg", "databricks_iceberg"),
        ("snowflake", "native", "snowflake"),
        ("snowflake", "iceberg", "snowflake_iceberg"),
    ],
)
def test_warehouse_spec(name, table_format, key):
    spec = auth.warehouse_spec(name, table_format)
    assert spec.key == key
    assert spec.known


def test_postgres_has_no_iceberg_tables():
    with pytest.raises(ConfigurationError, match="always native"):
        auth.warehouse_spec("postgresql", "iceberg")


def test_a_database_without_a_dialect_is_a_plain_ansi_warehouse():
    spec = auth.warehouse_spec("oracle", "native")
    assert (spec.key, spec.display_name, spec.known) == ("oracle", "oracle", False)
    assert spec.table_format is TableFormat.NATIVE
    assert spec.auth_modes == {"none", "password", "token"}


def test_warehouse_names_select_registered_dialects():
    dialects = {spec.sqlalchemy_name for spec in auth.WAREHOUSES}
    assert set(auth.WAREHOUSE_NAMES.values()) == dialects


def test_only_databricks_and_snowflake_take_separate_fields():
    assert {spec.display_name for spec in auth.WAREHOUSES if spec.preferred_fields} == {
        "Databricks",
        "Snowflake",
    }
