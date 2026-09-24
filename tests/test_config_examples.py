"""Every file in docs/examples/ loads, for every profile it declares (2026-09-24).

Each example claims an Engine DB, a warehouse dialect and an execution mode;
this checks it really resolves to them, with plausible values for the variables
it names. A new example that is not listed in EXPECTED fails here, so the
examples cannot drift from the loader. Pure parsing -- no database is touched.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml

from etl_craft.config import load_config
from etl_craft.dialects.engine_dialects import for_jdbc_url
from etl_craft.warehouse import active_catalog, warehouse_dialect

EXAMPLES = Path(__file__).parents[1] / "docs" / "examples"

PROFILE_VARS = (
    "ETL_CRAFT_PROFILE",
    "ETL_CRAFT_ENGINE_PROFILE",
    "ETL_CRAFT_WAREHOUSE_PROFILE",
    "ETL_CRAFT_ORCHESTRATION_PROFILE",
    "ETL_CRAFT_CLONING_PROFILE",
)

# Values for the variable names the examples use. Only the warehouse URL
# differs by example; it is supplied per file below.
VALUES = {
    "ENGINE_JDBC_URL": "jdbc:postgresql://engine-db:5432/etl_craft",
    "ENGINE_USER": "etl_craft",
    "ENGINE_AUTH_MODE": "password",
    "ENGINE_SECRET": "s",
    "ENGINE_KEY_FILE": "/keys/engine.key",
    "WAREHOUSE_USER": "etl_craft",
    "WAREHOUSE_AUTH_MODE": "password",
    "WAREHOUSE_SECRET": "s",
    "WAREHOUSE_KEY_FILE": "/keys/rsa_key.p8",
    "WAREHOUSE_CATALOG": "lake",
    "WAREHOUSE_SCHEMA": "analytics",
    "WAREHOUSE_TOKEN": "t",
    "WAREHOUSE_ACCOUNT": "myorg-myaccount",
    "WAREHOUSE_DATABASE": "ANALYTICS",
    "WAREHOUSE_WAREHOUSE": "TRANSFORM_WH",
    "WAREHOUSE_ROLE": "TRANSFORMER",
    "WAREHOUSE_CATALOG_URI": "http://iceberg-rest:8181",
    "WAREHOUSE_ICEBERG_WAREHOUSE": "s3://warehouse/",
    "WAREHOUSE_S3_ENDPOINT": "minio:9000",
    "WAREHOUSE_S3_REGION": "us-east-1",
    "WAREHOUSE_S3_URL_STYLE": "path",
    "WAREHOUSE_S3_USE_SSL": "false",
    "WAREHOUSE_S3_KEY_ID": "k",
    "WAREHOUSE_S3_SECRET": "s",
    "EMAIL_HOST": "smtp.internal",
    "EMAIL_PORT": "587",
    "EMAIL_FROM": "etl@example.com",
    "EMAIL_AUTH_MODE": "password",
    "EMAIL_USER": "etl",
    "EMAIL_USE_TLS": "true",
    "EMAIL_SECRET": "s",
}

POSTGRES_WAREHOUSE_URL = "jdbc:postgresql://warehouse-db:5432/analytics"


@dataclass(frozen=True)
class Expected:
    """What an example claims to configure."""

    engine: str
    warehouse: str
    mode: str
    warehouse_url: str | None = None
    table_format: str = "native"


EXPECTED = {
    "minimal-local.yml": Expected("sqlite", "duckdb", "local"),
    "secrets-environment.yml": Expected("postgresql", "postgres", "local", POSTGRES_WAREHOUSE_URL),
    # Its values come from secrets-file.env, not from anything set here.
    "secrets-file.yml": Expected("postgresql", "postgres", "local"),
    "orchestration-local.yml": Expected("sqlite", "duckdb", "local"),
    "orchestration-remote.yml": Expected(
        "postgresql", "postgres", "orchestrator", POSTGRES_WAREHOUSE_URL
    ),
    "engine-sqlite.yml": Expected("sqlite", "postgres", "local", POSTGRES_WAREHOUSE_URL),
    "engine-postgres.yml": Expected(
        "postgresql", "postgres", "orchestrator", POSTGRES_WAREHOUSE_URL
    ),
    "warehouse-postgres.yml": Expected(
        "postgresql", "postgres", "orchestrator", POSTGRES_WAREHOUSE_URL
    ),
    "warehouse-duckdb.yml": Expected("sqlite", "duckdb", "orchestrator"),
    "warehouse-duckdb-iceberg.yml": Expected(
        "postgresql", "duckdb_iceberg", "orchestrator", table_format="iceberg"
    ),
    "warehouse-trino-iceberg.yml": Expected(
        "postgresql",
        "trino_iceberg",
        "orchestrator",
        "jdbc:trino://trino:8080/iceberg/analytics",
        table_format="iceberg",
    ),
    "warehouse-databricks.yml": Expected(
        "postgresql",
        "databricks",
        "orchestrator",
        "jdbc:databricks://adb-1.azuredatabricks.net:443/default;httpPath=/sql/1.0/warehouses/a",
    ),
    "warehouse-databricks-iceberg.yml": Expected(
        "postgresql",
        "databricks_iceberg",
        "orchestrator",
        "jdbc:databricks://adb-1.azuredatabricks.net:443/default;httpPath=/sql/1.0/warehouses/a",
        table_format="iceberg",
    ),
    "warehouse-snowflake.yml": Expected("postgresql", "snowflake", "orchestrator"),
    "warehouse-snowflake-iceberg.yml": Expected(
        "postgresql", "snowflake_iceberg", "orchestrator", table_format="iceberg"
    ),
    "warehouse-snowflake-key-pair.yml": Expected(
        "postgresql",
        "snowflake",
        "orchestrator",
        "jdbc:snowflake://myorg-myaccount.snowflakecomputing.com/?db=ANALYTICS&schema=PUBLIC",
    ),
    "cloning.yml": Expected("postgresql", "postgres", "local", POSTGRES_WAREHOUSE_URL),
}


def _cases() -> list[tuple[str, str]]:
    cases = []
    for path in sorted(EXAMPLES.glob("*.yml")):
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        profiles = [key for key, value in raw["Engine"].items() if isinstance(value, dict)]
        cases.extend((path.name, profile) for profile in profiles)
    return cases


@pytest.fixture
def clean_environment(monkeypatch):
    for name in (*PROFILE_VARS, *VALUES, "WAREHOUSE_JDBC_URL"):
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


def test_every_example_is_listed_here_and_in_the_index():
    shipped = {path.name for path in EXAMPLES.glob("*.yml")}
    assert shipped == set(EXPECTED)
    index = (EXAMPLES / "README.md").read_text(encoding="utf-8")
    assert [name for name in sorted(shipped) if f"`{name}`" not in index] == []


@pytest.mark.parametrize(("name", "profile"), _cases())
def test_every_example_loads_for_every_profile(clean_environment, name, profile):
    expected = EXPECTED[name]
    clean_environment.setenv("ETL_CRAFT_PROFILE", profile)
    if name != "secrets-file.yml":
        for variable, value in VALUES.items():
            clean_environment.setenv(variable, value)
        if name == "warehouse-snowflake-key-pair.yml":
            clean_environment.setenv("WAREHOUSE_AUTH_MODE", "key_file")
        if expected.warehouse_url:
            clean_environment.setenv("WAREHOUSE_JDBC_URL", expected.warehouse_url)

    config = load_config(EXAMPLES / name)

    assert config.postgres.active.name == profile
    assert for_jdbc_url(config.postgres.active.jdbc_url).name == expected.engine
    assert config.mode == expected.mode
    assert config.warehouse is not None and config.warehouse.active.name == profile
    assert warehouse_dialect(config).key == expected.warehouse
    assert config.warehouse_table_format == expected.table_format
    # Every example names a catalog qualify() can build three-part names with.
    assert active_catalog(config)


def test_allow_schedule_and_email_follow_the_profile(clean_environment):
    for variable, value in VALUES.items():
        clean_environment.setenv(variable, value)
    clean_environment.setenv("WAREHOUSE_JDBC_URL", POSTGRES_WAREHOUSE_URL)
    path = EXAMPLES / "orchestration-remote.yml"

    clean_environment.setenv("ETL_CRAFT_PROFILE", "dev")
    dev = load_config(path)
    assert dev.orchestrator.allow_schedule is False
    assert dev.orchestrator.global_dag is False
    assert dev.email is None

    clean_environment.setenv("ETL_CRAFT_PROFILE", "prod")
    prod = load_config(path)
    assert prod.orchestrator.allow_schedule is True
    assert prod.orchestrator.global_dag is True
    assert prod.orchestrator.email_on_failure is True
    assert prod.orchestrator.email_recipients == ["data-alerts@example.com"]
    assert prod.email is not None and prod.email.active.port == 587


def test_the_secrets_file_example_reads_only_its_file(clean_environment):
    config = load_config(EXAMPLES / "secrets-file.yml")
    assert config.source.type == "file"
    assert config.postgres.active.user == "etl_craft"
    # A profile-specific name in the file wins for that profile.
    clean_environment.setenv("ETL_CRAFT_PROFILE", "prod")
    prod = load_config(EXAMPLES / "secrets-file.yml")
    assert prod.postgres.active.secret_var == "ENGINE_PROD_SECRET"


def test_cloning_example_follows_the_profile(clean_environment):
    for variable, value in VALUES.items():
        clean_environment.setenv(variable, value)
    clean_environment.setenv("WAREHOUSE_JDBC_URL", POSTGRES_WAREHOUSE_URL)
    scopes = {}
    for profile in ("dev", "sit", "uat", "prod"):
        clean_environment.setenv("ETL_CRAFT_PROFILE", profile)
        cloning = load_config(EXAMPLES / "cloning.yml").cloning
        scopes[profile] = (cloning.enabled, cloning.scope)
    # Scope none turns an enabled section off.
    assert scopes == {
        "dev": (False, "cfg"),
        "sit": (False, "none"),
        "uat": (True, "cfg"),
        "prod": (True, "all"),
    }
