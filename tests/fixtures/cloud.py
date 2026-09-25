"""Databricks and Snowflake credentials for the cloud suites, and configs that name them.

The credentials come from ``ETL_CRAFT_TEST_<VENDOR>_*`` variables; a test skips when they are
not set, and fails instead with ``ETL_CRAFT_REQUIRE_SERVICES=1``.
"""

import os

import pytest
import yaml

from etl_craft.config import load_config

DATABRICKS_VARS = ("JDBC_URL", "CATALOG", "SCHEMA", "TOKEN")
SNOWFLAKE_VARS = ("USER", "ACCOUNT", "DATABASE", "SCHEMA", "WAREHOUSE", "ROLE", "TOKEN")


def require_variables(vendor, names):
    """Skip, or fail when services are required, unless every credential variable is set."""
    missing = [f"ETL_CRAFT_TEST_{vendor}_{name}" for name in names]
    missing = [name for name in missing if not os.environ.get(name)]
    if missing:
        message = f"{vendor.title()} credentials are not set: {', '.join(missing)}"
        if os.environ.get("ETL_CRAFT_REQUIRE_SERVICES") == "1":
            pytest.fail(message, pytrace=False)
        pytest.skip(message)


def write_config(directory, name, fields, table_format):
    """Write and load a craft-connector.yml in ``directory`` for warehouse ``name``."""
    raw = {
        "Secrets": {"Source_type": "environment"},
        "Orchestration": {"Mode": "local"},
        "Engine": {"dev": {"jdbc_url": "jdbc:sqlite:engine.db", "schema": "main"}},
        "Warehouse": {"Name": name, "Table_format": str(table_format), "dev": fields},
    }
    path = directory / "craft-connector.yml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    return load_config(path)
