"""Managed PostgreSQL initialization, migrations and recovery on each platform."""

import os

import pytest

from etl_craft.config import load_config
from etl_craft.core.enums import ManagedProvider
from fixtures.cloud import require_variables
from fixtures.managed_postgres import managed_project, run_and_recover


@pytest.mark.parametrize(
    "provider",
    [
        pytest.param(ManagedProvider.SNOWFLAKE, marks=pytest.mark.cloud_snowflake),
        pytest.param(ManagedProvider.DATABRICKS, marks=pytest.mark.cloud_databricks),
    ],
)
def test_managed_engine_db_initializes_migrates_runs_and_recovers(provider, tmp_path, monkeypatch):
    require_variables(provider, ("ENGINE_CONFIG",))
    config = load_config(os.environ[f"ETL_CRAFT_TEST_{provider}_ENGINE_CONFIG"])
    assert config.engine.managed_provider == provider
    with managed_project(config, tmp_path, monkeypatch) as project:
        run_and_recover(project)
