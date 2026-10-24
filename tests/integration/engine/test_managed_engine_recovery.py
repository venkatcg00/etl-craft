"""The managed-cloud acceptance procedure stays executable on local PostgreSQL."""

import pytest

from etl_craft.core.enums import ManagedProvider
from fixtures.managed_postgres import managed_project, run_and_recover

pytestmark = pytest.mark.engine_postgres


@pytest.mark.parametrize("empty_engine_db", ["postgresql"], indirect=True)
def test_managed_acceptance_procedure_on_postgres(empty_engine_db, tmp_path, monkeypatch):
    assert empty_engine_db.config.engine.managed_provider == ManagedProvider.NONE
    with managed_project(empty_engine_db.config, tmp_path, monkeypatch) as project:
        run_and_recover(project)
