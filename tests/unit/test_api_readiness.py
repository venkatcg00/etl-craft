"""Unsigned readiness reports safe causes without publishing database exception details."""

import threading
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError

from etl_craft.api.app import create_app
from etl_craft.api.gateway import Gateway
from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.errors import ConfigurationError, MigrationError
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.models import Health

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("kind", [OperationalError, MigrationError, ConfigurationError])
def test_readiness_keeps_exception_details_out_of_responses_and_logs(monkeypatch, caplog, kind):
    secret = "private-db-password"
    detail = f"/private/project/config.yml internal-db.example {secret}"
    error = (
        OperationalError(
            "SELECT private_column FROM private_table", {"password": secret}, Exception(detail)
        )
        if kind is OperationalError
        else kind(detail)
    )
    outcome = [error]

    def readiness(ctx):
        if outcome:
            raise outcome[0]
        return Health()

    monkeypatch.setattr("etl_craft.api.app.readiness", readiness)
    ctx = OperationContext(None, SimpleNamespace(docs_site=None), SYSTEM_ACTOR)
    with TestClient(create_app(ctx, gateway=Gateway(threading.Event()))) as client:
        response = client.get("/health")
        assert response.status_code == 503
        assert "not ready" in response.text
        assert ("package version" in response.text) == (kind is MigrationError)
        for private in (
            secret,
            "/private/project",
            "internal-db.example",
            "SELECT",
            "private_table",
        ):
            assert private not in response.text
            assert private not in caplog.text
        assert kind.__name__ in caplog.text
        outcome.clear()
        assert client.get("/health").status_code == 200
